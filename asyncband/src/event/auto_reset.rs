// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;

/// A reusable signal that releases one waiter and resets automatically.
///
/// Each [`set`](Self::set) assigns a signal to one registered wait, or stores one signal
/// if no wait is queued. Repeated sets coalesce only while an unassigned signal is stored. A
/// signal assigned to a wait belongs to that wait until it completes or is cancelled; subsequent
/// sets can release other waits even before previously selected waits are polled again.
/// An unassigned signal can be cleared with [`reset`](Self::reset).
///
/// Waiting consumes a signal without returning it on completion. Unlike a
/// [`ManualResetEvent`](super::ManualResetEvent), this event does not release all observers of a
/// condition. Unlike a semaphore, it does not count unused signals or return a permit guard.
///
/// # Usage
///
/// Use this event for a single worker that rechecks external state after a signal. Publish the
/// state before calling `set`, and check the predicate in a loop. A signal arriving between the
/// predicate check and the first poll is retained, so the worker does not miss it. A leftover
/// signal can cause an extra predicate check without implying new work.
///
/// Multiple waits compete for signals. The simple check-then-wait loop is not a general
/// multi-consumer queue protocol: several changes can coalesce before those consumers register
/// their waits.
///
/// # Synchronization
///
/// Memory operations sequenced before a `set` are visible after a wait or
/// [`try_wait`](Self::try_wait) consumes its signal. This includes sets coalesced into a stored
/// signal and signals passed on after cancellation.
///
/// The event carries no application state: callers must synchronize access to external predicates
/// separately.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use asyncband::event::AutoResetEvent;
///
/// let event = AutoResetEvent::new();
/// event.set();
/// event.set();
/// event.wait().await;
/// assert!(!event.try_wait()); // The two sets coalesced into one signal.
///
/// # }
/// ```
pub struct AutoResetEvent {
    is_set: AtomicBool,
    // A stored signal and queued (unselected) waits never coexist. Detached, selected waits can
    // coexist with either: their signals are reserved until consumption or cancellation.
    waiters: Mutex<WaitList<Waiter>>,
}

impl AutoResetEvent {
    /// Creates an unset event.
    pub const fn new() -> Self {
        Self::with_state(false)
    }

    /// Creates an event with the specified initial state.
    ///
    /// If `is_set` is `true`, the event stores one signal for a future wait.
    pub const fn with_state(is_set: bool) -> Self {
        Self {
            is_set: AtomicBool::new(is_set),
            waiters: Mutex::new(WaitList::new()),
        }
    }

    /// Signals one registered wait, or stores one signal if no wait is queued.
    ///
    /// A stored signal is available to a future wait. Further sets coalesce while it remains
    /// unassigned. Creating a wait future does not register it; registration happens when it is
    /// first polled without a stored signal.
    ///
    /// # Panics
    ///
    /// Panics if waking a selected task panics. Its signal remains assigned and can still be
    /// consumed by polling that wait or passed on by dropping it.
    pub fn set(&self) {
        let waker = self.signal(&mut self.waiters.lock());
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Clears any stored, unassigned signal.
    ///
    /// Signals already assigned to waits remain theirs. Cancelling such a wait can still transfer
    /// or restore its signal after this call. If no signal is stored, this has no effect.
    pub fn reset(&self) {
        let _waiters = self.waiters.lock();
        self.is_set.store(false, Ordering::Relaxed);
    }

    /// Returns whether the event is currently set.
    ///
    /// The event is set while it stores an unassigned signal. Signals already assigned to waits
    /// are not reflected in this state.
    ///
    /// This is a snapshot only; it does not change the event or reserve a signal for a later wait.
    /// The state may change immediately after this call.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::event::AutoResetEvent;
    ///
    /// let event = AutoResetEvent::with_state(true);
    /// assert!(event.is_set());
    /// assert!(event.is_set());
    /// ```
    pub fn is_set(&self) -> bool {
        self.is_set.load(Ordering::Acquire)
    }

    /// Attempts to wait without registering a waiter.
    ///
    /// Returns `true` if a stored signal was consumed. This never takes a signal assigned to
    /// another wait. A `false` result is only a snapshot; use [`wait`](Self::wait) to wait for a
    /// future signal.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::event::AutoResetEvent;
    ///
    /// let event = AutoResetEvent::with_state(true);
    /// assert!(event.try_wait());
    /// assert!(!event.try_wait()); // A successful wait consumes the signal.
    /// ```
    pub fn try_wait(&self) -> bool {
        // Only consumption bypasses the waiter lock. Signals are stored under that lock only
        // when no wait is queued, so this cannot take a signal assigned to a registered wait.
        self.is_set.load(Ordering::Relaxed)
            && self
                .is_set
                .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
    }

    /// Waits for and consumes one signal.
    ///
    /// The first poll consumes a stored signal immediately, or registers the wait.
    /// Merely creating this future neither reserves a signal nor registers the wait.
    /// Signals assigned to other waits cannot be consumed by this wait.
    ///
    /// # Cancel safety
    ///
    /// Dropping this future before it returns `Ready` removes its registration. If a signal was
    /// assigned to it, that signal is passed to another registered wait or stored for a future
    /// wait, coalescing with any signal already stored. Dropping a completed wait does not return
    /// its consumed signal.
    pub async fn wait(&self) {
        Wait {
            event: self,
            waiter: None,
        }
        .await
    }

    /// Waits without borrowing the event.
    ///
    /// The future owns the [`Arc`], making it suitable for spawned tasks. Its waiting and
    /// cancellation semantics match [`wait`](Self::wait).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::event::AutoResetEvent;
    ///
    /// let event = Arc::new(AutoResetEvent::new());
    /// let waiter = tokio::spawn(event.clone().wait_owned());
    /// event.set();
    /// waiter.await.unwrap();
    /// # }
    /// ```
    pub async fn wait_owned(self: Arc<Self>) {
        self.wait().await;
    }

    fn poll_wait(&self, waiter_id: &mut Option<WaiterId>, cx: &mut Context<'_>) -> Poll<()> {
        // Registered waits own separate signals and must remove their nodes under the lock.
        if waiter_id.is_none() && self.try_wait() {
            return Poll::Ready(());
        }

        let (poll, retired_waker) = {
            let mut waiters = self.waiters.lock();
            match *waiter_id {
                Some(id) => match waiters.waiter_mut(id) {
                    Waiter::Notified => {
                        waiters.remove_unlinked_waiter(id);
                        *waiter_id = None;
                        (Poll::Ready(()), None)
                    }
                    Waiter::Waiting(waker) => {
                        let retired = (!waker.will_wake(cx.waker()))
                            .then(|| mem::replace(waker, cx.waker().clone()));
                        (Poll::Pending, retired)
                    }
                },
                // Recheck before enqueueing: set and cancellation handoff use this same lock.
                None if self.try_wait() => (Poll::Ready(()), None),
                None => {
                    *waiter_id = Some(waiters.push_back(Waiter::Waiting(cx.waker().clone())));
                    (Poll::Pending, None)
                }
            }
        };
        drop(retired_waker);
        poll
    }

    fn unregister_waiter(&self, id: WaiterId) {
        let (waiter, waker) = {
            let mut waiters = self.waiters.lock();
            // A selected waiter is already detached, but still owns its signal until removal.
            waiters.unlink_waiter(id, |_| true);
            let waiter = waiters.remove_unlinked_waiter(id);
            let waker = match &waiter {
                Waiter::Notified => self.signal(&mut waiters),
                Waiter::Waiting(_) => None,
            };
            (waiter, waker)
        };
        drop(waiter);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn signal(&self, waiters: &mut WaitList<Waiter>) -> Option<Waker> {
        if let Some((_, waiter)) = waiters.unlink_first_waiter(|_| true) {
            let Waiter::Waiting(waker) = mem::replace(waiter, Waiter::Notified) else {
                unreachable!("only unselected waits remain queued")
            };
            Some(waker)
        } else {
            // Publish every set, including coalesced sets and returned assigned signals.
            self.is_set.store(true, Ordering::Release);
            None
        }
    }
}

impl Default for AutoResetEvent {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for AutoResetEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let is_set = self.is_set();
        f.debug_struct("AutoResetEvent")
            .field("is_set", &is_set)
            .finish_non_exhaustive()
    }
}

enum Waiter {
    Waiting(Waker),
    Notified,
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct Wait<'a> {
    event: &'a AutoResetEvent,
    waiter: Option<WaiterId>,
}

impl Future for Wait<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.event.poll_wait(&mut this.waiter, cx)
    }
}

impl Drop for Wait<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.waiter.take() {
            self.event.unregister_waiter(id);
        }
    }
}
