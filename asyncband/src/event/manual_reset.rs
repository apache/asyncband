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
use crate::internal::waker_batch::WakerBatch;

/// A reusable signal that releases all waiters and remains set until explicitly reset.
///
/// Each [`set`](Self::set) releases all registered waits and makes future waits ready. The event
/// remains set until [`reset`](Self::reset) is called. A released wait remains ready even if the
/// event is reset before that wait is polled again.
///
/// # Usage
///
/// Use this event as a readiness gate, keeping it set for as long as the condition holds. Unlike
/// a latch, it can be reset and reused. Creating a wait future does not register it; registration
/// happens on the first poll that needs to wait. A `set` followed immediately by `reset` therefore
/// does not release an unpolled wait.
///
/// # Synchronization
///
/// An unset-to-set transition synchronizes with the waits it releases, with waits first polled
/// while the event remains set, and with successful [`try_wait`](Self::try_wait) calls that observe
/// that set state. Memory operations sequenced before [`set`](Self::set) are therefore visible
/// after those waits complete.
///
/// A `set` call that finds the event already set does not establish this guarantee.
///
/// The event carries no application state: callers must synchronize access to external predicates
/// separately.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use asyncband::event::ManualResetEvent;
///
/// let event = ManualResetEvent::new();
/// event.set();
/// event.wait().await;
/// event.wait().await; // Waiting leaves the event set.
/// event.reset();
/// assert!(!event.is_set());
///
/// # }
/// ```
pub struct ManualResetEvent {
    // Flag writes and waiter registration are serialized by the same lock.
    is_set: AtomicBool,
    waiters: Mutex<WaitList<Waiter>>,
}

impl ManualResetEvent {
    /// Creates an unset event.
    pub const fn new() -> Self {
        Self::with_state(false)
    }

    /// Creates an event with the specified initial state.
    ///
    /// If `is_set` is `true`, waits complete immediately until the event is reset.
    pub const fn with_state(is_set: bool) -> Self {
        Self {
            is_set: AtomicBool::new(is_set),
            waiters: Mutex::new(WaitList::new()),
        }
    }

    /// Signals all registered waits and keeps the event set.
    ///
    /// The event remains set until [`reset`](Self::reset) is called. Calling `set` while it is
    /// already set has no effect.
    pub fn set(&self) {
        let mut wakers = WakerBatch::new();
        {
            let mut waiters = self.waiters.lock();
            if self.is_set.load(Ordering::Relaxed) {
                return;
            }

            // Publish to successful observations that do not acquire the waiter lock.
            self.is_set.store(true, Ordering::Release);
            // Detach the complete cohort before invoking any waker. A wake callback may reset the
            // event and register a new wait, which must belong to the state current at that point.
            while let Some((_id, waiter)) = waiters.unlink_first_waiter(|waiter| {
                waiter.notified = true;
                true
            }) {
                if let Some(waker) = waiter.waker.take() {
                    wakers.push(waker);
                }
            }
        }
        wakers.wake_all();
    }

    /// Clears the set state.
    ///
    /// Waits already released by a preceding [`set`](Self::set) remain ready. If the event is
    /// already unset, this has no effect.
    pub fn reset(&self) {
        let _waiters = self.waiters.lock();
        // Reset cannot race ahead of a set that is still detaching its waiter cohort.
        self.is_set.store(false, Ordering::Relaxed);
    }

    /// Returns whether the event is currently set.
    ///
    /// This is a snapshot only; it does not change the event or reserve a signal for a later wait.
    /// The state may change immediately after this call.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::event::ManualResetEvent;
    ///
    /// let event = ManualResetEvent::with_state(true);
    /// assert!(event.is_set());
    /// assert!(event.is_set());
    /// ```
    pub fn is_set(&self) -> bool {
        self.is_set.load(Ordering::Acquire)
    }

    /// Attempts to wait without registering a waiter.
    ///
    /// Returns `true` if the event is set, leaving it set. A `false` result is only a snapshot;
    /// use [`wait`](Self::wait) to wait for a future signal.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::event::ManualResetEvent;
    ///
    /// let event = ManualResetEvent::with_state(true);
    /// assert!(event.try_wait());
    /// assert!(event.try_wait()); // A successful wait leaves the event set.
    /// ```
    pub fn try_wait(&self) -> bool {
        self.is_set()
    }

    /// Waits until the event is set.
    ///
    /// The first poll completes immediately if the event is set, or registers the wait.
    /// Merely creating this future does not register the wait. Once a [`set`](Self::set) releases
    /// a registered wait, a later [`reset`](Self::reset) cannot make that wait pending again.
    ///
    /// # Cancel safety
    ///
    /// Dropping this future before it returns `Ready` removes its registration. This does not
    /// change the event or affect other waits.
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
    /// use asyncband::event::ManualResetEvent;
    ///
    /// let event = Arc::new(ManualResetEvent::new());
    /// let waiter = tokio::spawn(event.clone().wait_owned());
    /// event.set();
    /// waiter.await.unwrap();
    /// # }
    /// ```
    pub async fn wait_owned(self: Arc<Self>) {
        self.wait().await;
    }

    /// Polls a wait, registering `waiter_id` on the first poll that observes an unset event.
    ///
    /// `set` unlinks every queued waiter and marks it notified, and a wait that starts while the
    /// event is set never enqueues. A linked waiter therefore always belongs to an unset event, so
    /// `notified` alone decides whether a registered waiter is already committed.
    fn poll_wait(&self, waiter_id: &mut Option<WaiterId>, cx: &mut Context<'_>) -> Poll<()> {
        // Only an unregistered wait can skip the lock: a released waiter still owns a node.
        let mut prepared_waker = if waiter_id.is_none() {
            if self.is_set() {
                return Poll::Ready(());
            }
            Some(cx.waker().clone())
        } else {
            None
        };

        let (poll, retired_waker) = loop {
            let mut waiters = self.waiters.lock();
            match *waiter_id {
                Some(id) if waiters.waiter_mut(id).notified => {
                    let waiter = waiters.remove_unlinked_waiter(id);
                    *waiter_id = None;
                    break (Poll::Ready(()), waiter.waker);
                }
                Some(id) => {
                    debug_assert!(
                        !self.is_set.load(Ordering::Relaxed),
                        "a linked waiter must belong to an unset event"
                    );
                    let waiter = waiters.waiter_mut(id);
                    let current = waiter
                        .waker
                        .as_ref()
                        .expect("an unnotified waiter must retain its waker");
                    if current.will_wake(cx.waker()) {
                        break (Poll::Pending, None);
                    }
                    if let Some(waker) = prepared_waker.take() {
                        break (Poll::Pending, waiter.waker.replace(waker));
                    }
                    // Clone only a changed waker, outside the lock, then recheck commitment:
                    // set/reset may run while preparing the replacement.
                    drop(waiters);
                    prepared_waker = Some(cx.waker().clone());
                }
                // A set after the false fast probe must either be observed here or release
                // the node that we register while holding this same lock.
                None if self.is_set.load(Ordering::Relaxed) => break (Poll::Ready(()), None),
                None => {
                    *waiter_id = Some(waiters.push_back(Waiter {
                        notified: false,
                        waker: prepared_waker.take(),
                    }));
                    break (Poll::Pending, None);
                }
            }
        };

        drop(prepared_waker);
        drop(retired_waker);
        poll
    }

    fn unregister_waiter(&self, id: WaiterId) {
        let waiter = {
            let mut waiters = self.waiters.lock();
            // Released waits retain their detached nodes until repolled or cancelled.
            waiters.unlink_waiter(id, |_| true);
            waiters.remove_unlinked_waiter(id)
        };
        drop(waiter);
    }
}

impl Default for ManualResetEvent {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ManualResetEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let is_set = self.is_set();
        f.debug_struct("ManualResetEvent")
            .field("is_set", &is_set)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct Waiter {
    notified: bool,
    waker: Option<Waker>,
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct Wait<'a> {
    event: &'a ManualResetEvent,
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
