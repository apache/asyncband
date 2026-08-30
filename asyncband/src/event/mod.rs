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

//! A reusable, level-triggered signal for coordinating tasks.
//!
//! A [`ManualResetEvent`] is either set or unset. Calling [`set`](ManualResetEvent::set) makes the
//! event ready, releases every waiter registered during the current unset period, and keeps later
//! waits ready. Calling [`reset`](ManualResetEvent::reset) makes subsequent waits block again.
//!
//! A waiter registered before `set` is committed to completion even if another task calls `reset`
//! before the waiter is polled again. This differs from a condition variable: the event retains its
//! set state and does not require an external predicate or mutex.
//!
//! Registration happens on a wait's first poll, so `set` followed immediately by `reset` is not a
//! reliable way to release everyone waiting at that moment: a future constructed before the `set`
//! but not yet polled was never a waiter of it. Keep the event set for as long as the condition it
//! reports holds.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use std::sync::Arc;
//!
//! use asyncband::event::ManualResetEvent;
//!
//! let ready = Arc::new(ManualResetEvent::new());
//! let waiter = tokio::spawn(ready.clone().wait_owned());
//!
//! ready.set();
//! waiter.await.unwrap();
//!
//! // The signal remains set until it is reset explicitly.
//! ready.wait().await;
//! ready.reset();
//! # }
//! ```

use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::waitset::wake_all;

/// A reusable event that releases all waiters when set and stays ready until reset.
///
/// `set` and `reset` are idempotent. A wait that has returned `Pending` and is still registered
/// when a successful unset to set transition occurs is committed to completion by that transition,
/// even if `reset` happens before the future is polled again.
///
/// Dropping a pending wait and a concurrent `set` linearize on the same internal lock, and
/// whichever acquires it first decides the outcome. If the drop wins, the waiter is already gone
/// and that `set` never commits it. If the `set` wins, the waiter is committed and the drop then
/// removes a node whose completion no one will observe; because `set` invokes wakers after
/// releasing the lock, that waker may still run after the drop has returned.
///
/// Neither order withholds the signal from another waiter: a commitment is not a permit that a
/// cancelled wait could consume.
///
/// Registration happens on the first poll, not when [`wait`](Self::wait) constructs the future, so
/// a wait first polled after a reset belongs to the new unset period even when it was constructed
/// before the preceding `set`.
///
/// # Synchronization
///
/// Memory operations sequenced before a [`set`](Self::set) call that performs an unset to set
/// transition have a happens-before relationship with code that runs after any wait completed by
/// that transition, including a wait first polled while the resulting set state is still current. A
/// producer may therefore publish shared state and then call `set`, and every waiter that call
/// releases observes it.
///
/// A `set` call that finds the event already set performs no transition and does not establish the
/// guarantee above.
///
/// The API makes no publication guarantee for state observed through [`is_set`](Self::is_set), so
/// that query can neither stand in for a wait nor support check-then-act: the state it reports may
/// change before the caller acts on it.
pub struct ManualResetEvent {
    state: Mutex<State>,
}

impl ManualResetEvent {
    /// Creates an unset event.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::event::ManualResetEvent;
    ///
    /// let event = ManualResetEvent::new();
    /// assert!(!event.is_set());
    /// ```
    pub const fn new() -> Self {
        Self::with_state(false)
    }

    /// Creates an event with the specified initial state.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::event::ManualResetEvent;
    ///
    /// let ready = ManualResetEvent::with_state(true);
    /// assert!(ready.is_set());
    /// ```
    pub const fn with_state(is_set: bool) -> Self {
        Self {
            state: Mutex::new(State {
                is_set,
                waiters: WaitList::new(),
            }),
        }
    }

    /// Returns whether the event is currently set.
    ///
    /// This is a snapshot only; it does not reserve or consume the set state.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::event::ManualResetEvent;
    ///
    /// let event = ManualResetEvent::new();
    /// assert!(!event.is_set());
    ///
    /// event.set();
    /// assert!(event.is_set());
    ///
    /// event.reset();
    /// assert!(!event.is_set());
    /// ```
    pub fn is_set(&self) -> bool {
        self.state.lock().is_set
    }

    /// Sets the event and releases every waiter registered during the current unset period.
    ///
    /// The event remains set until [`reset`](Self::reset) is called. Calling `set` while it is
    /// already set has no effect. Wakers are invoked after the internal lock is released; if one
    /// panics, the remaining waiters are still woken and the first panic reaches the caller.
    ///
    /// A waker may therefore re-enter the event. The whole registered cohort is detached before any
    /// waker runs, so a wait registered from a wake callback belongs to the period current when it
    /// registers rather than to this call. No ordering is guaranteed among the released waits.
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
    ///
    /// // The event stays ready, so every later wait completes without blocking.
    /// event.wait().await;
    /// event.wait().await;
    /// # }
    /// ```
    pub fn set(&self) {
        let wakers = {
            let mut state = self.state.lock();
            if state.is_set {
                return;
            }

            state.is_set = true;
            let mut wakers = Vec::new();
            while let Some((_id, waiter)) = state.waiters.unlink_first_waiter(|waiter| {
                waiter.notified = true;
                true
            }) {
                if let Some(waker) = waiter.waker.take() {
                    wakers.push(waker);
                }
            }
            wakers
        };

        wake_all(wakers.into_iter());
    }

    /// Resets the event so subsequent waits block until another [`set`](Self::set).
    ///
    /// Waiters already committed by a preceding `set` remain ready. Calling `reset` while the
    /// event is already unset has no effect.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::event::ManualResetEvent;
    ///
    /// let event = ManualResetEvent::with_state(true);
    /// event.wait().await;
    ///
    /// // Subsequent waits block again until the next `set`.
    /// event.reset();
    /// assert!(!event.is_set());
    ///
    /// event.set();
    /// event.wait().await;
    /// # }
    /// ```
    pub fn reset(&self) {
        self.state.lock().is_set = false;
    }

    /// Returns a future that waits until the event is set.
    ///
    /// A poll that observes the event set returns `Ready` immediately. A wait that has returned
    /// `Pending` and is still registered is committed to completion by the next
    /// [`set`](Self::set), even if a [`reset`](Self::reset) happens before it is polled again.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::event::ManualResetEvent;
    ///
    /// let event = ManualResetEvent::new();
    /// let waiter = async {
    ///     event.wait().await;
    ///     "released"
    /// };
    /// let setter = async { event.set() };
    ///
    /// let (released, ()) = tokio::join!(waiter, setter);
    /// assert_eq!(released, "released");
    /// # }
    /// ```
    pub async fn wait(&self) {
        let fut = ManualResetEventWait {
            waiter: None,
            event: self,
        };
        fut.await
    }

    /// Returns an owned future that waits until the event is set.
    ///
    /// The event must be held in an [`Arc`]. The returned future owns that `Arc` and therefore has
    /// no borrowing lifetime, which makes it suitable for spawned tasks.
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
    ///
    /// event.set();
    /// waiter.await.unwrap();
    /// # }
    /// ```
    pub async fn wait_owned(self: Arc<Self>) {
        let fut = OwnedManualResetEventWait {
            waiter: None,
            event: self,
        };
        fut.await
    }

    /// Polls a wait, registering `waiter_id` on the first poll that observes an unset event.
    ///
    /// `set` unlinks every queued waiter and marks it notified, and a wait that starts while the
    /// event is set never enqueues. A linked waiter therefore always belongs to an unset event, so
    /// `notified` alone decides whether a registered waiter is already committed.
    fn poll_wait(&self, waiter_id: &mut Option<WaiterId>, cx: &mut Context<'_>) -> Poll<()> {
        // Ready waits require no waker, and a pending wait normally keeps the same one. Inspect the
        // state before cloning to preserve those fast paths. If registration needs a new waker,
        // release the lock, clone, and repeat the full state check because the clone callback may
        // re-enter and set or reset the event.
        let mut prepared_waker = None;
        loop {
            let (poll, retired_waker) = {
                let mut state = self.state.lock();
                match *waiter_id {
                    Some(id) if state.waiters.waiter_mut(id).notified => {
                        let waiter = state.remove_waiter(id);
                        *waiter_id = None;
                        (Poll::Ready(()), waiter.waker)
                    }
                    Some(id) => {
                        debug_assert!(
                            !state.is_set,
                            "a linked waiter must belong to an unset event"
                        );
                        let waiter = state.waiters.waiter_mut(id);
                        if prepared_waker.is_none() && waiter.will_wake(cx.waker()) {
                            return Poll::Pending;
                        }
                        let Some(waker) = prepared_waker.take() else {
                            drop(state);
                            prepared_waker = Some(cx.waker().clone());
                            continue;
                        };
                        (Poll::Pending, Some(waiter.replace_waker(waker)))
                    }
                    None if state.is_set => (Poll::Ready(()), None),
                    None => {
                        let Some(waker) = prepared_waker.take() else {
                            drop(state);
                            prepared_waker = Some(cx.waker().clone());
                            continue;
                        };
                        *waiter_id = Some(state.waiters.push_back(Waiter {
                            notified: false,
                            waker: Some(waker),
                        }));
                        (Poll::Pending, None)
                    }
                }
            };

            drop(retired_waker);
            drop(prepared_waker);
            return poll;
        }
    }

    fn unregister_waiter(&self, waiter_id: &mut Option<WaiterId>) {
        let Some(id) = waiter_id.take() else {
            return;
        };
        let waiter = {
            let mut state = self.state.lock();
            state.remove_waiter(id)
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
        f.debug_struct("ManualResetEvent")
            .field("is_set", &self.is_set())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct State {
    is_set: bool,
    waiters: WaitList<Waiter>,
}

impl State {
    /// Removes a waiter whether or not [`ManualResetEvent::set`] already unlinked it.
    fn remove_waiter(&mut self, id: WaiterId) -> Waiter {
        // Unlinking is idempotent: a waiter that `set` detached keeps its node until it is removed
        // here, and an unconditional predicate never declines.
        self.waiters.unlink_waiter(id, |_| true);
        self.waiters.remove_unlinked_waiter(id)
    }
}

#[derive(Debug)]
struct Waiter {
    notified: bool,
    waker: Option<Waker>,
}

impl Waiter {
    fn will_wake(&self, waker: &Waker) -> bool {
        self.waker
            .as_ref()
            .expect("an unnotified waiter must retain its waker")
            .will_wake(waker)
    }

    fn replace_waker(&mut self, waker: Waker) -> Waker {
        let current = self
            .waker
            .as_mut()
            .expect("an unnotified waiter must retain its waker");
        mem::replace(current, waker)
    }
}

/// A borrowed future returned by [`ManualResetEvent::wait`].
///
/// Dropping a pending wait unregisters only that waiter; it leaves the event state and every other
/// waiter untouched.
#[must_use = "futures do nothing unless you `.await` or poll them"]
struct ManualResetEventWait<'a> {
    waiter: Option<WaiterId>,
    event: &'a ManualResetEvent,
}

impl fmt::Debug for ManualResetEventWait<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManualResetEventWait")
            .finish_non_exhaustive()
    }
}

impl Future for ManualResetEventWait<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self { waiter, event } = self.get_mut();
        event.poll_wait(waiter, cx)
    }
}

impl Drop for ManualResetEventWait<'_> {
    fn drop(&mut self) {
        self.event.unregister_waiter(&mut self.waiter);
    }
}

/// An owned future returned by [`ManualResetEvent::wait_owned`].
///
/// This behaves like [`ManualResetEventWait`] and keeps the event alive through its [`Arc`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
struct OwnedManualResetEventWait {
    waiter: Option<WaiterId>,
    event: Arc<ManualResetEvent>,
}

impl fmt::Debug for OwnedManualResetEventWait {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedManualResetEventWait")
            .finish_non_exhaustive()
    }
}

impl Future for OwnedManualResetEventWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self { waiter, event } = self.get_mut();
        event.poll_wait(waiter, cx)
    }
}

impl Drop for OwnedManualResetEventWait {
    fn drop(&mut self) {
        self.event.unregister_waiter(&mut self.waiter);
    }
}
