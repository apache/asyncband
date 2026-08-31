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
//! A [`ManualResetEvent`] is either set or unset. Calling [`set`](ManualResetEvent::set) releases
//! every registered wait and makes future waits ready. The signal remains set until
//! [`reset`](ManualResetEvent::reset) makes new waits block again.
//!
//! The retained set state distinguishes this primitive from a condition variable, whose
//! notifications are not buffered. Unlike a latch, a manual-reset event can be reset and reused.
//!
//! A wait registered before `set` is committed to completion even if another task calls `reset`
//! before that wait is polled again. Registration happens on the first poll, not when the future is
//! constructed, so `set` followed immediately by `reset` is not a pulse for unpolled futures. Keep
//! the event set for as long as the condition it represents holds.
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
use crate::internal::wake_all;

/// A reusable event that remains set until explicitly reset.
///
/// See the [module-level documentation](self) for its waiting semantics.
///
/// # Synchronization
///
/// An unset-to-set transition synchronizes with the waits it releases and with waits first polled
/// while the event remains set. Memory operations sequenced before [`set`](Self::set) are therefore
/// visible after those waits complete.
///
/// A `set` call that finds the event already set does not establish this guarantee.
/// [`is_set`](Self::is_set) is only a snapshot and cannot replace a wait or support check-then-act.
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

    /// Sets the event and releases every currently registered wait.
    ///
    /// The event remains set until [`reset`](Self::reset) is called. Calling `set` while it is
    /// already set has no effect. No ordering is guaranteed among the released waits.
    ///
    /// # Panics
    ///
    /// Panics if a registered waker panics. The state transition remains committed, and every
    /// remaining registered waker is still notified before the first panic resumes.
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
            // Detach the complete cohort before invoking any waker. A wake callback may reset the
            // event and register a new wait, which must belong to the state current at that point.
            let mut wakers = vec![];
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

    /// Resets the event so new waits block until another [`set`](Self::set).
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

    /// Waits until the event is set.
    ///
    /// If the event is already set, the wait completes immediately. Once a [`set`](Self::set)
    /// commits a registered wait, a later [`reset`](Self::reset) cannot make that wait pending
    /// again.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending wait unregisters only that call; it does not change the event or affect
    /// other waiters. If cancellation races with `set`, either cancellation unregisters first or
    /// `set` commits the wait first. A waker already detached by `set` may still run after the wait
    /// is dropped.
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

    /// Waits until the event is set without borrowing it.
    ///
    /// The event must be held in an [`Arc`]. The returned future owns that `Arc`, which makes it
    /// suitable for spawned tasks. Its waiting and cancellation semantics match
    /// [`wait`](Self::wait).
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
                    let retired = (!waiter.will_wake(cx.waker()))
                        .then(|| waiter.replace_waker(cx.waker().clone()));
                    (Poll::Pending, retired)
                }
                None if state.is_set => (Poll::Ready(()), None),
                None => {
                    *waiter_id = Some(state.waiters.push_back(Waiter {
                        notified: false,
                        waker: Some(cx.waker().clone()),
                    }));
                    (Poll::Pending, None)
                }
            }
        };

        drop(retired_waker);
        poll
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

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct ManualResetEventWait<'a> {
    waiter: Option<WaiterId>,
    event: &'a ManualResetEvent,
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

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct OwnedManualResetEventWait {
    waiter: Option<WaiterId>,
    event: Arc<ManualResetEvent>,
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
