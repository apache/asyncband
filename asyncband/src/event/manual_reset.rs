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

use std::borrow::Borrow;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use crate::internal::mutex::Mutex;
use crate::internal::waker_batch::WakerBatch;
use crate::internal::wakerset::WakerSet;
use crate::internal::wakerset::WakerToken;

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
/// while the event remains set, and with [`is_set`](Self::is_set) and [`try_wait`](Self::try_wait)
/// calls that observe that set state and return `true`. Memory operations sequenced before
/// [`set`](Self::set) are therefore visible after those waits complete or those calls return.
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
    // Flag writes and waiter registration are serialized by `state`.
    is_set: AtomicBool,
    state: Mutex<State>,
}

struct State {
    // Identifies the unset cycle collecting waiters. Compare identity, not order: the counter
    // wraps, and woken waiters may remain unpolled across later cycles. Using u64 keeps an old
    // waiter from matching again unless it survives a full 2^64-cycle wraparound.
    generation: u64,
    waiters: WakerSet,
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
            state: Mutex::new(State {
                generation: 0,
                waiters: WakerSet::new(),
            }),
        }
    }

    /// Signals all registered waits and keeps the event set.
    ///
    /// The event remains set until [`reset`](Self::reset) is called. Calling `set` while it is
    /// already set has no effect.
    pub fn set(&self) {
        let mut wakers = WakerBatch::new();
        {
            let mut state = self.state.lock();
            if self.is_set.load(Ordering::Relaxed) {
                return;
            }

            state.generation = state.generation.wrapping_add(1);
            if state.waiters.is_empty() {
                self.is_set.store(true, Ordering::Release);
                return;
            }
            // Detach the complete cohort before publishing `is_set`. Any wait that subsequently
            // observes `is_set() == true` already has its slot freed, and any wake callback that
            // resets the event registers into the new generation.
            state.waiters.drain_into(&mut wakers);
            self.is_set.store(true, Ordering::Release);
        }
        wakers.wake_all();
    }

    /// Clears the set state.
    ///
    /// Waits already released by a preceding [`set`](Self::set) remain ready. If the event is
    /// already unset, this has no effect.
    pub fn reset(&self) {
        let _state = self.state.lock();
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
    /// # Cancellation
    ///
    /// Dropping this future before it returns `Ready` removes its registration. This does not
    /// change the event or affect other waits.
    pub async fn wait(&self) {
        Wait {
            event: self,
            registration: None,
        }
        .await
    }

    /// Waits until the event is set while owning this [`Arc`].
    ///
    /// The future keeps the event alive without borrowing the caller's handle.
    ///
    /// # Cancellation
    ///
    /// Cancellation has the same signal semantics as [`wait`](Self::wait) and releases the
    /// future's `Arc`.
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
        Wait {
            event: self,
            registration: None,
        }
        .await
    }

    /// Polls a wait, registering in the current generation on the first poll that observes an unset
    /// event.
    ///
    /// `set` detaches every registered waker and advances `generation` before storing `true` with
    /// `Release`, so any wait observing `is_set()` has no active slot left to clean up, and a
    /// released wait remains ready even if the event is subsequently reset.
    fn poll_wait(&self, registration: &mut Option<Registration>, cx: &mut Context<'_>) -> Poll<()> {
        if self.is_set() {
            *registration = None;
            return Poll::Ready(());
        }

        let (poll, retired_waker) = {
            let mut state = self.state.lock();
            match registration {
                Some(reg) if state.generation != reg.generation => {
                    *registration = None;
                    (Poll::Ready(()), None)
                }
                Some(reg) => {
                    debug_assert!(
                        !self.is_set.load(Ordering::Relaxed),
                        "an active registration must belong to an unset event"
                    );
                    (Poll::Pending, state.waiters.update(&reg.token, cx.waker()))
                }
                // A set after the false fast probe must either be observed here or release the
                // slot that we register while holding this same lock.
                None if self.is_set.load(Ordering::Relaxed) => (Poll::Ready(()), None),
                None => {
                    *registration = Some(Registration {
                        generation: state.generation,
                        token: state.waiters.insert(cx.waker()),
                    });
                    (Poll::Pending, None)
                }
            }
        };

        drop(retired_waker);
        poll
    }

    fn unregister_waiter(&self, reg: Registration) {
        let waker = {
            let mut state = self.state.lock();
            // A set that raced ahead of cancellation already detached the entire generation.
            (state.generation == reg.generation).then(|| state.waiters.remove(reg.token))
        };
        drop(waker);
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

struct Registration {
    generation: u64,
    token: WakerToken,
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct Wait<E: Borrow<ManualResetEvent>> {
    event: E,
    registration: Option<Registration>,
}

impl<E: Borrow<ManualResetEvent> + Unpin> Future for Wait<E> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.event.borrow().poll_wait(&mut this.registration, cx)
    }
}

impl<E: Borrow<ManualResetEvent>> Drop for Wait<E> {
    fn drop(&mut self) {
        if let Some(reg) = self.registration.take() {
            let event = self.event.borrow();
            if !event.is_set() {
                event.unregister_waiter(reg);
            }
        }
    }
}
