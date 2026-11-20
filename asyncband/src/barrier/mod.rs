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

//! Synchronize a fixed number of tasks at a reusable rendezvous point.
//!
//! A [`Barrier`] releases one generation after the configured number of participants have awaited
//! [`Barrier::wait`]. It can then be reused for the next generation. Exactly one participant in
//! each generation receives a [`BarrierWaitResult`] marked as the leader.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use std::sync::Arc;
//! use std::sync::atomic::AtomicUsize;
//! use std::sync::atomic::Ordering;
//!
//! use asyncband::barrier::Barrier;
//!
//! let barrier = Arc::new(Barrier::new(3));
//! let arrived = Arc::new(AtomicUsize::new(0));
//! let mut workers = Vec::with_capacity(3);
//!
//! for _ in 0..3 {
//!     let barrier = barrier.clone();
//!     let arrived = arrived.clone();
//!     workers.push(tokio::spawn(async move {
//!         arrived.fetch_add(1, Ordering::Relaxed);
//!         let result = barrier.wait().await;
//!         assert_eq!(arrived.load(Ordering::Relaxed), 3);
//!         result.is_leader()
//!     }));
//! }
//!
//! let mut leader_count = 0;
//! for worker in workers {
//!     if worker.await.unwrap() {
//!         leader_count += 1;
//!     }
//! }
//! assert_eq!(leader_count, 1);
//! # }
//! ```

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use crate::internal::mutex::Mutex;
use crate::internal::wakerset::WakerSet;
use crate::internal::wakerset::WakerToken;

#[cfg(test)]
mod tests;

/// A reusable rendezvous point that releases tasks in fixed-size generations.
///
/// See the [module level documentation](self) for more.
#[derive(Debug)]
pub struct Barrier {
    n: u32,
    state: Mutex<BarrierState>,
}

struct BarrierState {
    arrived: u32,
    // Identifies the generation collecting arrivals. Compare identity, not order: the counter
    // wraps, and woken waiters may remain unpolled while later generations complete. Using u64
    // keeps an old waiter from matching again unless it survives a full 2^64-generation cycle.
    generation: u64,
    waiters: WakerSet,
}

impl fmt::Debug for BarrierState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BarrierState")
            .field("arrived", &self.arrived)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

/// The result of participating in one [`Barrier`] generation.
///
/// Exactly one participant in each completed generation is designated as the leader.
pub struct BarrierWaitResult(bool);

impl fmt::Debug for BarrierWaitResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BarrierWaitResult")
            .field("is_leader", &self.is_leader())
            .finish()
    }
}

impl BarrierWaitResult {
    /// Returns `true` if this participant is the leader for its barrier generation.
    ///
    /// Exactly one participant per generation returns `true`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::barrier::Barrier;
    ///
    /// let barrier = Barrier::new(1);
    /// assert!(barrier.wait().await.is_leader());
    /// # }
    /// ```
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.0
    }
}

impl Barrier {
    /// Creates a barrier for `n` participants.
    ///
    /// Each generation completes when `n` calls to [`wait`](Self::wait) have arrived. Passing zero
    /// selects the same immediately completing behavior as passing one.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::barrier::Barrier;
    ///
    /// let barrier = Barrier::new(3);
    /// ```
    pub const fn new(n: u32) -> Self {
        // Normalize an empty group to one participant so every wait completes its own generation.
        let n = if n > 0 { n } else { 1 };

        Self {
            n,
            state: Mutex::new(BarrierState {
                arrived: 0,
                generation: 0,
                waiters: WakerSet::new(),
            }),
        }
    }

    /// Arrives at the barrier and waits until all `n` participants in the current generation have
    /// arrived.
    ///
    /// The final participant to arrive completes the generation, wakes the waiting participants,
    /// and is designated as that generation's leader.
    ///
    /// # Returns
    ///
    /// Returns a [`BarrierWaitResult`] identifying whether this call completed the generation
    /// and was selected as its leader.
    ///
    /// # Cancellation
    ///
    /// An arrival is recorded when the future returned by `wait` is first polled. Once recorded,
    /// the arrival is not retracted if that future is dropped before the barrier completes.
    /// Cancellation therefore stops that caller from observing completion, but it does not make the
    /// caller leave the current barrier generation.
    ///
    /// Calling `wait` again after cancelling a pending call records another arrival. Repeatedly
    /// cancelling and retrying can therefore cause a generation to complete with fewer than `n`
    /// live wait futures. Callers that need to keep waiting after another operation completes
    /// first should retain and continue polling the same `wait` future instead of creating a
    /// new one.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::barrier::Barrier;
    ///
    /// let barrier = Barrier::new(1);
    /// assert!(barrier.wait().await.is_leader());
    /// # }
    /// ```
    pub async fn wait(&self) -> BarrierWaitResult {
        BarrierWait {
            barrier: self,
            registration: None,
        }
        .await
    }
}

/// The future awaited by [`Barrier::wait()`]. Its first poll records the arrival.
#[must_use = "futures do nothing unless you `.await` or poll them"]
struct BarrierWait<'a> {
    barrier: &'a Barrier,
    registration: Option<Registration>,
}

struct Registration {
    generation: u64,
    token: WakerToken,
}

impl Future for BarrierWait<'_> {
    type Output = BarrierWaitResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut state = this.barrier.state.lock();

        if let Some(registration) = &this.registration {
            if state.generation != registration.generation {
                // Completing this generation detached all of its registrations.
                this.registration = None;
                return Poll::Ready(BarrierWaitResult(false));
            }
            let retired_waker = state.waiters.update(&registration.token, cx.waker());
            drop(state);
            drop(retired_waker);
            return Poll::Pending;
        }

        state.arrived += 1;
        if state.arrived == this.barrier.n {
            state.arrived = 0;
            state.generation = state.generation.wrapping_add(1);
            let mut wakers = state.waiters.take_all();
            drop(state);
            wakers.wake_all();
            return Poll::Ready(BarrierWaitResult(true));
        }
        this.registration = Some(Registration {
            generation: state.generation,
            token: state.waiters.insert(cx.waker()),
        });
        Poll::Pending
    }
}

impl Drop for BarrierWait<'_> {
    fn drop(&mut self) {
        let Some(registration) = self.registration.take() else {
            return;
        };

        let mut state = self.barrier.state.lock();
        if state.generation != registration.generation {
            // The current generation may already have reused this slot.
            return;
        }

        let removed_waker = state.waiters.remove(registration.token);
        drop(state);
        drop(removed_waker);
    }
}
