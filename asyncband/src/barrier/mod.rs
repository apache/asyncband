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
//!
//! use asyncband::barrier::Barrier;
//!
//! let barrier = Arc::new(Barrier::new(3));
//! let mut tasks = vec![];
//!
//! for _ in 0..3 {
//!     let barrier = barrier.clone();
//!     let task = tokio::spawn(async move { barrier.wait().await.is_leader() });
//!     tasks.push(task);
//! }
//!
//! let mut leaders = 0;
//! for task in tasks {
//!     leaders += usize::from(task.await.unwrap());
//! }
//! assert_eq!(leaders, 1);
//! # }
//! ```

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use crate::internal::mutex::Mutex;
use crate::internal::waitset::WaitSet;
use crate::internal::waitset::WakerToken;
use crate::internal::wake_all;

/// A synchronization primitive for multiple tasks that need to wait for each other.
///
/// See the [module level documentation](self) for more.
#[derive(Debug)]
pub struct Barrier {
    n: u32,
    state: Mutex<BarrierState>,
}

struct BarrierState {
    arrived: u32,
    generation: usize,
    waiters: WaitSet,
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
    /// Creates a new barrier that can block the specified number of tasks.
    ///
    /// A barrier will block `n-1` tasks and release them all at once when the `n`th task arrives.
    ///
    /// # Arguments
    ///
    /// * `n`: The number of tasks to wait for. If `n` is 0, it will be treated as 1.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::barrier::Barrier;
    ///
    /// let barrier = Barrier::new(3); // Creates a barrier for 3 tasks
    /// ```
    pub fn new(n: u32) -> Self {
        // If n is 0, it's not clear what behavior the user wants.
        // std::sync::Barrier works with n = 0 the same as n = 1,
        // where every .wait() immediately unblocks, so we adopt that here as well.
        let n = if n > 0 { n } else { 1 };

        Self {
            n,
            state: Mutex::new(BarrierState {
                arrived: 0,
                generation: 0,
                // The final participant completes the generation without parking.
                waiters: WaitSet::with_capacity((n - 1) as usize),
            }),
        }
    }

    /// Waits for all tasks to reach this point.
    ///
    /// The barrier holds the current task until all `n` participants have arrived. The final
    /// participant is designated as the leader for this generation.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancellation safe.
    ///
    /// An arrival is recorded when the future returned by `wait` is first polled. Once recorded,
    /// the arrival is not retracted if that future is dropped before the barrier completes.
    /// Cancellation therefore stops that caller from observing completion, but it does not make the
    /// caller leave the current barrier generation.
    ///
    /// Calling `wait` again after canceling a pending call records another arrival. Repeatedly
    /// canceling and retrying can therefore cause a generation to complete with fewer than `n` live
    /// wait futures. Callers that need to keep waiting after another operation completes first
    /// should retain and continue polling the same `wait` future instead of creating a new one.
    ///
    /// Returns a [`BarrierWaitResult`] that identifies whether this participant is the leader.
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
        let generation = {
            let mut state = self.state.lock();
            let generation = state.generation;
            state.arrived += 1;

            // The final arrival completes this generation. Advance the generation while holding
            // the state lock, then wake the drained followers after releasing it.
            if state.arrived == self.n {
                state.arrived = 0;
                state.generation += 1;
                let wakers = state.waiters.drain();
                drop(state);
                wake_all(wakers);
                return BarrierWaitResult(true);
            }

            generation
        };

        let fut = BarrierWait {
            token: None,
            generation,
            barrier: self,
        };
        fut.await;
        BarrierWaitResult(false)
    }
}

/// A future returned by [`Barrier::wait()`].
///
/// This future will complete when all tasks have reached the barrier point.
#[must_use = "futures do nothing unless you `.await` or poll them"]
struct BarrierWait<'a> {
    token: Option<WakerToken>,
    generation: usize,
    barrier: &'a Barrier,
}

impl fmt::Debug for BarrierWait<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BarrierWait")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl Future for BarrierWait<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self {
            token,
            generation,
            barrier,
        } = self.get_mut();

        let retired_waker = {
            let mut state = barrier.state.lock();
            if *generation < state.generation {
                // Completion advances the generation and drains its old waiters under this same
                // lock, so no registration represented by this token remains in the wait set.
                *token = None;
                return Poll::Ready(());
            }
            state.waiters.register(token, cx.waker())
        };
        drop(retired_waker);
        Poll::Pending
    }
}

impl Drop for BarrierWait<'_> {
    fn drop(&mut self) {
        if self.token.is_some() {
            let removed_waker = {
                let mut state = self.barrier.state.lock();
                state.waiters.unregister(&mut self.token)
            };
            drop(removed_waker);
        }
    }
}
