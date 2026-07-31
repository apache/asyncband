// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A synchronization primitive that enables multiple tasks to wait for each other.
//!
//! The barrier ensures that no task proceeds past a certain point until all tasks have reached it.
//! This is useful for scenarios where multiple tasks need to proceed together after reaching a
//! certain point in their execution.
//!
//! A barrier enables multiple tasks to synchronize the beginning of some computation.
//! When a barrier is created, it is initialized with a count of the number of tasks
//! that will synchronize on the barrier. Each task can then call [`wait()`] on the
//! barrier to indicate it is ready to proceed. The barrier ensures that no task
//! proceeds past the barrier point until all tasks have made the call.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use std::sync::Arc;
//!
//! use mea::barrier::Barrier;
//!
//! let barrier = Arc::new(Barrier::new(3));
//! let mut handles = Vec::new();
//!
//! for i in 0..3 {
//!     let barrier = barrier.clone();
//!     handles.push(tokio::spawn(async move {
//!         println!("Task {} before barrier", i);
//!         let result = barrier.wait().await;
//!         println!("Task {} after barrier (leader: {})", i, result.is_leader());
//!     }));
//! }
//!
//! for handle in std::mem::take(&mut handles) {
//!     handle.await.unwrap();
//! }
//!
//! // The barrier can be reused (generation is increased).
//! for i in 0..3 {
//!     let barrier = barrier.clone();
//!     handles.push(tokio::spawn(async move {
//!         println!("Task {} before barrier", i);
//!         let result = barrier.wait().await;
//!         println!("Task {} after barrier (leader: {})", i, result.is_leader());
//!     }));
//! }
//!
//! for handle in handles {
//!     handle.await.unwrap();
//! }
//! # }
//! ```
//!
//! [`wait()`]: Barrier::wait

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use crate::internal::Mutex;
use crate::internal::WaitSet;

#[cfg(test)]
mod tests;

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

/// A `BarrierWaitResult` is returned by [`Barrier::wait()`] when all threads
/// in the [`Barrier`] have rendezvoused.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use mea::barrier::Barrier;
///
/// let barrier = Barrier::new(1);
/// let barrier_wait_result = barrier.wait().await;
/// # }
/// ```
pub struct BarrierWaitResult(bool);

impl fmt::Debug for BarrierWaitResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BarrierWaitResult")
            .field("is_leader", &self.is_leader())
            .finish()
    }
}

impl BarrierWaitResult {
    /// Returns `true` if this worker is the "leader" for the call to [`Barrier::wait()`].
    ///
    /// Only one worker will have `true` returned from their result, all other
    /// workers will have `false` returned.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use mea::barrier::Barrier;
    ///
    /// let barrier = Barrier::new(1);
    /// let barrier_wait_result = barrier.wait().await;
    /// println!("{:?}", barrier_wait_result.is_leader());
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
    /// use mea::barrier::Barrier;
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
                waiters: WaitSet::with_capacity(n as usize),
            }),
        }
    }

    /// Waits for all tasks to reach this point.
    ///
    /// The barrier will block the current task until all `n` tasks have called `wait()`.
    /// The last task to call `wait()` will be designated as the leader and receive `true`
    /// as the return value. All other tasks will receive `false`.
    ///
    /// # Returns
    ///
    /// Returns a `Future` that resolves to:
    /// * `true` if this task is the last (leader) task to arrive at the barrier
    /// * `false` for all other tasks
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use mea::barrier::Barrier;
    ///
    /// let barrier = Arc::new(Barrier::new(2));
    /// let barrier2 = barrier.clone();
    ///
    /// let handle = tokio::spawn(async move {
    ///     let result = barrier2.wait().await;
    ///     println!("Task 1: leader = {}", result.is_leader());
    /// });
    ///
    /// let result = barrier.wait().await;
    /// println!("Task 2: leader = {}", result.is_leader());
    /// handle.await.unwrap();
    /// # }
    /// ```
    pub async fn wait(&self) -> BarrierWaitResult {
        let generation = {
            let mut state = self.state.lock();
            let generation = state.generation;
            state.arrived += 1;

            // the last arriver is the leader;
            // wake up other waiters, increment the generation, and return
            if state.arrived == self.n {
                state.arrived = 0;
                state.generation += 1;
                let wakers = state.waiters.take_wakers();
                drop(state);
                for waker in wakers {
                    waker.wake();
                }
                return BarrierWaitResult(true);
            }

            generation
        };

        let fut = BarrierWait {
            idx: None,
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
    idx: Option<usize>,
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
            idx,
            generation,
            barrier,
        } = self.get_mut();

        let mut state = barrier.state.lock();
        if *generation < state.generation {
            Poll::Ready(())
        } else {
            state.waiters.register_waker(idx, cx);
            Poll::Pending
        }
    }
}
