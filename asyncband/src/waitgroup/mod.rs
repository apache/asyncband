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

//! A synchronization primitive for waiting on multiple tasks to complete.
//!
//! Similar to Go's WaitGroup, this type allows a task to wait for multiple other
//! tasks to finish. Each task holds a handle to the WaitGroup, and the main task
//! can wait for all handles to be dropped before proceeding.
//!
//! A WaitGroup waits for a collection of tasks to finish. The main task calls
//! [`clone()`] to create a new worker handle for each task, and can then wait
//! for all tasks to complete by calling `.await` on the WaitGroup.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use std::time::Duration;
//!
//! use asyncband::waitgroup::WaitGroup;
//! let wg = WaitGroup::new();
//!
//! for i in 0..3 {
//!     let wg = wg.clone();
//!     tokio::spawn(async move {
//!         println!("Task {} starting", i);
//!         tokio::time::sleep(Duration::from_millis(100)).await;
//!         // wg is automatically decremented when dropped
//!         drop(wg);
//!     });
//! }
//!
//! // Wait for all tasks to complete
//! wg.await;
//! println!("All tasks completed");
//! # }
//! ```
//!
//! [`clone()`]: WaitGroup::clone

use std::fmt;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::internal::CountdownState;

#[cfg(test)]
mod tests;

/// A synchronization primitive for waiting on multiple tasks to complete.
///
/// See the [module level documentation](self) for more.
pub struct WaitGroup {
    state: Arc<CountdownState>,
}

impl fmt::Debug for WaitGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitGroup").finish_non_exhaustive()
    }
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitGroup {
    /// Creates a new `WaitGroup`.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::waitgroup::WaitGroup;
    ///
    /// let wg = WaitGroup::new();
    /// ```
    pub fn new() -> Self {
        Self {
            state: Arc::new(CountdownState::new(1)),
        }
    }
}

impl Clone for WaitGroup {
    /// Creates a new worker handle for the WaitGroup.
    ///
    /// This increments the WaitGroup counter. The counter will be decremented
    /// when the new handle is dropped.
    ///
    /// # Panics
    ///
    /// Panics if the WaitGroup counter would overflow.
    fn clone(&self) -> Self {
        let sync = self.state.clone();
        if sync.increment(1) {
            panic!("WaitGroup counter overflow");
        }
        Self { state: sync }
    }
}

impl Drop for WaitGroup {
    fn drop(&mut self) {
        if self.state.decrement(1) {
            self.state.wake_all();
        }
    }
}

impl IntoFuture for WaitGroup {
    type Output = ();
    type IntoFuture = Wait;

    /// Converts the WaitGroup into a future that completes when all tasks finish. This decreases
    /// the WaitGroup counter.
    fn into_future(self) -> Self::IntoFuture {
        let state = self.state.clone();
        drop(self);
        Wait { idx: None, state }
    }
}

/// A future that completes when all tasks in a WaitGroup have finished.
///
/// This type is created by either: (1) calling `.await` on a `WaitGroup`, or (2) cloning
/// itself, which does not increase the WaitGroup counter, but creates a new future that
/// will complete when the WaitGroup counter reaches zero.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Wait {
    idx: Option<usize>,
    state: Arc<CountdownState>,
}

impl Clone for Wait {
    /// Creates a new future that also completes when the WaitGroup counter reaches zero.
    ///
    /// This does not increment the WaitGroup counter.
    fn clone(&self) -> Self {
        Wait {
            idx: None,
            state: self.state.clone(),
        }
    }
}

impl fmt::Debug for Wait {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wait").finish_non_exhaustive()
    }
}

impl Future for Wait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self { idx, state } = self.get_mut();

        // register waker if the counter is not zero
        if state.spin_wait(16).is_err() {
            state.register_waker(idx, cx);
            // double check after register waker, to catch the update between two steps
            if state.spin_wait(0).is_err() {
                return Poll::Pending;
            }
        }

        Poll::Ready(())
    }
}
