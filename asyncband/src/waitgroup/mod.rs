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

//! Wait for a set of worker handles to be dropped.
//!
//! A [`WaitGroup`] starts with one coordinator handle. Clone that handle once for each unit of work
//! and move the clones into their workers. Dropping a worker handle marks that worker as complete.
//! Awaiting the coordinator consumes it and waits until every remaining handle has been dropped.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use asyncband::waitgroup::WaitGroup;
//!
//! async fn do_work() {}
//!
//! let group = WaitGroup::new();
//! let mut tasks = Vec::new();
//!
//! for _ in 0..3 {
//!     let worker = group.clone();
//!     tasks.push(tokio::spawn(async move {
//!         do_work().await;
//!         drop(worker); // Signals completion. This would also happen at the end of the task.
//!     }));
//! }
//!
//! group.await;
//! for task in tasks {
//!     task.await.unwrap();
//! }
//! # }
//! ```

use std::fmt;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::internal::countdown::CountdownState;
use crate::internal::waitset::WakerToken;

#[cfg(test)]
mod tests;

/// A group of handles whose collective completion can be awaited.
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
    /// Creates a new worker handle for the wait group.
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

    /// Consumes this handle and waits for all other handles to be dropped.
    fn into_future(self) -> Self::IntoFuture {
        let state = self.state.clone();
        drop(self);
        Wait { token: None, state }
    }
}

/// A future that completes when every [`WaitGroup`] handle has been dropped.
///
/// Awaiting a [`WaitGroup`] creates this future. Cloning a `Wait` creates another observer without
/// adding a worker to the group.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Wait {
    token: Option<WakerToken>,
    state: Arc<CountdownState>,
}

impl Clone for Wait {
    /// Creates a new future that also completes when the WaitGroup counter reaches zero.
    ///
    /// This does not increment the WaitGroup counter.
    fn clone(&self) -> Self {
        Wait {
            token: None,
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
        let Self { token, state } = self.get_mut();
        state.poll_wait(token, cx)
    }
}

impl Drop for Wait {
    fn drop(&mut self) {
        self.state.unregister(&mut self.token);
    }
}
