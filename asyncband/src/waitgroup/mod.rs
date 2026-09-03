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
//! A [`WaitGroup`] coordinates completion without itself representing unfinished work. Call
//! [`WaitGroup::worker`] once for each unit of work and move the returned [`Worker`] into that
//! work. Workers may be cloned for nested work. Dropping the last worker completes the group.
//!
//! Awaiting the coordinator consumes it, preventing new top-level workers from being registered
//! after waiting begins. A worker cannot be awaited, so accidentally waiting from inside a worker
//! does not remove that worker from the group.
//!
//! Completion acquires the state published before every worker was dropped, so work performed by
//! those workers is visible after the wait returns.
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
//! let mut tasks = vec![];
//!
//! for _ in 0..3 {
//!     let worker = group.worker();
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

/// Coordinates a set of [`Worker`] handles whose completion can be awaited.
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
    /// Creates an empty `WaitGroup`.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::waitgroup::WaitGroup;
    ///
    /// let group = WaitGroup::new();
    /// ```
    pub fn new() -> Self {
        Self {
            state: Arc::new(CountdownState::new(0)),
        }
    }

    /// Creates a handle representing one unfinished unit of work.
    ///
    /// Clone the returned worker to register nested work. The group completes after every worker
    /// has been dropped.
    ///
    /// # Panics
    ///
    /// Panics if the worker count would overflow.
    pub fn worker(&self) -> Worker {
        let state = self.state.clone();
        if state.increment(1) {
            panic!("WaitGroup worker count overflow");
        }
        Worker { state }
    }

    /// Consumes this coordinator and returns a future that waits for all workers to be dropped.
    pub fn wait(self) -> Wait {
        self.into_future()
    }
}

impl IntoFuture for WaitGroup {
    type Output = ();
    type IntoFuture = Wait;

    /// Consumes this coordinator and waits for all workers to be dropped.
    fn into_future(self) -> Self::IntoFuture {
        Wait {
            token: None,
            state: self.state,
        }
    }
}

/// Keeps a [`WaitGroup`] pending until this handle and all of its clones are dropped.
///
/// A worker may be cloned to represent nested work. Workers deliberately cannot be awaited; only
/// the coordinating [`WaitGroup`] can begin a wait.
pub struct Worker {
    state: Arc<CountdownState>,
}

impl Clone for Worker {
    /// Creates a handle representing another unfinished unit of work.
    ///
    /// # Panics
    ///
    /// Panics if the worker count would overflow.
    fn clone(&self) -> Self {
        let state = self.state.clone();
        if state.increment(1) {
            panic!("WaitGroup worker count overflow");
        }
        Worker { state }
    }
}

impl fmt::Debug for Worker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Worker").finish_non_exhaustive()
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if self.state.decrement(1) {
            self.state.wake_all();
        }
    }
}

/// A future that completes when every [`Worker`] has been dropped.
///
/// Awaiting or calling [`WaitGroup::wait`] creates this future. Cloning a `Wait` creates another
/// observer without adding a worker to the group.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Wait {
    token: Option<WakerToken>,
    state: Arc<CountdownState>,
}

impl Clone for Wait {
    /// Creates a new future that observes the same group completion.
    ///
    /// This does not add a worker to the group.
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
