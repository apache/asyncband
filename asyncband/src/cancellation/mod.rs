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

//! Request cooperative cancellation without tracking task completion.
//!
//! A [`CancellationSource`] controls one signal. Its cloneable [`CancellationToken`] observers
//! can query or wait for that signal, but cannot request cancellation themselves. The request is
//! sticky: every current and future wait completes once cancellation has been requested.
//!
//! Cancellation is advisory. Observing it does not mean that a task has stopped or finished its
//! cleanup. Callers choose whether and when to stop work, and retain responsibility for joining
//! spawned tasks. Dropping a wait removes that wait's registration; it does not cancel other work.
//!
//! # Source lifetime
//!
//! Dropping the source does not request cancellation or wake waiters. If it was dropped without
//! calling [`cancel`](CancellationSource::cancel), tokens remain uncancelled and their waits stay
//! pending indefinitely. Use the wait alongside work that may finish normally, or retain the
//! source and explicitly cancel it when a stop request is required. A request already issued
//! remains observable after the source is dropped.
//!
//! # Examples
//!
//! ```
//! use asyncband::cancellation::CancellationSource;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let source = CancellationSource::new();
//! let token = source.token();
//! let observer = token.clone();
//!
//! source.cancel();
//! tokio::join!(token.cancelled(), observer.cancelled());
//! assert!(token.is_cancelled());
//! # }
//! ```

use std::future::Future;
use std::sync::Arc;

use crate::latch::Latch;

/// The authority to request cancellation for a set of [`CancellationToken`] observers.
///
/// This type is not cloneable. Share a reference, or explicitly put the source in an [`Arc`],
/// when multiple controllers need cancellation authority. Give workers tokens instead.
///
/// Dropping the source does not request cancellation. See the [module documentation](self) for
/// the behavior of tokens that outlive it.
#[derive(Debug)]
pub struct CancellationSource {
    signal: Arc<Latch>,
}

impl Default for CancellationSource {
    fn default() -> Self {
        Self::new()
    }
}

impl CancellationSource {
    /// Creates a source with no cancellation request.
    pub fn new() -> Self {
        Self {
            signal: Arc::new(Latch::new(1)),
        }
    }

    /// Returns a read-only observer of this source's signal.
    ///
    /// A token created after cancellation observes the same request immediately.
    pub fn token(&self) -> CancellationToken {
        CancellationToken {
            signal: self.signal.clone(),
        }
    }

    /// Requests cancellation and wakes registered waits.
    ///
    /// The request is persistent and this method is idempotent, including concurrent calls.
    /// It does not wait for tasks to observe the request, finish work, or complete cleanup.
    pub fn cancel(&self) {
        self.signal.count_down();
    }
}

/// A cloneable, read-only observer of a cooperative cancellation request.
///
/// Tokens do not keep any task running and do not delay its completion. Every wait registers
/// independently, including multiple waits using the same token. Cloning or dropping a token
/// never changes the cancellation state.
#[derive(Debug, Clone)]
pub struct CancellationToken {
    signal: Arc<Latch>,
}

impl CancellationToken {
    /// Returns whether cancellation has been requested.
    ///
    /// Once true, this remains true. A false result is only a snapshot. Dropping an uncancelled
    /// source leaves the result false; source destruction is not reported as cancellation.
    pub fn is_cancelled(&self) -> bool {
        self.signal.try_wait().is_ok()
    }

    /// Waits until cancellation is requested without consuming the signal.
    ///
    /// This method is cancel safe: dropping a pending wait unregisters only that wait. Later
    /// waits still observe any request, including one made before they are first polled.
    ///
    /// If the source is dropped before cancellation, this wait remains pending indefinitely.
    pub async fn cancelled(&self) {
        self.signal.wait().await;
    }

    /// Returns a cancellation wait that can outlive this token and move into a spawned task.
    ///
    /// The future owns an observer of the signal. Like [`cancelled`](Self::cancelled), it is
    /// cancel safe and remains pending if the source is dropped without requesting cancellation.
    pub fn cancelled_owned(&self) -> impl Future<Output = ()> + 'static {
        self.signal.clone().wait_owned()
    }
}
