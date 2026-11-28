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

//! Broadcast a sticky cooperative cancellation request to any number of observers.
//!
//! A [`CancellationSource`] explicitly requests cancellation, and its cloneable
//! [`CancellationToken`] handles query or wait for that request. Cancellation does not interrupt,
//! spawn, or join work; observers decide how to respond. Use the `shutdown` feature when
//! coordination must also wait for participating workers to finish.
//!
//! Dropping an uncancelled source does not request cancellation. Tokens remain not cancelled and
//! their pending waits remain pending; dropping the source simply removes the ability to issue a
//! request. A cancellation request already made remains observable after the source is dropped.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use asyncband::cancellation::CancellationSource;
//!
//! let source = CancellationSource::new();
//! let token = source.token();
//! let observer_token = token.clone();
//! let observer = tokio::spawn(async move {
//!     observer_token.cancelled().await;
//! });
//!
//! source.cancel();
//! assert!(token.is_cancelled());
//! observer.await.unwrap();
//! # }
//! ```

use std::future::Future;
use std::sync::Arc;

use crate::latch::Latch;

/// Controls a shared, one-way cancellation signal.
///
/// The source is not cloneable. Share it through `Arc` when multiple controllers need authority
/// to request cancellation. Dropping the source does not request cancellation; see the
/// [module-level documentation](self).
#[derive(Debug)]
#[must_use = "retain the source while cancellation may need to be requested"]
pub struct CancellationSource {
    latch: Arc<Latch>,
}

impl CancellationSource {
    /// Creates a source with cancellation not yet requested.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::cancellation::CancellationSource;
    ///
    /// let source = CancellationSource::new();
    /// assert!(!source.is_cancelled());
    /// ```
    pub fn new() -> Self {
        Self {
            latch: Arc::new(Latch::new(1)),
        }
    }

    /// Returns a cloneable observer of this source's cancellation request.
    pub fn token(&self) -> CancellationToken {
        CancellationToken {
            latch: self.latch.clone(),
        }
    }

    /// Requests cancellation for all tokens from this source.
    ///
    /// The request is sticky and repeated calls have no effect after the first request.
    pub fn cancel(&self) {
        self.latch.count_down();
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.latch.try_wait().is_ok()
    }
}

impl Default for CancellationSource {
    fn default() -> Self {
        Self::new()
    }
}

/// A cloneable observer of a [`CancellationSource`] request.
///
/// Tokens cannot request cancellation. Dropping a token or one of its waits does not change the
/// signal or affect other observers.
#[derive(Clone, Debug)]
pub struct CancellationToken {
    latch: Arc<Latch>,
}

impl CancellationToken {
    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.latch.try_wait().is_ok()
    }

    /// Waits until cancellation is requested.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending future unregisters only this wait. It does not change the cancellation
    /// signal or affect other observers; the wait may be recreated later.
    pub async fn cancelled(&self) {
        self.latch.wait().await;
    }

    /// Returns a future that waits for cancellation without borrowing this token.
    ///
    /// The future retains the shared signal state, so it may outlive this token.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending future unregisters only this wait and releases its reference to the
    /// signal state. It does not change the cancellation signal or affect other observers.
    pub fn cancelled_owned(&self) -> impl Future<Output = ()> + 'static {
        self.latch.clone().wait_owned()
    }
}
