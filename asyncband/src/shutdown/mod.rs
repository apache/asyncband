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

//! Coordination primitives for graceful task shutdown.
//!
//! This module provides [`new_pair`] to create a pair of handles for managing shutdown signals:
//!
//! * [`ShutdownSend`] can send a shutdown signal, and can wait for all the tasks to finish.
//! * [`ShutdownRecv`] can wait for the shutdown signal, and should be dropped when the task is
//!   done, which will notify the sender on all the tasks finished.
//! * [`ShutdownWatch`] can wait for the shutdown signal without blocking
//!   [`ShutdownSend::await_shutdown`].
//!
//! Internally, the shutdown signal is implemented using a countdown latch, and the task completion
//! is tracked using a wait group. [`ShutdownSend`] is cloneable, allowing multiple sources to send
//! the shutdown signal; [`ShutdownRecv`] is also cloneable, allowing multiple tasks to wait for the
//! same shutdown signal.
//!
//! [`ShutdownSend::await_shutdown`] would block until all the tasks are done, i.e., all the
//! [`ShutdownRecv`]s dropped.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! let (tx, rx) = asyncband::shutdown::new_pair();
//!
//! for i in 0..3 {
//!     let rx = rx.clone();
//!     tokio::spawn(async move {
//!         println!("Task {} starting", i);
//!         rx.is_shutdown().await;
//!         println!("Task {} done", i);
//!     });
//! }
//! drop(rx);
//!
//! tx.shutdown();
//! tx.await_shutdown().await;
//! # }
//! ```

use std::future::Future;
use std::future::IntoFuture;
use std::sync::Arc;

use crate::latch::Latch;
use crate::waitgroup::Wait;
use crate::waitgroup::WaitGroup;

/// Create a pair of handles for managing shutdown signals.
///
/// See the [module level documentation](self) for more.
pub fn new_pair() -> (ShutdownSend, ShutdownRecv) {
    let latch = Arc::new(Latch::new(1));
    let wg = WaitGroup::new();
    let send = ShutdownSend {
        latch: latch.clone(),
        wait: wg.clone().into_future(),
    };
    let recv = ShutdownRecv {
        latch,
        _wait_group: wg,
    };
    (send, recv)
}

/// A handle for sending shutdown signals.
///
/// See the [module level documentation](self) for more.
#[derive(Debug, Clone)]
pub struct ShutdownSend {
    latch: Arc<Latch>,
    wait: Wait,
}

impl ShutdownSend {
    /// Send a shutdown signal to all [`ShutdownRecv`] handles.
    pub fn shutdown(&self) {
        self.latch.count_down();
    }

    /// Wait for all [`ShutdownRecv`] handles to be dropped.
    pub async fn await_shutdown(self) {
        self.wait.await;
    }
}

/// A handle for receiving shutdown signals.
///
/// See the [module level documentation](self) for more.
#[derive(Debug, Clone)]
pub struct ShutdownRecv {
    latch: Arc<Latch>,
    // Keeps this receiver registered as a shutdown participant until it is dropped.
    _wait_group: WaitGroup,
}

impl ShutdownRecv {
    /// Returns a handle for watching the shutdown signal.
    ///
    /// The returned handle does not block [`ShutdownSend::await_shutdown`].
    pub fn watch(&self) -> ShutdownWatch {
        ShutdownWatch {
            latch: self.latch.clone(),
        }
    }

    /// Returns whether the shutdown signal has been received.
    pub fn is_shutdown_now(&self) -> bool {
        self.latch.try_wait().is_ok()
    }

    /// Returns a future that resolves when the shutdown signal is received.
    pub async fn is_shutdown(&self) {
        self.latch.wait().await;
    }

    /// Returns an owned future that resolves when the shutdown signal is received.
    ///
    /// The returned future has no lifetime constraints.
    pub fn is_shutdown_owned(&self) -> impl Future<Output = ()> + 'static {
        self.latch.clone().wait_owned()
    }
}

/// A handle for watching shutdown signals without participating in shutdown completion.
///
/// See the [module level documentation](self) for more.
#[derive(Debug, Clone)]
pub struct ShutdownWatch {
    latch: Arc<Latch>,
}

impl ShutdownWatch {
    /// Returns whether the shutdown signal has been received.
    pub fn is_shutdown_now(&self) -> bool {
        self.latch.try_wait().is_ok()
    }

    /// Returns a future that resolves when the shutdown signal is received.
    pub async fn is_shutdown(&self) {
        self.latch.wait().await;
    }

    /// Returns an owned future that resolves when the shutdown signal is received.
    ///
    /// The returned future has no lifetime constraints.
    pub fn is_shutdown_owned(&self) -> impl Future<Output = ()> + 'static {
        self.latch.clone().wait_owned()
    }
}
