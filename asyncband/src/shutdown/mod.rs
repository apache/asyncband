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
//! This module provides [`new`] to create a coordinator and an initial completion guard:
//!
//! * [`Shutdown`] can request shutdown and wait for all guards to be dropped.
//! * [`ShutdownGuard`] keeps shutdown completion pending until it is dropped and can observe the
//!   shutdown request.
//! * [`ShutdownWatch`] can observe the shutdown request without delaying completion.
//!
//! [`Shutdown`] is cloneable, allowing multiple control handles to request shutdown or wait for
//! completion. [`ShutdownGuard`] is also cloneable; each clone keeps completion pending
//! independently until it is dropped.
//!
//! Awaiting [`Shutdown`] requests shutdown and then waits until all [`ShutdownGuard`] handles have
//! been dropped. The request is made when the future is first polled, not when the value is created
//! or converted into a future. Call [`Shutdown::request_shutdown`] first when the request must be
//! issued before entering a cancellable operation such as `tokio::select!`.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use asyncband::shutdown;
//!
//! let (shutdown, guard) = shutdown::new();
//! let mut tasks = vec![];
//!
//! for _ in 0..3 {
//!     let guard = guard.clone();
//!     let task = tokio::spawn(async move {
//!         guard.shutdown_requested().await;
//!         1
//!     });
//!     tasks.push(task);
//! }
//! drop(guard);
//!
//! shutdown.await;
//! let mut completed = 0;
//! for task in tasks {
//!     completed += task.await.unwrap();
//! }
//! assert_eq!(completed, 3);
//! # }
//! ```

use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::latch::Latch;
use crate::waitgroup::Wait;
use crate::waitgroup::WaitGroup;

/// Creates a graceful shutdown coordinator and an initial completion guard.
///
/// See the [module level documentation](self) for more.
pub fn new() -> (Shutdown, ShutdownGuard) {
    let latch = Arc::new(Latch::new(1));
    let wg = WaitGroup::new();
    let shutdown = Shutdown {
        latch: latch.clone(),
        wait: wg.clone().into_future(),
    };
    let guard = ShutdownGuard {
        latch,
        wait_group: wg,
    };
    (shutdown, guard)
}

/// A future that requests shutdown and waits for all completion guards to be dropped.
///
/// See the [module level documentation](self) for more.
///
/// # Cancellation
///
/// The first poll requests shutdown. Dropping an unpolled future does not request it; dropping
/// a pending future after its first poll does not withdraw the request or drop completion
/// guards. Call [`request_shutdown`](Self::request_shutdown) before selecting when the request
/// must happen even if this future is never polled.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use std::future::pending;
///
/// use asyncband::shutdown;
///
/// let (shutdown, guard) = shutdown::new();
/// let worker = tokio::spawn(async move {
///     guard.shutdown_requested().await;
/// });
///
/// shutdown.request_shutdown();
///
/// tokio::select! {
///     _ = shutdown => {}
///     _ = pending::<()>() => {}
/// }
///
/// worker.await.unwrap();
/// # }
/// ```
#[must_use = "shutdown is not requested unless this handle is polled or request_shutdown is called"]
#[derive(Debug, Clone)]
pub struct Shutdown {
    latch: Arc<Latch>,
    wait: Wait,
}

impl Shutdown {
    /// Requests shutdown for all [`ShutdownGuard`] and [`ShutdownWatch`] handles.
    ///
    /// The request is sticky and this method is idempotent. Current and future observers from this
    /// pair will see the request.
    pub fn request_shutdown(&self) {
        self.latch.count_down();
    }
}

impl Future for Shutdown {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.request_shutdown();
        Pin::new(&mut this.wait).poll(cx)
    }
}

/// Keeps shutdown completion pending until the guard is dropped.
///
/// See the [module level documentation](self) for more.
#[derive(Debug, Clone)]
pub struct ShutdownGuard {
    latch: Arc<Latch>,
    #[expect(
        dead_code,
        reason = "keeps shutdown completion pending until this guard is dropped"
    )]
    wait_group: WaitGroup,
}

impl ShutdownGuard {
    /// Returns a handle that observes the shutdown request without participating in completion.
    ///
    /// The returned handle does not delay shutdown completion, but this guard still does. Use
    /// [`into_watch`](Self::into_watch) to stop keeping completion pending.
    pub fn watch(&self) -> ShutdownWatch {
        ShutdownWatch {
            latch: self.latch.clone(),
        }
    }

    /// Converts this guard into a watch that does not delay completion.
    pub fn into_watch(self) -> ShutdownWatch {
        let Self { latch, .. } = self;
        ShutdownWatch { latch }
    }

    /// Returns whether shutdown has been requested.
    pub fn is_shutdown_requested(&self) -> bool {
        self.latch.try_wait().is_ok()
    }

    /// Waits until shutdown is requested.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending future removes only this wait. It does not request shutdown, clear an
    /// existing request, or drop the handle borrowed by this call.
    pub async fn shutdown_requested(&self) {
        self.latch.wait().await;
    }

    /// Returns a future that observes a shutdown request without borrowing this handle.
    ///
    /// The future retains the request state but does not keep shutdown completion pending.
    ///
    /// # Cancellation
    ///
    /// Dropping the future removes only this wait. It does not request shutdown or clear an
    /// existing request.
    pub fn shutdown_requested_owned(&self) -> impl Future<Output = ()> + 'static {
        self.latch.clone().wait_owned()
    }
}

/// Observes graceful shutdown requests without participating in completion.
///
/// See the [module level documentation](self) for more.
#[derive(Debug, Clone)]
pub struct ShutdownWatch {
    latch: Arc<Latch>,
}

impl ShutdownWatch {
    /// Returns whether shutdown has been requested.
    pub fn is_shutdown_requested(&self) -> bool {
        self.latch.try_wait().is_ok()
    }

    /// Waits until shutdown is requested.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending future removes only this wait. It does not request shutdown, clear an
    /// existing request, or drop the handle borrowed by this call.
    pub async fn shutdown_requested(&self) {
        self.latch.wait().await;
    }

    /// Returns a future that observes a shutdown request without borrowing this handle.
    ///
    /// The future retains the request state but does not keep shutdown completion pending.
    ///
    /// # Cancellation
    ///
    /// Dropping the future removes only this wait. It does not request shutdown or clear an
    /// existing request.
    pub fn shutdown_requested_owned(&self) -> impl Future<Output = ()> + 'static {
        self.latch.clone().wait_owned()
    }
}
