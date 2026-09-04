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

//! Synchronous interoperability for runtime-agnostic futures.
//!
//! This module bridges synchronous Rust code to a single future. Enable it with the opt-in
//! `blocking` Cargo feature.
//!
//! [`FutureExt`] lets synchronous callers wait until a future completes, either indefinitely or
//! until a timeout elapses:
//!
//! ```
//! use std::time::Duration;
//!
//! use asyncband::blocking::FutureExt as _;
//!
//! assert_eq!(async { 42 }.block_on(), 42);
//! assert_eq!(async { 42 }.wait_timeout(Duration::ZERO), Some(42));
//! ```
//!
//! Callers that prefer function syntax can invoke the same trait method with UFCS, for example
//! `FutureExt::block_on(future)`; there is no separate free-function entry point.
//!
//! # Execution model
//!
//! These operations use a lightweight single-future executor, not a general-purpose async runtime.
//! They poll the future on the calling thread and wait on a private parker while the future is
//! pending. Recursive calls receive a separate parker, and the parker does not share the
//! notification token used by [`thread::park`](std::thread::park).
//!
//! No timer, I/O, or task scheduler is provided. Futures that depend on a runtime-specific driver,
//! such as Tokio timers, I/O resources, or spawned tasks, may therefore make no progress. This
//! module is intended for runtime-agnostic futures, including the primitives provided by this
//! crate.
//!
//! # Executor threads
//!
//! Do not call [`FutureExt::block_on`] or [`FutureExt::wait_timeout`] from an async executor task.
//! Blocking an executor thread can starve other tasks and can deadlock when the future waits on
//! work assigned to that executor. Call these methods only from synchronous code, such as `main`,
//! a dedicated thread, or a sync-to-async boundary.

mod executor;
mod parker;

use std::future::IntoFuture;
use std::time::Duration;

/// Extension methods for waiting on a future from synchronous code.
///
/// # Example
///
/// ```
/// use std::time::Duration;
///
/// use asyncband::blocking::FutureExt as _;
///
/// assert_eq!(async { 42 }.block_on(), 42);
/// assert_eq!(async { 42 }.wait_timeout(Duration::ZERO), Some(42));
/// ```
pub trait FutureExt: IntoFuture {
    /// Blocks the current thread until this future is ready.
    ///
    /// See the [`blocking`](crate::blocking) module documentation for runtime limitations and
    /// executor-thread caveats.
    fn block_on(self) -> Self::Output
    where
        Self: Sized,
    {
        executor::block_on(self)
    }

    /// Blocks the current thread until this future is ready or `timeout` elapses.
    ///
    /// The future is polled before checking the timeout, so an immediately ready future succeeds
    /// even when `timeout` is zero. Returns [`None`] on timeout and drops the future, cancelling it
    /// according to that future's cancellation semantics.
    ///
    /// The timeout cannot interrupt a call to [`Future::poll`]. See the
    /// [`blocking`](crate::blocking) module documentation for other runtime limitations and
    /// executor-thread caveats.
    fn wait_timeout(self, timeout: Duration) -> Option<Self::Output>
    where
        Self: Sized,
    {
        executor::wait_timeout(self, timeout)
    }
}

impl<F: IntoFuture> FutureExt for F {}
