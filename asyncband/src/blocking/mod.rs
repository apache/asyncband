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

// Portions of the polling loop are adapted from Pollster 1.0.1.
// Reusing a thread-local Parker/Waker pair, with a fresh pair for recursive calls, is adapted from
// futures-lite 2.6.1. Both incorporated upstream portions use the Apache-2.0 option. See the
// project LICENSE file for the exact upstream revisions and source paths.

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

mod parker;

use std::cell::RefCell;
use std::future::IntoFuture;
use std::pin::pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use self::parker::Parker;

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
        let mut future = pin!(self.into_future());

        with_parker(|parker, waker| {
            let mut context = Context::from_waker(waker);

            loop {
                match future.as_mut().poll(&mut context) {
                    Poll::Pending => parker.park(),
                    Poll::Ready(output) => return output,
                }
            }
        })
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
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            // A duration beyond Instant's range cannot expire during the process lifetime.
            return Some(FutureExt::block_on(self));
        };
        let mut future = pin!(self.into_future());

        with_parker(|parker, waker| {
            let mut context = Context::from_waker(waker);

            loop {
                if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                    return Some(output);
                }

                let now = Instant::now();
                if now >= deadline {
                    return None;
                }
                parker.park_timeout(deadline.saturating_duration_since(now));
            }
        })
    }
}

impl<F: IntoFuture> FutureExt for F {}

fn parker_and_waker() -> (Parker, Waker) {
    let parker = Parker::new();
    let waker = parker.waker();
    (parker, waker)
}

thread_local! {
    // Holding the mutable borrow while polling makes a recursive call take the fresh-parker path
    // instead of sharing a notification token.
    static CACHE: RefCell<(Parker, Waker)> = RefCell::new(parker_and_waker());
}

fn with_parker<T>(wait: impl FnOnce(&Parker, &Waker) -> T) -> T {
    CACHE.with(|cache| {
        let cached;
        let fresh;
        let (parker, waker) = match cache.try_borrow_mut() {
            Ok(pair) => {
                cached = pair;
                &*cached
            }
            Err(_) => {
                fresh = parker_and_waker();
                &fresh
            }
        };

        wait(parker, waker)
    })
}
