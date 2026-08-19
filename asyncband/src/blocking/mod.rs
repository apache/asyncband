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
//! [`block_on()`] waits until a future completes. [`block_on_for()`] bounds how long the outer
//! blocking loop waits, and [`BlockingExt`] provides both operations in suffix position:
//!
//! ```
//! use std::time::Duration;
//!
//! use asyncband::blocking::BlockingExt as _;
//! use asyncband::blocking::block_on;
//! use asyncband::blocking::block_on_for;
//!
//! assert_eq!(block_on(async { 42 }), 42);
//! assert_eq!(async { 42 }.block_on(), 42);
//! assert_eq!(block_on_for(async { 42 }, Duration::from_secs(1)), Ok(42));
//! assert_eq!(async { 42 }.block_on_for(Duration::from_secs(1)), Ok(42));
//! ```
//!
//! # Execution model
//!
//! These functions form a lightweight single-future executor, not a general-purpose async
//! runtime. They poll the future on the calling thread and park that thread while the future is
//! pending. A wake notification unparks it for another poll.
//!
//! No timer, I/O, or task scheduler is provided. Futures that depend on a runtime-specific driver,
//! such as Tokio timers, I/O resources, or spawned tasks, may therefore make no progress. This
//! module is intended for runtime-agnostic futures, including the primitives provided by this
//! crate.
//!
//! # Bounded waits
//!
//! [`block_on_for()`] limits the outer blocking loop and drops the future when the wait expires.
//! The future is always polled at least once, so an already-ready future succeeds even when the
//! maximum wait is [`Duration::ZERO`].
//!
//! This is not an asynchronous timeout or timer context. The limit is cooperative: it cannot
//! interrupt work already running inside one call to [`Future::poll`], and it does not make timers
//! inside the future advance. A duration too large to represent as an [`Instant`] deadline is
//! treated as having no practical deadline.
//!
//! # Executor threads
//!
//! Do not call these functions from an async executor task. Blocking an executor thread can starve
//! other tasks and can deadlock when the future waits on work assigned to that executor. Call them
//! only from synchronous code, such as `main`, a dedicated thread, or a sync-to-async boundary.

use std::fmt;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;
use std::time::Instant;

// The single-future parking loop is adapted from pollster 1.0.1, licensed under MIT OR
// Apache-2.0: https://docs.rs/pollster/1.0.1/src/pollster/lib.rs.html
thread_local! {
    static LOCAL_WAKER: Waker = Waker::from(Arc::new(ThreadSignal(thread::current())));
}

/// Extension methods for waiting on a future from synchronous code.
///
/// # Example
///
/// ```
/// use asyncband::blocking::BlockingExt as _;
///
/// assert_eq!(async { 42 }.block_on(), 42);
/// ```
pub trait BlockingExt: IntoFuture {
    /// Blocks the current thread until this future is ready.
    ///
    /// See the [`blocking`](crate::blocking) module documentation for runtime limitations and
    /// executor-thread caveats.
    fn block_on(self) -> Self::Output
    where
        Self: Sized,
    {
        block_on(self)
    }

    /// Blocks the current thread until this future is ready or `max_wait` expires.
    ///
    /// The future is dropped when the wait expires. This method bounds the outer blocking loop; it
    /// is not an asynchronous timer and cannot interrupt a call to [`Future::poll`].
    fn block_on_for(self, max_wait: Duration) -> Result<Self::Output, Timeout>
    where
        Self: Sized,
    {
        block_on_for(self, max_wait)
    }
}

impl<F: IntoFuture> BlockingExt for F {}

struct ThreadSignal(thread::Thread);

impl Wake for ThreadSignal {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Blocks the current thread until `future` is ready.
///
/// The future is polled on the current thread. When it returns [`Poll::Pending`], the thread parks
/// until its waker is notified. This function does not schedule tasks, provide timers, or drive
/// I/O.
///
/// # Example
///
/// ```
/// use asyncband::blocking::block_on;
///
/// assert_eq!(block_on(async { 42 }), 42);
/// ```
pub fn block_on<F: IntoFuture>(future: F) -> F::Output {
    let mut future = pin!(future.into_future());

    LOCAL_WAKER.with(|waker| {
        let mut context = Context::from_waker(waker);

        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Pending => thread::park(),
                Poll::Ready(output) => return output,
            }
        }
    })
}

/// The error returned when [`block_on_for()`] exhausts its maximum wait.
///
/// The future is dropped when this error is returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeout;

impl fmt::Display for Timeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("future did not become ready within the blocking wait")
    }
}

impl std::error::Error for Timeout {}

/// Blocks the current thread until `future` is ready or `max_wait` expires.
///
/// The future is always polled once. After a pending result, the current thread parks for the
/// remaining wait or until the future's waker is notified. If the wait expires while the future is
/// still pending, the future is dropped and [`Timeout`] is returned.
///
/// This function bounds the outer blocking loop. It cannot interrupt a long-running call to
/// [`Future::poll`] and does not provide a timer driver for the future. Durations too large to form
/// an [`Instant`] deadline are treated as having no practical deadline.
///
/// # Example
///
/// ```
/// use std::time::Duration;
///
/// use asyncband::blocking::block_on_for;
///
/// assert_eq!(block_on_for(async { 42 }, Duration::from_secs(1)), Ok(42));
/// ```
pub fn block_on_for<F: IntoFuture>(future: F, max_wait: Duration) -> Result<F::Output, Timeout> {
    let Some(deadline) = Instant::now().checked_add(max_wait) else {
        return Ok(block_on(future));
    };
    let mut future = pin!(future.into_future());

    LOCAL_WAKER.with(|waker| {
        let mut context = Context::from_waker(waker);

        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return Ok(output),
                Poll::Pending => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(Timeout);
                    }
                    thread::park_timeout(deadline - now);
                }
            }
        }
    })
}
