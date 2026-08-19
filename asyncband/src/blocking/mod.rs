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
//! [`block_on()`] waits until a future completes, and [`FutureExt`] provides the same operation in
//! suffix position:
//!
//! ```
//! use asyncband::blocking::FutureExt as _;
//! use asyncband::blocking::block_on;
//!
//! assert_eq!(block_on(async { 42 }), 42);
//! assert_eq!(async { 42 }.block_on(), 42);
//! ```
//!
//! # Execution model
//!
//! [`block_on()`] is a lightweight single-future executor, not a general-purpose async runtime. It
//! polls the future on the calling thread and waits on a signal dedicated to that invocation while
//! the future is pending. The dedicated signal keeps nested calls and unrelated uses of
//! [`thread::park`](std::thread::park) on the same thread from consuming each other's wake-ups.
//!
//! No timer, I/O, or task scheduler is provided. Futures that depend on a runtime-specific driver,
//! such as Tokio timers, I/O resources, or spawned tasks, may therefore make no progress. This
//! module is intended for runtime-agnostic futures, including the primitives provided by this
//! crate.
//!
//! # Executor threads
//!
//! Do not call [`block_on()`] from an async executor task. Blocking an executor thread can starve
//! other tasks and can deadlock when the future waits on work assigned to that executor. Call it
//! only from synchronous code, such as `main`, a dedicated thread, or a sync-to-async boundary.

use std::future::IntoFuture;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

/// Extension methods for waiting on a future from synchronous code.
///
/// # Example
///
/// ```
/// use asyncband::blocking::FutureExt as _;
///
/// assert_eq!(async { 42 }.block_on(), 42);
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
        block_on(self)
    }
}

impl<F: IntoFuture> FutureExt for F {}

// The per-invocation signal is adapted from pollster 0.4.0, licensed under MIT OR Apache-2.0:
// https://docs.rs/pollster/0.4.0/src/pollster/lib.rs.html
struct Parker {
    notified: Mutex<bool>,
    condvar: Condvar,
}

impl Parker {
    fn new() -> Self {
        Self {
            notified: Mutex::new(false),
            condvar: Condvar::new(),
        }
    }

    fn park(&self) {
        let mut notified = self.notified.lock().unwrap();
        while !*notified {
            notified = self.condvar.wait(notified).unwrap();
        }
        *notified = false;
    }

    fn unpark(&self) {
        let should_notify = {
            let mut notified = self.notified.lock().unwrap();
            if *notified {
                false
            } else {
                *notified = true;
                true
            }
        };

        if should_notify {
            self.condvar.notify_one();
        }
    }
}

impl Wake for Parker {
    fn wake(self: Arc<Self>) {
        self.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.unpark();
    }
}

/// Blocks the current thread until `future` is ready.
///
/// The future is polled on the current thread. When it returns [`Poll::Pending`], the thread waits
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
    let parker = Arc::new(Parker::new());
    let waker = Waker::from(Arc::clone(&parker));
    let mut context = Context::from_waker(&waker);

    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Pending => parker.park(),
            Poll::Ready(output) => return output,
        }
    }
}
