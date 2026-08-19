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
//! polls the future on the calling thread and waits on a private parker while the future is
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
//! Do not call [`block_on()`] from an async executor task. Blocking an executor thread can starve
//! other tasks and can deadlock when the future waits on work assigned to that executor. Call it
//! only from synchronous code, such as `main`, a dedicated thread, or a sync-to-async boundary.

use std::cell::RefCell;
use std::future::IntoFuture;
use std::pin::pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use parking::Parker;

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

fn parker_and_waker() -> (Parker, Waker) {
    let parker = Parker::new();
    let waker = Waker::from(parker.unparker());
    (parker, waker)
}

thread_local! {
    // This cache follows futures-lite's block_on design. Holding the mutable borrow while polling
    // makes a recursive call take the fresh-parker path instead of sharing a notification token.
    static CACHE: RefCell<(Parker, Waker)> = RefCell::new(parker_and_waker());
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
        let mut context = Context::from_waker(waker);

        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Pending => parker.park(),
                Poll::Ready(output) => return output,
            }
        }
    })
}
