// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A minimal blocking executor for a single future.
//!
//! This module is opt-in and disabled by default. Enable it with the `block_on` Cargo feature.
//!
//! It exposes [`block_on()`] to run an [`IntoFuture`] to completion, [`block_on_timeout()`] for a
//! deadline-bounded variant, and [`FutureExt`] to call either operation in suffix position:
//!
//! ```
//! use std::time::Duration;
//!
//! use asyncband::block_on::FutureExt as _;
//! use asyncband::block_on::block_on;
//! use asyncband::block_on::block_on_timeout;
//!
//! let value = block_on(async { 42 });
//! assert_eq!(value, 42);
//!
//! let value = async { 42 }.block_on();
//! assert_eq!(value, 42);
//!
//! let value = block_on_timeout(async { 42 }, Duration::from_secs(1));
//! assert_eq!(value, Ok(42));
//!
//! let value = async { 42 }.block_on_timeout(Duration::from_secs(1));
//! assert_eq!(value, Ok(42));
//! ```
//!
//! # Design
//!
//! This is a lightweight single-future executor rather than a general-purpose async runtime. The
//! current thread is parked while the future is pending, and a thread-local waker unparks it when
//! the future becomes ready.
//!
//! The parking loop follows the implementation in
//! [`pollster`](https://github.com/zesterer/pollster/blob/master/src/lib.rs), licensed under
//! MIT OR Apache-2.0. The timeout variant extends that loop with [`thread::park_timeout`].
//!
//! # Runtime-specific futures
//!
//! `block_on` does not provide a timer or I/O driver. Futures that depend on a particular runtime's
//! driver, such as Tokio timers, I/O resources, or `spawn`, may therefore never make progress here.
//! It is intended for runtime-agnostic futures, including the primitives provided by this crate.
//!
//! # Timeouts
//!
//! [`block_on_timeout()`] adds a wall-clock deadline to the outer blocking loop. When the deadline
//! is reached, the future is dropped and [`Timeout`] is returned. This prevents a `block_on` call
//! from waiting forever when a future never wakes, but it is not a timer context: it does not make
//! runtime-specific timers inside the future fire, and it cannot interrupt work that is already
//! running inside a single `poll`.
//!
//! # Executor threads
//!
//! Do not call `block_on` from inside an async executor's task. Blocking an executor thread can
//! starve other tasks and, when the future waits on work that the same executor would otherwise
//! run, can deadlock. Use it only from synchronous code, such as `main`, dedicated threads, or the
//! boundary between synchronous and asynchronous code.
//!
//! [`block_on()`]: fn.block_on.html
//! [`block_on_timeout()`]: fn.block_on_timeout.html
//! [`FutureExt`]: trait.FutureExt.html
//! [`Timeout`]: struct.Timeout.html

use std::fmt;
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

// The parking loop and thread-local waker below follow pollster:
// https://github.com/zesterer/pollster/blob/master/src/lib.rs
// The timeout variant extends the same loop with `thread::park_timeout`.
thread_local! {
    // A reusable waker per thread. It is created on first use and owns a handle to the current
    // thread, so waking it only needs to unpark that thread.
    static LOCAL_WAKER: Waker = {
        let signal = Arc::new(Signal {
            owning_thread: thread::current(),
        });
        Waker::from(signal)
    };
}

/// An extension trait that allows blocking on a future in suffix position.
///
/// # Example
///
/// ```
/// use asyncband::block_on::FutureExt as _;
///
/// let value = async { 42 }.block_on();
/// assert_eq!(value, 42);
/// ```
pub trait FutureExt: IntoFuture {
    /// Blocks the current thread until the future is ready.
    ///
    /// This consumes `self`. See the [module-level documentation](self) for the limitations of
    /// this single-future executor and the deadlock caveats that apply when used on an executor
    /// thread.
    fn block_on(self) -> Self::Output
    where
        Self: Sized,
    {
        block_on(self)
    }

    /// Blocks the current thread until the future is ready or the deadline expires.
    ///
    /// Returns `Ok` with the future's output when it becomes ready in time, or [`Timeout`] when the
    /// deadline is reached first. The future is dropped when the deadline wins.
    ///
    /// The timeout is a wall-clock bound on this blocking call, not a timer context for the future
    /// itself. See [`block_on_timeout()`] for details.
    fn block_on_timeout(self, timeout: Duration) -> Result<Self::Output, Timeout>
    where
        Self: Sized,
    {
        block_on_timeout(self, timeout)
    }
}

impl<F: IntoFuture> FutureExt for F {}

/// The thread that owns a [`Signal`].
struct Signal {
    owning_thread: thread::Thread,
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.owning_thread.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.owning_thread.unpark();
    }
}

/// Blocks the current thread until the future is ready.
///
/// The future is polled in a loop. When it returns [`Poll::Pending`], the thread parks until a
/// waker is triggered by another thread. This is intentionally a single-future executor: it does
/// not schedule tasks, provide timers, or drive I/O.
///
/// # Example
///
/// ```
/// use asyncband::block_on::block_on;
///
/// let value = block_on(async { 42 });
/// assert_eq!(value, 42);
/// ```
pub fn block_on<F: IntoFuture>(fut: F) -> F::Output {
    let mut fut = pin!(fut.into_future());

    LOCAL_WAKER.with(|waker| {
        let mut context = Context::from_waker(waker);

        loop {
            match fut.as_mut().poll(&mut context) {
                Poll::Pending => thread::park(),
                Poll::Ready(item) => break item,
            }
        }
    })
}

/// The error returned when [`block_on_timeout()`] reaches its deadline before the future becomes
/// ready.
///
/// The future is dropped when this error is returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeout;

impl fmt::Display for Timeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("block_on timed out before the future became ready")
    }
}

impl std::error::Error for Timeout {}

/// Blocks the current thread until the future is ready or the deadline expires.
///
/// The future is polled in a loop. When it returns [`Poll::Pending`], the thread parks for the
/// remaining time, or until a waker is triggered. If the deadline is reached first, the future is
/// dropped and [`Timeout`] is returned.
///
/// The deadline is enforced by the blocking loop and is therefore cooperative: it cannot interrupt
/// a single poll that runs for a long time. It also does not provide a timer driver, so it cannot
/// wake runtime-specific timers inside the future.
///
/// # Example
///
/// ```
/// use std::time::Duration;
///
/// use asyncband::block_on::block_on_timeout;
///
/// let value = block_on_timeout(async { 42 }, Duration::from_secs(1));
/// assert_eq!(value, Ok(42));
/// ```
pub fn block_on_timeout<F: IntoFuture>(fut: F, timeout: Duration) -> Result<F::Output, Timeout> {
    let mut fut = pin!(fut.into_future());
    let deadline = Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(Instant::now);

    LOCAL_WAKER.with(|waker| {
        let mut context = Context::from_waker(waker);

        loop {
            match fut.as_mut().poll(&mut context) {
                Poll::Ready(item) => return Ok(item),
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

#[cfg(test)]
mod tests;
