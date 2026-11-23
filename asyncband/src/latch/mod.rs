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

//! Wait for a one-way countdown to reach zero.
//!
//! A [`Latch`] starts with a fixed count. [`Latch::count_down`] decrements it by one and
//! [`Latch::arrive`] decrements it by an arbitrary amount. Both operations saturate at zero. Once
//! zero is reached, all current and future waits complete immediately; a latch cannot be reset or
//! reused for another countdown.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use std::sync::Arc;
//!
//! use asyncband::latch::Latch;
//!
//! let latch = Arc::new(Latch::new(3));
//! let mut tasks = vec![];
//!
//! for _ in 0..3 {
//!     let latch = latch.clone();
//!     let task = tokio::spawn(async move { latch.count_down() });
//!     tasks.push(task);
//! }
//!
//! latch.wait().await;
//! for task in tasks {
//!     task.await.unwrap();
//! }
//! assert_eq!(latch.count(), 0);
//! # }
//! ```

use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use crate::internal::mutex::Mutex;
use crate::internal::wakerset::WakerSet;
use crate::internal::wakerset::WakerToken;

/// A one-shot countdown that can wake any number of waiting tasks.
///
/// See the [module level documentation](self) for more.
#[derive(Debug)]
pub struct Latch {
    state: AtomicU32,
    waiters: Mutex<WakerSet>,
}

impl Latch {
    /// Creates a new latch initialized with the given count.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::latch::Latch;
    ///
    /// let latch = Latch::new(3);
    /// ```
    pub const fn new(count: u32) -> Self {
        Self {
            state: AtomicU32::new(count),
            waiters: Mutex::new(WakerSet::new()),
        }
    }

    /// Returns the current count.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::latch::Latch;
    ///
    /// let latch = Latch::new(5);
    /// assert_eq!(latch.count(), 5);
    /// ```
    pub fn count(&self) -> u32 {
        self.state.load(Ordering::Acquire)
    }

    /// Decrements the latch count by one, waking up all pending tasks if the counter reaches zero.
    ///
    /// If the current count is zero, this method has no effect.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::latch::Latch;
    ///
    /// let latch = Latch::new(2);
    /// latch.count_down();
    /// assert_eq!(latch.count(), 1);
    /// ```
    pub fn count_down(&self) {
        self.arrive(1);
    }

    /// Decrements the latch count by `n`, waking up all waiting tasks if the counter reaches zero.
    ///
    /// The count saturates at zero. Passing zero or calling this after the latch has completed has
    /// no effect.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::latch::Latch;
    ///
    /// let latch = Latch::new(5);
    /// latch.arrive(3);
    /// assert_eq!(latch.count(), 2);
    /// latch.arrive(10);
    /// assert_eq!(latch.count(), 0);
    /// ```
    pub fn arrive(&self, n: u32) {
        if n == 0 {
            return;
        }
        let mut cnt = self.count();
        loop {
            if cnt == 0 {
                return;
            }

            let new_cnt = cnt.saturating_sub(n);
            match self.state.compare_exchange_weak(
                cnt,
                new_cnt,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if new_cnt == 0 {
                        let wakers = mem::take(&mut *self.waiters.lock());
                        wakers.wake_all();
                    }
                    return;
                }
                Err(observed) => cnt = observed,
            }
        }
    }

    /// Checks whether the countdown has reached zero without waiting.
    ///
    /// # Errors
    ///
    /// Returns `Err(count)` with the observed nonzero count if the latch is not complete. The
    /// count may decrease immediately after this observation.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::latch::Latch;
    ///
    /// let latch = Latch::new(2);
    /// assert_eq!(latch.try_wait(), Err(2));
    /// latch.count_down();
    /// assert_eq!(latch.try_wait(), Err(1));
    /// latch.count_down();
    /// assert_eq!(latch.try_wait(), Ok(()));
    /// ```
    pub fn try_wait(&self) -> Result<(), u32> {
        match self.count() {
            0 => Ok(()),
            count => Err(count),
        }
    }

    /// Waits until the countdown reaches zero.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending future unregisters only this wait. It does not change the countdown or
    /// affect other waiters.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::latch::Latch;
    ///
    /// let latch = Latch::new(1);
    /// latch.count_down();
    /// latch.wait().await;
    /// # }
    /// ```
    pub async fn wait(&self) {
        LatchWait {
            latch: self,
            token: None,
        }
        .await;
    }

    /// Waits until the countdown reaches zero while owning this [`Arc`].
    ///
    /// The future keeps the latch alive without borrowing the caller's handle.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending future unregisters only this wait and releases its `Arc`. It does not
    /// change the countdown or affect other waiters.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::latch::Latch;
    ///
    /// let latch = Arc::new(Latch::new(1));
    /// let waiter = tokio::spawn(latch.clone().wait_owned());
    /// latch.count_down();
    /// waiter.await.unwrap();
    /// # }
    /// ```
    pub async fn wait_owned(self: Arc<Self>) {
        self.wait().await;
    }
}

struct LatchWait<'a> {
    latch: &'a Latch,
    token: Option<WakerToken>,
}

impl Future for LatchWait<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let Self { latch, token } = self.get_mut();
        if latch.try_wait().is_ok() {
            // The zero transition owns detaching every registration. Avoid taking the waiter lock
            // again when the completed future is dropped.
            *token = None;
            return Poll::Ready(());
        }

        let mut waiters = latch.waiters.lock();
        if latch.count() == 0 {
            // A concurrent zero transition detaches the registrations under this same lock.
            *token = None;
            return Poll::Ready(());
        }

        let retired_waker = waiters.register(token, cx.waker());
        drop(waiters);
        drop(retired_waker);
        Poll::Pending
    }
}

impl Drop for LatchWait<'_> {
    fn drop(&mut self) {
        if self.token.is_none() {
            return;
        }

        let mut waiters = self.latch.waiters.lock();
        if self.latch.count() == 0 {
            // The terminal zero transition owns this registration or has already detached it.
            return;
        }

        let removed_waker = waiters.unregister(&mut self.token);
        drop(waiters);
        drop(removed_waker);
    }
}
