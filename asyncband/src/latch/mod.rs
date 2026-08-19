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

//! A countdown latch that allows one or more tasks to wait until a set of operations completes.
//!
//! Unlike a barrier, a latch's count can only decrease and cannot be reused once it reaches zero.
//! This makes it ideal for scenarios where you need to wait for a specific number of events or
//! operations to complete.
//!
//! A latch starts with an initial count and tasks can wait for this count to reach zero.
//! The count can be decremented by calling [`count_down()`] or [`arrive()`]. Once the count
//! reaches zero, all waiting tasks are unblocked.
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
//! let mut handles = Vec::new();
//!
//! for i in 0..3 {
//!     let latch = latch.clone();
//!     handles.push(tokio::spawn(async move {
//!         println!("Task {} starting", i);
//!         // Simulate some work
//!         latch.count_down(); // Signal completion
//!     }));
//! }
//!
//! // Wait for all tasks to complete
//! latch.wait().await;
//! println!("All tasks completed");
//! # }
//! ```
//!
//! [`count_down()`]: Latch::count_down
//! [`arrive()`]: Latch::arrive

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::internal::CountdownState;
use crate::internal::WaitRegistration;

/// A synchronization primitive that can be used to coordinate multiple tasks.
///
/// See the [module level documentation](self) for more.
#[derive(Debug)]
pub struct Latch {
    state: CountdownState,
}

impl Latch {
    /// Creates a new latch initialized with the given count.
    ///
    /// # Arguments
    ///
    /// * `count` - The initial count value. Tasks will wait until this count reaches zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::latch::Latch;
    ///
    /// let latch = Latch::new(3); // Creates a latch with count of 3
    /// ```
    pub fn new(count: u32) -> Self {
        Self {
            state: CountdownState::new(count),
        }
    }

    /// Returns the current count.
    ///
    /// This method is typically used for debugging and testing purposes.
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
        self.state.state()
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
    /// latch.count_down(); // Count is now 1
    /// latch.count_down(); // Count is now 0, all waiting tasks are woken
    /// ```
    pub fn count_down(&self) {
        if self.state.decrement(1) {
            self.state.wake_all();
        }
    }

    /// Decrements the latch count by `n`, waking up all waiting tasks if the counter reaches zero.
    ///
    /// This method provides a way to decrement the counter by more than one at a time.
    /// It will not cause an overflow when decrementing the counter.
    ///
    /// # Arguments
    ///
    /// * `n` - The amount to decrement the counter by
    ///
    /// # Behavior
    ///
    /// * If `n` is zero or the counter has already reached zero, nothing happens
    /// * If the current count is greater than `n`, it is decremented by `n`
    /// * If the current count is greater than 0 but less than or equal to `n`, the count becomes
    ///   zero and all waiting tasks are woken
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::latch::Latch;
    ///
    /// let latch = Latch::new(5);
    /// latch.arrive(3); // Count is now 2
    /// latch.arrive(2); // Count is now 0, all waiting tasks are woken
    /// ```
    pub fn arrive(&self, n: u32) {
        if n != 0 && self.state.decrement(n) {
            self.state.wake_all();
        }
    }

    /// Attempts to wait for the latch count to reach zero without blocking.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the count is zero
    /// * `Err(count)` if the count is not zero, where `count` is the current count
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
        self.state.spin_wait(0)
    }

    /// Returns a future that will complete when the latch count reaches zero.
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
    /// let latch2 = latch.clone();
    ///
    /// // Spawn a task that will wait for the latch
    /// let handle = tokio::spawn(async move {
    ///     latch2.wait().await;
    ///     println!("Latch reached zero!");
    /// });
    ///
    /// // Count down the latch
    /// latch.count_down();
    /// handle.await.unwrap();
    /// # }
    /// ```
    pub async fn wait(&self) {
        let fut = LatchWait {
            registration: None,
            latch: self,
        };
        fut.await
    }

    /// Returns a future that will complete when the latch count reaches zero.
    ///
    /// The latch must be wrapped in an [`Arc`] to call this method. Thus, the returned future has
    /// no lifetime constraints.
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
    /// let latch2 = latch.clone();
    ///
    /// // Spawn a task that will wait for the latch
    /// let handle = tokio::spawn(async move {
    ///     latch2.wait_owned().await;
    ///     println!("Latch reached zero!");
    /// });
    ///
    /// // Count down the latch
    /// latch.count_down();
    /// handle.await.unwrap();
    /// # }
    /// ```
    pub async fn wait_owned(self: Arc<Self>) {
        let fut = OwnedLatchWait {
            registration: None,
            latch: self,
        };
        fut.await
    }
}

impl Latch {
    fn intern_poll(
        &self,
        registration: &mut Option<WaitRegistration>,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        self.state.poll_wait(registration, cx)
    }
}

/// A wait future returned by [`Latch::wait()`].
///
/// This future will complete when the latch count reaches zero.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct LatchWait<'a> {
    registration: Option<WaitRegistration>,
    latch: &'a Latch,
}

impl fmt::Debug for LatchWait<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LatchWait").finish_non_exhaustive()
    }
}

impl Future for LatchWait<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self {
            registration,
            latch,
        } = self.get_mut();
        latch.intern_poll(registration, cx)
    }
}

impl Drop for LatchWait<'_> {
    fn drop(&mut self) {
        if self.registration.is_some() {
            self.latch.state.unregister_waker(&mut self.registration);
        }
    }
}

/// An owned wait future returned by [`Latch::wait()`].
///
/// This future will complete when the latch count reaches zero.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct OwnedLatchWait {
    registration: Option<WaitRegistration>,
    latch: Arc<Latch>,
}

impl fmt::Debug for OwnedLatchWait {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedLatchWait").finish_non_exhaustive()
    }
}

impl Future for OwnedLatchWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self {
            registration,
            latch,
        } = self.get_mut();
        latch.intern_poll(registration, cx)
    }
}

impl Drop for OwnedLatchWait {
    fn drop(&mut self) {
        if self.registration.is_some() {
            self.latch.state.unregister_waker(&mut self.registration);
        }
    }
}
