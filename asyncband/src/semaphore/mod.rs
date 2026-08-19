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

//! An async counting semaphore for controlling access to a set of resources.
//!
//! A semaphore maintains a set of permits. Permits are used to synchronize access
//! to a pool of resources. Each [`acquire`] call blocks until a permit is available,
//! and then takes one permit. Each [`release`] call adds a new permit, potentially
//! releasing a blocked acquirer.
//!
//! Semaphores are often used to restrict the number of tasks that can access some
//! (physical or logical) resource. For example, here is a class that uses a
//! semaphore to control access to a pool of connections:
//!
//! # Examples
//!
//! ## Basic usage
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use asyncband::semaphore::Semaphore;
//!
//! let semaphore = Semaphore::new(3);
//! let a_permit = semaphore.acquire(1).await;
//! let two_permits = semaphore.acquire(2).await;
//!
//! assert_eq!(semaphore.available_permits(), 0);
//!
//! let permit_attempt = semaphore.try_acquire(1);
//! assert!(permit_attempt.is_none());
//! # }
//! ```
//!
//! ## Limit the number of simultaneously opened files in your program
//!
//! Most operating systems have limits on the number of open file
//! handles. Even in systems without explicit limits, resource constraints
//! implicitly set an upper bound on the number of open files. If your
//! program attempts to open a large number of files and exceeds this
//! limit, it will result in an error.
//!
//! This example uses a Semaphore with 100 permits. By acquiring a permit from
//! the Semaphore before accessing a file, you ensure that your program opens
//! no more than 100 files at a time. When trying to open the 101st
//! file, the program will wait until a permit becomes available before
//! proceeding to open another file.
//!
//! ```
//! use std::fs::File;
//! use std::io::Result;
//! use std::io::Write;
//! use std::sync::LazyLock;
//!
//! use asyncband::semaphore::Semaphore;
//!
//! static PERMITS: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(100));
//!
//! async fn write_to_file(message: &[u8]) -> Result<()> {
//!     let _permit = PERMITS.acquire(1).await;
//!     let mut buffer = File::create("example.txt")?;
//!     buffer.write_all(message)?;
//!     Ok(()) // Permit goes out of scope here, and is available again for acquisition
//! }
//! ```
//!
//! [`acquire`]: Semaphore::acquire
//! [`release`]: Semaphore::release

use std::sync::Arc;

use crate::internal;

#[cfg(test)]
mod tests;

/// An async counting semaphore for controlling access to a set of resources.
///
/// See the [module level documentation](self) for more.
#[derive(Debug)]
pub struct Semaphore {
    s: internal::Semaphore,
}

impl Semaphore {
    /// Creates a new semaphore with the given number of permits.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Semaphore::new(5); // Creates a semaphore with 5 permits
    /// ```
    pub const fn new(permits: usize) -> Self {
        Self {
            s: internal::Semaphore::new(permits),
        }
    }

    /// Returns the current number of permits available.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Semaphore::new(2);
    /// assert_eq!(sem.available_permits(), 2);
    ///
    /// let permit = sem.try_acquire(1).unwrap();
    /// assert_eq!(sem.available_permits(), 1);
    /// ```
    pub fn available_permits(&self) -> usize {
        self.s.available_permits()
    }

    /// Reduces the semaphore's permits by a maximum of `n`.
    ///
    /// Returns the actual number of permits that were reduced. This may be less
    /// than `n` if there are insufficient permits available.
    ///
    /// This is useful when you want to permanently remove permits from the semaphore.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Semaphore::new(5);
    /// assert_eq!(sem.forget(3), 3); // Removes 3 permits
    /// assert_eq!(sem.available_permits(), 2);
    ///
    /// // Trying to forget more permits than available
    /// assert_eq!(sem.forget(3), 2); // Only removes remaining 2 permits
    /// assert_eq!(sem.available_permits(), 0);
    /// ```
    pub fn forget(&self, n: usize) -> usize {
        self.s.forget(n)
    }

    /// Reduces the semaphore's permits by exactly `n`.
    ///
    /// If the semaphore has not enough permits, this would enqueue front an empty waiter to
    /// consume the permits, which ensures the permits are reduced by exactly `n`.
    ///
    /// This is useful when you want to permanently remove permits from the semaphore.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Semaphore::new(5);
    /// sem.forget_exact(3); // Removes 3 permits
    /// assert_eq!(sem.available_permits(), 2);
    ///
    /// // Trying to forget more permits than available
    /// sem.forget_exact(3); // Only removes remaining 2 permits
    /// assert_eq!(sem.available_permits(), 0);
    ///
    /// sem.release(5); // Adds 5 permits
    /// assert_eq!(sem.available_permits(), 4); // Only 4 permits are available
    /// ```
    pub fn forget_exact(&self, n: usize) {
        self.s.forget_exact(n);
    }

    /// Adds `n` new permits to the semaphore.
    ///
    /// # Panics
    ///
    /// Panics if adding the permits would cause the total number of permits to overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Semaphore::new(0);
    /// sem.release(2); // Adds 2 permits
    /// assert_eq!(sem.available_permits(), 2);
    /// ```
    pub fn release(&self, permits: usize) {
        self.s.release(permits);
    }

    /// Attempts to acquire `n` permits from the semaphore without blocking.
    ///
    /// If the permits are successfully acquired, a [`SemaphorePermit`] is returned.
    /// The permits will be automatically returned to the semaphore when the permit
    /// is dropped, unless [`forget`] is called.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Semaphore::new(2);
    ///
    /// // First acquisition succeeds
    /// let permit1 = sem.try_acquire(1).unwrap();
    /// assert_eq!(sem.available_permits(), 1);
    ///
    /// // Second acquisition succeeds
    /// let permit2 = sem.try_acquire(1).unwrap();
    /// assert_eq!(sem.available_permits(), 0);
    ///
    /// // Third acquisition fails
    /// assert!(sem.try_acquire(1).is_none());
    /// ```
    ///
    /// [`forget`]: SemaphorePermit::forget
    pub fn try_acquire(&self, permits: usize) -> Option<SemaphorePermit<'_>> {
        if self.s.try_acquire(permits) {
            Some(SemaphorePermit { sem: self, permits })
        } else {
            None
        }
    }

    /// Attempts to acquire `n` permits from the semaphore without blocking.
    ///
    /// This method performs as a combinator of [`Semaphore::try_acquire`] and
    /// [`Semaphore::forget`].
    pub fn try_acquire_and_forget(&self, permits: usize) -> bool {
        self.s.try_acquire(permits)
    }

    /// Acquires `n` permits from the semaphore.
    ///
    /// If the permits are not immediately available, this method will wait until they become
    /// available. Returns a [`SemaphorePermit`] that will release the permits when dropped.
    ///
    /// # Cancel safety
    ///
    /// This method uses a queue to fairly distribute permits in the order they were requested.
    /// Cancelling a call to `acquire` makes you lose your place in the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Arc::new(Semaphore::new(2));
    /// let sem2 = sem.clone();
    ///
    /// let handle = tokio::spawn(async move {
    ///     let permit = sem2.acquire(1).await;
    ///     // Do some work with the permit.
    ///     // Permit is automatically released when dropped.
    /// });
    ///
    /// let permit = sem.acquire(1).await;
    /// // Do some work with the permit
    /// drop(permit); // Explicitly release the permit
    ///
    /// handle.await.unwrap();
    /// # }
    /// ```
    pub async fn acquire(&self, permits: usize) -> SemaphorePermit<'_> {
        self.s.acquire(permits).await;
        SemaphorePermit { sem: self, permits }
    }

    /// Acquires `n` permits from the semaphore.
    ///
    /// This method performs as a combinator of [`Semaphore::acquire`] and
    /// [`Semaphore::forget`].
    pub async fn acquire_and_forget(&self, permits: usize) {
        self.s.acquire(permits).await;
    }

    /// Attempts to acquire `n` permits from the semaphore without blocking.
    ///
    /// The semaphore must be wrapped in an [`Arc`] to call this method.
    ///
    /// If the permits are successfully acquired, a [`OwnedSemaphorePermit`] is returned.
    /// The permits will be automatically returned to the semaphore when the permit
    /// is dropped, unless [`forget`] is called.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Arc::new(Semaphore::new(2));
    ///
    /// let p1 = sem.clone().try_acquire_owned(1).unwrap();
    /// assert_eq!(sem.available_permits(), 1);
    ///
    /// let p2 = sem.clone().try_acquire_owned(1).unwrap();
    /// assert_eq!(sem.available_permits(), 0);
    ///
    /// let p3 = sem.try_acquire_owned(1);
    /// assert!(p3.is_none());
    /// ```
    ///
    /// [`forget`]: SemaphorePermit::forget
    pub fn try_acquire_owned(self: Arc<Self>, permits: usize) -> Option<OwnedSemaphorePermit> {
        if self.s.try_acquire(permits) {
            Some(OwnedSemaphorePermit { sem: self, permits })
        } else {
            None
        }
    }

    /// Attempts to acquire `n` permits from the semaphore without blocking.
    ///
    /// The semaphore must be wrapped in an [`Arc`] to call this method.
    ///
    /// This method performs as a combinator of [`Semaphore::try_acquire_owned`] and
    /// [`Semaphore::forget`].
    pub fn try_acquire_owned_and_forget(self: Arc<Self>, permits: usize) -> bool {
        self.s.try_acquire(permits)
    }

    /// Acquires `n` permits from the semaphore.
    ///
    /// The semaphore must be wrapped in an [`Arc`] to call this method.
    ///
    /// If the permits are not immediately available, this method will wait until they become
    /// available. Returns a [`OwnedSemaphorePermit`] that will release the permits when dropped.
    ///
    /// # Cancel safety
    ///
    /// This method uses a queue to fairly distribute permits in the order they were requested.
    /// Cancelling a call to `acquire_owned` makes you lose your place in the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Arc::new(Semaphore::new(3));
    /// let mut join_handles = Vec::new();
    ///
    /// for _ in 0..5 {
    ///     let permit = sem.clone().acquire_owned(1).await;
    ///     join_handles.push(tokio::spawn(async move {
    ///         // perform task...
    ///         // explicitly own `permit` in the task
    ///         drop(permit);
    ///     }));
    /// }
    ///
    /// for handle in join_handles {
    ///     handle.await.unwrap();
    /// }
    /// # }
    /// ```
    pub async fn acquire_owned(self: Arc<Self>, permits: usize) -> OwnedSemaphorePermit {
        self.s.acquire(permits).await;
        OwnedSemaphorePermit { sem: self, permits }
    }

    /// Acquires `n` permits from the semaphore.
    ///
    /// The semaphore must be wrapped in an [`Arc`] to call this method.
    ///
    /// This method performs as a combinator of [`Semaphore::acquire_owned`] and
    /// [`Semaphore::forget`].
    pub async fn acquire_owned_and_forget(self: Arc<Self>, permits: usize) {
        self.s.acquire(permits).await;
    }
}

/// A permit from the semaphore.
///
/// This type is created by the [`acquire`] and [`try_acquire`] methods on [`Semaphore`].
/// When the permit is dropped, the permits will be returned to the semaphore unless
/// [`forget`] is called.
///
/// [`acquire`]: Semaphore::acquire
/// [`try_acquire`]: Semaphore::try_acquire
/// [`forget`]: SemaphorePermit::forget
#[must_use = "permits are released immediately when dropped"]
#[derive(Debug)]
pub struct SemaphorePermit<'a> {
    sem: &'a Semaphore,
    permits: usize,
}

impl SemaphorePermit<'_> {
    /// Forgets the permit **without** releasing it back to the semaphore.
    ///
    /// This can be used to permanently reduce the number of permits available
    /// from a semaphore.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Arc::new(Semaphore::new(10));
    /// {
    ///     let permit = sem.try_acquire(5).unwrap();
    ///     assert_eq!(sem.available_permits(), 5);
    ///     permit.forget();
    /// }
    ///
    /// // Since we forgot the permit, available permits won't go back to
    /// // its initial value even after the permit is dropped
    /// assert_eq!(sem.available_permits(), 5);
    /// ```
    pub fn forget(mut self) {
        self.permits = 0;
    }

    /// Merge two [`SemaphorePermit`] instances together, consuming `other`
    /// without releasing the permits it holds.
    ///
    /// Permits held by both `self` and `other` are released when `self` drops.
    ///
    /// # Panics
    ///
    /// This function panics if permits from different [`Semaphore`] instances
    /// are merged.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Arc::new(Semaphore::new(10));
    /// let mut permit = sem.try_acquire(1).unwrap();
    ///
    /// for _ in 0..9 {
    ///     let new_permit = sem.try_acquire(1).unwrap();
    ///     // Merge individual permits into a single one.
    ///     permit.merge(new_permit)
    /// }
    ///
    /// assert_eq!(sem.available_permits(), 0);
    ///
    /// // Release all permits in a single batch.
    /// drop(permit);
    ///
    /// assert_eq!(sem.available_permits(), 10);
    /// ```
    #[track_caller]
    pub fn merge(&mut self, mut other: Self) {
        assert!(
            std::ptr::eq(self.sem, other.sem),
            "merging permits from different semaphore instances"
        );
        self.permits += other.permits;
        other.permits = 0;
    }

    /// Splits `n` permits from `self` and returns a new [`SemaphorePermit`] instance that holds `n`
    /// permits.
    ///
    /// If there are insufficient permits, and it is impossible to reduce by `n`, returns `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Arc::new(Semaphore::new(3));
    ///
    /// let mut p1 = sem.try_acquire(3).unwrap();
    /// let p2 = p1.split(1).unwrap();
    ///
    /// assert_eq!(p1.permits(), 2);
    /// assert_eq!(p2.permits(), 1);
    /// ```
    pub fn split(&mut self, n: usize) -> Option<Self> {
        if n > self.permits {
            return None;
        }

        self.permits -= n;

        Some(Self {
            sem: self.sem,
            permits: n,
        })
    }

    /// Returns the number of permits this permit holds.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Semaphore::new(5);
    /// let permit = sem.try_acquire(3).unwrap();
    /// assert_eq!(permit.permits(), 3);
    /// ```
    pub fn permits(&self) -> usize {
        self.permits
    }
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        self.sem.release(self.permits);
    }
}

/// An owned permit from the semaphore.
///
/// This type is created by the [`acquire_owned`] method.
///
/// [`acquire_owned`]: Semaphore::acquire_owned
#[must_use = "permits are released immediately when dropped"]
#[derive(Debug)]
pub struct OwnedSemaphorePermit {
    sem: Arc<Semaphore>,
    permits: usize,
}

impl OwnedSemaphorePermit {
    /// Forgets the permit **without** releasing it back to the semaphore.
    ///
    /// This can be used to permanently reduce the number of permits available
    /// from a semaphore.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Arc::new(Semaphore::new(10));
    /// {
    ///     let permit = sem.clone().try_acquire_owned(5).unwrap();
    ///     assert_eq!(sem.available_permits(), 5);
    ///     permit.forget();
    /// }
    ///
    /// // Since we forgot the permit, available permits won't go back to
    /// // its initial value even after the permit is dropped
    /// assert_eq!(sem.available_permits(), 5);
    /// ```
    pub fn forget(mut self) {
        self.permits = 0;
    }

    /// Merge two [`SemaphorePermit`] instances together, consuming `other`
    /// without releasing the permits it holds.
    ///
    /// Permits held by both `self` and `other` are released when `self` drops.
    ///
    /// # Panics
    ///
    /// This function panics if permits from different [`Semaphore`] instances
    /// are merged.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Arc::new(Semaphore::new(10));
    /// let mut permit = sem.try_acquire(1).unwrap();
    ///
    /// for _ in 0..9 {
    ///     let new_permit = sem.try_acquire(1).unwrap();
    ///     // Merge individual permits into a single one.
    ///     permit.merge(new_permit)
    /// }
    ///
    /// assert_eq!(sem.available_permits(), 0);
    ///
    /// // Release all permits in a single batch.
    /// drop(permit);
    ///
    /// assert_eq!(sem.available_permits(), 10);
    /// ```
    #[track_caller]
    pub fn merge(&mut self, mut other: Self) {
        assert!(
            Arc::ptr_eq(&self.sem, &other.sem),
            "merging permits from different semaphore instances"
        );
        self.permits += other.permits;
        other.permits = 0;
    }

    /// Splits `n` permits from `self` and returns a new [`OwnedSemaphorePermit`] instance that
    /// holds `n` permits.
    ///
    /// If there are insufficient permits, and it is impossible to reduce by `n`, returns `None`.
    ///
    /// # Note
    ///
    /// It will clone the owned `Arc<Semaphore>` to construct the new instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Arc::new(Semaphore::new(3));
    ///
    /// let mut p1 = sem.try_acquire_owned(3).unwrap();
    /// let p2 = p1.split(1).unwrap();
    ///
    /// assert_eq!(p1.permits(), 2);
    /// assert_eq!(p2.permits(), 1);
    /// ```
    pub fn split(&mut self, n: usize) -> Option<Self> {
        if n > self.permits {
            return None;
        }

        self.permits -= n;

        Some(Self {
            sem: self.sem.clone(),
            permits: n,
        })
    }

    /// Returns the number of permits this permit holds.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Semaphore::new(5);
    /// let permit = sem.try_acquire(3).unwrap();
    /// assert_eq!(permit.permits(), 3);
    /// ```
    pub fn permits(&self) -> usize {
        self.permits
    }
}

impl Drop for OwnedSemaphorePermit {
    fn drop(&mut self) {
        self.sem.release(self.permits);
    }
}
