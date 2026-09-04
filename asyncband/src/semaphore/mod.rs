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

// Portions of this file originated from Tokio 1.42.0's Semaphore and permit APIs.
// Copyright (c) Tokio Contributors
// The Tokio-derived portions remain licensed under the MIT License.
// Asyncband substantially changed the contract: it has no closed state or close errors, accepts
// usize permit counts without reserved flag bits, and adds direct drain and exact-reduction
// operations backed by Asyncband's own waiter queue.
// Upstream source:
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/semaphore.rs

//! Limit concurrent access with a set of permits.
//!
//! [`Semaphore::acquire`] waits for the requested number of permits and returns a guard that puts
//! them back when dropped. [`Semaphore::try_acquire`] performs the same operation without waiting,
//! while [`Semaphore::release`] adds permits that were not represented by a guard.
//!
//! # Examples
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
//!
//! drop(a_permit);
//! assert_eq!(semaphore.available_permits(), 1);
//! # }
//! ```

use std::sync::Arc;

use crate::internal::semaphore;

#[cfg(test)]
mod tests;

/// An async counting semaphore for controlling access to a set of resources.
///
/// See the [module level documentation](self) for more.
#[derive(Debug)]
pub struct Semaphore {
    s: semaphore::Semaphore,
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
            s: semaphore::Semaphore::new(permits),
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

    /// Atomically drains up to `up_to` permits that are currently available.
    ///
    /// Returns the number of permits actually drained, which may be less than `up_to`.
    /// This operation neither creates nor cancels a deficit against future releases and does not
    /// affect permits that have already been acquired.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Semaphore::new(4);
    /// let held = sem.try_acquire(2).unwrap();
    ///
    /// assert_eq!(sem.drain_permits(1), 1);
    /// assert_eq!(sem.available_permits(), 1);
    /// assert_eq!(sem.drain_permits(3), 1);
    /// assert_eq!(sem.available_permits(), 0);
    ///
    /// drop(held);
    /// assert_eq!(sem.available_permits(), 2);
    /// ```
    #[must_use = "`drain_permits` may drain fewer permits than requested"]
    pub fn drain_permits(&self, up_to: usize) -> usize {
        self.s.drain_permits(up_to)
    }

    /// Reduces the semaphore's logical permit balance by exactly `n`.
    ///
    /// This method returns immediately. If fewer than `n` permits are currently available, future
    /// releases repay the resulting deficit before ordinary queued acquisitions may proceed.
    /// Permits that have already been acquired remain valid and are not revoked.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Semaphore::new(1);
    /// sem.reduce_permits(3); // Consumes 1 available permit and records a deficit of 2.
    /// assert_eq!(sem.available_permits(), 0);
    ///
    /// sem.release(2); // Repays the deficit, so neither permit becomes available.
    /// assert_eq!(sem.available_permits(), 0);
    ///
    /// sem.release(1); // With the deficit repaid, this permit becomes available.
    /// assert_eq!(sem.available_permits(), 1);
    /// ```
    pub fn reduce_permits(&self, n: usize) {
        self.s.reduce_permits(n);
    }

    /// Adds `n` new permits to the semaphore.
    ///
    /// # Panics
    ///
    /// Panics if adding the permits would overflow the total permit count, or if notifying a waiter
    /// panics. Added permits remain available, and notification is still attempted for every other
    /// eligible waiter before the panic resumes.
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

    /// Acquires `n` permits from the semaphore.
    ///
    /// If the permits are not immediately available, this method will wait until they become
    /// available. Returns a [`SemaphorePermit`] that will release the permits when dropped.
    ///
    /// # Cancel safety
    ///
    /// Pending acquisitions complete in order. Cancelling this call loses its place among them.
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
    /// [`forget`]: OwnedSemaphorePermit::forget
    pub fn try_acquire_owned(self: Arc<Self>, permits: usize) -> Option<OwnedSemaphorePermit> {
        if self.s.try_acquire(permits) {
            Some(OwnedSemaphorePermit { sem: self, permits })
        } else {
            None
        }
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
    /// Pending acquisitions complete in order. Cancelling this call loses its place among them.
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
    /// let mut join_handles = vec![];
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
    /// This function panics if permits from different [`Semaphore`] instances are merged or if
    /// their combined permit count exceeds `usize::MAX`.
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
        self.permits = self
            .permits
            .checked_add(other.permits)
            .expect("merged permit count would overflow usize::MAX");
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

    /// Merge two [`OwnedSemaphorePermit`] instances together, consuming `other`
    /// without releasing the permits it holds.
    ///
    /// Permits held by both `self` and `other` are released when `self` drops.
    ///
    /// # Panics
    ///
    /// This function panics if permits from different [`Semaphore`] instances are merged or if
    /// their combined permit count exceeds `usize::MAX`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Arc::new(Semaphore::new(10));
    /// let mut permit = sem.clone().try_acquire_owned(1).unwrap();
    ///
    /// for _ in 0..9 {
    ///     let new_permit = sem.clone().try_acquire_owned(1).unwrap();
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
        self.permits = self
            .permits
            .checked_add(other.permits)
            .expect("merged permit count would overflow usize::MAX");
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
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let sem = Arc::new(Semaphore::new(5));
    /// let permit = sem.try_acquire_owned(3).unwrap();
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
