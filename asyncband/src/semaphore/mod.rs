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
//! let limits = Semaphore::new(4);
//! let batch = limits.acquire(3).await;
//! assert_eq!(limits.available_permits(), 1);
//!
//! assert!(limits.try_acquire(2).is_none());
//!
//! drop(batch);
//! assert_eq!(limits.available_permits(), 4);
//! # }
//! ```

use std::ptr;
use std::sync::Arc;

use crate::internal::semaphore;

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

    /// Removes exactly `n` permits, consuming future releases if fewer are available.
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

    /// Adds `permits` new permits to the semaphore.
    ///
    /// # Panics
    ///
    /// Panics if adding the permits would overflow the total permit count.
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

    /// Attempts to claim `permits` immediately without waiting.
    ///
    /// Returns [`None`] if fewer than `permits` permits are available. Otherwise returns a
    /// [`SemaphorePermit`] whose destructor releases those permits back to the semaphore unless
    /// [`SemaphorePermit::forget`] consumes it first.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let quota = Semaphore::new(3);
    ///
    /// let first = quota.try_acquire(2).unwrap();
    /// assert_eq!(quota.available_permits(), 1);
    /// assert!(quota.try_acquire(2).is_none());
    ///
    /// drop(first);
    /// assert_eq!(quota.available_permits(), 3);
    /// ```
    pub fn try_acquire(&self, permits: usize) -> Option<SemaphorePermit<'_>> {
        if self.s.try_acquire(permits) {
            Some(SemaphorePermit { sem: self, permits })
        } else {
            None
        }
    }

    /// Waits until `permits` permits are available and returns a guard holding them.
    ///
    /// Dropping the returned [`SemaphorePermit`] returns its permits to this semaphore unless
    /// [`SemaphorePermit::forget`] consumes it first.
    ///
    /// Pending acquisitions are served in queue order. A larger request can hold up smaller
    /// requests behind it until enough permits become available.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending future removes the request and returns any permits already assigned to
    /// it. Retrying joins the back of the queue.
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
    /// let quota = Arc::new(Semaphore::new(2));
    /// let worker_quota = quota.clone();
    ///
    /// let task = tokio::spawn(async move {
    ///     let _lease = worker_quota.acquire(1).await;
    ///     42
    /// });
    ///
    /// let lease = quota.acquire(1).await;
    /// assert_eq!(lease.permits(), 1);
    /// drop(lease);
    ///
    /// assert_eq!(task.await.unwrap(), 42);
    /// # }
    /// ```
    pub async fn acquire(&self, permits: usize) -> SemaphorePermit<'_> {
        self.s.acquire(permits).await;
        SemaphorePermit { sem: self, permits }
    }

    /// Attempts to claim `permits` immediately on an [`Arc`]-shared semaphore without waiting.
    ///
    /// Returns [`None`] if fewer than `permits` permits are available. Otherwise returns an
    /// [`OwnedSemaphorePermit`] that keeps the `Arc` alive and releases its permits on drop unless
    /// [`OwnedSemaphorePermit::forget`] consumes it first.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let quota = Arc::new(Semaphore::new(4));
    ///
    /// let lease = quota.clone().try_acquire_owned(3).unwrap();
    /// assert_eq!(quota.available_permits(), 1);
    /// assert!(quota.clone().try_acquire_owned(2).is_none());
    ///
    /// drop(lease);
    /// assert_eq!(quota.available_permits(), 4);
    /// ```
    pub fn try_acquire_owned(self: Arc<Self>, permits: usize) -> Option<OwnedSemaphorePermit> {
        if self.s.try_acquire(permits) {
            Some(OwnedSemaphorePermit { sem: self, permits })
        } else {
            None
        }
    }

    /// Waits on an [`Arc`]-shared semaphore until `permits` permits are available and returns an
    /// owned guard.
    ///
    /// Dropping the returned [`OwnedSemaphorePermit`] releases both the permits and its reference
    /// to the semaphore unless [`OwnedSemaphorePermit::forget`] consumes it first.
    ///
    /// Pending acquisitions are served in queue order. A larger request can hold up smaller
    /// requests behind it until enough permits become available.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending future removes the request and returns any permits already assigned to
    /// it. Retrying joins the back of the queue. The future also releases its owned `Arc`.
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
    /// let quota = Arc::new(Semaphore::new(2));
    /// let mut workers = Vec::new();
    ///
    /// for job_id in 0..4usize {
    ///     let lease = quota.clone().acquire_owned(1).await;
    ///     workers.push(tokio::spawn(async move {
    ///         let _lease = lease;
    ///         job_id * 2
    ///     }));
    /// }
    ///
    /// let mut results = Vec::new();
    /// for worker in workers {
    ///     results.push(worker.await.unwrap());
    /// }
    /// assert_eq!(results, [0, 2, 4, 6]);
    /// # }
    /// ```
    pub async fn acquire_owned(self: Arc<Self>, permits: usize) -> OwnedSemaphorePermit {
        self.s.acquire(permits).await;
        OwnedSemaphorePermit { sem: self, permits }
    }
}

/// Borrowed RAII guard representing permits acquired from a [`Semaphore`].
///
/// Created by [`Semaphore::acquire`] and [`Semaphore::try_acquire`]. Dropping this guard returns
/// its permits to the semaphore unless [`SemaphorePermit::forget`] is called first.
#[must_use = "permits are released immediately when dropped"]
#[derive(Debug)]
pub struct SemaphorePermit<'a> {
    sem: &'a Semaphore,
    permits: usize,
}

impl SemaphorePermit<'_> {
    /// Consumes this guard without returning its permits to the semaphore.
    ///
    /// The acquired capacity remains permanently deducted unless explicitly restored later via
    /// [`Semaphore::release`]. To deduct permits without first holding a guard, use
    /// [`Semaphore::reduce_permits`].
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let quota = Semaphore::new(6);
    /// let held = quota.try_acquire(4).unwrap();
    /// held.forget();
    ///
    /// assert_eq!(quota.available_permits(), 2);
    /// quota.release(4);
    /// assert_eq!(quota.available_permits(), 6);
    /// ```
    pub fn forget(mut self) {
        self.permits = 0;
    }

    /// Combines the permits held by `other` into `self`.
    ///
    /// `other` is consumed without releasing its permits; all combined permits are later released
    /// together when `self` drops (or retained permanently if forgotten).
    ///
    /// # Panics
    ///
    /// Panics if `self` and `other` belong to different [`Semaphore`] instances or if their
    /// combined permit count overflows `usize::MAX`.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let quota = Semaphore::new(8);
    /// let mut combined = quota.try_acquire(2).unwrap();
    /// combined.merge(quota.try_acquire(3).unwrap());
    ///
    /// assert_eq!(combined.permits(), 5);
    /// assert_eq!(quota.available_permits(), 3);
    ///
    /// drop(combined);
    /// assert_eq!(quota.available_permits(), 8);
    /// ```
    #[track_caller]
    pub fn merge(&mut self, mut other: Self) {
        assert!(
            ptr::eq(self.sem, other.sem),
            "cannot merge permits from different semaphores"
        );
        self.permits = self
            .permits
            .checked_add(other.permits)
            .expect("merged permit count would overflow usize::MAX");
        other.permits = 0;
    }

    /// Detaches `n` permits from `self` into a new borrowed guard.
    ///
    /// Returns [`None`] without modifying `self` when `n` exceeds [`Self::permits`].
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let quota = Semaphore::new(5);
    /// let mut whole = quota.try_acquire(5).unwrap();
    /// let part = whole.split(2).unwrap();
    ///
    /// assert_eq!(whole.permits(), 3);
    /// assert_eq!(part.permits(), 2);
    /// assert!(whole.split(4).is_none());
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

    /// Returns how many permits this guard currently holds.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let quota = Semaphore::new(7);
    /// let guard = quota.try_acquire(4).unwrap();
    /// assert_eq!(guard.permits(), 4);
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

/// Owned RAII guard representing permits acquired from an [`Arc`]-shared [`Semaphore`].
///
/// Created by [`Semaphore::acquire_owned`] and [`Semaphore::try_acquire_owned`]. Dropping this
/// guard returns its permits to the semaphore unless [`OwnedSemaphorePermit::forget`] is called
/// first.
#[must_use = "permits are released immediately when dropped"]
#[derive(Debug)]
pub struct OwnedSemaphorePermit {
    sem: Arc<Semaphore>,
    permits: usize,
}

impl OwnedSemaphorePermit {
    /// Consumes this guard without returning its permits to the semaphore.
    ///
    /// The acquired capacity remains permanently deducted unless explicitly restored later via
    /// [`Semaphore::release`]. To deduct permits without first holding a guard, use
    /// [`Semaphore::reduce_permits`].
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let quota = Arc::new(Semaphore::new(6));
    /// let held = quota.clone().try_acquire_owned(4).unwrap();
    /// held.forget();
    ///
    /// assert_eq!(quota.available_permits(), 2);
    /// quota.release(4);
    /// assert_eq!(quota.available_permits(), 6);
    /// ```
    pub fn forget(mut self) {
        self.permits = 0;
    }

    /// Combines the permits held by `other` into `self`.
    ///
    /// `other` is consumed without releasing its permits; all combined permits are later released
    /// together when `self` drops (or retained permanently if forgotten).
    ///
    /// # Panics
    ///
    /// Panics if `self` and `other` belong to different [`Semaphore`] instances or if their
    /// combined permit count overflows `usize::MAX`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let quota = Arc::new(Semaphore::new(8));
    /// let mut combined = quota.clone().try_acquire_owned(2).unwrap();
    /// combined.merge(quota.clone().try_acquire_owned(3).unwrap());
    ///
    /// assert_eq!(combined.permits(), 5);
    /// assert_eq!(quota.available_permits(), 3);
    ///
    /// drop(combined);
    /// assert_eq!(quota.available_permits(), 8);
    /// ```
    #[track_caller]
    pub fn merge(&mut self, mut other: Self) {
        assert!(
            Arc::ptr_eq(&self.sem, &other.sem),
            "cannot merge permits from different semaphores"
        );
        self.permits = self
            .permits
            .checked_add(other.permits)
            .expect("merged permit count would overflow usize::MAX");
        other.permits = 0;
    }

    /// Detaches `n` permits from `self` into a new owned guard, cloning the underlying [`Arc`].
    ///
    /// Returns [`None`] without modifying `self` when `n` exceeds [`Self::permits`].
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let quota = Arc::new(Semaphore::new(5));
    /// let mut whole = quota.try_acquire_owned(5).unwrap();
    /// let part = whole.split(2).unwrap();
    ///
    /// assert_eq!(whole.permits(), 3);
    /// assert_eq!(part.permits(), 2);
    /// assert!(whole.split(4).is_none());
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

    /// Returns how many permits this guard currently holds.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::semaphore::Semaphore;
    ///
    /// let quota = Arc::new(Semaphore::new(7));
    /// let guard = quota.try_acquire_owned(4).unwrap();
    /// assert_eq!(guard.permits(), 4);
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
