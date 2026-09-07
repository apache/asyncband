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

// Portions of the RwLock API originated from Tokio 1.42.0.
// Copyright (c) Tokio Contributors
// The Tokio-derived portions remain licensed under the MIT License.
// Asyncband retains semaphore-based fair scheduling and has substantially rewritten the guard
// lifecycle around RAII access tokens, separating permit ownership from data projection. Borrowed
// and owned guards move tokens on projection and downgrade without manual destruction suppression.
// Upstream source:
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/rwlock.rs

//! Shared and exclusive access to a value, with asynchronous waiting.
//!
//! A read guard allows inspection alongside other readers, up to the configured reader limit.
//! A write guard allows mutation and excludes every other guard. Both release their access on
//! drop, including during unwinding; a panic does not poison the lock.
//!
//! Waiting requests are served in queue order. A queued writer blocks readers behind it, even
//! while earlier readers still hold the lock. Consequently, keeping a read guard while waiting
//! for a write guard, or for another read behind a queued writer, can deadlock. The `try_` methods
//! never wait or reserve a queue position. Dropping a pending acquisition cancels its request;
//! a subsequent acquisition starts again at the back of the queue.
//!
//! # Updating and inspecting
//!
//! ```
//! use asyncband::rwlock::RwLock;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let routes = RwLock::new(vec!["/health"]);
//! let mut edit = routes.write().await;
//! edit.push("/metrics");
//!
//! // Downgrading retains access to the just-published value without an unlocked interval.
//! let snapshot = edit.downgrade();
//! let other_reader = routes.read().await;
//! assert_eq!(*snapshot, *other_reader);
//! assert!(routes.try_write().is_none());
//! # }
//! ```
//!
//! # Selecting a component
//!
//! A mapped guard keeps the original lock held but exposes only the selected component. Mapping
//! can be repeated, and `filter_map` returns the original guard when the component is absent.
//! A projection closure that panics releases its guard during unwinding.
//!
//! ```
//! use asyncband::rwlock::MappedRwLockWriteGuard;
//! use asyncband::rwlock::RwLock;
//! use asyncband::rwlock::RwLockWriteGuard;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let queue = RwLock::new(vec![Some(String::from("pending"))]);
//! let slot = RwLockWriteGuard::map(queue.write().await, |items| &mut items[0]);
//! let mut message = MappedRwLockWriteGuard::filter_map(slot, Option::as_mut).unwrap();
//! message.push_str(" review");
//! let message = message.downgrade();
//! assert_eq!(&*message, "pending review");
//! # }
//! ```
//!
//! # Keeping the lock alive
//!
//! Owned guards retain the `Arc` passed to acquisition, allowing the guard to outlive that call's
//! local scope. Projecting or downgrading an owned guard retains the same ownership. Values with
//! borrowed data still obey their original lifetime constraints.
//!
//! ```
//! use std::sync::Arc;
//!
//! use asyncband::rwlock::OwnedRwLockReadGuard;
//! use asyncband::rwlock::RwLock;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let catalog = Arc::new(RwLock::new(vec![String::from("index")]));
//! let entry = OwnedRwLockReadGuard::map(catalog.read_owned().await, |items| &items[0]);
//! tokio::spawn(async move {
//!     assert_eq!(&*entry, "index");
//! })
//! .await
//! .unwrap();
//! # }
//! ```

use std::cell::UnsafeCell;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use self::access::ReadAccess;
use self::access::WriteAccess;
use crate::internal::semaphore::Semaphore;

mod access;
mod mapped_read_guard;
mod mapped_write_guard;
mod owned_mapped_read_guard;
mod owned_mapped_write_guard;
mod owned_read_guard;
mod owned_write_guard;
mod read_guard;
mod write_guard;

pub use self::mapped_read_guard::MappedRwLockReadGuard;
pub use self::mapped_write_guard::MappedRwLockWriteGuard;
pub use self::owned_mapped_read_guard::OwnedMappedRwLockReadGuard;
pub use self::owned_mapped_write_guard::OwnedMappedRwLockWriteGuard;
pub use self::owned_read_guard::OwnedRwLockReadGuard;
pub use self::owned_write_guard::OwnedRwLockWriteGuard;
pub use self::read_guard::RwLockReadGuard;
pub use self::write_guard::RwLockWriteGuard;

/// A value with fair, asynchronous shared or exclusive access.
///
/// See the [module documentation](self) for ordering, cancellation, and guard projection.
pub struct RwLock<T: ?Sized> {
    max_readers: usize,
    s: Semaphore,
    c: UnsafeCell<T>,
}

// SAFETY: Moving the lock transfers its value; no guard can outlive a borrowed lock.
unsafe impl<T: ?Sized + Send> Send for RwLock<T> {}
// SAFETY: Readers can share &T; writers can access T from another thread, exclusively.
unsafe impl<T: ?Sized + Send + Sync> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    /// Wraps a value with a reader limit of `usize::MAX >> 1`.
    pub const fn new(value: T) -> Self {
        Self::with_max_readers(value, NonZeroUsize::new(usize::MAX >> 1).unwrap())
    }

    /// Wraps a value with an explicit nonzero limit on simultaneously held read guards.
    ///
    /// A downgraded guard occupies one reader slot. A write guard excludes all reader slots,
    /// regardless of the limit. Every `NonZeroUsize` is accepted.
    pub const fn with_max_readers(value: T, max_readers: NonZeroUsize) -> Self {
        Self {
            max_readers: max_readers.get(),
            s: Semaphore::new(max_readers.get()),
            c: UnsafeCell::new(value),
        }
    }

    /// Unwraps the value by consuming its lock.
    pub fn into_inner(self) -> T {
        self.c.into_inner()
    }
}

impl<T: ?Sized> RwLock<T> {
    /// Borrows the value exclusively through an exclusive borrow of the lock itself.
    ///
    /// This requires no acquisition because existing guards prevent borrowing the lock mutably.
    pub fn get_mut(&mut self) -> &mut T {
        self.c.get_mut()
    }

    /// Waits for a reader slot and returns a guard borrowing this lock.
    ///
    /// An earlier queued writer must finish first. Cancelling this future releases its queue
    /// position and any reserved permits. See [queue ordering](self) before acquiring recursively.
    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        self.s.acquire(1).await;
        read_guard::new(ReadAccess::new(self))
    }

    /// Returns a read guard immediately, or `None` when a reader slot cannot be acquired.
    ///
    /// Readers cannot bypass an earlier queued writer.
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        self.s
            .try_acquire(1)
            .then(|| read_guard::new(ReadAccess::new(self)))
    }

    /// Waits for exclusive access and returns a guard borrowing this lock.
    ///
    /// Cancelling this future releases its queue position and any reserved permits.
    pub async fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.s.acquire(self.max_readers).await;
        write_guard::new(WriteAccess::new(self, self.max_readers))
    }

    /// Returns a write guard immediately, or `None` when exclusive access is unavailable.
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        self.s
            .try_acquire(self.max_readers)
            .then(|| write_guard::new(WriteAccess::new(self, self.max_readers)))
    }

    /// Waits for a reader slot and returns a guard retaining this `Arc`.
    ///
    /// Ordering and cancellation follow [`Self::read`]. The guard keeps the lock alive without
    /// borrowing the caller's `Arc`.
    pub async fn read_owned(self: Arc<Self>) -> OwnedRwLockReadGuard<T> {
        self.s.acquire(1).await;
        owned_read_guard::new(ReadAccess::new(self))
    }

    /// Returns an owned read guard immediately, or `None` when no reader slot is available.
    ///
    /// This consumes the passed `Arc` even when acquisition fails.
    pub fn try_read_owned(self: Arc<Self>) -> Option<OwnedRwLockReadGuard<T>> {
        self.s
            .try_acquire(1)
            .then(|| owned_read_guard::new(ReadAccess::new(self)))
    }

    /// Waits for exclusive access and returns a guard retaining this `Arc`.
    ///
    /// Ordering and cancellation follow [`Self::write`].
    pub async fn write_owned(self: Arc<Self>) -> OwnedRwLockWriteGuard<T> {
        let permits = self.max_readers;
        self.s.acquire(permits).await;
        owned_write_guard::new(WriteAccess::new(self, permits))
    }

    /// Returns an owned write guard immediately, or `None` when exclusive access is unavailable.
    ///
    /// This consumes the passed `Arc` even when acquisition fails.
    pub fn try_write_owned(self: Arc<Self>) -> Option<OwnedRwLockWriteGuard<T>> {
        let permits = self.max_readers;
        self.s
            .try_acquire(permits)
            .then(|| owned_write_guard::new(WriteAccess::new(self, permits)))
    }
}

impl<T> From<T> for RwLock<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for RwLock<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("RwLock");
        match self.try_read() {
            Some(value) => debug.field("data", &&*value),
            None => debug.field("data", &format_args!("<locked>")),
        };
        debug.finish()
    }
}

#[cfg(doctest)]
mod compile_fail_tests {
    /// ```compile_fail
    /// use asyncband::rwlock::RwLockWriteGuard;
    ///
    /// fn shorten<'lock, 'short: 'lock>(
    ///     guard: RwLockWriteGuard<'lock, &'static str>,
    ///     value: &'short str,
    /// ) -> RwLockWriteGuard<'lock, &'short str> {
    ///     let mut guard: RwLockWriteGuard<'lock, &'short str> = guard;
    ///     *guard = value;
    ///     guard
    /// }
    /// ```
    struct RwLockWriteGuardIsInvariant;

    /// ```compile_fail
    /// use asyncband::rwlock::OwnedRwLockWriteGuard;
    ///
    /// fn shorten<'short>(
    ///     guard: OwnedRwLockWriteGuard<&'static str>,
    ///     value: &'short str,
    /// ) -> OwnedRwLockWriteGuard<&'short str> {
    ///     let mut guard: OwnedRwLockWriteGuard<&'short str> = guard;
    ///     *guard = value;
    ///     guard
    /// }
    /// ```
    struct OwnedRwLockWriteGuardIsInvariant;

    /// ```compile_fail
    /// use asyncband::rwlock::MappedRwLockWriteGuard;
    ///
    /// fn shorten<'lock, 'short: 'lock>(
    ///     guard: MappedRwLockWriteGuard<'lock, &'static str>,
    ///     value: &'short str,
    /// ) -> MappedRwLockWriteGuard<'lock, &'short str> {
    ///     let mut guard: MappedRwLockWriteGuard<'lock, &'short str> = guard;
    ///     *guard = value;
    ///     guard
    /// }
    /// ```
    struct MappedRwLockWriteGuardIsInvariant;

    /// ```compile_fail
    /// use asyncband::rwlock::OwnedMappedRwLockWriteGuard;
    ///
    /// fn shorten<'short>(
    ///     guard: OwnedMappedRwLockWriteGuard<(), &'static str>,
    ///     value: &'short str,
    /// ) -> OwnedMappedRwLockWriteGuard<(), &'short str> {
    ///     let mut guard: OwnedMappedRwLockWriteGuard<(), &'short str> = guard;
    ///     *guard = value;
    ///     guard
    /// }
    /// ```
    struct OwnedMappedRwLockWriteGuardIsInvariant;
}
