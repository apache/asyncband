// This file contains code derived from Tokio 1.42.0.
// Copyright (c) Tokio Contributors
// The derived code remains licensed under the MIT License.
// The incorporated code has been modified for use in Apache Asyncband.
// Upstream source:
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/rwlock.rs

//! Shared read access or exclusive write access to a value.
//!
//! Any number of readers may hold the lock together. A writer waits for existing readers and then
//! holds the lock alone, allowing it to modify the protected value.
//!
//! Requests are considered in arrival order. Once a writer is waiting ahead of a reader, that
//! reader waits until the writer has acquired and released the lock. This prevents a steady stream
//! of readers from starving writers.
//!
//! Read guards dereference to `&T`; write guards dereference to `&mut T`. Dropping a guard releases
//! its access. The mapping APIs can narrow a guard to one component without unlocking in between.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use asyncband::rwlock::RwLock;
//!
//! let lock = RwLock::new(5);
//!
//! // many reader locks can be held at once
//! {
//!     let r1 = lock.read().await;
//!     let r2 = lock.read().await;
//!     assert_eq!(*r1, 5);
//!     assert_eq!(*r2, 5);
//! } // read locks are dropped at this point
//!
//! // only one write lock may be held, however
//! {
//!     let mut w = lock.write().await;
//!     *w += 1;
//!     assert_eq!(*w, 6);
//! } // write lock is dropped here
//!
//! # }
//! ```

use std::cell::UnsafeCell;
use std::fmt;
use std::num::NonZeroUsize;

use crate::internal::semaphore::Semaphore;

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

/// A reader-writer lock that allows multiple readers or a single writer at a time.
///
/// See the [module level documentation](self) for more.
pub struct RwLock<T: ?Sized> {
    /// Maximum number of concurrent readers.
    ///
    /// This is ensured to be non-zero.
    max_readers: usize,
    /// Semaphore to coordinate read and write access to T
    s: Semaphore,
    /// The inner data.
    c: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for RwLock<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for RwLock<T> {}

impl<T> From<T> for RwLock<T> {
    fn from(t: T) -> Self {
        Self::new(t)
    }
}

impl<T: Default> Default for RwLock<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("RwLock");
        match self.try_read() {
            Some(inner) => d.field("data", &&*inner),
            None => d.field("data", &format_args!("<locked>")),
        };
        d.finish()
    }
}

impl<T> RwLock<T> {
    /// Creates a new reader-writer lock in an unlocked state ready for use.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::rwlock::RwLock;
    ///
    /// let rwlock = RwLock::new(5);
    /// ```
    pub const fn new(t: T) -> RwLock<T> {
        // large enough while not touch the edge
        RwLock::with_max_readers(t, NonZeroUsize::new(usize::MAX >> 1).unwrap())
    }

    /// Creates a new reader-writer lock in an unlocked state, and allows a maximum of
    /// `max_readers` concurrent readers.
    ///
    /// This method is typically used for debugging and testing purposes.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    ///
    /// use asyncband::rwlock::RwLock;
    ///
    /// let max_readers = NonZeroUsize::new(1024).expect("max_readers must be non-zero");
    /// let rwlock = RwLock::with_max_readers(5, max_readers);
    /// ```
    pub const fn with_max_readers(t: T, max_readers: NonZeroUsize) -> RwLock<T> {
        let max_readers = max_readers.get();
        let s = Semaphore::new(max_readers);
        let c = UnsafeCell::new(t);
        RwLock { max_readers, c, s }
    }

    /// Consumes the lock, returning the underlying data.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::rwlock::RwLock;
    ///
    /// let lock = RwLock::new(1);
    /// let n = lock.into_inner();
    /// assert_eq!(n, 1);
    /// ```
    pub fn into_inner(self) -> T {
        self.c.into_inner()
    }
}

impl<T: ?Sized> RwLock<T> {
    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the `RwLock` mutably, no actual locking needs to take place: the
    /// mutable borrow statically guarantees no locks exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::rwlock::RwLock;
    ///
    /// let mut lock = RwLock::new(1);
    /// let n = lock.get_mut();
    /// *n = 2;
    /// ```
    pub fn get_mut(&mut self) -> &mut T {
        self.c.get_mut()
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
