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

//! A reader-writer lock that allows multiple readers or a single writer at a time.
//!
//! This type of lock allows a number of readers or at most one writer at any point in time. The
//! write portion of this lock typically allows modification of the underlying data (exclusive
//! access) and the read portion of this lock typically allows for read-only access (shared access).
//!
//! In comparison, a [`Mutex`] does not distinguish between readers or writers that acquire the
//! lock, therefore causing any tasks waiting for the lock to become available to yield. An RwLock
//! will allow any number of readers to acquire the lock as long as a writer is not holding the
//! lock.
//!
//! The priority policy of Tokio's read-write lock is fair (or [write-preferring]), in order to
//! ensure that readers cannot starve writers. Fairness is ensured using a first-in, first-out queue
//! for the tasks awaiting the lock; if a task that wishes to acquire the write lock is at the head
//! of the queue, read locks will not be given out until the write lock has been released. This is
//! in contrast to the Rust standard library's `std::sync::RwLock`, where the priority policy is
//! dependent on the operating system's implementation.
//!
//! The type parameter `T` represents the data that this lock protects. It is required that `T`
//! satisfies [`Send`] to be shared across threads. The RAII guards returned from the locking
//! methods implement [`Deref`] (and [`DerefMut`] for the `write` method) to allow access to the
//! content of the lock.
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
//!
//! [`Mutex`]: crate::mutex::Mutex
//! [`Deref`]: std::ops::Deref
//! [`DerefMut`]: std::ops::DerefMut
//! [write-preferring]: https://en.wikipedia.org/wiki/Readers%E2%80%93writer_lock#Priority_policies

use std::cell::UnsafeCell;
use std::fmt;
use std::num::NonZeroUsize;

use crate::internal::Semaphore;

mod mapped_read_guard;
pub use mapped_read_guard::MappedRwLockReadGuard;
mod mapped_write_guard;
pub use mapped_write_guard::MappedRwLockWriteGuard;
mod owned_mapped_read_guard;
pub use owned_mapped_read_guard::OwnedMappedRwLockReadGuard;
mod owned_mapped_write_guard;
pub use owned_mapped_write_guard::OwnedMappedRwLockWriteGuard;
mod owned_read_guard;
pub use owned_read_guard::OwnedRwLockReadGuard;
mod owned_write_guard;
pub use owned_write_guard::OwnedRwLockWriteGuard;
mod read_guard;
pub use read_guard::RwLockReadGuard;
mod write_guard;
pub use write_guard::RwLockWriteGuard;

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
