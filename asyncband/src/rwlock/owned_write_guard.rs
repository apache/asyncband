// Portions derived from Tokio 1.42.0's RwLock implementation:
// Copyright (c) Tokio Contributors
// Licensed under the MIT License. See the LICENSE file in the project root.
// Modified by the Apache Software Foundation.
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/rwlock/owned_write_guard.rs

use std::fmt;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::rwlock::OwnedMappedRwLockWriteGuard;
use crate::rwlock::OwnedRwLockReadGuard;
use crate::rwlock::RwLock;

impl<T: ?Sized> RwLock<T> {
    /// Locks this `RwLock` with exclusive write access, causing the current task to yield until the
    /// lock has been acquired.
    ///
    /// The calling task will yield while other writers or readers currently have access to the
    /// lock.
    ///
    /// This method is identical to [`RwLock::write`], except that the returned guard references the
    /// `RwLock` with an [`Arc`] rather than by borrowing it. Therefore, the `RwLock` must be
    /// wrapped in an `Arc` to call this method, and the guard will live for the `'static` lifetime,
    /// as it keeps the `RwLock` alive by holding an `Arc`.
    ///
    /// Returns an RAII guard which will drop the write access of this `RwLock` when dropped.
    ///
    /// # Cancel safety
    ///
    /// This method uses a queue to fairly distribute locks in the order they were requested.
    /// Cancelling a call to `write_owned` makes you lose your place in the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::rwlock::RwLock;
    ///
    /// let lock = Arc::new(RwLock::new(1));
    /// let mut n = lock.write_owned().await;
    /// *n = 2;
    /// # }
    /// ```
    pub async fn write_owned(self: Arc<Self>) -> OwnedRwLockWriteGuard<T> {
        self.s.acquire(self.max_readers).await;
        OwnedRwLockWriteGuard {
            permits_acquired: self.max_readers,
            lock: self,
        }
    }

    /// Attempts to acquire this `RwLock` with exclusive write access.
    ///
    /// If the access couldn't be acquired immediately, returns `None`. Otherwise, an RAII guard is
    /// returned which will release write access when dropped.
    ///
    /// This method is identical to [`RwLock::try_write`], except that the returned guard references
    /// the `RwLock` with an [`Arc`] rather than by borrowing it. Therefore, the `RwLock` must
    /// be wrapped in an `Arc` to call this method, and the guard will live for the `'static`
    /// lifetime, as it keeps the `RwLock` alive by holding an `Arc`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::rwlock::RwLock;
    ///
    /// let lock = Arc::new(RwLock::new(1));
    ///
    /// let v = lock.try_read().unwrap();
    /// assert!(lock.clone().try_write_owned().is_none());
    /// drop(v);
    ///
    /// let mut v = lock.try_write_owned().unwrap();
    /// *v = 2;
    /// ```
    pub fn try_write_owned(self: Arc<Self>) -> Option<OwnedRwLockWriteGuard<T>> {
        if self.s.try_acquire(self.max_readers) {
            Some(OwnedRwLockWriteGuard {
                permits_acquired: self.max_readers,
                lock: self,
            })
        } else {
            None
        }
    }
}

/// Owned RAII structure used to release the exclusive write access of a lock when dropped.
///
/// This structure is created by the [`RwLock::write`] method.
///
/// See the [module level documentation](crate::rwlock) for more.
///
/// # Variance
///
/// The guard is invariant over `T`, as required for mutable access:
///
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
#[must_use = "if unused the RwLock will immediately unlock"]
pub struct OwnedRwLockWriteGuard<T: ?Sized> {
    pub(super) permits_acquired: usize,
    pub(super) lock: Arc<RwLock<T>>,
}

unsafe impl<T: ?Sized + Send + Sync> Send for OwnedRwLockWriteGuard<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for OwnedRwLockWriteGuard<T> {}

impl<T: ?Sized> Drop for OwnedRwLockWriteGuard<T> {
    fn drop(&mut self) {
        self.lock.s.release(self.permits_acquired);
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for OwnedRwLockWriteGuard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for OwnedRwLockWriteGuard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized> Deref for OwnedRwLockWriteGuard<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.c.get() }
    }
}

impl<T: ?Sized> DerefMut for OwnedRwLockWriteGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.c.get() }
    }
}

impl<T: ?Sized> OwnedRwLockWriteGuard<T> {
    /// Makes a new [`OwnedMappedRwLockWriteGuard`] for a component of the locked
    /// data.
    ///
    /// This operation cannot fail as the `OwnedRwLockWriteGuard` passed in already locked the
    /// rwlock.
    ///
    /// This is an associated function that needs to be used as `OwnedRwLockWriteGuard::map(...)`.
    ///
    /// A method would interfere with methods of the same name on the contents of the locked data.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::rwlock::OwnedRwLockWriteGuard;
    /// use asyncband::rwlock::RwLock;
    ///
    /// #[derive(Debug)]
    /// struct Foo {
    ///     a: u32,
    ///     b: String,
    /// }
    ///
    /// let rwlock = Arc::new(RwLock::new(Foo {
    ///     a: 1,
    ///     b: "hello".to_owned(),
    /// }));
    ///
    /// let mut guard = rwlock.write_owned().await;
    /// let mut mapped_guard = OwnedRwLockWriteGuard::map(guard, |foo| &mut foo.b);
    ///
    /// mapped_guard.push_str(" world");
    /// assert_eq!(&*mapped_guard, "hello world");
    /// # }
    /// ```
    pub fn map<U, F>(orig: Self, f: F) -> OwnedMappedRwLockWriteGuard<T, U>
    where
        F: FnOnce(&mut T) -> &mut U,
        U: ?Sized,
    {
        // SAFETY: We have exclusive write access to the data through the rwlock.
        // The data pointer is valid for the lifetime of the guard.
        let d = NonNull::from(f(unsafe { &mut *orig.lock.c.get() }));
        let orig = ManuallyDrop::new(orig);

        let permits_acquired = orig.permits_acquired;
        // SAFETY: The original guard is wrapped in `ManuallyDrop` and will not be dropped.
        // This allows us to safely move the `Arc` out of it and transfer ownership to the new
        // guard.
        let lock = unsafe { std::ptr::read(&orig.lock) };

        OwnedMappedRwLockWriteGuard::new(d, lock, permits_acquired)
    }

    /// Attempts to make a new [`OwnedMappedRwLockWriteGuard`] for a component of the
    /// locked data. The original guard is returned if the closure returns `None`.
    ///
    /// This operation cannot fail as the `OwnedRwLockWriteGuard` passed in already locked the
    /// rwlock.
    ///
    /// This is an associated function that needs to be used as
    /// `OwnedRwLockWriteGuard::filter_map(...)`.
    ///
    /// A method would interfere with methods of the same name on the contents of the locked data.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::rwlock::OwnedRwLockWriteGuard;
    /// use asyncband::rwlock::RwLock;
    ///
    /// #[derive(Debug)]
    /// struct Foo {
    ///     a: u32,
    ///     b: String,
    /// }
    ///
    /// let rwlock = Arc::new(RwLock::new(Foo {
    ///     a: 1,
    ///     b: "hello".to_owned(),
    /// }));
    ///
    /// let mut guard = rwlock.write_owned().await;
    /// let mut mapped_guard = OwnedRwLockWriteGuard::filter_map(guard, |foo| {
    ///     if foo.b.len() > 3 {
    ///         Some(&mut foo.b)
    ///     } else {
    ///         None
    ///     }
    /// })
    /// .expect("should have mapped");
    ///
    /// mapped_guard.push_str(" world");
    /// assert_eq!(&*mapped_guard, "hello world");
    /// # }
    /// ```
    pub fn filter_map<U, F>(orig: Self, f: F) -> Result<OwnedMappedRwLockWriteGuard<T, U>, Self>
    where
        F: FnOnce(&mut T) -> Option<&mut U>,
        U: ?Sized,
    {
        // SAFETY: We have exclusive write access to the data through the rwlock.
        // The data pointer is valid for the lifetime of the guard.
        let d = match f(unsafe { &mut *orig.lock.c.get() }) {
            Some(d) => NonNull::from(d),
            None => return Err(orig),
        };

        let orig = ManuallyDrop::new(orig);

        let permits_acquired = orig.permits_acquired;
        // SAFETY: The original guard is wrapped in `ManuallyDrop` and will not be dropped.
        // This allows us to safely move the `Arc` out of it and transfer ownership to the new
        // guard.
        let lock = unsafe { std::ptr::read(&orig.lock) };

        Ok(OwnedMappedRwLockWriteGuard::new(d, lock, permits_acquired))
    }

    /// Atomically downgrades the write lock to a read lock.
    ///
    /// This method changes the lock from exclusive mode to shared mode atomically,
    /// preventing other writers from acquiring the lock in between.
    ///
    /// The returned `OwnedRwLockReadGuard` has a `'static` lifetime, as it keeps
    /// the `RwLock` alive by holding an `Arc`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::rwlock::RwLock;
    ///
    /// let lock = Arc::new(RwLock::new(1));
    ///
    /// let mut write_guard = lock.clone().write_owned().await;
    /// *write_guard = 42;
    ///
    /// let read_guard = write_guard.downgrade();
    /// assert_eq!(*read_guard, 42);
    ///
    /// assert!(lock.clone().try_write_owned().is_none());
    ///
    /// drop(read_guard);
    /// assert!(lock.clone().try_write_owned().is_some());
    /// # }
    /// ```
    pub fn downgrade(self) -> OwnedRwLockReadGuard<T> {
        // Prevent the original write guard from running its Drop implementation,
        // which would release all permits. This must be done BEFORE any operation
        // that might panic to ensure panic safety.
        let guard = ManuallyDrop::new(self);

        // Release max_readers - 1 permits to convert the write lock to a read lock.
        // The remaining 1 permit is kept for the read lock.
        guard.lock.s.release(guard.permits_acquired - 1);

        // SAFETY: The `guard` is wrapped in `ManuallyDrop`, so its destructor will not be run.
        // We can safely move the `Arc` out of the guard, as the guard is not used after this.
        // This is a standard way to transfer ownership from a `ManuallyDrop` wrapper.
        let lock = unsafe { std::ptr::read(&guard.lock) };
        OwnedRwLockReadGuard { lock }
    }
}
