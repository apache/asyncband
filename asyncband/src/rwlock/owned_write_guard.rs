// This file contains code derived from Tokio 1.42.0's RwLock implementation.
// Copyright (c) Tokio Contributors
// The derived code remains licensed under the MIT License.
// The incorporated code has been modified for use in Apache Asyncband.
// Upstream source:
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
    /// Waits for exclusive write access and returns a guard that owns this [`Arc`].
    ///
    /// Owning the `Arc` lets the guard be moved wherever a `'static` value is required.
    ///
    /// # Cancel safety
    ///
    /// Pending lock requests complete in order. Cancelling this call loses its place among them.
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

    /// Acquires exclusive write access without waiting and returns a guard that owns this [`Arc`].
    ///
    /// Returns `None` if write access is unavailable.
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

/// An owned guard that provides exclusive access to a [`RwLock`]'s value.
///
/// [`RwLock::write_owned`] and [`RwLock::try_write_owned`] create this guard. It keeps the lock
/// alive without borrowing it and releases the lock when dropped.
#[must_use = "dropping the guard releases its write access immediately"]
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
    /// Projects this guard to a mutable component of the protected value.
    ///
    /// Call this as `OwnedRwLockWriteGuard::map(...)` so a method named `map` on `T` remains
    /// accessible.
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

    /// Attempts to project this guard to a mutable component of the protected value.
    ///
    /// The original guard is returned when `f` returns `None`. Call this as
    /// `OwnedRwLockWriteGuard::filter_map(...)` so a method with the same name on `T` remains
    /// accessible.
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
