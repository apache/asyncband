// This file contains code derived from Tokio 1.42.0's RwLock implementation.
// Copyright (c) Tokio Contributors
// The derived code remains licensed under the MIT License.
// The incorporated code has been modified for use in Apache Asyncband.
// Upstream source:
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/rwlock/owned_read_guard.rs

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use crate::rwlock::OwnedMappedRwLockReadGuard;
use crate::rwlock::RwLock;

impl<T: ?Sized> RwLock<T> {
    /// Waits for shared read access and returns a guard that owns this [`Arc`].
    ///
    /// Other readers may hold the lock at the same time. Owning the `Arc` lets the guard be moved
    /// wherever a `'static` value is required.
    ///
    /// A writer already waiting ahead of this request must acquire and release the lock first.
    /// Holding a read guard, queuing a write request, and then waiting for another read guard in
    /// the same task can therefore deadlock.
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
    /// let lock_clone = lock.clone();
    ///
    /// let n = lock.read_owned().await;
    /// assert_eq!(*n, 1);
    ///
    /// tokio::spawn(async move {
    ///     // while the outer read lock is held, we acquire a read lock, too
    ///     let r = lock_clone.read_owned().await;
    ///     assert_eq!(*r, 1);
    /// })
    /// .await
    /// .unwrap();
    /// # }
    /// ```
    pub async fn read_owned(self: Arc<Self>) -> OwnedRwLockReadGuard<T> {
        self.s.acquire(1).await;
        OwnedRwLockReadGuard { lock: self }
    }

    /// Acquires shared read access without waiting and returns a guard that owns this [`Arc`].
    ///
    /// Returns `None` if read access is unavailable.
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
    /// let v = lock.clone().try_read_owned().unwrap();
    /// assert_eq!(*v, 1);
    /// drop(v);
    ///
    /// let v = lock.try_write().unwrap();
    /// assert!(lock.clone().try_read_owned().is_none());
    /// ```
    pub fn try_read_owned(self: Arc<Self>) -> Option<OwnedRwLockReadGuard<T>> {
        if self.s.try_acquire(1) {
            Some(OwnedRwLockReadGuard { lock: self })
        } else {
            None
        }
    }
}

/// An owned guard that provides shared access to a [`RwLock`]'s value.
///
/// [`RwLock::read_owned`] and [`RwLock::try_read_owned`] create this guard. It keeps the lock alive
/// without borrowing it and releases this reader's access when dropped.
#[must_use = "dropping the guard releases its read access immediately"]
pub struct OwnedRwLockReadGuard<T: ?Sized> {
    pub(super) lock: Arc<RwLock<T>>,
}

impl<T: ?Sized> Drop for OwnedRwLockReadGuard<T> {
    fn drop(&mut self) {
        self.lock.s.release(1);
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for OwnedRwLockReadGuard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for OwnedRwLockReadGuard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized> Deref for OwnedRwLockReadGuard<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.c.get() }
    }
}

impl<T: ?Sized> OwnedRwLockReadGuard<T> {
    /// Projects this guard to a shared component of the protected value.
    ///
    /// Call this as `OwnedRwLockReadGuard::map(...)` so a method named `map` on `T` remains
    /// accessible.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::rwlock::OwnedRwLockReadGuard;
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
    /// let guard = rwlock.read_owned().await;
    /// let mapped_guard = OwnedRwLockReadGuard::map(guard, |foo| &foo.a);
    ///
    /// assert_eq!(*mapped_guard, 1);
    /// # }
    /// ```
    pub fn map<U, F>(orig: Self, f: F) -> OwnedMappedRwLockReadGuard<T, U>
    where
        F: FnOnce(&T) -> &U,
        U: ?Sized,
    {
        // SAFETY: orig.lock.c.get() is a valid pointer to T that was created when the lock was
        // acquired. The guard guarantees shared access to the data through the rwlock, so
        // dereferencing is safe.
        let d = std::ptr::NonNull::from(f(unsafe { &*orig.lock.c.get() }));
        let orig = std::mem::ManuallyDrop::new(orig);

        // Safely extract the Arc from the guard
        let lock = unsafe { std::ptr::read(&orig.lock) };

        OwnedMappedRwLockReadGuard::new(d, lock)
    }

    /// Attempts to project this guard to a shared component of the protected value.
    ///
    /// The original guard is returned when `f` returns `None`. Call this as
    /// `OwnedRwLockReadGuard::filter_map(...)` so a method with the same name on `T` remains
    /// accessible.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::rwlock::OwnedRwLockReadGuard;
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
    /// let guard = rwlock.read_owned().await;
    /// let mapped_guard =
    ///     OwnedRwLockReadGuard::filter_map(guard, |foo| if foo.a > 0 { Some(&foo.b) } else { None })
    ///         .expect("should have mapped");
    ///
    /// assert_eq!(&*mapped_guard, "hello");
    /// # }
    /// ```
    pub fn filter_map<U, F>(orig: Self, f: F) -> Result<OwnedMappedRwLockReadGuard<T, U>, Self>
    where
        F: FnOnce(&T) -> Option<&U>,
        U: ?Sized,
    {
        // SAFETY: orig.lock.c.get() is a valid pointer to T that was created when the lock was
        // acquired. The guard guarantees shared access to the data through the rwlock, so
        // dereferencing is safe.
        match f(unsafe { &*orig.lock.c.get() }) {
            Some(d) => {
                let d = std::ptr::NonNull::from(d);
                let orig = std::mem::ManuallyDrop::new(orig);

                // Safely extract the Arc from the guard
                let lock = unsafe { std::ptr::read(&orig.lock) };

                Ok(OwnedMappedRwLockReadGuard::new(d, lock))
            }
            None => Err(orig),
        }
    }
}
