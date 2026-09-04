// This file contains code derived from Tokio 1.42.0's RwLock implementation.
// Copyright (c) Tokio Contributors
// The derived code remains licensed under the MIT License.
// The incorporated code has been modified for use in Apache Asyncband.
// Upstream source:
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/rwlock/read_guard.rs

use std::fmt;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::NonNull;

use crate::rwlock::MappedRwLockReadGuard;
use crate::rwlock::RwLock;

impl<T: ?Sized> RwLock<T> {
    /// Waits for shared read access and returns a borrowed guard.
    ///
    /// Other readers may hold the lock at the same time. A writer already waiting ahead of this
    /// request must acquire and release the lock first.
    ///
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
    /// let n = lock.read().await;
    /// assert_eq!(*n, 1);
    ///
    /// tokio::spawn(async move {
    ///     // while the outer read lock is held, we acquire a read lock, too
    ///     let r = lock_clone.read().await;
    ///     assert_eq!(*r, 1);
    /// })
    /// .await
    /// .unwrap();
    /// # }
    /// ```
    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        self.s.acquire(1).await;
        RwLockReadGuard { lock: self }
    }

    /// Acquires shared read access without waiting, or returns `None` if it is unavailable.
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
    /// assert_eq!(*v, 1);
    /// drop(v);
    ///
    /// let v = lock.try_write().unwrap();
    /// assert!(lock.try_read().is_none());
    /// ```
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        if self.s.try_acquire(1) {
            Some(RwLockReadGuard { lock: self })
        } else {
            None
        }
    }
}

/// A borrowed guard that provides shared access to a [`RwLock`]'s value.
///
/// [`RwLock::read`] and [`RwLock::try_read`] create this guard. Dropping it releases this reader's
/// access.
#[must_use = "dropping the guard releases its read access immediately"]
pub struct RwLockReadGuard<'a, T: ?Sized> {
    pub(super) lock: &'a RwLock<T>,
}

unsafe impl<T: ?Sized + Sync> Send for RwLockReadGuard<'_, T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for RwLockReadGuard<'_, T> {}

impl<T: ?Sized> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.s.release(1);
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.c.get() }
    }
}

impl<'a, T: ?Sized> RwLockReadGuard<'a, T> {
    /// Projects this guard to a shared component of the protected value.
    ///
    /// Call this as `RwLockReadGuard::map(...)` so a method named `map` on `T` remains accessible.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::rwlock::RwLock;
    /// use asyncband::rwlock::RwLockReadGuard;
    ///
    /// #[derive(Debug, Clone)]
    /// struct Foo(String);
    ///
    /// let rwlock = RwLock::new(Foo("hello".to_owned()));
    ///
    /// let guard = rwlock.read().await;
    /// let mapped_guard = RwLockReadGuard::map(guard, |f| &f.0);
    ///
    /// assert_eq!(&*mapped_guard, "hello");
    /// # }
    /// ```
    pub fn map<U, F>(orig: Self, f: F) -> MappedRwLockReadGuard<'a, U>
    where
        F: FnOnce(&T) -> &U,
        U: ?Sized,
    {
        let d = NonNull::from(f(&*orig));
        let orig = ManuallyDrop::new(orig);
        MappedRwLockReadGuard::new(d, &orig.lock.s)
    }

    /// Attempts to project this guard to a shared component of the protected value.
    ///
    /// The original guard is returned when `f` returns `None`. Call this as
    /// `RwLockReadGuard::filter_map(...)` so a method with the same name on `T` remains accessible.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::rwlock::RwLock;
    /// use asyncband::rwlock::RwLockReadGuard;
    ///
    /// #[derive(Debug, Clone)]
    /// struct Foo(String);
    ///
    /// let rwlock = RwLock::new(Foo("hello".to_owned()));
    ///
    /// let guard = rwlock.read().await;
    /// let mapped_guard =
    ///     RwLockReadGuard::filter_map(guard, |f| if f.0.len() > 3 { Some(&f.0) } else { None })
    ///         .expect("should have mapped");
    ///
    /// assert_eq!(&*mapped_guard, "hello");
    /// # }
    /// ```
    pub fn filter_map<U, F>(orig: Self, f: F) -> Result<MappedRwLockReadGuard<'a, U>, Self>
    where
        F: FnOnce(&T) -> Option<&U>,
        U: ?Sized,
    {
        match f(&*orig) {
            Some(d) => {
                let d = NonNull::from(d);
                let orig = ManuallyDrop::new(orig);
                Ok(MappedRwLockReadGuard::new(d, &orig.lock.s))
            }
            None => Err(orig),
        }
    }
}
