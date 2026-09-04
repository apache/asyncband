// This file contains code derived from Tokio 1.42.0's RwLock implementation.
// Copyright (c) Tokio Contributors
// The derived code remains licensed under the MIT License.
// The incorporated code has been modified for use in Apache Asyncband.
// See the project LICENSE file for the exact upstream revision and source path.

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use crate::rwlock::OwnedMappedRwLockReadGuard;
use crate::rwlock::RwLock;

impl<T: ?Sized> RwLock<T> {
    /// Locks this `RwLock` with shared read access, causing the current task to yield until the
    /// lock has been acquired.
    ///
    /// The calling task will yield until there are no writers which hold the lock. There may be
    /// other readers inside the lock when the task resumes.
    ///
    /// This method is identical to [`RwLock::read`], except that the returned guard references the
    /// `RwLock` with an [`Arc`] rather than by borrowing it. Therefore, the `RwLock` must be
    /// wrapped in an `Arc` to call this method, and the guard will live for the `'static` lifetime,
    /// as it keeps the `RwLock` alive by holding an `Arc`.
    ///
    /// Note that under the priority policy of [`RwLock`], read locks are not granted until prior
    /// write locks, to prevent starvation. Therefore, deadlock may occur if a read lock is held
    /// by the current task, a write lock attempt is made, and then a subsequent read lock attempt
    /// is made by the current task.
    ///
    /// Returns an RAII guard which will drop this read access of the `RwLock` when dropped.
    ///
    /// # Cancel safety
    ///
    /// This method uses a queue to fairly distribute locks in the order they were requested.
    /// Cancelling a call to `read_owned` makes you lose your place in the queue.
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

    /// Attempts to acquire this `RwLock` with shared read access.
    ///
    /// If the access couldn't be acquired immediately, returns `None`. Otherwise, an RAII guard is
    /// returned which will release read access when dropped.
    ///
    /// This method is identical to [`RwLock::try_read`], except that the returned guard references
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

/// Owned RAII structure used to release the shared read access of a lock when dropped.
///
/// This structure is created by the [`RwLock::read`] method.
///
/// See the [module level documentation](crate::rwlock) for more.
#[must_use = "if unused the RwLock will immediately unlock"]
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
    /// Makes a new [`OwnedMappedRwLockReadGuard`] for a component of the locked
    /// data.
    ///
    /// This operation cannot fail as the `OwnedRwLockReadGuard` passed in already locked the
    /// rwlock.
    ///
    /// This is an associated function that needs to be used as `OwnedRwLockReadGuard::map(...)`.
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

    /// Attempts to make a new [`OwnedMappedRwLockReadGuard`] for a component of the locked data.
    /// The original guard is returned if the closure returns `None`.
    ///
    /// This operation cannot fail as the `OwnedRwLockReadGuard` passed in already locked the
    /// rwlock.
    ///
    /// This is an associated function that needs to be used as
    /// `OwnedRwLockReadGuard::filter_map(...)`.
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
