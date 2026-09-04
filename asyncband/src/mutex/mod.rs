// Portions derived from Tokio 1.41.0:
// Copyright (c) Tokio Contributors
// Licensed under the MIT License. See the LICENSE file in the project root.
// Modified by the Apache Software Foundation.
// https://github.com/tokio-rs/tokio/blob/01e04daaa162ce6122bb894fdda0b6803dd32093/tokio/src/sync/mutex.rs

//! An async mutex for protecting shared data.
//!
//! Unlike a standard mutex, this implementation is designed to work with async/await,
//! ensuring tasks yield properly when the lock is contended. This makes it suitable
//! for protecting shared resources in async code.
//!
//! This mutex will block tasks waiting for the lock to become available. The
//! mutex can be created via [`new`] and the protected data can be accessed
//! via the async [`lock`] method.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use std::sync::Arc;
//!
//! use asyncband::mutex::Mutex;
//!
//! let mutex = Arc::new(Mutex::new(0));
//! let mut handles = vec![];
//!
//! for i in 0..3 {
//!     let mutex = mutex.clone();
//!     handles.push(tokio::spawn(async move {
//!         let mut lock = mutex.lock().await;
//!         *lock += i;
//!     }));
//! }
//!
//! for handle in handles {
//!     handle.await.unwrap();
//! }
//!
//! let final_value = mutex.lock().await;
//! assert_eq!(*final_value, 3); // 0 + 1 + 2
//!
//! #  }
//! ```
//!
//! [`new`]: Mutex::new
//! [`lock`]: Mutex::lock

use std::cell::UnsafeCell;
use std::fmt;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::internal::semaphore;

/// An async mutex for protecting shared data.
///
/// See the [module level documentation](self) for more.
pub struct Mutex<T: ?Sized> {
    /// Semaphore used to control access to protected data, ensuring mutual exclusion
    s: semaphore::Semaphore,
    /// Container storing the protected data, allowing interior mutability
    c: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for Mutex<T> {}
unsafe impl<T: ?Sized + Send> Sync for Mutex<T> {}

impl<T> From<T> for Mutex<T> {
    fn from(t: T) -> Self {
        Self::new(t)
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("Mutex");
        match self.try_lock() {
            Some(inner) => d.field("data", &&*inner),
            None => d.field("data", &format_args!("<locked>")),
        };
        d.finish()
    }
}

impl<T> Mutex<T> {
    /// Creates a new mutex in an unlocked state ready for use.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mutex::Mutex;
    ///
    /// let mutex = Mutex::new(5);
    /// ```
    pub const fn new(t: T) -> Self {
        let s = semaphore::Semaphore::new(1);
        let c = UnsafeCell::new(t);
        Self { s, c }
    }

    /// Consumes the mutex, returning the underlying data.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mutex::Mutex;
    ///
    /// let mutex = Mutex::new(1);
    /// let n = mutex.into_inner();
    /// assert_eq!(n, 1);
    /// ```
    pub fn into_inner(self) -> T {
        self.c.into_inner()
    }
}

impl<T: ?Sized> Mutex<T> {
    /// Locks this mutex, causing the current task to yield until the lock has been acquired. When
    /// the lock has been acquired, function returns a [`MutexGuard`].
    ///
    /// This method is async and will yield the current task if the mutex is currently held by
    /// another task. When the mutex becomes available, the task will be woken up and given the
    /// lock.
    ///
    /// # Cancel safety
    ///
    /// This method uses a queue to fairly distribute locks in the order they were requested.
    /// Cancelling a call to `lock` makes you lose your place in the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mutex::Mutex;
    ///
    /// let mutex = Mutex::new(1);
    ///
    /// let mut n = mutex.lock().await;
    /// *n = 2;
    /// # }
    /// ```
    pub async fn lock(&self) -> MutexGuard<'_, T> {
        self.s.acquire(1).await;
        MutexGuard { lock: self }
    }

    /// Attempts to acquire the lock, and returns `None` if the lock is currently held somewhere
    /// else.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mutex::Mutex;
    ///
    /// let mutex = Mutex::new(1);
    /// let mut guard = mutex.try_lock().expect("mutex is locked");
    /// *guard += 1;
    /// assert_eq!(2, *guard);
    /// ```
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        if self.s.try_acquire(1) {
            let guard = MutexGuard { lock: self };
            Some(guard)
        } else {
            None
        }
    }

    /// Locks this mutex, causing the current task to yield until the lock has been acquired. When
    /// the lock has been acquired, this returns an [`OwnedMutexGuard`].
    ///
    /// This method is async and will yield the current task if the mutex is currently held by
    /// another task. When the mutex becomes available, the task will be woken up and given the
    /// lock.
    ///
    /// This method is identical to [`Mutex::lock`], except that the returned guard references the
    /// `Mutex` with an [`Arc`] rather than by borrowing it. Therefore, the `Mutex` must be
    /// wrapped in an `Arc` to call this method, and the guard will live for the `'static` lifetime,
    /// as it keeps the `Mutex` alive by holding an `Arc`.
    ///
    /// # Cancel safety
    ///
    /// This method uses a queue to fairly distribute locks in the order they were requested.
    /// Cancelling a call to `lock_owned` makes you lose your place in the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::mutex::Mutex;
    ///
    /// let mutex = Arc::new(Mutex::new(1));
    ///
    /// let mut n = mutex.clone().lock_owned().await;
    /// *n = 2;
    /// # }
    /// ```
    pub async fn lock_owned(self: Arc<Self>) -> OwnedMutexGuard<T> {
        self.s.acquire(1).await;
        OwnedMutexGuard { lock: self }
    }

    /// Attempts to acquire the lock, and returns `None` if the lock is currently held somewhere
    /// else.
    ///
    /// This method is identical to [`Mutex::try_lock`], except that the returned guard references
    /// the `Mutex` with an [`Arc`] rather than by borrowing it. Therefore, the `Mutex` must be
    /// wrapped in an `Arc` to call this method, and the guard will live for the `'static` lifetime,
    /// as it keeps the `Mutex` alive by holding an `Arc`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::mutex::Mutex;
    ///
    /// let mutex = Arc::new(Mutex::new(1));
    /// let mut guard = mutex.clone().try_lock_owned().expect("mutex is locked");
    /// *guard += 1;
    /// assert_eq!(2, *guard);
    /// ```
    pub fn try_lock_owned(self: Arc<Self>) -> Option<OwnedMutexGuard<T>> {
        if self.s.try_acquire(1) {
            let guard = OwnedMutexGuard { lock: self };
            Some(guard)
        } else {
            None
        }
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the `Mutex` mutably, no actual locking needs to take place: the
    /// mutable borrow statically guarantees no locks exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mutex::Mutex;
    ///
    /// let mut mutex = Mutex::new(1);
    /// let n = mutex.get_mut();
    /// *n = 2;
    /// ```
    pub fn get_mut(&mut self) -> &mut T {
        self.c.get_mut()
    }
}

/// RAII structure used to release the exclusive lock on a mutex when dropped.
///
/// This structure is created by the [`lock`] and [`try_lock`] methods on [`Mutex`].
///
/// [`lock`]: Mutex::lock
/// [`try_lock`]: Mutex::try_lock
///
/// See the [module level documentation](self) for more.
///
/// # Variance
///
/// The guard is invariant over `T`, as required for mutable access:
///
/// ```compile_fail
/// use asyncband::mutex::MutexGuard;
///
/// fn shorten<'lock, 'short: 'lock>(
///     guard: MutexGuard<'lock, &'static str>,
///     value: &'short str,
/// ) -> MutexGuard<'lock, &'short str> {
///     let mut guard: MutexGuard<'lock, &'short str> = guard;
///     *guard = value;
///     guard
/// }
/// ```
#[must_use = "if unused the Mutex will immediately unlock"]
pub struct MutexGuard<'a, T: ?Sized> {
    lock: &'a Mutex<T>,
}

#[cfg(feature = "condvar")]
pub(crate) fn guard_lock<'a, T: ?Sized>(guard: &MutexGuard<'a, T>) -> &'a Mutex<T> {
    guard.lock
}

unsafe impl<T: ?Sized + Send + Sync> Sync for MutexGuard<'_, T> {}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.s.release(1);
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.c.get() }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.c.get() }
    }
}

impl<'a, T: ?Sized> MutexGuard<'a, T> {
    /// Makes a new [`MappedMutexGuard`] for a component of the locked data.
    ///
    /// This operation cannot fail as the `MutexGuard` passed in already locked the mutex.
    ///
    /// This is an associated function that needs to be used as `MutexGuard::map(...)`. A method
    /// would interfere with methods of the same name on the contents of the locked data.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mutex::Mutex;
    /// use asyncband::mutex::MutexGuard;
    ///
    /// #[derive(Debug)]
    /// struct User {
    ///     id: u32,
    ///     profile: UserProfile,
    /// }
    ///
    /// #[derive(Debug)]
    /// struct UserProfile {
    ///     email: String,
    ///     name: String,
    /// }
    ///
    /// let user = User {
    ///     id: 1,
    ///     profile: UserProfile {
    ///         email: "user@example.com".to_owned(),
    ///         name: "Alice".to_owned(),
    ///     },
    /// };
    ///
    /// let mutex = Mutex::new(user);
    /// let guard = mutex.lock().await;
    ///
    /// // Map to only access the user's profile, allowing fine-grained locking
    /// let profile_guard = MutexGuard::map(guard, |user| &mut user.profile);
    /// assert_eq!(profile_guard.email, "user@example.com");
    /// # }
    /// ```
    pub fn map<U, F>(mut orig: Self, f: F) -> MappedMutexGuard<'a, U>
    where
        F: FnOnce(&mut T) -> &mut U,
        U: ?Sized,
    {
        let d = NonNull::from(f(&mut *orig));
        let orig = ManuallyDrop::new(orig);
        MappedMutexGuard {
            d,
            s: &orig.lock.s,
            variance: PhantomData,
        }
    }

    /// Attempts to make a new [`MappedMutexGuard`] for a component of the locked data. The
    /// original guard is returned if the closure returns `None`.
    ///
    /// This operation cannot fail as the `MutexGuard` passed in already locked the mutex.
    ///
    /// This is an associated function that needs to be used as `MutexGuard::filter_map(...)`.
    ///
    /// A method would interfere with methods of the same name on the contents of the locked data.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mutex::Mutex;
    /// use asyncband::mutex::MutexGuard;
    ///
    /// #[derive(Debug)]
    /// struct Database {
    ///     users: std::collections::HashMap<u32, String>,
    ///     admin_user_id: Option<u32>,
    /// }
    ///
    /// let mut db = Database {
    ///     users: std::collections::HashMap::new(),
    ///     admin_user_id: Some(1),
    /// };
    /// db.users.insert(1, "admin@example.com".to_owned());
    ///
    /// let mutex = Mutex::new(db);
    /// let guard = mutex.lock().await;
    ///
    /// // Try to map to admin user's email if admin exists
    /// let admin_email_guard = MutexGuard::filter_map(guard, |db| {
    ///     if let Some(admin_id) = db.admin_user_id {
    ///         db.users.get_mut(&admin_id)
    ///     } else {
    ///         None
    ///     }
    /// })
    /// .expect("admin user should exist");
    ///
    /// assert_eq!(&*admin_email_guard, "admin@example.com");
    /// # }
    /// ```
    pub fn filter_map<U, F>(mut orig: Self, f: F) -> Result<MappedMutexGuard<'a, U>, Self>
    where
        F: FnOnce(&mut T) -> Option<&mut U>,
        U: ?Sized,
    {
        match f(&mut *orig) {
            Some(d) => {
                let d = NonNull::from(d);
                let orig = ManuallyDrop::new(orig);
                Ok(MappedMutexGuard {
                    d,
                    s: &orig.lock.s,
                    variance: PhantomData,
                })
            }
            None => Err(orig),
        }
    }
}

/// An owned handle to a held `Mutex`.
///
/// This guard is only available from a [`Mutex`] that is wrapped in an [`Arc`]. It is identical to
/// [`MutexGuard`], except that rather than borrowing the `Mutex`, it clones the `Arc`, incrementing
/// the reference count. This means that unlike `MutexGuard`, it will have the `'static` lifetime.
///
/// As long as you have this guard, you have exclusive access to the underlying `T`. The guard
/// internally keeps a reference-counted pointer to the original `Mutex`, so even if the lock goes
/// away, the guard remains valid.
///
/// The lock is automatically released whenever the guard is dropped, at which point `lock` will
/// succeed yet again.
///
/// See the [module level documentation](self) for more.
///
/// # Variance
///
/// The guard is invariant over `T`, as required for mutable access:
///
/// ```compile_fail
/// use asyncband::mutex::OwnedMutexGuard;
///
/// fn shorten<'short>(
///     guard: OwnedMutexGuard<&'static str>,
///     value: &'short str,
/// ) -> OwnedMutexGuard<&'short str> {
///     let mut guard: OwnedMutexGuard<&'short str> = guard;
///     *guard = value;
///     guard
/// }
/// ```
#[must_use = "if unused the Mutex will immediately unlock"]
pub struct OwnedMutexGuard<T: ?Sized> {
    lock: Arc<Mutex<T>>,
}

#[cfg(feature = "condvar")]
pub(crate) fn owned_guard_lock<T: ?Sized>(guard: &OwnedMutexGuard<T>) -> Arc<Mutex<T>> {
    guard.lock.clone()
}

unsafe impl<T: ?Sized + Send + Sync> Sync for OwnedMutexGuard<T> {}

impl<T: ?Sized> Drop for OwnedMutexGuard<T> {
    fn drop(&mut self) {
        self.lock.s.release(1);
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for OwnedMutexGuard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for OwnedMutexGuard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized> Deref for OwnedMutexGuard<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.c.get() }
    }
}

impl<T: ?Sized> DerefMut for OwnedMutexGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.c.get() }
    }
}

impl<T: ?Sized> OwnedMutexGuard<T> {
    /// Makes a new [`OwnedMappedMutexGuard`] for a component of the locked data.
    ///
    /// This operation cannot fail as the `OwnedMutexGuard` passed in already locked the mutex.
    ///
    /// This is an associated function that needs to be used as `OwnedMutexGuard::map(...)`. A
    /// method would interfere with methods of the same name on the contents of the locked data.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::mutex::Mutex;
    /// use asyncband::mutex::OwnedMutexGuard;
    ///
    /// struct Config {
    ///     name: String,
    ///     value: u32,
    /// }
    ///
    /// let config = Config {
    ///     name: "front size".to_owned(),
    ///     value: 42,
    /// };
    ///
    /// let mutex = Arc::new(Mutex::new(config));
    /// let guard = mutex.clone().lock_owned().await;
    ///
    /// // Map to access only the value field
    /// let value_guard = OwnedMutexGuard::map(guard, |config| &mut config.value);
    /// assert_eq!(*value_guard, 42);
    /// # }
    /// ```
    pub fn map<U, F>(mut orig: Self, f: F) -> OwnedMappedMutexGuard<T, U>
    where
        F: FnOnce(&mut T) -> &mut U,
        U: ?Sized,
    {
        let d = NonNull::from(f(&mut *orig));

        let guard = ManuallyDrop::new(orig);

        let lock = unsafe { std::ptr::read(&guard.lock) };

        OwnedMappedMutexGuard {
            lock,
            d,
            variance: PhantomData,
        }
    }

    /// Attempts to make a new [`OwnedMappedMutexGuard`] for a component of the locked data. The
    /// original guard is returned if the closure returns `None`.
    ///
    /// This operation cannot fail as the `OwnedMutexGuard` passed in already locked the mutex.
    ///
    /// This is an associated function that needs to be used as `OwnedMutexGuard::filter_map(...)`.
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
    /// use asyncband::mutex::Mutex;
    /// use asyncband::mutex::OwnedMutexGuard;
    ///
    /// let data = vec![1, 2, 3, 4, 5];
    /// let mutex = Arc::new(Mutex::new(data));
    /// let guard = mutex.clone().lock_owned().await;
    ///
    /// // Map to the first element
    /// let first_guard =
    ///     OwnedMutexGuard::filter_map(guard, |vec| vec.get_mut(0)).expect("vec should not be empty");
    ///
    /// assert_eq!(*first_guard, 1);
    /// # }
    /// ```
    pub fn filter_map<U, F>(mut orig: Self, f: F) -> Result<OwnedMappedMutexGuard<T, U>, Self>
    where
        F: FnOnce(&mut T) -> Option<&mut U>,
        U: ?Sized,
    {
        match f(&mut *orig) {
            Some(d) => {
                let d = NonNull::from(d);
                let guard = ManuallyDrop::new(orig);

                // SAFETY: We safely extract the Arc from the ManuallyDrop guard
                let lock = unsafe { std::ptr::read(&guard.lock) };

                Ok(OwnedMappedMutexGuard {
                    lock,
                    d,
                    variance: PhantomData,
                })
            }
            None => Err(orig),
        }
    }
}

/// RAII structure used to release the exclusive lock on a mutex when dropped, for a mapped
/// component of the locked data.
///
/// This structure is created by the [`map`] and [`filter_map`] methods on [`MutexGuard`]. It allows
/// you to hold a lock on a subfield of the protected data, enabling more fine-grained access
/// control while maintaining the same locking semantics.
///
/// As long as you have this guard, you have exclusive access to the underlying `T`. The guard
/// internally keeps a reference to the original mutex's semaphore, so the original lock is
/// maintained until this guard is dropped.
///
/// `MappedMutexGuard` implements [`Send`] and [`Sync`]
/// when the underlying data type supports these traits, allowing it to be used across task
/// boundaries and shared between threads safely.
///
/// See the [module level documentation](self) for more.
///
/// [`map`]: MutexGuard::map
/// [`filter_map`]: MutexGuard::filter_map
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use asyncband::mutex::Mutex;
/// use asyncband::mutex::MutexGuard;
///
/// #[derive(Debug)]
/// struct User {
///     id: u32,
///     profile: UserProfile,
/// }
///
/// #[derive(Debug)]
/// struct UserProfile {
///     email: String,
///     name: String,
/// }
///
/// let user = User {
///     id: 1,
///     profile: UserProfile {
///         email: "user@example.com".to_owned(),
///         name: "Alice".to_owned(),
///     },
/// };
///
/// let mutex = Mutex::new(user);
/// let guard = mutex.lock().await;
/// let profile_guard = MutexGuard::map(guard, |user| &mut user.profile);
///
/// // Now we can only access the user's profile
/// assert_eq!(profile_guard.email, "user@example.com");
/// # }
/// ```
///
/// # Variance
///
/// The guard is invariant over `T`, as required for mutable access:
///
/// ```compile_fail
/// use asyncband::mutex::MappedMutexGuard;
///
/// fn shorten<'lock, 'short: 'lock>(
///     guard: MappedMutexGuard<'lock, &'static str>,
///     value: &'short str,
/// ) -> MappedMutexGuard<'lock, &'short str> {
///     let mut guard: MappedMutexGuard<'lock, &'short str> = guard;
///     *guard = value;
///     guard
/// }
/// ```
#[must_use = "if unused the Mutex will immediately unlock"]
pub struct MappedMutexGuard<'a, T: ?Sized> {
    /// Non-null pointer to the mapped data
    d: NonNull<T>,
    /// Reference to the original mutex's semaphore, used for releasing the lock
    s: &'a semaphore::Semaphore,
    // Mutable access requires invariance over T.
    variance: PhantomData<&'a mut T>,
}

// SAFETY: MappedMutexGuard can be safely sent between threads when T: Send.
// The guard holds exclusive access to the data protected by the mutex lock,
// and the NonNull<T> pointer remains valid for the guard's lifetime.
// This is essential for async tasks that may be moved between threads at .await points.
unsafe impl<T: ?Sized + Send> Send for MappedMutexGuard<'_, T> {}

// SAFETY: MappedMutexGuard can be safely shared between threads (Sync) when T: Sync.
// Through &MappedMutexGuard, you can only get &T, so if T itself allows sharing references
// across threads, then sharing MappedMutexGuard references is also safe.
unsafe impl<T: ?Sized + Sync> Sync for MappedMutexGuard<'_, T> {}

impl<T: ?Sized> Drop for MappedMutexGuard<'_, T> {
    fn drop(&mut self) {
        self.s.release(1);
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MappedMutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for MappedMutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized> Deref for MappedMutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // SAFETY: we hold the lock and the NonNull pointer is valid for the guard's lifetime
        unsafe { self.d.as_ref() }
    }
}

impl<T: ?Sized> DerefMut for MappedMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: we hold the lock and the NonNull pointer is valid for the guard's lifetime
        unsafe { self.d.as_mut() }
    }
}

impl<'a, T: ?Sized> MappedMutexGuard<'a, T> {
    /// Makes a new [`MappedMutexGuard`] for a component of the locked data.
    ///
    /// This operation cannot fail as the `MappedMutexGuard` passed in already locked the mutex.
    ///
    /// This is an associated function that needs to be used as `MappedMutexGuard::map(...)`. A
    /// method would interfere with methods of the same name on the contents of the locked data.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mutex::MappedMutexGuard;
    /// use asyncband::mutex::Mutex;
    /// use asyncband::mutex::MutexGuard;
    ///
    /// #[derive(Debug)]
    /// struct User {
    ///     id: u32,
    ///     profile: UserProfile,
    /// }
    ///
    /// #[derive(Debug)]
    /// struct UserProfile {
    ///     email: String,
    ///     name: String,
    /// }
    ///
    /// let user = User {
    ///     id: 1,
    ///     profile: UserProfile {
    ///         email: "user@example.com".to_owned(),
    ///         name: "Alice".to_owned(),
    ///     },
    /// };
    ///
    /// let mutex = Mutex::new(user);
    /// let guard = mutex.lock().await;
    ///
    /// // First map to user profile
    /// let profile_guard = MutexGuard::map(guard, |user| &mut user.profile);
    /// // Then map to the email field specifically
    /// let email_guard = MappedMutexGuard::map(profile_guard, |profile| &mut profile.email);
    ///
    /// assert_eq!(&*email_guard, "user@example.com");
    /// # }
    /// ```
    pub fn map<U, F>(mut orig: Self, f: F) -> MappedMutexGuard<'a, U>
    where
        F: FnOnce(&mut T) -> &mut U,
        U: ?Sized,
    {
        // Use DerefMut to safely get mutable reference, avoiding explicit unsafe block
        let d = NonNull::from(f(&mut *orig));
        let orig = ManuallyDrop::new(orig);
        MappedMutexGuard {
            d,
            s: orig.s,
            variance: PhantomData,
        }
    }

    /// Attempts to make a new [`MappedMutexGuard`] for a component of the locked data. The
    /// original guard is returned if the closure returns `None`.
    ///
    /// This operation cannot fail as the `MappedMutexGuard` passed in already locked the mutex.
    ///
    /// This is an associated function that needs to be used as `MappedMutexGuard::filter_map(...)`.
    /// A method would interfere with methods of the same name on the contents of the locked
    /// data.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mutex::MappedMutexGuard;
    /// use asyncband::mutex::Mutex;
    /// use asyncband::mutex::MutexGuard;
    ///
    /// #[derive(Debug)]
    /// struct Data {
    ///     id: u32,
    ///     value: Option<String>,
    /// }
    ///
    /// let data = Data {
    ///     id: 1,
    ///     value: Some("hello".to_owned()),
    /// };
    ///
    /// let mutex = Mutex::new(data);
    /// let guard = mutex.lock().await;
    ///
    /// // First map to the value field
    /// let value_guard = MutexGuard::map(guard, |data| &mut data.value);
    /// // Then try to map to the inner string if it exists
    /// let string_guard =
    ///     MappedMutexGuard::filter_map(value_guard, |opt| opt.as_mut()).expect("value should exist");
    ///
    /// assert_eq!(&*string_guard, "hello");
    /// # }
    /// ```
    pub fn filter_map<U, F>(mut orig: Self, f: F) -> Result<MappedMutexGuard<'a, U>, Self>
    where
        F: FnOnce(&mut T) -> Option<&mut U>,
        U: ?Sized,
    {
        // Use DerefMut to safely get mutable reference, avoiding explicit unsafe block
        match f(&mut *orig) {
            Some(d) => {
                let d = NonNull::from(d);
                let orig = ManuallyDrop::new(orig);
                Ok(MappedMutexGuard {
                    d,
                    s: orig.s,
                    variance: PhantomData,
                })
            }
            None => Err(orig),
        }
    }
}

/// An owned handle to a held `Mutex` for a mapped component of the locked data.
///
/// This guard is only available from a [`Mutex`] that is wrapped in an [`Arc`]. It is similar to
/// [`MappedMutexGuard`], except that rather than borrowing the `Mutex`, it clones the `Arc`,
/// incrementing the reference count. This means that unlike `MappedMutexGuard`, it will have
/// the `'static` lifetime.
///
/// This structure is created by the [`map`] and [`filter_map`] methods on [`OwnedMutexGuard`].
/// It allows you to hold a lock on a subfield of the protected data, enabling more fine-grained
/// access control while maintaining the same locking semantics.
///
/// As long as you have this guard, you have exclusive access to the underlying `U`. The guard
/// internally keeps a reference-counted pointer to the original `Mutex`, so even if the lock goes
/// away, the guard remains valid.
///
/// The lock is automatically released whenever the guard is dropped, at which point `lock` will
/// succeed yet again.
///
/// See the [module level documentation](self) for more.
///
/// [`map`]: OwnedMutexGuard::map
/// [`filter_map`]: OwnedMutexGuard::filter_map
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use std::sync::Arc;
///
/// use asyncband::mutex::Mutex;
/// use asyncband::mutex::OwnedMutexGuard;
///
/// struct Data {
///     value: u32,
/// }
///
/// let data = Data { value: 42 };
/// let mutex = Arc::new(Mutex::new(data));
/// let guard = mutex.clone().lock_owned().await;
/// let value_guard = OwnedMutexGuard::map(guard, |data| &mut data.value);
///
/// assert_eq!(*value_guard, 42);
/// # }
/// ```
///
/// # Variance
///
/// The guard is invariant over its mapped type `U`, as required for mutable access:
///
/// ```compile_fail
/// use asyncband::mutex::OwnedMappedMutexGuard;
///
/// fn shorten<'short>(
///     guard: OwnedMappedMutexGuard<(), &'static str>,
///     value: &'short str,
/// ) -> OwnedMappedMutexGuard<(), &'short str> {
///     let mut guard: OwnedMappedMutexGuard<(), &'short str> = guard;
///     *guard = value;
///     guard
/// }
/// ```
#[must_use = "if unused the Mutex will immediately unlock"]
pub struct OwnedMappedMutexGuard<T: ?Sized, U: ?Sized> {
    // This Arc acts as an ownership certificate, ensuring the Mutex remains valid
    // and the lock is not released
    lock: Arc<Mutex<T>>,
    // This NonNull pointer precisely points to the subfield U, telling us which
    // memory location we can operate on, with compile-time guarantee of non-null
    d: NonNull<U>,
    // Mutable access requires invariance over U.
    variance: PhantomData<*mut U>,
}

// SAFETY: OwnedMappedMutexGuard can be safely sent between threads when T: Send and U: Send.
// It holds exclusive access to the data protected by the mutex lock, and the raw pointer
// remains valid for the guard's lifetime. This is essential for async tasks that may be
// moved between threads at .await points.
unsafe impl<T: ?Sized + Send, U: ?Sized + Send> Send for OwnedMappedMutexGuard<T, U> {}

// SAFETY: OwnedMappedMutexGuard can be safely shared between threads (Sync) when T: Send + Sync and
// U: Send + Sync. Through &OwnedMappedMutexGuard, you can only get &U, so if U itself allows
// sharing references across threads, then sharing OwnedMappedMutexGuard references is also safe.
// We require T: Send + Sync for maximum safety and ecosystem compatibility.
unsafe impl<T: ?Sized + Send + Sync, U: ?Sized + Send + Sync> Sync for OwnedMappedMutexGuard<T, U> {}

impl<T: ?Sized, U: ?Sized> Drop for OwnedMappedMutexGuard<T, U> {
    fn drop(&mut self) {
        // Release the lock by calling release on the semaphore
        self.lock.s.release(1);
    }
}

impl<T: ?Sized, U: ?Sized + fmt::Debug> fmt::Debug for OwnedMappedMutexGuard<T, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized, U: ?Sized + fmt::Display> fmt::Display for OwnedMappedMutexGuard<T, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized, U: ?Sized> Deref for OwnedMappedMutexGuard<T, U> {
    type Target = U;
    fn deref(&self) -> &Self::Target {
        // SAFETY: we hold the lock and the NonNull pointer is valid for the guard's lifetime
        // The Arc ensures the underlying data remains valid
        unsafe { self.d.as_ref() }
    }
}

impl<T: ?Sized, U: ?Sized> DerefMut for OwnedMappedMutexGuard<T, U> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: we hold the lock and the NonNull pointer is valid for the guard's lifetime
        // The Arc ensures the underlying data remains valid
        unsafe { self.d.as_mut() }
    }
}

impl<T: ?Sized, U: ?Sized> OwnedMappedMutexGuard<T, U> {
    /// Makes a new [`OwnedMappedMutexGuard`] for a component of the locked data.
    ///
    /// This operation cannot fail as the `OwnedMappedMutexGuard` passed in already locked the
    /// mutex.
    ///
    /// This is an associated function that needs to be used as `OwnedMappedMutexGuard::map(...)`. A
    /// method would interfere with methods of the same name on the contents of the locked data.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::mutex::Mutex;
    /// use asyncband::mutex::OwnedMappedMutexGuard;
    /// use asyncband::mutex::OwnedMutexGuard;
    ///
    /// #[derive(Debug)]
    /// struct Config {
    ///     host: String,
    ///     port: u16,
    /// }
    ///
    /// let config = Config {
    ///     host: "localhost".to_owned(),
    ///     port: 8080,
    /// };
    ///
    /// let mutex = Arc::new(Mutex::new(config));
    /// let guard = mutex.clone().lock_owned().await;
    ///
    /// // First map to config
    /// let config_guard = OwnedMutexGuard::map(guard, |config| &mut config.host);
    /// // Then map to the host string specifically
    /// let host_guard = OwnedMappedMutexGuard::map(config_guard, |host| host.as_mut_str());
    ///
    /// assert_eq!(&*host_guard, "localhost");
    /// # }
    /// ```
    pub fn map<V, F>(mut orig: Self, f: F) -> OwnedMappedMutexGuard<T, V>
    where
        F: FnOnce(&mut U) -> &mut V,
        V: ?Sized,
    {
        // Use DerefMut to maintain consistency with other map implementations
        let d = NonNull::from(f(&mut *orig));
        let orig = ManuallyDrop::new(orig);

        // SAFETY: We safely extract the Arc from the ManuallyDrop guard
        let lock = unsafe { std::ptr::read(&orig.lock) };

        OwnedMappedMutexGuard {
            lock,
            d,
            variance: PhantomData,
        }
    }

    /// Attempts to make a new [`OwnedMappedMutexGuard`] for a component of the locked data. The
    /// original guard is returned if the closure returns `None`.
    ///
    /// This operation cannot fail as the `OwnedMappedMutexGuard` passed in already locked the
    /// mutex.
    ///
    /// This is an associated function that needs to be used as
    /// `OwnedMappedMutexGuard::filter_map(...)`. A method would interfere with methods of the same
    /// name on the contents of the locked data.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::mutex::Mutex;
    /// use asyncband::mutex::OwnedMappedMutexGuard;
    /// use asyncband::mutex::OwnedMutexGuard;
    ///
    /// #[derive(Debug)]
    /// struct Node {
    ///     value: i32,
    ///     left: Option<Box<Node>>,
    ///     right: Option<Box<Node>>,
    /// }
    ///
    /// let node = Node {
    ///     value: 10,
    ///     left: Some(Box::new(Node {
    ///         value: 5,
    ///         left: None,
    ///         right: None,
    ///     })),
    ///     right: None,
    /// };
    ///
    /// let mutex = Arc::new(Mutex::new(node));
    /// let guard = mutex.clone().lock_owned().await;
    ///
    /// // First map to left child
    /// let left_guard = OwnedMutexGuard::map(guard, |node| &mut node.left);
    /// // Try to access the left child if it exists
    /// let child_guard = OwnedMappedMutexGuard::filter_map(left_guard, |left| {
    ///     left.as_mut().map(|boxed| boxed.as_mut())
    /// })
    /// .expect("left child should exist");
    ///
    /// assert_eq!(child_guard.value, 5);
    /// # }
    /// ```
    pub fn filter_map<V, F>(mut orig: Self, f: F) -> Result<OwnedMappedMutexGuard<T, V>, Self>
    where
        F: FnOnce(&mut U) -> Option<&mut V>,
        V: ?Sized,
    {
        // Use DerefMut to maintain consistency with other filter_map implementations
        match f(&mut *orig) {
            Some(d) => {
                let d = NonNull::from(d);
                let orig = ManuallyDrop::new(orig);

                // SAFETY: We safely extract the Arc from the ManuallyDrop guard
                let lock = unsafe { std::ptr::read(&orig.lock) };

                Ok(OwnedMappedMutexGuard {
                    lock,
                    d,
                    variance: PhantomData,
                })
            }
            None => Err(orig),
        }
    }
}
