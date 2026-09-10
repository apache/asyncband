// This file contains code derived from Tokio 1.42.0's RwLock implementation.
// Copyright (c) Tokio Contributors
// The derived code remains licensed under the MIT License.
// The incorporated code has been modified for use in Apache Asyncband.
// Upstream source:
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/rwlock/write_guard_mapped.rs

use std::fmt;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;

use crate::internal::semaphore;
use crate::rwlock::MappedRwLockReadGuard;

/// A borrowed write guard projected to one component of the protected value.
///
/// [`RwLockWriteGuard::map`](crate::rwlock::RwLockWriteGuard::map) and
/// [`RwLockWriteGuard::filter_map`](crate::rwlock::RwLockWriteGuard::filter_map) create this guard.
/// It keeps the original write access active while exposing only the projected component.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use asyncband::rwlock::RwLock;
/// use asyncband::rwlock::RwLockWriteGuard;
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
/// let rwlock = RwLock::new(user);
/// let mut guard = rwlock.write().await;
/// let mut profile_guard = RwLockWriteGuard::map(guard, |user| &mut user.profile);
///
/// // Now we can only access and modify the user's profile
/// profile_guard.email = "newemail@example.com".to_owned();
/// assert_eq!(profile_guard.email, "newemail@example.com");
/// # }
/// ```
#[must_use = "dropping the guard releases its write access immediately"]
pub struct MappedRwLockWriteGuard<'a, T: ?Sized> {
    d: NonNull<T>,
    s: &'a semaphore::Semaphore,
    permits_acquired: usize,
    // Mutable access requires invariance over T.
    variance: PhantomData<&'a mut T>,
}

// SAFETY: A `&MappedRwLockWriteGuard` can be safely shared between threads because it provides
// exclusive access to the data, and the `T: Send + Sync` bound prevents data races.
unsafe impl<T: ?Sized + Send + Sync> Sync for MappedRwLockWriteGuard<'_, T> {}

// SAFETY: `MappedRwLockWriteGuard` owns the lock and can be safely sent to another thread.
// The `T: Send` bound ensures that the data can be safely accessed by the new thread,
// and the guard's lifetime guarantees that the data remains valid.
unsafe impl<T: ?Sized + Send> Send for MappedRwLockWriteGuard<'_, T> {}

impl<'a, T: ?Sized> MappedRwLockWriteGuard<'a, T> {
    pub(crate) fn new(d: NonNull<T>, s: &'a semaphore::Semaphore, permits_acquired: usize) -> Self {
        Self {
            d,
            s,
            permits_acquired,
            variance: PhantomData,
        }
    }
}

impl<T: ?Sized> Drop for MappedRwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        self.s.release(self.permits_acquired);
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MappedRwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for MappedRwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized> Deref for MappedRwLockWriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // SAFETY: we hold the write lock and the NonNull pointer is valid for the guard's lifetime
        unsafe { self.d.as_ref() }
    }
}

impl<T: ?Sized> DerefMut for MappedRwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: we hold the write lock and the NonNull pointer is valid for the guard's lifetime
        unsafe { self.d.as_mut() }
    }
}

impl<'a, T: ?Sized> MappedRwLockWriteGuard<'a, T> {
    /// Projects this guard to a deeper mutable component.
    ///
    /// The returned guard keeps the same write access active. Call this as
    /// `MappedRwLockWriteGuard::map(...)` so a method named `map` on `T` remains accessible.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::rwlock::MappedRwLockWriteGuard;
    /// use asyncband::rwlock::RwLock;
    /// use asyncband::rwlock::RwLockWriteGuard;
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
    /// let rwlock = RwLock::new(user);
    /// let mut guard = rwlock.write().await;
    /// // First map to the profile field
    /// let mut profile_guard = RwLockWriteGuard::map(guard, |user| &mut user.profile);
    /// // Then map to the email field specifically
    /// let mut email_guard = MappedRwLockWriteGuard::map(profile_guard, |profile| &mut profile.email);
    ///
    /// *email_guard = "newemail@example.com".to_owned();
    /// assert_eq!(&*email_guard, "newemail@example.com");
    /// # }
    /// ```
    pub fn map<U, F>(mut orig: Self, f: F) -> MappedRwLockWriteGuard<'a, U>
    where
        F: FnOnce(&mut T) -> &mut U,
        U: ?Sized,
    {
        // SAFETY: orig.d is a valid NonNull<T> pointer that was created from a valid reference
        // when the original MappedRwLockWriteGuard was constructed. The guard guarantees exclusive
        // access to the data through the rwlock, so dereferencing is safe.
        let d = NonNull::from(f(unsafe { orig.d.as_mut() }));
        let permits_acquired = orig.permits_acquired;
        let orig = ManuallyDrop::new(orig);
        MappedRwLockWriteGuard::new(d, orig.s, permits_acquired)
    }

    /// Attempts to project this guard to a deeper mutable component.
    ///
    /// The original guard is returned when `f` returns `None`. Call this as
    /// `MappedRwLockWriteGuard::filter_map(...)` so a method with the same name on `T` remains
    /// accessible.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::rwlock::MappedRwLockWriteGuard;
    /// use asyncband::rwlock::RwLock;
    /// use asyncband::rwlock::RwLockWriteGuard;
    ///
    /// #[derive(Debug)]
    /// struct Document {
    ///     title: String,
    ///     content: String,
    ///     metadata: Option<Metadata>,
    /// }
    ///
    /// #[derive(Debug)]
    /// struct Metadata {
    ///     author: String,
    ///     version: Option<u32>,
    /// }
    ///
    /// let doc = Document {
    ///     title: "My Document".to_owned(),
    ///     content: "Initial content".to_owned(),
    ///     metadata: Some(Metadata {
    ///         author: "Alice".to_owned(),
    ///         version: Some(1),
    ///     }),
    /// };
    ///
    /// let rwlock = RwLock::new(doc);
    /// let mut guard = rwlock.write().await;
    ///
    /// // First map to the metadata field
    /// let meta_guard = RwLockWriteGuard::map(guard, |doc| &mut doc.metadata);
    ///
    /// // Try to map to the version number if metadata and version both exist
    /// let version_result = MappedRwLockWriteGuard::filter_map(meta_guard, |meta_opt| {
    ///     meta_opt.as_mut()?.version.as_mut()
    /// });
    /// match version_result {
    ///     Ok(mut version_guard) => {
    ///         *version_guard += 1; // Increment version
    ///         assert_eq!(*version_guard, 2);
    ///     }
    ///     Err(_) => {
    ///         // Handle case where metadata or version doesn't exist
    ///         println!("No version to update");
    ///     }
    /// }
    /// # }
    /// ```
    pub fn filter_map<U, F>(mut orig: Self, f: F) -> Result<MappedRwLockWriteGuard<'a, U>, Self>
    where
        F: FnOnce(&mut T) -> Option<&mut U>,
        U: ?Sized,
    {
        // SAFETY: orig.d is a valid NonNull<T> pointer that was created from a valid reference
        // when the original MappedRwLockWriteGuard was constructed. The guard guarantees exclusive
        // access to the data through the rwlock, so dereferencing is safe.
        match f(unsafe { orig.d.as_mut() }) {
            Some(d) => {
                let d = NonNull::from(d);
                let permits_acquired = orig.permits_acquired;
                let orig = ManuallyDrop::new(orig);
                Ok(MappedRwLockWriteGuard::new(d, orig.s, permits_acquired))
            }
            None => Err(orig),
        }
    }

    /// Atomically downgrades the write lock to a read lock while preserving the mapping.
    ///
    /// This method changes the lock from exclusive mode to shared mode atomically,
    /// preventing other writers from acquiring the lock in between.
    ///
    /// The returned `MappedRwLockReadGuard` preserves the original mapping to the specific
    /// component of the data.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::rwlock::RwLock;
    /// use asyncband::rwlock::RwLockWriteGuard;
    ///
    /// #[derive(Debug)]
    /// struct Counter {
    ///     value: i32,
    ///     name: String,
    /// }
    ///
    /// let lock = Arc::new(RwLock::new(Counter {
    ///     value: 0,
    ///     name: "counter".to_owned(),
    /// }));
    ///
    /// let write_guard = lock.write().await;
    /// let mut value_write_guard = RwLockWriteGuard::map(write_guard, |counter| &mut counter.value);
    /// *value_write_guard = 42;
    ///
    /// let value_read_guard = value_write_guard.downgrade();
    /// assert_eq!(*value_read_guard, 42);
    ///
    /// assert!(lock.try_write().is_none());
    ///
    /// drop(value_read_guard);
    /// assert!(lock.try_write().is_some());
    /// # }
    /// ```
    pub fn downgrade(self) -> MappedRwLockReadGuard<'a, T> {
        // Prevent the original write guard from running its Drop implementation,
        // which would release all permits. This must be done BEFORE any operation
        // that might panic to ensure panic safety.
        let guard = ManuallyDrop::new(self);

        // Release max_readers - 1 permits to convert the write lock to a read lock.
        guard.s.release(guard.permits_acquired - 1);

        // Create the mapped read guard with 1 permit (standard for read locks)
        MappedRwLockReadGuard::new(guard.d, guard.s)
    }
}
