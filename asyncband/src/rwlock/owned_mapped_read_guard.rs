// Portions derived from Tokio 1.42.0's RwLock implementation.
// Copyright (c) Tokio Contributors
// Licensed under the MIT License.
// Modified by the Apache Software Foundation.
// See the project LICENSE file for the exact upstream revision and source paths.

use std::fmt;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::rwlock::RwLock;

/// Owned RAII structure used to release the shared read access of a lock when dropped, for a mapped
/// component of the locked data.
///
/// This guard is only available from a [`RwLock`] that is wrapped in an [`Arc`]. It is similar to
/// [`MappedRwLockReadGuard`], except that rather than borrowing the `RwLock`, it clones the `Arc`,
/// incrementing the reference count. This means that unlike `MappedRwLockReadGuard`, it will have
/// the `'static` lifetime.
///
/// As long as you have this guard, you have shared read access to the underlying `U`. The guard
/// internally keeps an `Arc` reference to the original rwlock, so the original lock is
/// maintained until this guard is dropped.
///
/// `OwnedMappedRwLockReadGuard` implements [`Send`] and [`Sync`]
/// when the underlying data type supports these traits, allowing it to be used across task
/// boundaries and shared between threads safely.
///
/// [`map`]: crate::rwlock::OwnedRwLockReadGuard::map
/// [`filter_map`]: crate::rwlock::OwnedRwLockReadGuard::filter_map
/// [`OwnedRwLockReadGuard`]: crate::rwlock::OwnedRwLockReadGuard
/// [`MappedRwLockReadGuard`]: crate::rwlock::MappedRwLockReadGuard
///
/// See the [module level documentation](crate::rwlock) for more.
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
/// let rwlock = Arc::new(RwLock::new(user));
/// let guard = rwlock.read_owned().await;
/// let profile_guard = OwnedRwLockReadGuard::map(guard, |user| &user.profile);
///
/// // Now we can only access the user's profile
/// assert_eq!(profile_guard.email, "user@example.com");
/// # }
/// ```
#[must_use = "if unused the RwLock will immediately unlock"]
pub struct OwnedMappedRwLockReadGuard<T: ?Sized, U: ?Sized> {
    // This Arc acts as an ownership certificate, ensuring the RwLock remains valid
    // and the lock is not released
    lock: Arc<RwLock<T>>,
    // This NonNull pointer precisely points to the subfield U, telling us which
    // memory location we can operate on
    d: NonNull<U>,
    variance: PhantomData<fn() -> U>,
}

// SAFETY: Arc<RwLock<T>> is Send when T: Send + Sync, and we only provide shared access (&U)
// through deref(), so U: Sync is sufficient for safe cross-thread transfer.
unsafe impl<T: ?Sized + Send + Sync, U: ?Sized + Sync> Send for OwnedMappedRwLockReadGuard<T, U> {}

// SAFETY: OwnedMappedRwLockReadGuard can be safely shared between threads when T: Send + Sync and
// U: Sync. Multiple threads can hold &OwnedMappedRwLockReadGuard and call deref() concurrently,
// which only returns &U.
unsafe impl<T: ?Sized + Send + Sync, U: ?Sized + Sync> Sync for OwnedMappedRwLockReadGuard<T, U> {}

impl<T: ?Sized, U: ?Sized> OwnedMappedRwLockReadGuard<T, U> {
    pub(crate) fn new(d: NonNull<U>, lock: Arc<RwLock<T>>) -> Self {
        Self {
            d,
            lock,
            variance: PhantomData,
        }
    }
}
impl<T: ?Sized, U: ?Sized> Drop for OwnedMappedRwLockReadGuard<T, U> {
    fn drop(&mut self) {
        self.lock.s.release(1);
    }
}

impl<T: ?Sized, U: ?Sized + fmt::Debug> fmt::Debug for OwnedMappedRwLockReadGuard<T, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized, U: ?Sized + fmt::Display> fmt::Display for OwnedMappedRwLockReadGuard<T, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized, U: ?Sized> Deref for OwnedMappedRwLockReadGuard<T, U> {
    type Target = U;
    fn deref(&self) -> &Self::Target {
        // SAFETY: we hold the read lock and the NonNull pointer is valid for the guard's lifetime
        unsafe { self.d.as_ref() }
    }
}

impl<T: ?Sized, U: ?Sized> OwnedMappedRwLockReadGuard<T, U> {
    /// Makes a new [`OwnedMappedRwLockReadGuard`] for a component of the locked data.
    ///
    /// This operation cannot fail as the `OwnedMappedRwLockReadGuard` passed in already locked the
    /// rwlock.
    ///
    /// This is an associated function that needs to be used as
    /// `OwnedMappedRwLockReadGuard::map(...)`.
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
    /// use asyncband::rwlock::OwnedMappedRwLockReadGuard;
    /// use asyncband::rwlock::OwnedRwLockReadGuard;
    /// use asyncband::rwlock::RwLock;
    ///
    /// #[derive(Debug)]
    /// struct ServerStats {
    ///     uptime: u64,
    ///     connection_info: ConnectionInfo,
    /// }
    ///
    /// #[derive(Debug)]
    /// struct ConnectionInfo {
    ///     active_connections: u32,
    ///     max_connections: u32,
    /// }
    ///
    /// let stats = ServerStats {
    ///     uptime: 86400, // 1 day in seconds
    ///     connection_info: ConnectionInfo {
    ///         active_connections: 150,
    ///         max_connections: 1000,
    ///     },
    /// };
    ///
    /// let rwlock = Arc::new(RwLock::new(stats));
    /// let guard = rwlock.read_owned().await;
    /// // Map to connection info for cross-task monitoring
    /// let conn_guard = OwnedRwLockReadGuard::map(guard, |stats| &stats.connection_info);
    /// // Further map to active connections count
    /// let active_guard = OwnedMappedRwLockReadGuard::map(conn_guard, |conn| &conn.active_connections);
    ///
    /// assert_eq!(*active_guard, 150);
    /// # }
    /// ```
    pub fn map<V, F>(orig: Self, f: F) -> OwnedMappedRwLockReadGuard<T, V>
    where
        F: FnOnce(&U) -> &V,
        V: ?Sized,
    {
        // SAFETY: orig.d is a valid NonNull<U> pointer that was created from a valid reference
        // when the original OwnedMappedRwLockReadGuard was constructed. The guard guarantees shared
        // access to the data through the rwlock, so dereferencing is safe.
        let d = NonNull::from(f(unsafe { orig.d.as_ref() }));
        let orig = ManuallyDrop::new(orig);

        // SAFETY: The original guard is wrapped in `ManuallyDrop` and will not be dropped.
        // This allows us to safely move the `Arc` out of it and transfer ownership to the new
        // guard.
        let lock = unsafe { std::ptr::read(&orig.lock) };

        OwnedMappedRwLockReadGuard::new(d, lock)
    }

    /// Attempts to make a new [`OwnedMappedRwLockReadGuard`] for a component of the locked data.
    /// The original guard is returned if the closure returns `None`.
    ///
    /// This operation cannot fail as the `OwnedMappedRwLockReadGuard` passed in already locked the
    /// rwlock.
    ///
    /// This is an associated function that needs to be used as
    /// `OwnedMappedRwLockReadGuard::filter_map(...)`.
    ///
    /// A method would interfere with methods of the same name on the contents of the locked data.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::collections::HashMap;
    /// use std::sync::Arc;
    ///
    /// use asyncband::rwlock::OwnedMappedRwLockReadGuard;
    /// use asyncband::rwlock::OwnedRwLockReadGuard;
    /// use asyncband::rwlock::RwLock;
    ///
    /// #[derive(Debug)]
    /// struct Cache {
    ///     entries: HashMap<String, CacheEntry>,
    ///     stats: CacheStats,
    /// }
    ///
    /// #[derive(Debug)]
    /// struct CacheEntry {
    ///     data: String,
    ///     metadata: Option<String>,
    /// }
    ///
    /// #[derive(Debug)]
    /// struct CacheStats {
    ///     hits: u64,
    /// }
    ///
    /// let mut entries = HashMap::new();
    /// entries.insert(
    ///     "key1".to_owned(),
    ///     CacheEntry {
    ///         data: "cached_data".to_owned(),
    ///         metadata: Some("important".to_owned()),
    ///     },
    /// );
    ///
    /// let cache = Cache {
    ///     entries,
    ///     stats: CacheStats { hits: 42 },
    /// };
    ///
    /// let rwlock = Arc::new(RwLock::new(cache));
    /// let guard = rwlock.read_owned().await;
    ///
    /// // Map to a specific cache entry for cross-task reading
    /// let entry_guard = OwnedRwLockReadGuard::map(guard, |cache| cache.entries.get("key1").unwrap());
    ///
    /// // Try to map to the metadata if it exists
    /// let metadata_guard =
    ///     OwnedMappedRwLockReadGuard::filter_map(entry_guard, |entry| entry.metadata.as_ref())
    ///         .expect("entry should have metadata");
    ///
    /// assert_eq!(&*metadata_guard, "important");
    /// # }
    /// ```
    pub fn filter_map<V, F>(orig: Self, f: F) -> Result<OwnedMappedRwLockReadGuard<T, V>, Self>
    where
        F: FnOnce(&U) -> Option<&V>,
        V: ?Sized,
    {
        // SAFETY: orig.d is a valid NonNull<U> pointer that was created from a valid reference
        // when the original OwnedMappedRwLockReadGuard was constructed. The guard guarantees shared
        // access to the data through the rwlock, so dereferencing is safe.
        match f(unsafe { orig.d.as_ref() }) {
            Some(d) => {
                let d = NonNull::from(d);
                let orig = ManuallyDrop::new(orig);

                // SAFETY: The original guard is wrapped in `ManuallyDrop` and will not be dropped.
                // This allows us to safely move the `Arc` out of it and transfer ownership to the
                // new guard.
                let lock = unsafe { std::ptr::read(&orig.lock) };

                Ok(OwnedMappedRwLockReadGuard::new(d, lock))
            }
            None => Err(orig),
        }
    }
}
