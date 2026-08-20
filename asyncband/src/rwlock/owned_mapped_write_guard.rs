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

use std::fmt;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::rwlock::OwnedMappedRwLockReadGuard;
use crate::rwlock::RwLock;

/// Owned RAII structure used to release the exclusive write access of a lock when dropped, for a
/// mapped component of the locked data.
///
/// This guard is only available from a [`RwLock`] that is wrapped in an [`Arc`]. It is similar to
/// [`MappedRwLockWriteGuard`], except that rather than borrowing the `RwLock`, it clones the `Arc`,
/// incrementing the reference count. This means that unlike `MappedRwLockWriteGuard`, it will have
/// the `'static` lifetime.
///
/// As long as you have this guard, you have exclusive write access to the underlying `T`. The guard
/// internally keeps an `Arc` reference to the original rwlock and tracks the number of permits
/// acquired, so the original lock is maintained until this guard is dropped.
///
/// `OwnedMappedRwLockWriteGuard` implements [`Send`] and [`Sync`]
/// when the underlying data type supports these traits, allowing it to be used across task
/// boundaries and shared between threads safely.
///
/// [`map`]: crate::rwlock::OwnedRwLockWriteGuard::map
/// [`filter_map`]: crate::rwlock::OwnedRwLockWriteGuard::filter_map
/// [`OwnedRwLockWriteGuard`]: crate::rwlock::OwnedRwLockWriteGuard
/// [`MappedRwLockWriteGuard`]: crate::rwlock::MappedRwLockWriteGuard
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
/// use asyncband::rwlock::OwnedRwLockWriteGuard;
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
/// let mut guard = rwlock.write_owned().await;
/// let mut profile_guard = OwnedRwLockWriteGuard::map(guard, |user| &mut user.profile);
///
/// // Now we can only access and modify the user's profile
/// profile_guard.email = "newemail@example.com".to_owned();
/// assert_eq!(profile_guard.email, "newemail@example.com");
/// # }
/// ```
///
/// # Variance
///
/// The guard is invariant over its mapped type `U`, as required for mutable access:
///
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
#[must_use = "if unused the RwLock will immediately unlock"]
pub struct OwnedMappedRwLockWriteGuard<T: ?Sized, U: ?Sized> {
    d: NonNull<U>,
    lock: Arc<RwLock<T>>,
    permits_acquired: usize,
    // Mutable access requires invariance over U.
    variance: PhantomData<*mut U>,
}

// SAFETY: Sharing &Guard across threads is safe when T: Send + Sync and U: Sync.
// Arc<RwLock<T>> requires T: Send + Sync for thread safety.
// &Guard only provides &U (via Deref), so U: Sync ensures safe concurrent access.
unsafe impl<T: ?Sized + Send + Sync, U: ?Sized + Sync> Sync for OwnedMappedRwLockWriteGuard<T, U> {}

// SAFETY: Sending Guard across threads is safe when T: Send + Sync and U: Send.
// Arc<RwLock<T>> requires T: Send + Sync to be Send.
// Guard transfers exclusive access to U, so U: Send ensures safe access from new thread.
unsafe impl<T: ?Sized + Send + Sync, U: ?Sized + Send> Send for OwnedMappedRwLockWriteGuard<T, U> {}

impl<T: ?Sized, U: ?Sized> OwnedMappedRwLockWriteGuard<T, U> {
    pub(crate) fn new(d: NonNull<U>, lock: Arc<RwLock<T>>, permits_acquired: usize) -> Self {
        Self {
            d,
            lock,
            permits_acquired,
            variance: PhantomData,
        }
    }
}
impl<T: ?Sized, U: ?Sized> Drop for OwnedMappedRwLockWriteGuard<T, U> {
    fn drop(&mut self) {
        self.lock.s.release(self.permits_acquired);
    }
}

impl<T: ?Sized, U: ?Sized + fmt::Debug> fmt::Debug for OwnedMappedRwLockWriteGuard<T, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized, U: ?Sized + fmt::Display> fmt::Display for OwnedMappedRwLockWriteGuard<T, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized, U: ?Sized> Deref for OwnedMappedRwLockWriteGuard<T, U> {
    type Target = U;
    fn deref(&self) -> &Self::Target {
        // SAFETY: we hold the write lock and the NonNull pointer is valid for the guard's lifetime
        unsafe { self.d.as_ref() }
    }
}

impl<T: ?Sized, U: ?Sized> DerefMut for OwnedMappedRwLockWriteGuard<T, U> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: we hold the write lock and the NonNull pointer is valid for the guard's lifetime
        unsafe { self.d.as_mut() }
    }
}

impl<T: ?Sized, U: ?Sized> OwnedMappedRwLockWriteGuard<T, U> {
    /// Makes a new [`OwnedMappedRwLockWriteGuard`] for a component of the locked data.
    ///
    /// This operation cannot fail as the `OwnedMappedRwLockWriteGuard` passed in already locked the
    /// rwlock.
    ///
    /// This is an associated function that needs to be used as
    /// `OwnedMappedRwLockWriteGuard::map(...)`.
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
    /// use asyncband::rwlock::OwnedMappedRwLockWriteGuard;
    /// use asyncband::rwlock::OwnedRwLockWriteGuard;
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
    /// let mut guard = rwlock.write_owned().await;
    /// // First map to the profile field
    /// let mut profile_guard = OwnedRwLockWriteGuard::map(guard, |user| &mut user.profile);
    /// // Then map to the email field specifically
    /// let mut email_guard =
    ///     OwnedMappedRwLockWriteGuard::map(profile_guard, |profile| &mut profile.email);
    ///
    /// *email_guard = "newemail@example.com".to_owned();
    /// assert_eq!(&*email_guard, "newemail@example.com");
    /// # }
    /// ```
    pub fn map<V, F>(mut orig: Self, f: F) -> OwnedMappedRwLockWriteGuard<T, V>
    where
        F: FnOnce(&mut U) -> &mut V,
        V: ?Sized,
    {
        // SAFETY: orig.d is a valid NonNull<U> pointer that was created from a valid reference
        // when the original OwnedMappedRwLockWriteGuard was constructed. The guard guarantees
        // exclusive access to the data through the rwlock, so dereferencing is safe.
        let d = NonNull::from(f(unsafe { orig.d.as_mut() }));
        let orig = ManuallyDrop::new(orig);

        let permits_acquired = orig.permits_acquired;
        // SAFETY: The original guard is wrapped in `ManuallyDrop` and will not be dropped.
        // This allows us to safely move the `Arc` out of it and transfer ownership to the new
        // guard.
        let lock = unsafe { std::ptr::read(&orig.lock) };

        OwnedMappedRwLockWriteGuard::new(d, lock, permits_acquired)
    }

    /// Attempts to make a new [`OwnedMappedRwLockWriteGuard`] for a component of the locked data.
    /// The original guard is returned if the closure returns `None`.
    ///
    /// This operation cannot fail as the `OwnedMappedRwLockWriteGuard` passed in already locked the
    /// rwlock.
    ///
    /// This is an associated function that needs to be used as
    /// `OwnedMappedRwLockWriteGuard::filter_map(...)`.
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
    /// use asyncband::rwlock::OwnedMappedRwLockWriteGuard;
    /// use asyncband::rwlock::OwnedRwLockWriteGuard;
    /// use asyncband::rwlock::RwLock;
    ///
    /// #[derive(Debug)]
    /// struct AppState {
    ///     user_count: u64,
    ///     metrics: Option<Metrics>,
    /// }
    ///
    /// #[derive(Debug)]
    /// struct Metrics {
    ///     requests_per_second: f64,
    ///     error_rate: f64,
    /// }
    ///
    /// let state = AppState {
    ///     user_count: 100,
    ///     metrics: Some(Metrics {
    ///         requests_per_second: 150.5,
    ///         error_rate: 0.01,
    ///     }),
    /// };
    ///
    /// let rwlock = Arc::new(RwLock::new(state));
    /// let guard = rwlock.write_owned().await;
    ///
    /// // First, map to the `metrics` field, which is an Option.
    /// // This gives us an OwnedMappedRwLockWriteGuard<AppState, Option<Metrics>>
    /// let metrics_opt_guard = OwnedRwLockWriteGuard::map(guard, |state| &mut state.metrics);
    ///
    /// // Now, on the mapped guard, try to map into the Option.
    /// // This is the correct usage of OwnedMappedRwLockWriteGuard::filter_map.
    /// let metrics_result =
    ///     OwnedMappedRwLockWriteGuard::filter_map(metrics_opt_guard, |metrics_opt| {
    ///         metrics_opt.as_mut()
    ///     });
    ///
    /// match metrics_result {
    ///     Ok(mut metrics_guard) => {
    ///         // Update metrics across tasks
    ///         metrics_guard.requests_per_second = 200.0;
    ///         metrics_guard.error_rate = 0.005;
    ///         assert_eq!(metrics_guard.requests_per_second, 200.0);
    ///     }
    ///     Err(_original_guard) => {
    ///         // Metrics not available, original guard is returned
    ///         println!("Metrics not enabled");
    ///     }
    /// }
    /// # }
    /// ```
    pub fn filter_map<V, F>(mut orig: Self, f: F) -> Result<OwnedMappedRwLockWriteGuard<T, V>, Self>
    where
        F: FnOnce(&mut U) -> Option<&mut V>,
        V: ?Sized,
    {
        // SAFETY: orig.d is a valid NonNull<U> pointer that was created from a valid reference
        // when the original OwnedMappedRwLockWriteGuard was constructed. The guard guarantees
        // exclusive access to the data through the rwlock, so dereferencing is safe.
        match f(unsafe { orig.d.as_mut() }) {
            Some(d) => {
                let d = NonNull::from(d);
                let orig = ManuallyDrop::new(orig);

                let permits_acquired = orig.permits_acquired;
                // SAFETY: The original guard is wrapped in `ManuallyDrop` and will not be dropped.
                // This allows us to safely move the `Arc` out of it and transfer ownership to the
                // new guard.
                let lock = unsafe { std::ptr::read(&orig.lock) };

                Ok(OwnedMappedRwLockWriteGuard::new(d, lock, permits_acquired))
            }
            None => Err(orig),
        }
    }

    /// Atomically downgrades the write lock to a read lock while preserving the mapping.
    ///
    /// This method changes the lock from exclusive mode to shared mode atomically,
    /// preventing other writers from acquiring the lock in between.
    ///
    /// The returned `OwnedMappedRwLockReadGuard` preserves the original mapping and
    /// has a `'static` lifetime.
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
    /// struct Database {
    ///     connection_count: u32,
    ///     status: String,
    /// }
    ///
    /// let db = Arc::new(RwLock::new(Database {
    ///     connection_count: 0,
    ///     status: "idle".to_owned(),
    /// }));
    ///
    /// let write_guard = db.clone().write_owned().await;
    /// let mut count_write_guard =
    ///     OwnedRwLockWriteGuard::map(write_guard, |db| &mut db.connection_count);
    /// *count_write_guard = 5;
    ///
    /// let count_read_guard = count_write_guard.downgrade();
    /// assert_eq!(*count_read_guard, 5);
    ///
    /// assert!(db.clone().try_write_owned().is_none());
    ///
    /// drop(count_read_guard);
    /// assert!(db.clone().try_write_owned().is_some());
    /// # }
    /// ```
    pub fn downgrade(self) -> OwnedMappedRwLockReadGuard<T, U> {
        // Prevent the original write guard from running its Drop implementation,
        // which would release all permits. This must be done BEFORE any operation
        // that might panic to ensure panic safety.
        let guard = ManuallyDrop::new(self);

        // Release max_readers - 1 permits to convert the write lock to a read lock.
        guard.lock.s.release(guard.permits_acquired - 1);

        // SAFETY: The `guard` is wrapped in `ManuallyDrop`, so its destructor will not be run.
        // We can safely move the `Arc` out of the guard, as the guard is not used after this.
        // This is a standard way to transfer ownership from a `ManuallyDrop` wrapper.
        let lock = unsafe { std::ptr::read(&guard.lock) };
        OwnedMappedRwLockReadGuard::new(guard.d, lock)
    }
}
