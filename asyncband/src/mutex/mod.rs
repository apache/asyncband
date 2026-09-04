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

// Portions of the owned and mapped guard APIs originated from Tokio 1.41.0's Mutex implementation.
// Copyright (c) Tokio Contributors
// The Tokio-derived portions remain licensed under the MIT License.
// Asyncband independently built the mutex on its own semaphore and substantially changed the
// incorporated guard implementation: the try-lock error type and semaphore closure are absent,
// projected guards use NonNull pointers with explicit invariance, and projected guards can be
// mapped repeatedly in both borrowed and owned forms.
// Upstream source:
// https://github.com/tokio-rs/tokio/blob/01e04daaa162ce6122bb894fdda0b6803dd32093/tokio/src/sync/mutex.rs

//! Mutual exclusion that yields the current task while waiting.
//!
//! A successful lock operation returns a guard that provides exclusive access to the protected
//! value and releases the lock when dropped. The guard may be held across `.await` points. When
//! that is unnecessary, a synchronous mutex is usually cheaper.
//!
//! Waiters are served through Asyncband's fair semaphore queue. Cancelling a pending lock
//! operation removes that waiter from the queue; a later attempt joins at the back. A panic while
//! holding a guard releases the lock without poisoning it.
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
//! let counter = Arc::new(Mutex::new(0));
//! let mut tasks = Vec::new();
//!
//! for _ in 0..3 {
//!     let counter = counter.clone();
//!     tasks.push(tokio::spawn(async move {
//!         *counter.lock().await += 1;
//!     }));
//! }
//!
//! for task in tasks {
//!     task.await.unwrap();
//! }
//!
//! assert_eq!(*counter.lock().await, 3);
//!
//! # }
//! ```

use std::cell::UnsafeCell;
use std::fmt;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::internal::semaphore;

/// An asynchronous mutex backed by Asyncband's fair semaphore.
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
    /// Wraps `t` in an unlocked mutex.
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

    /// Unwraps the protected value.
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
    /// Waits for exclusive access and returns a borrowed guard.
    ///
    /// # Cancel safety
    ///
    /// Waiters enter the mutex's fair queue. Dropping this future before it completes removes the
    /// waiter, so retrying starts again at the back of the queue.
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

    /// Acquires the mutex without waiting, or returns `None` when it is already held.
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

    /// Waits for exclusive access and returns a guard that owns this [`Arc`].
    ///
    /// The owned guard keeps the mutex alive instead of borrowing it, which allows the guard to be
    /// moved wherever a `'static` value is required.
    ///
    /// # Cancel safety
    ///
    /// Waiters enter the mutex's fair queue. Dropping this future before it completes removes the
    /// waiter, so retrying starts again at the back of the queue.
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

    /// Acquires the mutex without waiting and returns a guard that owns this [`Arc`].
    ///
    /// Returns `None` when another guard currently holds the mutex.
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

    /// Borrows the protected value mutably without locking.
    ///
    /// The exclusive borrow of the mutex already prevents any guard from existing at the same
    /// time.
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

/// A borrowed proof of exclusive access to a [`Mutex`].
///
/// [`Mutex::lock`] and [`Mutex::try_lock`] create this guard. It dereferences to the protected
/// value and returns its single semaphore permit when dropped.
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
#[must_use = "dropping the guard releases the mutex immediately"]
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
    /// Projects this guard to a mutable component of the protected value.
    ///
    /// The returned guard owns the same lock permit and releases it when dropped. Call this as
    /// `MutexGuard::map(...)` so a method named `map` on `T` remains accessible through deref.
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

    /// Attempts to project this guard to a mutable component of the protected value.
    ///
    /// The original guard is returned when `f` returns `None`. Call this as
    /// `MutexGuard::filter_map(...)` so a method with the same name on `T` remains accessible.
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

/// A proof of exclusive access that owns an [`Arc`] containing its [`Mutex`].
///
/// [`Mutex::lock_owned`] and [`Mutex::try_lock_owned`] create this guard. Owning the `Arc` lets the
/// guard outlive the reference used to acquire it; dropping the guard releases the lock permit and
/// its share of the `Arc`.
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
#[must_use = "dropping the guard releases the mutex immediately"]
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
    /// Projects this guard to a mutable component of the protected value.
    ///
    /// The returned guard retains the same `Arc` and lock permit. Call this as
    /// `OwnedMutexGuard::map(...)` so a method named `map` on `T` remains accessible through deref.
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

    /// Attempts to project this guard to a mutable component of the protected value.
    ///
    /// The original guard is returned when `f` returns `None`. Call this as
    /// `OwnedMutexGuard::filter_map(...)` so a method with the same name on `T` remains accessible.
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

/// A borrowed mutex guard projected to a mutable component of the protected value.
///
/// [`MutexGuard::map`] and [`MutexGuard::filter_map`] create this guard. It retains the original
/// semaphore permit while exposing only the projected component, and releases that permit when
/// dropped.
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
#[must_use = "dropping the guard releases the mutex immediately"]
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
    /// Projects an already mapped guard to a deeper mutable component.
    ///
    /// The returned guard retains the same lock permit. Call this as
    /// `MappedMutexGuard::map(...)` so a method named `map` on `T` remains accessible through
    /// deref.
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

    /// Attempts to project an already mapped guard to a deeper mutable component.
    ///
    /// The original mapped guard is returned when `f` returns `None`. Call this as
    /// `MappedMutexGuard::filter_map(...)` so a method with the same name on `T` remains
    /// accessible.
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

/// An owned mutex guard projected to a mutable component of the protected value.
///
/// [`OwnedMutexGuard::map`] and [`OwnedMutexGuard::filter_map`] create this guard. It keeps the
/// original mutex alive through an `Arc`, retains its lock permit, and exposes only the projected
/// component until dropped.
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
#[must_use = "dropping the guard releases the mutex immediately"]
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
    /// Projects an owned mapped guard to a deeper mutable component.
    ///
    /// The returned guard retains the same `Arc` and lock permit. Call this as
    /// `OwnedMappedMutexGuard::map(...)` so a method named `map` on `U` remains accessible through
    /// deref.
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

    /// Attempts to project an owned mapped guard to a deeper mutable component.
    ///
    /// The original mapped guard is returned when `f` returns `None`. Call this as
    /// `OwnedMappedMutexGuard::filter_map(...)` so a method with the same name on `U` remains
    /// accessible through deref.
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
