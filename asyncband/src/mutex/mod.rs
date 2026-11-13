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
// incorporated guard implementation: the try-lock error type and support for closing the semaphore
// are absent, projected guards use NonNull pointers with explicit invariance, and projected guards
// can be mapped repeatedly in both borrowed and owned forms.
// Upstream source:
// https://github.com/tokio-rs/tokio/blob/01e04daaa162ce6122bb894fdda0b6803dd32093/tokio/src/sync/mutex.rs

//! Mutual exclusion that yields the current task while waiting.
//!
//! A successful lock operation returns a guard that provides exclusive access to the protected
//! value and releases the lock when dropped. The guard may be held across `.await` points. When
//! that is unnecessary, a synchronous mutex is usually cheaper.
//!
//! Lock requests complete in the order they begin waiting. Cancelling a pending request loses its
//! place, so a later attempt waits behind requests already in progress. A panic while holding a
//! guard releases the lock without poisoning it.
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
//! let mut tasks = vec![];
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
use std::ptr;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::internal::semaphore;

/// An asynchronous mutex that grants exclusive access in request order.
///
/// See the [module level documentation](self) for more.
pub struct Mutex<T: ?Sized> {
    s: semaphore::Semaphore,
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
    /// let protected = Mutex::new(String::from("ready"));
    /// let value = protected.into_inner();
    /// assert_eq!(value, "ready");
    /// ```
    pub fn into_inner(self) -> T {
        self.c.into_inner()
    }
}

impl<T: ?Sized> Mutex<T> {
    /// Waits for exclusive access and returns a borrowed guard.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending future removes this request and releases any assigned permit. A later
    /// attempt joins the back of the queue. No guard is returned on cancellation.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mutex::Mutex;
    ///
    /// let entries = Mutex::new(vec![10]);
    ///
    /// let mut guard = entries.lock().await;
    /// guard.push(20);
    /// assert_eq!(*guard, [10, 20]);
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
    /// let count = Mutex::new(5);
    /// {
    ///     let mut guard = count.try_lock().unwrap();
    ///     *guard += 3;
    ///     assert!(count.try_lock().is_none());
    /// }
    /// assert_eq!(count.into_inner(), 8);
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
    /// # Cancellation
    ///
    /// Dropping a pending future removes this request and releases any assigned permit. A later
    /// attempt joins the back of the queue. No guard is returned, and the future releases its owned
    /// `Arc`.
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
    /// let shared = Arc::new(Mutex::new(String::from("init")));
    /// let worker_shared = shared.clone();
    ///
    /// tokio::spawn(async move {
    ///     let mut guard = worker_shared.lock_owned().await;
    ///     guard.push_str("-done");
    /// })
    /// .await
    /// .unwrap();
    ///
    /// assert_eq!(shared.lock().await.as_str(), "init-done");
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
    /// let shared = Arc::new(Mutex::new(vec!["a"]));
    /// let mut guard = shared.clone().try_lock_owned().unwrap();
    /// guard.push("b");
    /// assert!(shared.clone().try_lock_owned().is_none());
    /// drop(guard);
    /// assert_eq!(shared.try_lock_owned().unwrap().len(), 2);
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
    /// let mut protected = Mutex::new(String::from("draft"));
    /// protected.get_mut().push_str("-final");
    /// assert_eq!(protected.into_inner(), "draft-final");
    /// ```
    pub fn get_mut(&mut self) -> &mut T {
        self.c.get_mut()
    }
}

/// A borrowed proof of exclusive access to a [`Mutex`].
///
/// [`Mutex::lock`] and [`Mutex::try_lock`] create this guard. It dereferences to the protected
/// value and releases the lock when dropped.
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
        self.lock.s.release_with_lock(1);
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
    /// The returned guard keeps the same mutex locked. Call this as
    /// `MutexGuard::map(...)` so a method named `map` on `T` remains accessible through deref.
    ///
    /// # Panics
    ///
    /// If `f` panics, unwinding drops the consumed guard and releases its lock access. Changes
    /// already made through the guard are not rolled back.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mutex::Mutex;
    /// use asyncband::mutex::MutexGuard;
    ///
    /// struct Account {
    ///     display_name: String,
    ///     balance: u64,
    /// }
    ///
    /// let account = Mutex::new(Account {
    ///     display_name: "ops".to_owned(),
    ///     balance: 100,
    /// });
    ///
    /// {
    ///     let guard = account.lock().await;
    ///     let mut balance = MutexGuard::map(guard, |acc| &mut acc.balance);
    ///     *balance += 50;
    /// }
    ///
    /// assert_eq!(account.lock().await.balance, 150);
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
    /// Call this as `MutexGuard::filter_map(...)` so a method with the same name on `T` remains
    /// accessible.
    ///
    /// # Errors
    ///
    /// Returns `Err(orig)` if `f` returns `None`. The original guard still holds its lock access;
    /// no unlock occurs on this path.
    ///
    /// # Panics
    ///
    /// If `f` panics, unwinding drops the consumed guard and releases its lock access. Changes
    /// already made through the guard are not rolled back.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::collections::BTreeMap;
    ///
    /// use asyncband::mutex::Mutex;
    /// use asyncband::mutex::MutexGuard;
    ///
    /// let routes = Mutex::new(BTreeMap::from([("v1", 8080_u16)]));
    /// let guard = routes.lock().await;
    /// let mut port = MutexGuard::filter_map(guard, |map| map.get_mut("v1")).unwrap();
    /// *port = 8443;
    /// drop(port);
    ///
    /// assert_eq!(routes.lock().await.get("v1"), Some(&8443));
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
/// guard outlive the reference used to acquire it; dropping the guard releases the lock and its
/// share of the `Arc`.
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
        self.lock.s.release_with_lock(1);
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
    /// The returned guard retains the same `Arc` and keeps the mutex locked. Call this as
    /// `OwnedMutexGuard::map(...)` so a method named `map` on `T` remains accessible through deref.
    ///
    /// # Panics
    ///
    /// If `f` panics, unwinding drops the consumed guard and releases its lock access. Changes
    /// already made through the guard are not rolled back.
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
    /// struct Limits {
    ///     label: &'static str,
    ///     max_in_flight: usize,
    /// }
    ///
    /// let limits = Arc::new(Mutex::new(Limits {
    ///     label: "ingress",
    ///     max_in_flight: 32,
    /// }));
    ///
    /// {
    ///     let guard = limits.clone().lock_owned().await;
    ///     let mut max_in_flight = OwnedMutexGuard::map(guard, |cfg| &mut cfg.max_in_flight);
    ///     *max_in_flight = 64;
    /// }
    ///
    /// assert_eq!(limits.lock().await.max_in_flight, 64);
    /// # }
    /// ```
    pub fn map<U, F>(mut orig: Self, f: F) -> OwnedMappedMutexGuard<T, U>
    where
        F: FnOnce(&mut T) -> &mut U,
        U: ?Sized,
    {
        let d = NonNull::from(f(&mut *orig));

        let guard = ManuallyDrop::new(orig);

        // SAFETY: The guard is wrapped in `ManuallyDrop` and will not be dropped,
        // so the `Arc` can be moved out to transfer lock ownership to the new guard.
        let lock = unsafe { ptr::read(&guard.lock) };

        OwnedMappedMutexGuard {
            lock,
            d,
            variance: PhantomData,
        }
    }

    /// Attempts to project this guard to a mutable component of the protected value.
    ///
    /// Call this as `OwnedMutexGuard::filter_map(...)` so a method with the same name on `T`
    /// remains accessible.
    ///
    /// # Errors
    ///
    /// Returns `Err(orig)` if `f` returns `None`. The original guard still holds its lock access;
    /// no unlock occurs on this path.
    ///
    /// # Panics
    ///
    /// If `f` panics, unwinding drops the consumed guard and releases its lock access. Changes
    /// already made through the guard are not rolled back.
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
    /// let queue = Arc::new(Mutex::new(vec![10, 20, 30]));
    /// let guard = queue.clone().lock_owned().await;
    /// let mut head = OwnedMutexGuard::filter_map(guard, |items| items.first_mut()).unwrap();
    /// *head += 5;
    /// drop(head);
    ///
    /// assert_eq!(*queue.lock().await, [15, 20, 30]);
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

                // SAFETY: The guard is wrapped in `ManuallyDrop` and will not be dropped,
                // so the `Arc` can be moved out to transfer lock ownership to the new guard.
                let lock = unsafe { ptr::read(&guard.lock) };

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
/// [`MutexGuard::map`] and [`MutexGuard::filter_map`] create this guard. It keeps the mutex locked
/// while exposing only the projected component.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use asyncband::mutex::Mutex;
/// use asyncband::mutex::MutexGuard;
///
/// struct Envelope {
///     id: u64,
///     payload: String,
/// }
///
/// let mutex = Mutex::new(Envelope {
///     id: 7,
///     payload: "pending".to_owned(),
/// });
///
/// {
///     let guard = mutex.lock().await;
///     let mut payload = MutexGuard::map(guard, |env| &mut env.payload);
///     *payload = "committed".to_owned();
/// }
///
/// assert_eq!(mutex.lock().await.payload, "committed");
/// # }
/// ```
#[must_use = "dropping the guard releases the mutex immediately"]
pub struct MappedMutexGuard<'a, T: ?Sized> {
    d: NonNull<T>,
    s: &'a semaphore::Semaphore,
    // Mutable access requires invariance over T.
    variance: PhantomData<&'a mut T>,
}

// SAFETY: The guard exclusively borrows the locked mutex and remains valid for `'a`, so moving it
// across threads is safe when `T: Send`.
unsafe impl<T: ?Sized + Send> Send for MappedMutexGuard<'_, T> {}

// SAFETY: `&MappedMutexGuard<'_, T>` yields only `&T`, so sharing it across threads is safe when
// `T: Sync`.
unsafe impl<T: ?Sized + Sync> Sync for MappedMutexGuard<'_, T> {}

impl<T: ?Sized> Drop for MappedMutexGuard<'_, T> {
    fn drop(&mut self) {
        self.s.release_with_lock(1);
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
        // SAFETY: `self` holds exclusive mutex access and `d` remains valid for `'a`.
        unsafe { self.d.as_ref() }
    }
}

impl<T: ?Sized> DerefMut for MappedMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: `self` holds exclusive mutex access and `d` remains valid for `'a`.
        unsafe { self.d.as_mut() }
    }
}

impl<'a, T: ?Sized> MappedMutexGuard<'a, T> {
    /// Projects an already mapped guard to a deeper mutable component.
    ///
    /// The returned guard keeps the same mutex locked. Call this as
    /// `MappedMutexGuard::map(...)` so a method named `map` on `T` remains accessible through
    /// deref.
    ///
    /// # Panics
    ///
    /// If `f` panics, unwinding drops the consumed guard and releases its lock access. Changes
    /// already made through the guard are not rolled back.
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
    /// struct Counters {
    ///     inner: InnerCounters,
    /// }
    ///
    /// struct InnerCounters {
    ///     accepted: u64,
    /// }
    ///
    /// let counters = Mutex::new(Counters {
    ///     inner: InnerCounters { accepted: 0 },
    /// });
    ///
    /// {
    ///     let guard = counters.lock().await;
    ///     let inner = MutexGuard::map(guard, |c| &mut c.inner);
    ///     let mut accepted = MappedMutexGuard::map(inner, |i| &mut i.accepted);
    ///     *accepted += 1;
    /// }
    ///
    /// assert_eq!(counters.lock().await.inner.accepted, 1);
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
            s: orig.s,
            variance: PhantomData,
        }
    }

    /// Attempts to project an already mapped guard to a deeper mutable component.
    ///
    /// Call this as `MappedMutexGuard::filter_map(...)` so a method with the same name on `T`
    /// remains accessible.
    ///
    /// # Errors
    ///
    /// Returns `Err(orig)` if `f` returns `None`. The original guard still holds its lock access;
    /// no unlock occurs on this path.
    ///
    /// # Panics
    ///
    /// If `f` panics, unwinding drops the consumed guard and releases its lock access. Changes
    /// already made through the guard are not rolled back.
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
    /// struct Slot {
    ///     pending: Option<u32>,
    /// }
    ///
    /// let slot = Mutex::new(Slot { pending: Some(4) });
    /// let guard = slot.lock().await;
    /// let pending = MutexGuard::map(guard, |s| &mut s.pending);
    /// let mut value = MappedMutexGuard::filter_map(pending, |opt| opt.as_mut()).unwrap();
    /// *value *= 2;
    /// drop(value);
    ///
    /// assert_eq!(slot.lock().await.pending, Some(8));
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
/// original mutex alive and locked while exposing only the projected component.
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
/// struct Metrics {
///     dropped: usize,
/// }
///
/// let metrics = Arc::new(Mutex::new(Metrics { dropped: 0 }));
/// {
///     let guard = metrics.clone().lock_owned().await;
///     let mut dropped = OwnedMutexGuard::map(guard, |m| &mut m.dropped);
///     *dropped += 3;
/// }
///
/// assert_eq!(metrics.lock().await.dropped, 3);
/// # }
/// ```
#[must_use = "dropping the guard releases the mutex immediately"]
pub struct OwnedMappedMutexGuard<T: ?Sized, U: ?Sized> {
    lock: Arc<Mutex<T>>,
    d: NonNull<U>,
    // Mutable access requires invariance over U.
    variance: PhantomData<*mut U>,
}

// SAFETY: The guard holds exclusive mutex access and keeps the `Arc` alive, so moving it across
// threads is safe when `T: Send` and `U: Send`.
unsafe impl<T: ?Sized + Send, U: ?Sized + Send> Send for OwnedMappedMutexGuard<T, U> {}

// SAFETY: `&OwnedMappedMutexGuard<T, U>` yields only `&U`, and dropping/cloning lock state requires
// `Mutex<T>: Sync`, so sharing across threads is safe when `T: Send + Sync` and `U: Send + Sync`.
unsafe impl<T: ?Sized + Send + Sync, U: ?Sized + Send + Sync> Sync for OwnedMappedMutexGuard<T, U> {}

impl<T: ?Sized, U: ?Sized> Drop for OwnedMappedMutexGuard<T, U> {
    fn drop(&mut self) {
        self.lock.s.release_with_lock(1);
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
        // SAFETY: `self` owns the `Arc<Mutex<T>>` and holds exclusive lock access, so `d` stays
        // valid.
        unsafe { self.d.as_ref() }
    }
}

impl<T: ?Sized, U: ?Sized> DerefMut for OwnedMappedMutexGuard<T, U> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: `self` owns the `Arc<Mutex<T>>` and holds exclusive lock access, so `d` stays
        // valid.
        unsafe { self.d.as_mut() }
    }
}

impl<T: ?Sized, U: ?Sized> OwnedMappedMutexGuard<T, U> {
    /// Projects an owned mapped guard to a deeper mutable component.
    ///
    /// The returned guard retains the same `Arc` and keeps the mutex locked. Call this as
    /// `OwnedMappedMutexGuard::map(...)` so a method named `map` on `U` remains accessible through
    /// deref.
    ///
    /// # Panics
    ///
    /// If `f` panics, unwinding drops the consumed guard and releases its lock access. Changes
    /// already made through the guard are not rolled back.
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
    /// struct Config {
    ///     endpoint: Endpoint,
    /// }
    ///
    /// struct Endpoint {
    ///     host: String,
    /// }
    ///
    /// let config = Arc::new(Mutex::new(Config {
    ///     endpoint: Endpoint {
    ///         host: "127.0.0.1".to_owned(),
    ///     },
    /// }));
    ///
    /// {
    ///     let guard = config.clone().lock_owned().await;
    ///     let endpoint = OwnedMutexGuard::map(guard, |c| &mut c.endpoint);
    ///     let mut host = OwnedMappedMutexGuard::map(endpoint, |e| &mut e.host);
    ///     *host = "localhost".to_owned();
    /// }
    ///
    /// assert_eq!(config.lock().await.endpoint.host, "localhost");
    /// # }
    /// ```
    pub fn map<V, F>(mut orig: Self, f: F) -> OwnedMappedMutexGuard<T, V>
    where
        F: FnOnce(&mut U) -> &mut V,
        V: ?Sized,
    {
        let d = NonNull::from(f(&mut *orig));
        let orig = ManuallyDrop::new(orig);

        // SAFETY: The guard is wrapped in `ManuallyDrop` and will not be dropped,
        // so the `Arc` can be moved out to transfer lock ownership to the new guard.
        let lock = unsafe { ptr::read(&orig.lock) };

        OwnedMappedMutexGuard {
            lock,
            d,
            variance: PhantomData,
        }
    }

    /// Attempts to project an owned mapped guard to a deeper mutable component.
    ///
    /// Call this as `OwnedMappedMutexGuard::filter_map(...)` so a method with the same name on
    /// `U` remains accessible through deref.
    ///
    /// # Errors
    ///
    /// Returns `Err(orig)` if `f` returns `None`. The original guard still holds its lock access;
    /// no unlock occurs on this path.
    ///
    /// # Panics
    ///
    /// If `f` panics, unwinding drops the consumed guard and releases its lock access. Changes
    /// already made through the guard are not rolled back.
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
    /// struct Entry {
    ///     next: Option<Box<u64>>,
    /// }
    ///
    /// let head = Arc::new(Mutex::new(Entry {
    ///     next: Some(Box::new(5)),
    /// }));
    ///
    /// {
    ///     let guard = head.clone().lock_owned().await;
    ///     let next = OwnedMutexGuard::map(guard, |e| &mut e.next);
    ///     let mut tail = OwnedMappedMutexGuard::filter_map(next, |opt| opt.as_deref_mut()).unwrap();
    ///     *tail += 7;
    /// }
    ///
    /// assert_eq!(head.lock().await.next.as_deref(), Some(&12));
    /// # }
    /// ```
    pub fn filter_map<V, F>(mut orig: Self, f: F) -> Result<OwnedMappedMutexGuard<T, V>, Self>
    where
        F: FnOnce(&mut U) -> Option<&mut V>,
        V: ?Sized,
    {
        match f(&mut *orig) {
            Some(d) => {
                let d = NonNull::from(d);
                let orig = ManuallyDrop::new(orig);

                // SAFETY: The guard is wrapped in `ManuallyDrop` and will not be dropped,
                // so the `Arc` can be moved out to transfer lock ownership to the new guard.
                let lock = unsafe { ptr::read(&orig.lock) };

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

#[cfg(doctest)]
mod compile_fail_tests {
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
    struct MutexGuardIsInvariant;

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
    struct OwnedMutexGuardIsInvariant;

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
    struct MappedMutexGuardIsInvariant;

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
    struct OwnedMappedMutexGuardIsInvariant;
}
