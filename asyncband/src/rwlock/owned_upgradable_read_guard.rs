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
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::sync::Arc;

use crate::rwlock::OwnedRwLockReadGuard;
use crate::rwlock::OwnedRwLockWriteGuard;
use crate::rwlock::RwLock;

impl<T: ?Sized> RwLock<T> {
    /// Acquires an owned upgradable read guard from an [`Arc`].
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
    /// let lock = Arc::new(RwLock::new(5));
    /// let guard = lock.clone().upgradable_read_owned().await;
    /// assert_eq!(*guard, 5);
    /// # }
    /// ```
    pub async fn upgradable_read_owned(self: Arc<Self>) -> OwnedRwLockUpgradableReadGuard<T> {
        self.raw.upgradable_read().await;
        OwnedRwLockUpgradableReadGuard { lock: self }
    }

    /// Attempts to acquire an owned upgradable read guard without waiting.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::rwlock::RwLock;
    ///
    /// let lock = Arc::new(RwLock::new(1));
    /// let guard = lock
    ///     .clone()
    ///     .try_upgradable_read_owned()
    ///     .expect("lock is available");
    /// assert!(lock.clone().try_upgradable_read_owned().is_none());
    ///
    /// drop(guard);
    /// assert!(lock.try_upgradable_read_owned().is_some());
    /// ```
    pub fn try_upgradable_read_owned(self: Arc<Self>) -> Option<OwnedRwLockUpgradableReadGuard<T>> {
        self.raw
            .try_upgradable_read()
            .then(|| OwnedRwLockUpgradableReadGuard { lock: self })
    }
}

/// An owned shared read guard that can be atomically promoted to exclusive write access.
///
/// The guard keeps its lock alive and can be moved independently of the borrow used to acquire it.
/// At most one owned or borrowed upgradable guard may exist for a lock.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use std::sync::Arc;
///
/// use asyncband::rwlock::OwnedRwLockUpgradableReadGuard;
/// use asyncband::rwlock::RwLock;
///
/// let lock = Arc::new(RwLock::new(4));
/// let guard: OwnedRwLockUpgradableReadGuard<i32> = lock.clone().upgradable_read_owned().await;
///
/// let value = tokio::spawn(async move { *guard }).await.unwrap();
/// assert_eq!(value, 4);
/// # }
/// ```
#[must_use = "if unused the RwLock will immediately unlock"]
pub struct OwnedRwLockUpgradableReadGuard<T: ?Sized> {
    pub(super) lock: Arc<RwLock<T>>,
}

unsafe impl<T: ?Sized + Send + Sync> Send for OwnedRwLockUpgradableReadGuard<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for OwnedRwLockUpgradableReadGuard<T> {}

impl<T: ?Sized> Drop for OwnedRwLockUpgradableReadGuard<T> {
    fn drop(&mut self) {
        self.lock.raw.unlock_upgradable();
    }
}

impl<T: ?Sized> Deref for OwnedRwLockUpgradableReadGuard<T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: an upgradable guard owns shared access to the protected value.
        unsafe { &*self.lock.c.get() }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for OwnedRwLockUpgradableReadGuard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for OwnedRwLockUpgradableReadGuard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized> OwnedRwLockUpgradableReadGuard<T> {
    /// Atomically promotes this guard to owned exclusive write access.
    ///
    /// Cancelling the operation releases the upgradable read guard.
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
    /// let guard = lock.clone().upgradable_read_owned().await;
    /// let mut writer = guard.upgrade().await;
    /// *writer += 1;
    ///
    /// assert_eq!(*writer, 2);
    /// # }
    /// ```
    pub async fn upgrade(self) -> OwnedRwLockWriteGuard<T> {
        let guard = ManuallyDrop::new(self);
        // SAFETY: the source guard will never be dropped; ownership of its Arc moves into this
        // local, which remains alive across the raw upgrade future and moves into the result.
        let lock = unsafe { std::ptr::read(&guard.lock) };
        lock.raw.upgrade().await;
        OwnedRwLockWriteGuard { lock }
    }

    /// Attempts to promote immediately, returning the original guard when other readers remain.
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
    /// let upgradable = lock.clone().upgradable_read_owned().await;
    /// let reader = lock.read().await;
    ///
    /// let upgradable = upgradable
    ///     .try_upgrade()
    ///     .expect_err("reader still holds the lock");
    /// drop(reader);
    /// let writer = upgradable.try_upgrade().expect("last reader has left");
    /// assert_eq!(*writer, 1);
    /// # }
    /// ```
    pub fn try_upgrade(self) -> Result<OwnedRwLockWriteGuard<T>, Self> {
        if self.lock.raw.try_upgrade() {
            let guard = ManuallyDrop::new(self);
            // SAFETY: the source guard will not drop and ownership moves to the write guard.
            let lock = unsafe { std::ptr::read(&guard.lock) };
            Ok(OwnedRwLockWriteGuard { lock })
        } else {
            Err(self)
        }
    }

    /// Atomically converts this guard to an owned ordinary shared read guard.
    ///
    /// Downgrading relinquishes the unique promotion reservation while retaining shared access.
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
    /// let upgradable = lock.clone().upgradable_read_owned().await;
    /// let reader = upgradable.downgrade();
    ///
    /// assert!(lock.clone().try_upgradable_read_owned().is_some());
    /// assert!(lock.clone().try_write_owned().is_none());
    /// drop(reader);
    /// assert!(lock.try_write_owned().is_some());
    /// # }
    /// ```
    pub fn downgrade(self) -> OwnedRwLockReadGuard<T> {
        let guard = ManuallyDrop::new(self);
        guard.lock.raw.downgrade_upgradable_to_read();
        // SAFETY: the source guard will not drop and ownership moves to the read guard.
        let lock = unsafe { std::ptr::read(&guard.lock) };
        OwnedRwLockReadGuard { lock }
    }
}
