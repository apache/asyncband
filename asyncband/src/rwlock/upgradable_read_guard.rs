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

use crate::rwlock::RwLock;
use crate::rwlock::RwLockReadGuard;
use crate::rwlock::RwLockWriteGuard;

impl<T: ?Sized> RwLock<T> {
    /// Acquires shared read access that can later be atomically upgraded.
    ///
    /// At most one upgradable read guard may exist at a time. Ordinary readers can coexist with
    /// it. The upgradable guard reserves promotion priority over requests made after it acquired
    /// the lock.
    ///
    /// Cancelling this operation loses its position in the lock's FIFO queue.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::rwlock::RwLock;
    ///
    /// let lock = RwLock::new(7);
    /// let upgradable = lock.upgradable_read().await;
    ///
    /// assert_eq!(*upgradable, 7);
    /// assert!(lock.try_read().is_some());
    /// assert!(lock.try_upgradable_read().is_none());
    /// # }
    /// ```
    pub async fn upgradable_read(&self) -> RwLockUpgradableReadGuard<'_, T> {
        self.raw.upgradable_read().await;
        RwLockUpgradableReadGuard { lock: self }
    }

    /// Attempts to acquire upgradable shared access without waiting.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::rwlock::RwLock;
    ///
    /// let lock = RwLock::new(1);
    /// let guard = lock.try_upgradable_read().expect("lock is available");
    /// assert!(lock.try_upgradable_read().is_none());
    ///
    /// drop(guard);
    /// assert!(lock.try_upgradable_read().is_some());
    /// ```
    pub fn try_upgradable_read(&self) -> Option<RwLockUpgradableReadGuard<'_, T>> {
        self.raw
            .try_upgradable_read()
            .then(|| RwLockUpgradableReadGuard { lock: self })
    }
}

/// A shared read guard that can be atomically promoted to exclusive write access.
///
/// Ordinary readers may coexist with this guard, but only one upgradable guard may exist. Use
/// [`upgrade`](Self::upgrade) to wait for exclusive access without releasing the reservation.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use asyncband::rwlock::RwLock;
/// use asyncband::rwlock::RwLockUpgradableReadGuard;
///
/// let lock = RwLock::new(3);
/// let guard: RwLockUpgradableReadGuard<'_, i32> = lock.upgradable_read().await;
/// assert_eq!(*guard, 3);
/// # }
/// ```
#[must_use = "if unused the RwLock will immediately unlock"]
pub struct RwLockUpgradableReadGuard<'a, T: ?Sized> {
    pub(super) lock: &'a RwLock<T>,
}

unsafe impl<T: ?Sized + Send + Sync> Send for RwLockUpgradableReadGuard<'_, T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for RwLockUpgradableReadGuard<'_, T> {}

impl<T: ?Sized> Drop for RwLockUpgradableReadGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.raw.unlock_upgradable();
    }
}

impl<T: ?Sized> Deref for RwLockUpgradableReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: an upgradable guard owns shared access to the protected value.
        unsafe { &*self.lock.c.get() }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockUpgradableReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for RwLockUpgradableReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<'a, T: ?Sized> RwLockUpgradableReadGuard<'a, T> {
    /// Atomically promotes this guard to exclusive write access.
    ///
    /// Existing readers are allowed to finish and new requests wait behind the upgrade. Cancelling
    /// the operation releases the upgradable read guard.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::rwlock::RwLock;
    ///
    /// let lock = RwLock::new(1);
    /// let guard = lock.upgradable_read().await;
    /// let mut writer = guard.upgrade().await;
    /// *writer += 1;
    ///
    /// assert_eq!(*writer, 2);
    /// # }
    /// ```
    pub async fn upgrade(self) -> RwLockWriteGuard<'a, T> {
        // The raw upgrade future assumes responsibility for releasing upgradable ownership if it
        // is cancelled, so the source guard must no longer run its destructor after this point.
        let guard = ManuallyDrop::new(self);
        guard.lock.raw.upgrade().await;
        RwLockWriteGuard { lock: guard.lock }
    }

    /// Attempts to promote immediately, returning the original guard when other readers remain.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::rwlock::RwLock;
    ///
    /// let lock = RwLock::new(1);
    /// let upgradable = lock.upgradable_read().await;
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
    pub fn try_upgrade(self) -> Result<RwLockWriteGuard<'a, T>, Self> {
        if self.lock.raw.try_upgrade() {
            let guard = ManuallyDrop::new(self);
            Ok(RwLockWriteGuard { lock: guard.lock })
        } else {
            Err(self)
        }
    }

    /// Atomically converts this guard to an ordinary shared read guard.
    ///
    /// Downgrading relinquishes the unique promotion reservation while retaining shared access.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::rwlock::RwLock;
    ///
    /// let lock = RwLock::new(1);
    /// let upgradable = lock.upgradable_read().await;
    /// let reader = upgradable.downgrade();
    ///
    /// assert!(lock.try_upgradable_read().is_some());
    /// assert!(lock.try_write().is_none());
    /// drop(reader);
    /// assert!(lock.try_write().is_some());
    /// # }
    /// ```
    pub fn downgrade(self) -> RwLockReadGuard<'a, T> {
        let guard = ManuallyDrop::new(self);
        guard.lock.raw.downgrade_upgradable_to_read();
        RwLockReadGuard { lock: guard.lock }
    }
}
