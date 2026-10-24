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
use std::mem;
use std::num::NonZeroUsize;
use std::ops::Deref;

use crate::internal::semaphore::Semaphore;
use crate::rwlock::RwLock;
use crate::rwlock::RwLockReadGuard;
use crate::rwlock::RwLockWriteGuard;
use crate::rwlock::access::ReadAccess;
use crate::rwlock::access::WriteAccess;
use crate::rwlock::write_guard;

/// Shared and exclusive access with a single reader that can reserve an atomic upgrade.
///
/// Ordinary readers and one upgradable reader may coexist, up to the configured reader limit.
/// Writers exclude all readers. An upgrade retains its read permit until it owns every permit,
/// so no writer can modify the value between inspection and mutation. Panics do not poison the
/// lock. Ordinary read and write guards are the same types returned by [`RwLock`].
///
/// # Ordering
///
/// Writers and upgradable readers acquire a shared reservation in FIFO order, before requesting
/// data permits. An upgradable reader retains this reservation until upgrade, downgrade, or drop.
/// A writer releases it once it has exclusive access. Thus a writer waiting behind an upgradable
/// reader cannot enter the data-permit queue ahead of that reader's upgrade and deadlock it.
/// A writer already waiting for the reservation precedes a later upgradable acquisition.
///
/// Ordinary readers bypass the reservation. Data permits are assigned in FIFO order: once a
/// writer or upgrade queues for them, later readers cannot pass it. Readers already queued before
/// an upgrade may finish first. With a reader limit of one, upgrading needs no additional permits
/// and completes immediately, even if readers are queued.
///
/// There is no single FIFO order across both queues. Readers may pass writers still waiting for
/// the reservation, including while an upgradable guard is held. Progress assumes holders release
/// their guards and granted futures are polled or cancelled; an indefinitely held upgradable guard
/// can indefinitely delay writers. Like [`RwLock`], recursive acquisition can deadlock: do not
/// retain another read guard while awaiting an upgrade, or await a writer while retaining the
/// upgradable guard. An ordinary read guard cannot be upgraded.
///
/// # Synchronization
///
/// Writes made under an exclusive guard happen-before subsequent successful read, upgradable-read,
/// or write acquisitions after that guard is dropped or downgraded. A successful upgrade also
/// synchronizes with the readers whose release allows it to complete. These guarantees apply to
/// both asynchronous and successful `try_` acquisitions; failed `try_` calls grant no access.
///
/// # Cancellation
///
/// Dropping a pending acquisition releases its reservation and any assigned permits, losing its
/// queue position. [`upgrade`](RwLockUpgradableReadGuard::upgrade) consumes its guard; cancelling
/// that future releases the guard as well, including before its first poll.
///
/// # Example
///
/// ```
/// use asyncband::rwlock::RwLockUpgradableReadGuard;
/// use asyncband::rwlock::UpgradableRwLock;
///
/// # #[tokio::main]
/// # async fn main() {
/// let routes = UpgradableRwLock::new(vec!["/health"]);
/// let snapshot = routes.upgradable_read().await;
/// let snapshot = if !snapshot.contains(&"/metrics") {
///     let mut edit = RwLockUpgradableReadGuard::upgrade(snapshot).await;
///     edit.push("/metrics");
///     edit.downgrade()
/// } else {
///     snapshot.downgrade()
/// };
/// assert_eq!(*snapshot, ["/health", "/metrics"]);
/// # }
/// ```
pub struct UpgradableRwLock<T: ?Sized> {
    // Every operation taking more than one data permit must first hold this reservation.
    // It is never exposed, nor is the inner lock, so writers cannot bypass it.
    reservation: Semaphore,
    inner: RwLock<T>,
}

impl<T> UpgradableRwLock<T> {
    /// Wraps a value with a reader limit of `usize::MAX >> 1`.
    pub const fn new(value: T) -> Self {
        Self::with_max_readers(value, NonZeroUsize::new(usize::MAX >> 1).unwrap())
    }

    /// Wraps a value with an explicit nonzero limit on simultaneously held read guards.
    ///
    /// An upgradable or downgraded guard occupies one reader slot. Every `NonZeroUsize` is
    /// accepted.
    pub const fn with_max_readers(value: T, max_readers: NonZeroUsize) -> Self {
        Self {
            reservation: Semaphore::new(1),
            inner: RwLock::with_max_readers(value, max_readers),
        }
    }

    /// Unwraps the value by consuming its lock.
    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }
}

impl<T: ?Sized> UpgradableRwLock<T> {
    /// Borrows the value exclusively through an exclusive borrow of the lock itself.
    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    /// Waits for a reader slot, bypassing the reservation queue.
    ///
    /// Writers and upgrades already queued for data permits must finish first.
    ///
    /// # Cancel safety
    ///
    /// Cancellation releases any assigned permit and loses the request's queue position.
    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        self.inner.read().await
    }

    /// Returns a read guard immediately, or `None` when a reader slot cannot be acquired.
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        self.inner.try_read()
    }

    /// Waits for the reservation, then for exclusive access.
    ///
    /// See [ordering](Self#ordering) for how ordinary readers may pass this request.
    ///
    /// # Cancel safety
    ///
    /// Cancellation releases the reservation and any assigned permits and loses both queue
    /// positions.
    pub async fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.reservation.acquire(1).await;
        let reservation = ReadAccess::new(&self.reservation);
        let write = self.inner.write().await;
        drop(reservation);
        write
    }

    /// Returns a write guard immediately, or `None` when the reservation or exclusive access is
    /// unavailable. Failure releases any reservation acquired by this call.
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        if !self.reservation.try_acquire(1) {
            return None;
        }
        let reservation = ReadAccess::new(&self.reservation);
        let write = self.inner.try_write();
        drop(reservation);
        write
    }

    /// Waits for the reservation, then for one reader slot.
    ///
    /// The returned guard excludes other upgradable readers and reserves priority over later
    /// writers. Ordinary readers can coexist with it.
    ///
    /// # Cancel safety
    ///
    /// Cancellation releases the reservation and any assigned permit and loses both queue
    /// positions.
    pub async fn upgradable_read(&self) -> RwLockUpgradableReadGuard<'_, T> {
        self.reservation.acquire(1).await;
        let reservation = ReadAccess::new(&self.reservation);
        self.inner.s.acquire(1).await;
        RwLockUpgradableReadGuard {
            access: ReadAccess::new(&self.inner),
            reservation,
        }
    }

    /// Returns an upgradable guard immediately, or `None` when the reservation or a reader slot
    /// is unavailable. Failure releases any reservation acquired by this call.
    pub fn try_upgradable_read(&self) -> Option<RwLockUpgradableReadGuard<'_, T>> {
        if !self.reservation.try_acquire(1) {
            return None;
        }
        let reservation = ReadAccess::new(&self.reservation);
        if !self.inner.s.try_acquire(1) {
            return None;
        }
        Some(RwLockUpgradableReadGuard {
            access: ReadAccess::new(&self.inner),
            reservation,
        })
    }
}

impl<T> From<T> for UpgradableRwLock<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for UpgradableRwLock<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for UpgradableRwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("UpgradableRwLock");
        match self.try_read() {
            Some(inner) => d.field("data", &&*inner),
            None => d.field("data", &format_args!("<locked>")),
        };
        d.finish()
    }
}

/// Shared access with the unique reservation to upgrade to exclusive access.
///
/// Created by [`UpgradableRwLock::upgradable_read`]. Dropping it releases its reader slot and
/// reservation. This guard cannot be mapped: promotion always protects the original value.
#[must_use = "dropping the guard releases its read access and upgrade reservation immediately"]
pub struct RwLockUpgradableReadGuard<'a, T: ?Sized> {
    // Unlike an ordinary read guard, this token must retain T: Send + Sync for Send: moving the
    // guard to another thread also moves the ability to acquire mutable access by upgrading.
    access: ReadAccess<&'a RwLock<T>>,
    reservation: ReadAccess<&'a Semaphore>,
}

impl<T: ?Sized> Deref for RwLockUpgradableReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the token owns one data permit, excluding writers for this borrow's lifetime.
        // Upgrade consumes the guard, so it cannot overlap a shared reference borrowed from it.
        unsafe { &*self.access.owner().c.get() }
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
    /// Waits for exclusive access without releasing the held read permit.
    ///
    /// The reservation prevents writers from queuing ahead of this upgrade. Ordinary readers
    /// already ahead in the data-permit queue may finish first; later readers cannot pass it.
    ///
    /// # Cancel safety
    ///
    /// This consumes the guard. Dropping the future releases the original reader slot, any
    /// partially or fully assigned upgrade permits, and the reservation. This also holds if the
    /// future has never been polled. The original guard is not returned on cancellation.
    pub async fn upgrade(this: Self) -> RwLockWriteGuard<'a, T> {
        let lock = *this.access.owner();
        // Keep both RAII tokens alive across the await. The semaphore's acquisition future owns
        // additional permits until completion and returns them if cancelled, even after a grant.
        lock.s.acquire(lock.max_readers - 1).await;
        this.into_write()
    }

    /// Converts immediately to exclusive access, or returns the unchanged guard on failure.
    ///
    /// Failure neither consumes additional permits nor reserves a queue position. With a reader
    /// limit of one, this always succeeds.
    ///
    /// ```
    /// use asyncband::rwlock::RwLockUpgradableReadGuard;
    /// use asyncband::rwlock::UpgradableRwLock;
    ///
    /// let lock = UpgradableRwLock::new(1);
    /// let upgradable = lock.try_upgradable_read().unwrap();
    /// let reader = lock.try_read().unwrap();
    /// let upgradable = RwLockUpgradableReadGuard::try_upgrade(upgradable).unwrap_err();
    /// drop(reader);
    /// let mut writer = RwLockUpgradableReadGuard::try_upgrade(upgradable).unwrap();
    /// *writer += 1;
    /// ```
    pub fn try_upgrade(this: Self) -> Result<RwLockWriteGuard<'a, T>, Self> {
        let lock = *this.access.owner();
        if lock.s.try_acquire(lock.max_readers - 1) {
            Ok(this.into_write())
        } else {
            Err(this)
        }
    }

    /// Releases the upgrade reservation while retaining an ordinary reader slot.
    ///
    /// There is no unlocked interval. A queued writer may prevent new readers from joining, but
    /// cannot modify the value until the returned guard is dropped.
    pub fn downgrade(self) -> RwLockReadGuard<'a, T> {
        let Self {
            access,
            reservation,
        } = self;
        let read = RwLockReadGuard { access };
        drop(reservation);
        read
    }

    // Called only after acquiring all max_readers - 1 additional data permits. No await or
    // callback occurs during the transfer. The borrowed token has no owner allocation to free.
    fn into_write(self) -> RwLockWriteGuard<'a, T> {
        let Self {
            access,
            reservation,
        } = self;
        let lock = *access.owner();
        mem::forget(access);
        let write = write_guard::from_access(WriteAccess::new(lock, lock.max_readers));
        drop(reservation);
        write
    }
}

#[cfg(doctest)]
mod compile_fail_tests {
    /// ```compile_fail
    /// use asyncband::rwlock::RwLockUpgradableReadGuard;
    ///
    /// fn shorten<'lock, 'short: 'lock>(
    ///     guard: RwLockUpgradableReadGuard<'lock, &'static str>,
    /// ) -> RwLockUpgradableReadGuard<'lock, &'short str> {
    ///     guard
    /// }
    /// ```
    struct UpgradableGuardIsInvariant;

    /// ```compile_fail
    /// use std::sync::MutexGuard;
    /// use asyncband::rwlock::RwLockUpgradableReadGuard;
    ///
    /// fn assert_send<T: Send>() {}
    /// // MutexGuard is Sync but not Send: promotion could move its value on another thread.
    /// assert_send::<RwLockUpgradableReadGuard<'_, MutexGuard<'static, ()>>>();
    /// ```
    struct UpgradableGuardRequiresSend;

    /// ```compile_fail
    /// use std::cell::Cell;
    /// use asyncband::rwlock::RwLockUpgradableReadGuard;
    ///
    /// fn assert_send<T: Send>() {}
    /// assert_send::<RwLockUpgradableReadGuard<'_, Cell<u8>>>();
    /// ```
    struct UpgradableGuardRequiresSync;

    /// ```compile_fail
    /// use asyncband::rwlock::UpgradableRwLock;
    ///
    /// let lock = UpgradableRwLock::new(0);
    /// let guard = lock.try_upgradable_read().unwrap();
    /// let borrowed = &*guard;
    /// let mut writer = RwLockUpgradableReadGuard::try_upgrade(guard).unwrap();
    /// *writer = 1;
    /// assert_eq!(*borrowed, 0);
    /// ```
    struct UpgradeExcludesOutstandingBorrows;
}
