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

// Portions of the guard API originated from Tokio 1.42.0's RwLock implementation.
// Copyright (c) Tokio Contributors
// The Tokio-derived portions remain licensed under the MIT License.
// Asyncband replaced guard-local destruction and manual ownership transfers with movable RAII
// access tokens. Projection moves the token, and downgrade establishes a read token before waking
// waiters. The public documentation and examples describe Asyncband's access and projection model.
// Upstream source:
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/rwlock/owned_write_guard.rs

use std::fmt;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::rwlock::OwnedMappedRwLockWriteGuard;
use crate::rwlock::OwnedRwLockReadGuard;
use crate::rwlock::RwLock;
use crate::rwlock::access::WriteAccess;

impl<T: ?Sized> RwLock<T> {
    /// Waits for exclusive access and returns a guard retaining this `Arc`.
    ///
    /// Ordering follows [`Self::write`].
    ///
    /// # Cancel safety
    ///
    /// Pending lock requests complete in order. Cancelling this future loses its place among them
    /// and releases any reserved permits.
    pub async fn write_owned(self: Arc<Self>) -> OwnedRwLockWriteGuard<T> {
        self.s.acquire(self.max_readers).await;
        let permits_acquired = self.max_readers;
        OwnedRwLockWriteGuard {
            access: WriteAccess::new(self, permits_acquired),
        }
    }

    /// Returns an owned write guard immediately, or `None` when exclusive access is unavailable.
    ///
    /// This consumes the passed `Arc` even when acquisition fails.
    pub fn try_write_owned(self: Arc<Self>) -> Option<OwnedRwLockWriteGuard<T>> {
        if self.s.try_acquire(self.max_readers) {
            let permits_acquired = self.max_readers;
            Some(OwnedRwLockWriteGuard {
                access: WriteAccess::new(self, permits_acquired),
            })
        } else {
            None
        }
    }
}

/// Exclusive access to a locked value kept alive by an `Arc`.
///
/// Created by [`RwLock::write_owned`]. Dropping the guard releases its access.
#[must_use = "dropping the guard releases its write access immediately"]
pub struct OwnedRwLockWriteGuard<T: ?Sized> {
    access: WriteAccess<Arc<RwLock<T>>>,
}

unsafe impl<T: ?Sized + Send + Sync> Send for OwnedRwLockWriteGuard<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for OwnedRwLockWriteGuard<T> {}

impl<T: ?Sized + fmt::Debug> fmt::Debug for OwnedRwLockWriteGuard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for OwnedRwLockWriteGuard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized> Deref for OwnedRwLockWriteGuard<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // SAFETY: `access` holds the write permit on the lock, so this guard is the only path to
        // the cell while it lives.
        unsafe { &*self.access.owner().c.get() }
    }
}

impl<T: ?Sized> DerefMut for OwnedRwLockWriteGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: `access` holds the write permit on the lock, so this guard is the only path to
        // the cell while it lives.
        unsafe { &mut *self.access.owner().c.get() }
    }
}

impl<T: ?Sized> OwnedRwLockWriteGuard<T> {
    /// Selects a component while retaining the same lock access.
    ///
    /// The closure runs while the original guard is held. If it panics, that guard is released.
    /// Call this as `OwnedRwLockWriteGuard::map(guard, f)` to avoid shadowing methods of the
    /// value.
    pub fn map<U, F>(mut orig: Self, f: F) -> OwnedMappedRwLockWriteGuard<T, U>
    where
        F: FnOnce(&mut T) -> &mut U,
        U: ?Sized,
    {
        let d = NonNull::from(f(&mut *orig));
        OwnedMappedRwLockWriteGuard::new(d, orig.access)
    }

    /// Selects a component, or returns the still-held original guard when the closure returns
    /// `None`.
    ///
    /// A panic in the closure releases the guard. Call this as
    /// `OwnedRwLockWriteGuard::filter_map(guard, f)`.
    pub fn filter_map<U, F>(mut orig: Self, f: F) -> Result<OwnedMappedRwLockWriteGuard<T, U>, Self>
    where
        F: FnOnce(&mut T) -> Option<&mut U>,
        U: ?Sized,
    {
        let d = match f(&mut *orig) {
            Some(d) => NonNull::from(d),
            None => return Err(orig),
        };
        Ok(OwnedMappedRwLockWriteGuard::new(d, orig.access))
    }

    /// Retains shared access while releasing exclusive access.
    ///
    /// There is no unlocked interval in which another writer can modify the value. Queued
    /// requests retain their order, so a waiting writer can prevent later readers from joining.
    pub fn downgrade(self) -> OwnedRwLockReadGuard<T> {
        OwnedRwLockReadGuard {
            access: self.access.downgrade(),
        }
    }
}
