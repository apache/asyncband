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
// Asyncband rewrote the guard lifecycle around private access tokens; see LICENSE.
// Upstream source:
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/rwlock/owned_write_guard_mapped.rs

use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::rwlock::OwnedMappedRwLockReadGuard;
use crate::rwlock::RwLock;
use crate::rwlock::access::WriteAccess;

/// Exclusive access to a projection of a locked value kept alive by an `Arc`.
///
/// Use [`OwnedRwLockWriteGuard::map`](crate::rwlock::OwnedRwLockWriteGuard::map) to select a
/// component. Dropping the guard releases its access.
#[must_use = "dropping the guard releases its write access immediately"]
pub struct OwnedMappedRwLockWriteGuard<T: ?Sized, U: ?Sized> {
    d: NonNull<U>,
    access: WriteAccess<Arc<RwLock<T>>>,
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
    pub(crate) fn new(d: NonNull<U>, access: WriteAccess<Arc<RwLock<T>>>) -> Self {
        Self {
            d,
            access,
            variance: PhantomData,
        }
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
    /// Selects a component while retaining the same lock access.
    ///
    /// The closure runs while the original guard is held. If it panics, that guard is released.
    /// Call this as `OwnedMappedRwLockWriteGuard::map(guard, f)` to avoid shadowing methods
    /// of the value.
    pub fn map<V, F>(mut orig: Self, f: F) -> OwnedMappedRwLockWriteGuard<T, V>
    where
        F: FnOnce(&mut U) -> &mut V,
        V: ?Sized,
    {
        let d = NonNull::from(f(&mut *orig));
        OwnedMappedRwLockWriteGuard::new(d, orig.access)
    }

    /// Selects a component, or returns the still-held original guard when the closure returns
    /// `None`.
    ///
    /// A panic in the closure releases the guard. Call this as
    /// `OwnedMappedRwLockWriteGuard::filter_map(guard, f)`.
    pub fn filter_map<V, F>(mut orig: Self, f: F) -> Result<OwnedMappedRwLockWriteGuard<T, V>, Self>
    where
        F: FnOnce(&mut U) -> Option<&mut V>,
        V: ?Sized,
    {
        match f(&mut *orig) {
            Some(d) => {
                let d = NonNull::from(d);
                Ok(OwnedMappedRwLockWriteGuard::new(d, orig.access))
            }
            None => Err(orig),
        }
    }

    /// Retains shared access to the same projection while releasing exclusive access.
    ///
    /// There is no unlocked interval in which another writer can modify the value. Queued
    /// requests retain their order, so a waiting writer can prevent later readers from joining.
    pub fn downgrade(self) -> OwnedMappedRwLockReadGuard<T, U> {
        OwnedMappedRwLockReadGuard::new(self.d, self.access.downgrade())
    }
}
