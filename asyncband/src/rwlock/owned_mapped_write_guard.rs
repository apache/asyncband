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
use crate::rwlock::owned_mapped_read_guard;

/// Exclusive access to a projection of a locked value kept alive by an `Arc`.
///
/// Use [`OwnedRwLockWriteGuard::map`](crate::rwlock::OwnedRwLockWriteGuard::map) to select a
/// component. Dropping the guard releases its access.
#[must_use = "dropping the guard releases its write access immediately"]
pub struct OwnedMappedRwLockWriteGuard<T: ?Sized, U: ?Sized> {
    data: NonNull<U>,
    access: WriteAccess<Arc<RwLock<T>>>,
    variance: PhantomData<*mut U>,
}

pub fn new<T: ?Sized, U: ?Sized>(
    data: NonNull<U>,
    access: WriteAccess<Arc<RwLock<T>>>,
) -> OwnedMappedRwLockWriteGuard<T, U> {
    OwnedMappedRwLockWriteGuard {
        data,
        access,
        variance: PhantomData,
    }
}

// SAFETY: The Arc keeps T alive across threads; the projection only exposes exclusive access to U.
unsafe impl<T: ?Sized + Send + Sync, U: ?Sized + Send> Send for OwnedMappedRwLockWriteGuard<T, U> {}
// SAFETY: A shared guard reference only exposes &U, and the Arc is safe to share.
unsafe impl<T: ?Sized + Send + Sync, U: ?Sized + Sync> Sync for OwnedMappedRwLockWriteGuard<T, U> {}

impl<T: ?Sized, U: ?Sized> Deref for OwnedMappedRwLockWriteGuard<T, U> {
    type Target = U;

    fn deref(&self) -> &U {
        // SAFETY: The access token holds exclusive access and keeps the projection valid.
        unsafe { self.data.as_ref() }
    }
}

impl<T: ?Sized, U: ?Sized> DerefMut for OwnedMappedRwLockWriteGuard<T, U> {
    fn deref_mut(&mut self) -> &mut U {
        // SAFETY: The write token excludes all other access, and self is exclusively borrowed.
        unsafe { self.data.as_mut() }
    }
}

impl<T: ?Sized, U: ?Sized> OwnedMappedRwLockWriteGuard<T, U> {
    /// Selects a component while retaining the same lock access.
    ///
    /// The closure runs while the original guard is held. If it panics, that guard is released.
    /// Call this as `OwnedMappedRwLockWriteGuard::map(guard, project)` to avoid shadowing methods
    /// of the value.
    pub fn map<V: ?Sized, F>(mut orig: Self, project: F) -> OwnedMappedRwLockWriteGuard<T, V>
    where
        F: FnOnce(&mut U) -> &mut V,
    {
        let data = NonNull::from(project(&mut *orig));
        new(data, orig.access)
    }

    /// Selects a component, or returns the still-held original guard when the closure returns
    /// `None`.
    ///
    /// A panic in the closure releases the guard. Call this as
    /// `OwnedMappedRwLockWriteGuard::filter_map(guard, project)`.
    pub fn filter_map<V: ?Sized, F>(
        mut orig: Self,
        project: F,
    ) -> Result<OwnedMappedRwLockWriteGuard<T, V>, Self>
    where
        F: FnOnce(&mut U) -> Option<&mut V>,
    {
        let Some(data) = project(&mut *orig).map(NonNull::from) else {
            return Err(orig);
        };
        Ok(new(data, orig.access))
    }

    /// Retains shared access to the same projection while releasing exclusive access.
    ///
    /// There is no unlocked interval in which another writer can modify the value. Queued
    /// requests retain their order, so a waiting writer can prevent later readers from joining.
    pub fn downgrade(self) -> OwnedMappedRwLockReadGuard<T, U> {
        owned_mapped_read_guard::new(self.data, self.access.downgrade())
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
