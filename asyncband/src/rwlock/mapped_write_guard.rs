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
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/rwlock/write_guard_mapped.rs

use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;

use crate::internal::semaphore::Semaphore;
use crate::rwlock::MappedRwLockReadGuard;
use crate::rwlock::access::WriteAccess;
use crate::rwlock::mapped_read_guard;

/// Exclusive access to a projection of a locked value borrowed for the guard lifetime.
///
/// Use [`RwLockWriteGuard::map`](crate::rwlock::RwLockWriteGuard::map) to select a component.
/// Dropping the guard releases its access.
#[must_use = "dropping the guard releases its write access immediately"]
pub struct MappedRwLockWriteGuard<'a, T: ?Sized> {
    data: NonNull<T>,
    access: WriteAccess<&'a Semaphore>,
    variance: PhantomData<&'a mut T>,
}

pub fn new<'a, T: ?Sized>(
    data: NonNull<T>,
    access: WriteAccess<&'a Semaphore>,
) -> MappedRwLockWriteGuard<'a, T> {
    MappedRwLockWriteGuard {
        data,
        access,
        variance: PhantomData,
    }
}

// SAFETY: Moving this guard transfers exclusive access without moving or dropping T.
unsafe impl<T: ?Sized + Send> Send for MappedRwLockWriteGuard<'_, T> {}
// SAFETY: Sharing the guard only exposes &T; the access token keeps the lock held.
unsafe impl<T: ?Sized + Send + Sync> Sync for MappedRwLockWriteGuard<'_, T> {}

impl<T: ?Sized> Deref for MappedRwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: The access token holds exclusive access and keeps the projection valid.
        unsafe { self.data.as_ref() }
    }
}

impl<T: ?Sized> DerefMut for MappedRwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: The write token excludes all other access, and self is exclusively borrowed.
        unsafe { self.data.as_mut() }
    }
}

impl<'a, T: ?Sized> MappedRwLockWriteGuard<'a, T> {
    /// Selects a component while retaining the same lock access.
    ///
    /// The closure runs while the original guard is held. If it panics, that guard is released.
    /// Call this as `MappedRwLockWriteGuard::map(guard, project)` to avoid shadowing methods of the
    /// value.
    pub fn map<U: ?Sized, F>(mut orig: Self, project: F) -> MappedRwLockWriteGuard<'a, U>
    where
        F: FnOnce(&mut T) -> &mut U,
    {
        let data = NonNull::from(project(&mut *orig));
        new(data, orig.access)
    }

    /// Selects a component, or returns the still-held original guard when the closure returns
    /// `None`.
    ///
    /// A panic in the closure releases the guard. Call this as
    /// `MappedRwLockWriteGuard::filter_map(guard, project)`.
    pub fn filter_map<U: ?Sized, F>(
        mut orig: Self,
        project: F,
    ) -> Result<MappedRwLockWriteGuard<'a, U>, Self>
    where
        F: FnOnce(&mut T) -> Option<&mut U>,
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
    pub fn downgrade(self) -> MappedRwLockReadGuard<'a, T> {
        mapped_read_guard::new(self.data, self.access.downgrade())
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MappedRwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for MappedRwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}
