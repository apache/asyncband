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
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/rwlock/read_guard.rs

use std::fmt;
use std::ops::Deref;
use std::ptr::NonNull;

use crate::rwlock::MappedRwLockReadGuard;
use crate::rwlock::RwLock;
use crate::rwlock::access::ReadAccess;
use crate::rwlock::mapped_read_guard;

/// Shared access to a locked value borrowed for the guard lifetime.
///
/// Created by [`RwLock::read`]. Dropping the guard releases its access.
#[must_use = "dropping the guard releases its read access immediately"]
pub struct RwLockReadGuard<'a, T: ?Sized> {
    access: ReadAccess<&'a RwLock<T>>,
}

pub fn new<'a, T: ?Sized>(access: ReadAccess<&'a RwLock<T>>) -> RwLockReadGuard<'a, T> {
    RwLockReadGuard { access }
}

// SAFETY: Moving this guard transfers shared access without moving or dropping T.
unsafe impl<T: ?Sized + Sync> Send for RwLockReadGuard<'_, T> {}
// SAFETY: Sharing the guard only exposes &T; the access token keeps the lock held.
unsafe impl<T: ?Sized + Send + Sync> Sync for RwLockReadGuard<'_, T> {}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: The access token holds shared access and keeps the value valid.
        unsafe { &*self.access.owner().c.get() }
    }
}

impl<'a, T: ?Sized> RwLockReadGuard<'a, T> {
    /// Selects a component while retaining the same lock access.
    ///
    /// The closure runs while the original guard is held. If it panics, that guard is released.
    /// Call this as `RwLockReadGuard::map(guard, project)` to avoid shadowing methods of the value.
    pub fn map<U: ?Sized, F>(orig: Self, project: F) -> MappedRwLockReadGuard<'a, U>
    where
        F: FnOnce(&T) -> &U,
    {
        let data = NonNull::from(project(&*orig));
        mapped_read_guard::new(data, orig.access.into_semaphore())
    }

    /// Selects a component, or returns the still-held original guard when the closure returns
    /// `None`.
    ///
    /// A panic in the closure releases the guard. Call this as `RwLockReadGuard::filter_map(guard,
    /// project)`.
    pub fn filter_map<U: ?Sized, F>(
        orig: Self,
        project: F,
    ) -> Result<MappedRwLockReadGuard<'a, U>, Self>
    where
        F: FnOnce(&T) -> Option<&U>,
    {
        let Some(data) = project(&*orig).map(NonNull::from) else {
            return Err(orig);
        };
        Ok(mapped_read_guard::new(data, orig.access.into_semaphore()))
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}
