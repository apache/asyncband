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
// Upstream sources:
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/rwlock/read_guard.rs
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/rwlock/write_guard_mapped.rs

use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr::NonNull;

use crate::internal::semaphore::Semaphore;
use crate::rwlock::access::ReadAccess;

/// Shared access to a projection of a locked value borrowed for the guard lifetime.
///
/// Use [`RwLockReadGuard::map`](crate::rwlock::RwLockReadGuard::map) to select a component.
/// Dropping the guard releases its access.
#[must_use = "dropping the guard releases its read access immediately"]
pub struct MappedRwLockReadGuard<'a, T: ?Sized> {
    d: NonNull<T>,
    access: ReadAccess<&'a Semaphore>,
    variance: PhantomData<fn() -> T>,
}

// SAFETY: MappedRwLockReadGuard is Send when T: Sync. We don't require T: Send because
// the guard RwLockReadGuard doesn't transfer ownership of T - it only holds a shared reference.
// When moved to another thread, the guard maintains the read lock and the new thread
// can safely access &T (which is allowed since T: Sync). The semaphore reference
// and NonNull pointer are both safe to transfer between threads.
unsafe impl<T: ?Sized + Sync> Send for MappedRwLockReadGuard<'_, T> {}

// SAFETY: `&MappedRwLockReadGuard` can be shared between threads if `T: Sync`.
// Accessing the guard only provides a `&T`, which is safe to share concurrently when `T: Sync`.
unsafe impl<T: ?Sized + Sync> Sync for MappedRwLockReadGuard<'_, T> {}

impl<'a, T: ?Sized> MappedRwLockReadGuard<'a, T> {
    pub(crate) fn new(d: NonNull<T>, access: ReadAccess<&'a Semaphore>) -> Self {
        Self {
            d,
            access,
            variance: PhantomData,
        }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MappedRwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for MappedRwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized> Deref for MappedRwLockReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // SAFETY: we hold the read lock and the NonNull pointer is valid for the guard's lifetime
        unsafe { self.d.as_ref() }
    }
}

impl<'a, T: ?Sized> MappedRwLockReadGuard<'a, T> {
    /// Selects a component while retaining the same lock access.
    ///
    /// The closure runs while the original guard is held. If it panics, that guard is released.
    /// Call this as `MappedRwLockReadGuard::map(guard, f)` to avoid shadowing methods of the
    /// value.
    pub fn map<U, F>(orig: Self, f: F) -> MappedRwLockReadGuard<'a, U>
    where
        F: FnOnce(&T) -> &U,
        U: ?Sized,
    {
        let d = NonNull::from(f(&*orig));
        MappedRwLockReadGuard::new(d, orig.access)
    }

    /// Selects a component, or returns the still-held original guard when the closure returns
    /// `None`.
    ///
    /// A panic in the closure releases the guard. Call this as
    /// `MappedRwLockReadGuard::filter_map(guard, f)`.
    pub fn filter_map<U, F>(orig: Self, f: F) -> Result<MappedRwLockReadGuard<'a, U>, Self>
    where
        F: FnOnce(&T) -> Option<&U>,
        U: ?Sized,
    {
        match f(&*orig) {
            Some(d) => {
                let d = NonNull::from(d);
                Ok(MappedRwLockReadGuard::new(d, orig.access))
            }
            None => Err(orig),
        }
    }
}
