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

/// Exclusive access to a projection of a locked value borrowed for the guard lifetime.
///
/// Use [`RwLockWriteGuard::map`](crate::rwlock::RwLockWriteGuard::map) to select a component.
/// Dropping the guard releases its access.
#[must_use = "dropping the guard releases its write access immediately"]
pub struct MappedRwLockWriteGuard<'a, T: ?Sized> {
    d: NonNull<T>,
    access: WriteAccess<&'a Semaphore>,
    // Mutable access requires invariance over T.
    variance: PhantomData<&'a mut T>,
}

// SAFETY: A `&MappedRwLockWriteGuard` can be safely shared between threads because it provides
// exclusive access to the data, and the `T: Send + Sync` bound prevents data races.
unsafe impl<T: ?Sized + Send + Sync> Sync for MappedRwLockWriteGuard<'_, T> {}

// SAFETY: `MappedRwLockWriteGuard` owns the lock and can be safely sent to another thread.
// The `T: Send` bound ensures that the data can be safely accessed by the new thread,
// and the guard's lifetime guarantees that the data remains valid.
unsafe impl<T: ?Sized + Send> Send for MappedRwLockWriteGuard<'_, T> {}

impl<'a, T: ?Sized> MappedRwLockWriteGuard<'a, T> {
    pub(crate) fn new(d: NonNull<T>, access: WriteAccess<&'a Semaphore>) -> Self {
        Self {
            d,
            access,
            variance: PhantomData,
        }
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

impl<T: ?Sized> Deref for MappedRwLockWriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // SAFETY: we hold the write lock and the NonNull pointer is valid for the guard's lifetime
        unsafe { self.d.as_ref() }
    }
}

impl<T: ?Sized> DerefMut for MappedRwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: we hold the write lock and the NonNull pointer is valid for the guard's lifetime
        unsafe { self.d.as_mut() }
    }
}

impl<'a, T: ?Sized> MappedRwLockWriteGuard<'a, T> {
    /// Selects a component while retaining the same lock access.
    ///
    /// The closure runs while the original guard is held. If it panics, that guard is released.
    /// Call this as `MappedRwLockWriteGuard::map(guard, f)` to avoid shadowing methods of the
    /// value.
    pub fn map<U, F>(mut orig: Self, f: F) -> MappedRwLockWriteGuard<'a, U>
    where
        F: FnOnce(&mut T) -> &mut U,
        U: ?Sized,
    {
        let d = NonNull::from(f(&mut *orig));
        MappedRwLockWriteGuard::new(d, orig.access)
    }

    /// Selects a component, or returns the still-held original guard when the closure returns
    /// `None`.
    ///
    /// A panic in the closure releases the guard. Call this as
    /// `MappedRwLockWriteGuard::filter_map(guard, f)`.
    pub fn filter_map<U, F>(mut orig: Self, f: F) -> Result<MappedRwLockWriteGuard<'a, U>, Self>
    where
        F: FnOnce(&mut T) -> Option<&mut U>,
        U: ?Sized,
    {
        match f(&mut *orig) {
            Some(d) => {
                let d = NonNull::from(d);
                Ok(MappedRwLockWriteGuard::new(d, orig.access))
            }
            None => Err(orig),
        }
    }

    /// Retains shared access to the same projection while releasing exclusive access.
    ///
    /// There is no unlocked interval in which another writer can modify the value. Queued
    /// requests retain their order, so a waiting writer can prevent later readers from joining.
    pub fn downgrade(self) -> MappedRwLockReadGuard<'a, T> {
        MappedRwLockReadGuard::new(self.d, self.access.downgrade())
    }
}
