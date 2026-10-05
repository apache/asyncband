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
// Upstream sources:
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/rwlock/owned_read_guard.rs
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/rwlock/owned_write_guard_mapped.rs

use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::rwlock::RwLock;
use crate::rwlock::access::ReadAccess;

/// Shared access to a projection of a locked value kept alive by an `Arc`.
///
/// Use [`OwnedRwLockReadGuard::map`](crate::rwlock::OwnedRwLockReadGuard::map) to select a
/// component. Dropping the guard releases its access.
#[must_use = "dropping the guard releases its read access immediately"]
pub struct OwnedMappedRwLockReadGuard<T: ?Sized, U: ?Sized> {
    access: ReadAccess<Arc<RwLock<T>>>,
    d: NonNull<U>,
    variance: PhantomData<fn() -> U>,
}

// SAFETY: Arc<RwLock<T>> is Send when T: Send + Sync, and we only provide shared access (&U)
// through deref(), so U: Sync is sufficient for safe cross-thread transfer.
unsafe impl<T: ?Sized + Send + Sync, U: ?Sized + Sync> Send for OwnedMappedRwLockReadGuard<T, U> {}

// SAFETY: OwnedMappedRwLockReadGuard can be safely shared between threads when T: Send + Sync and
// U: Sync. Multiple threads can hold &OwnedMappedRwLockReadGuard and call deref() concurrently,
// which only returns &U.
unsafe impl<T: ?Sized + Send + Sync, U: ?Sized + Sync> Sync for OwnedMappedRwLockReadGuard<T, U> {}

impl<T: ?Sized, U: ?Sized> OwnedMappedRwLockReadGuard<T, U> {
    pub(crate) fn new(d: NonNull<U>, access: ReadAccess<Arc<RwLock<T>>>) -> Self {
        Self {
            d,
            access,
            variance: PhantomData,
        }
    }
}

impl<T: ?Sized, U: ?Sized + fmt::Debug> fmt::Debug for OwnedMappedRwLockReadGuard<T, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized, U: ?Sized + fmt::Display> fmt::Display for OwnedMappedRwLockReadGuard<T, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized, U: ?Sized> Deref for OwnedMappedRwLockReadGuard<T, U> {
    type Target = U;
    fn deref(&self) -> &Self::Target {
        // SAFETY: we hold the read lock and the NonNull pointer is valid for the guard's lifetime
        unsafe { self.d.as_ref() }
    }
}

impl<T: ?Sized, U: ?Sized> OwnedMappedRwLockReadGuard<T, U> {
    /// Selects a component while retaining the same lock access.
    ///
    /// The closure runs while the original guard is held. If it panics, that guard is released.
    /// Call this as `OwnedMappedRwLockReadGuard::map(guard, f)` to avoid shadowing methods of
    /// the value.
    pub fn map<V, F>(orig: Self, f: F) -> OwnedMappedRwLockReadGuard<T, V>
    where
        F: FnOnce(&U) -> &V,
        V: ?Sized,
    {
        // SAFETY: orig.d is a valid NonNull<U> pointer that was created from a valid reference
        // when the original OwnedMappedRwLockReadGuard was constructed. The guard guarantees shared
        // access to the data through the rwlock, so dereferencing is safe.
        let d = NonNull::from(f(unsafe { orig.d.as_ref() }));
        OwnedMappedRwLockReadGuard::new(d, orig.access)
    }

    /// Selects a component, or returns the still-held original guard when the closure returns
    /// `None`.
    ///
    /// A panic in the closure releases the guard. Call this as
    /// `OwnedMappedRwLockReadGuard::filter_map(guard, f)`.
    pub fn filter_map<V, F>(orig: Self, f: F) -> Result<OwnedMappedRwLockReadGuard<T, V>, Self>
    where
        F: FnOnce(&U) -> Option<&V>,
        V: ?Sized,
    {
        // SAFETY: orig.d is a valid NonNull<U> pointer that was created from a valid reference
        // when the original OwnedMappedRwLockReadGuard was constructed. The guard guarantees shared
        // access to the data through the rwlock, so dereferencing is safe.
        match f(unsafe { orig.d.as_ref() }) {
            Some(d) => {
                let d = NonNull::from(d);
                Ok(OwnedMappedRwLockReadGuard::new(d, orig.access))
            }
            None => Err(orig),
        }
    }
}
