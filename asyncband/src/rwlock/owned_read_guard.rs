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
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/rwlock/owned_read_guard.rs

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use crate::rwlock::OwnedMappedRwLockReadGuard;
use crate::rwlock::RwLock;
use crate::rwlock::access::ReadAccess;

impl<T: ?Sized> RwLock<T> {
    /// Waits for a reader slot and returns a guard retaining this `Arc`.
    ///
    /// Ordering and cancellation follow [`Self::read`]. The guard keeps the lock alive without
    /// borrowing the caller's `Arc`.
    pub async fn read_owned(self: Arc<Self>) -> OwnedRwLockReadGuard<T> {
        self.s.acquire(1).await;
        OwnedRwLockReadGuard {
            access: ReadAccess::new(self),
        }
    }

    /// Returns an owned read guard immediately, or `None` when no reader slot is available.
    ///
    /// This consumes the passed `Arc` even when acquisition fails.
    pub fn try_read_owned(self: Arc<Self>) -> Option<OwnedRwLockReadGuard<T>> {
        if self.s.try_acquire(1) {
            Some(OwnedRwLockReadGuard {
                access: ReadAccess::new(self),
            })
        } else {
            None
        }
    }
}

/// Shared access to a locked value kept alive by an `Arc`.
///
/// Created by [`RwLock::read_owned`]. Dropping the guard releases its access.
#[must_use = "dropping the guard releases its read access immediately"]
pub struct OwnedRwLockReadGuard<T: ?Sized> {
    pub(super) access: ReadAccess<Arc<RwLock<T>>>,
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for OwnedRwLockReadGuard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for OwnedRwLockReadGuard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized> Deref for OwnedRwLockReadGuard<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.access.owner().c.get() }
    }
}

impl<T: ?Sized> OwnedRwLockReadGuard<T> {
    /// Selects a component while retaining the same lock access.
    ///
    /// The closure runs while the original guard is held. If it panics, that guard is released.
    /// Call this as `OwnedRwLockReadGuard::map(guard, f)` to avoid shadowing methods of the
    /// value.
    pub fn map<U, F>(orig: Self, f: F) -> OwnedMappedRwLockReadGuard<T, U>
    where
        F: FnOnce(&T) -> &U,
        U: ?Sized,
    {
        // SAFETY: The guard keeps the lock alive and holds shared access, so the pointer to the
        // value is valid and dereferencing it is safe.
        let d = std::ptr::NonNull::from(f(unsafe { &*orig.access.owner().c.get() }));
        OwnedMappedRwLockReadGuard::new(d, orig.access)
    }

    /// Selects a component, or returns the still-held original guard when the closure returns
    /// `None`.
    ///
    /// A panic in the closure releases the guard. Call this as
    /// `OwnedRwLockReadGuard::filter_map(guard, f)`.
    pub fn filter_map<U, F>(orig: Self, f: F) -> Result<OwnedMappedRwLockReadGuard<T, U>, Self>
    where
        F: FnOnce(&T) -> Option<&U>,
        U: ?Sized,
    {
        // SAFETY: The guard keeps the lock alive and holds shared access, so the pointer to the
        // value is valid and dereferencing it is safe.
        match f(unsafe { &*orig.access.owner().c.get() }) {
            Some(d) => {
                let d = std::ptr::NonNull::from(d);
                Ok(OwnedMappedRwLockReadGuard::new(d, orig.access))
            }
            None => Err(orig),
        }
    }
}
