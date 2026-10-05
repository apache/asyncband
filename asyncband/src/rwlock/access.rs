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

//! Tokens that own acquired `RwLock` permits and release them on drop.
//!
//! Guards hold a token instead of implementing `Drop`, so projecting or downgrading a guard moves
//! the token into the new guard.

use std::sync::Arc;

use crate::internal::semaphore::Semaphore;
use crate::rwlock::RwLock;

/// Keeps the lock's semaphore alive while a token holds permits from it.
pub trait Owner {
    /// Returns the semaphore that the token's permits belong to.
    fn semaphore(&self) -> &Semaphore;
}

impl Owner for &Semaphore {
    fn semaphore(&self) -> &Semaphore {
        self
    }
}

impl<T: ?Sized> Owner for &RwLock<T> {
    fn semaphore(&self) -> &Semaphore {
        &self.s
    }
}

impl<T: ?Sized> Owner for Arc<RwLock<T>> {
    fn semaphore(&self) -> &Semaphore {
        &self.s
    }
}

/// Owns one read permit.
pub struct ReadAccess<O: Owner> {
    // `None` only after the owner moved to a replacement token.
    owner: Option<O>,
}

impl<O: Owner> ReadAccess<O> {
    /// Takes over one permit already acquired from `owner`.
    pub fn new(owner: O) -> Self {
        Self { owner: Some(owner) }
    }

    /// Returns the owner that the permit was acquired from.
    pub fn owner(&self) -> &O {
        self.owner.as_ref().expect("token still has its owner")
    }
}

impl<'a, T: ?Sized> ReadAccess<&'a RwLock<T>> {
    /// Drops the value type so that mapped guards need not name it.
    pub fn into_semaphore(mut self) -> ReadAccess<&'a Semaphore> {
        let lock = self.owner.take().expect("token still has its owner");
        ReadAccess::new(&lock.s)
    }
}

impl<O: Owner> Drop for ReadAccess<O> {
    fn drop(&mut self) {
        if let Some(owner) = &self.owner {
            owner.semaphore().release(1);
        }
    }
}

/// Owns every permit of a lock, which together grant write access.
pub struct WriteAccess<O: Owner> {
    // `None` only after the owner moved to a replacement token.
    owner: Option<O>,
    permits_acquired: usize,
}

impl<O: Owner> WriteAccess<O> {
    /// Takes over `permits_acquired` permits already acquired from `owner`.
    pub fn new(owner: O, permits_acquired: usize) -> Self {
        Self {
            owner: Some(owner),
            permits_acquired,
        }
    }

    /// Returns the owner that the permits were acquired from.
    pub fn owner(&self) -> &O {
        self.owner.as_ref().expect("token still has its owner")
    }

    /// Releases all permits but one, which the returned token keeps.
    pub fn downgrade(mut self) -> ReadAccess<O> {
        let owner = self.owner.take().expect("token still has its owner");
        let read = ReadAccess::new(owner);
        read.owner().semaphore().release(self.permits_acquired - 1);
        read
    }
}

impl<'a, T: ?Sized> WriteAccess<&'a RwLock<T>> {
    /// Drops the value type so that mapped guards need not name it.
    pub fn into_semaphore(mut self) -> WriteAccess<&'a Semaphore> {
        let lock = self.owner.take().expect("token still has its owner");
        WriteAccess::new(&lock.s, self.permits_acquired)
    }
}

impl<O: Owner> Drop for WriteAccess<O> {
    fn drop(&mut self) {
        if let Some(owner) = &self.owner {
            owner.semaphore().release(self.permits_acquired);
        }
    }
}
