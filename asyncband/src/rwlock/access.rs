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

//! Owns acquired permits independently of the guard's data projection.
//!
//! Tokens are created only after acquisition succeeds. Their optional owner is present until an
//! ownership transfer; taking it disarms the old token without suppressing Rust's drop machinery.
//! All supported owners are references or Arcs, so Option adds no storage to them.

use std::sync::Arc;

use crate::internal::semaphore::Semaphore;
use crate::rwlock::RwLock;

pub trait Owner {
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

pub struct ReadAccess<O: Owner> {
    owner: Option<O>,
}

impl<O: Owner> ReadAccess<O> {
    /// Takes responsibility for one already-acquired permit.
    pub fn new(owner: O) -> Self {
        Self { owner: Some(owner) }
    }

    pub fn owner(&self) -> &O {
        self.owner.as_ref().unwrap()
    }
}

impl<'a, T: ?Sized> ReadAccess<&'a RwLock<T>> {
    /// A borrowed projection no longer needs the original value's type.
    pub fn into_semaphore(mut self) -> ReadAccess<&'a Semaphore> {
        ReadAccess::new(&self.owner.take().unwrap().s)
    }
}

impl<O: Owner> Drop for ReadAccess<O> {
    fn drop(&mut self) {
        if let Some(owner) = &self.owner {
            owner.semaphore().release(1);
        }
    }
}

pub struct WriteAccess<O: Owner> {
    owner: Option<O>,
    permits: usize,
}

impl<O: Owner> WriteAccess<O> {
    /// Takes responsibility for all permits of a lock, which must already be acquired.
    pub fn new(owner: O, permits: usize) -> Self {
        Self {
            owner: Some(owner),
            permits,
        }
    }

    pub fn owner(&self) -> &O {
        self.owner.as_ref().unwrap()
    }

    pub fn downgrade(mut self) -> ReadAccess<O> {
        let read = ReadAccess::new(self.owner.take().unwrap());
        // Keep the retained permit and owner in a live token before release can invoke wakers.
        // If waking panics, unwinding drops this token instead of leaking a permit or an Arc.
        read.owner().semaphore().release(self.permits - 1);
        read
    }
}

impl<'a, T: ?Sized> WriteAccess<&'a RwLock<T>> {
    pub fn into_semaphore(mut self) -> WriteAccess<&'a Semaphore> {
        WriteAccess::new(&self.owner.take().unwrap().s, self.permits)
    }
}

impl<O: Owner> Drop for WriteAccess<O> {
    fn drop(&mut self) {
        if let Some(owner) = &self.owner {
            owner.semaphore().release(self.permits);
        }
    }
}
