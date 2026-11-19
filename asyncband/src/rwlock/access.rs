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

use std::future::poll_fn;
use std::mem;
use std::sync::Arc;
use std::task::Poll;
use std::task::Waker;
use std::task::ready;

use crate::internal::semaphore::Semaphore;
use crate::internal::waitlist::WaiterId;
use crate::rwlock::RwLock;

/// Keeps the lock's semaphore alive while a token holds permits from it.
pub trait Owner: Clone {
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
    owner: O,
}

impl<O: Owner> ReadAccess<O> {
    pub fn acquire(owner: O) -> impl Future<Output = Self> {
        let mut request = ReadRequest {
            owner: Some(owner),
            waiter_id: None,
        };
        poll_fn(move |cx| request.poll(cx.waker()))
    }

    pub fn try_acquire(owner: O) -> Option<Self> {
        if owner.semaphore().try_acquire(1) {
            Some(Self { owner })
        } else {
            None
        }
    }

    /// Returns the owner that the permit was acquired from.
    pub fn owner(&self) -> &O {
        &self.owner
    }
}

// Keep the owner and its queue registration together so waiting does not also need a
// self-referential future borrowing the owner's semaphore.
struct ReadRequest<O: Owner> {
    owner: Option<O>,
    waiter_id: Option<WaiterId>,
}

impl<O: Owner> ReadRequest<O> {
    fn poll(&mut self, waker: &Waker) -> Poll<ReadAccess<O>> {
        let owner = self
            .owner
            .as_ref()
            .expect("read acquisition polled after completion");
        ready!(
            owner
                .semaphore()
                .poll_acquire(1, &mut self.waiter_id, waker)
        );
        Poll::Ready(ReadAccess {
            owner: self.owner.take().unwrap(),
        })
    }
}

impl<O: Owner> Drop for ReadRequest<O> {
    fn drop(&mut self) {
        if let Some(id) = self.waiter_id {
            self.owner
                .as_ref()
                .unwrap()
                .semaphore()
                .cancel_acquire(1, id);
        }
    }
}

impl<'a, T: ?Sized> ReadAccess<&'a RwLock<T>> {
    /// Drops the value type so that mapped guards need not name it.
    pub fn into_semaphore(self) -> ReadAccess<&'a Semaphore> {
        let lock = self.owner;
        // The new token takes over releasing the permit; forgetting this one avoids releasing it
        // during the transfer. Its borrowed owner needs no cleanup.
        mem::forget(self);
        ReadAccess { owner: &lock.s }
    }
}

impl<O: Owner> Drop for ReadAccess<O> {
    fn drop(&mut self) {
        self.owner.semaphore().release(1);
    }
}

/// Owns every permit of a lock, which together grant write access.
pub struct WriteAccess<O: Owner> {
    owner: O,
    permits_acquired: usize,
}

impl<O: Owner> WriteAccess<O> {
    /// Takes over `permits_acquired` permits already acquired from `owner`.
    pub fn new(owner: O, permits_acquired: usize) -> Self {
        Self {
            owner,
            permits_acquired,
        }
    }

    /// Returns the owner that the permits were acquired from.
    pub fn owner(&self) -> &O {
        &self.owner
    }

    /// Releases all permits but one, which the returned token keeps.
    pub fn downgrade(mut self) -> ReadAccess<O> {
        let read = ReadAccess {
            owner: self.owner.clone(),
        };
        self.permits_acquired -= 1;
        drop(self);
        read
    }
}

impl<'a, T: ?Sized> WriteAccess<&'a RwLock<T>> {
    /// Drops the value type so that mapped guards need not name it.
    pub fn into_semaphore(self) -> WriteAccess<&'a Semaphore> {
        let lock = self.owner;
        let permits_acquired = self.permits_acquired;
        // The new token takes over releasing the permits; forgetting this one avoids releasing them
        // during the transfer. Its borrowed owner needs no cleanup.
        mem::forget(self);
        WriteAccess::new(&lock.s, permits_acquired)
    }
}

impl<O: Owner> Drop for WriteAccess<O> {
    fn drop(&mut self) {
        self.owner.semaphore().release(self.permits_acquired);
    }
}
