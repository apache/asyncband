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
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::Poll;
use std::task::Waker;
use std::task::ready;

use crate::internal::semaphore::Semaphore;
use crate::internal::waitlist::WaiterId;
use crate::rwlock::RwLock;

/// Semaphore state shared by a [`RwLock`] and its type-erased mapped guards.
pub struct LockState {
    max_readers: usize,
    s: Semaphore,
}

impl LockState {
    const SPARE_PERMIT_MAX_READERS: usize = usize::MAX >> 1;

    pub const fn new(max_readers: NonZeroUsize) -> Self {
        let max_readers = max_readers.get();
        // Scale reader slots to two permits and keep one spare permit whenever the sum fits in
        // `usize`, so uncontended write unlocks and downgrades bypass the semaphore's wait-queue
        // lock while still accepting every `NonZeroUsize` limit.
        let initial_permits = if max_readers <= Self::SPARE_PERMIT_MAX_READERS {
            (max_readers << 1) + 1
        } else {
            max_readers
        };
        Self {
            max_readers,
            s: Semaphore::new(initial_permits),
        }
    }

    #[inline]
    pub const fn semaphore(&self) -> &Semaphore {
        &self.s
    }

    #[inline]
    pub const fn reader_permits(&self) -> usize {
        1 + ((self.max_readers <= Self::SPARE_PERMIT_MAX_READERS) as usize)
    }

    #[inline]
    pub const fn writer_permits(&self) -> usize {
        self.max_readers << ((self.max_readers <= Self::SPARE_PERMIT_MAX_READERS) as u32)
    }

    #[inline]
    pub const fn upgrade_permits(&self) -> usize {
        self.writer_permits() - self.reader_permits()
    }
}

/// Keeps a semaphore alive while a token holds permits from it.
pub trait Owner {
    /// Returns the semaphore that the token's permits belong to.
    fn semaphore(&self) -> &Semaphore;

    /// Returns the permit count corresponding to one shared slot.
    fn reader_permits(&self) -> usize;
}

/// Keeps a lock's semaphore alive while a token holds exclusive permits from it.
pub trait ExclusiveOwner: Owner {
    /// Returns the permit count corresponding to exclusive access.
    fn writer_permits(&self) -> usize;

    /// Returns the permit count released when downgrading exclusive access to one reader slot.
    fn upgrade_permits(&self) -> usize;
}

impl Owner for &Semaphore {
    #[inline]
    fn semaphore(&self) -> &Semaphore {
        self
    }

    #[inline]
    fn reader_permits(&self) -> usize {
        1
    }
}

impl Owner for &LockState {
    #[inline]
    fn semaphore(&self) -> &Semaphore {
        LockState::semaphore(self)
    }

    #[inline]
    fn reader_permits(&self) -> usize {
        LockState::reader_permits(self)
    }
}

impl ExclusiveOwner for &LockState {
    #[inline]
    fn writer_permits(&self) -> usize {
        LockState::writer_permits(self)
    }

    #[inline]
    fn upgrade_permits(&self) -> usize {
        LockState::upgrade_permits(self)
    }
}

impl<T: ?Sized> Owner for &RwLock<T> {
    #[inline]
    fn semaphore(&self) -> &Semaphore {
        self.state.semaphore()
    }

    #[inline]
    fn reader_permits(&self) -> usize {
        self.state.reader_permits()
    }
}

impl<T: ?Sized> ExclusiveOwner for &RwLock<T> {
    #[inline]
    fn writer_permits(&self) -> usize {
        self.state.writer_permits()
    }

    #[inline]
    fn upgrade_permits(&self) -> usize {
        self.state.upgrade_permits()
    }
}

impl<T: ?Sized> Owner for Arc<RwLock<T>> {
    #[inline]
    fn semaphore(&self) -> &Semaphore {
        self.state.semaphore()
    }

    #[inline]
    fn reader_permits(&self) -> usize {
        self.state.reader_permits()
    }
}

impl<T: ?Sized> ExclusiveOwner for Arc<RwLock<T>> {
    #[inline]
    fn writer_permits(&self) -> usize {
        self.state.writer_permits()
    }

    #[inline]
    fn upgrade_permits(&self) -> usize {
        self.state.upgrade_permits()
    }
}

/// Owns one read slot.
pub struct ReadAccess<O: Owner> {
    owner: O,
}

impl<O: Owner> ReadAccess<O> {
    #[inline]
    pub fn acquire(owner: O) -> impl Future<Output = Self> {
        let mut request = ReadRequest {
            owner: Some(owner),
            waiter_id: None,
        };
        poll_fn(move |cx| request.poll(cx.waker()))
    }

    #[inline]
    pub fn try_acquire(owner: O) -> Option<Self> {
        if owner.semaphore().try_acquire(owner.reader_permits()) {
            Some(Self { owner })
        } else {
            None
        }
    }

    /// Returns the owner that the slot was acquired from.
    #[inline]
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
    #[inline]
    fn poll(&mut self, waker: &Waker) -> Poll<ReadAccess<O>> {
        let owner = self
            .owner
            .as_ref()
            .expect("read acquisition polled after completion");
        ready!(
            owner
                .semaphore()
                .poll_acquire(owner.reader_permits(), &mut self.waiter_id, waker)
        );
        Poll::Ready(ReadAccess {
            owner: self.owner.take().unwrap(),
        })
    }
}

impl<O: Owner> Drop for ReadRequest<O> {
    #[inline]
    fn drop(&mut self) {
        if let Some(id) = self.waiter_id {
            let owner = self.owner.as_ref().unwrap();
            owner.semaphore().cancel_acquire(owner.reader_permits(), id);
        }
    }
}

impl<'a, T: ?Sized> ReadAccess<&'a RwLock<T>> {
    /// Drops the value type so that mapped guards need not name it.
    #[inline]
    pub fn into_state(self) -> ReadAccess<&'a LockState> {
        let lock = self.owner;
        // The new token takes over releasing the slot; forgetting this one avoids releasing it
        // during the transfer. Its borrowed owner needs no cleanup.
        mem::forget(self);
        ReadAccess { owner: &lock.state }
    }
}

impl<O: Owner> Drop for ReadAccess<O> {
    #[inline]
    fn drop(&mut self) {
        self.owner.semaphore().release(self.owner.reader_permits());
    }
}

/// Owns exclusive permits of a lock, which together grant write access.
pub struct WriteAccess<O: ExclusiveOwner> {
    owner: Option<O>,
}

impl<O: ExclusiveOwner> WriteAccess<O> {
    /// Takes over exclusive permits already acquired from `owner`.
    #[inline]
    pub fn new(owner: O) -> Self {
        Self { owner: Some(owner) }
    }

    /// Returns the owner that the permits were acquired from.
    #[inline]
    pub fn owner(&self) -> &O {
        self.owner
            .as_ref()
            .expect("write access token must own a lock")
    }

    /// Releases all permits beyond one reader slot, which the returned token keeps.
    #[inline]
    pub fn downgrade(mut self) -> ReadAccess<O> {
        let owner = self
            .owner
            .take()
            .expect("write access token must own a lock");
        owner.semaphore().release(owner.upgrade_permits());
        ReadAccess { owner }
    }
}

impl<'a, T: ?Sized> WriteAccess<&'a RwLock<T>> {
    /// Drops the value type so that mapped guards need not name it.
    #[inline]
    pub fn into_state(mut self) -> WriteAccess<&'a LockState> {
        let lock = self
            .owner
            .take()
            .expect("write access token must own a lock");
        WriteAccess::new(&lock.state)
    }
}

impl<O: ExclusiveOwner> Drop for WriteAccess<O> {
    #[inline]
    fn drop(&mut self) {
        if let Some(owner) = self.owner.take() {
            owner.semaphore().release(owner.writer_permits());
        }
    }
}
