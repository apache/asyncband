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

//! The channel's capacity: a counting semaphore with a close operation and a fair wait queue.
//!
//! This channel-local semaphore keeps its permit counter and channel state in one atomic. The
//! general-purpose semaphore has neither a close operation nor acquisition errors.
//!
//! `state` is the available permit count, plus two sentinel values at the top of the range:
//!
//! * `CLOSED`: the receiver is gone. No permits are issued or returned, and waiters drain with an
//!   error.
//! * `WAITING`: the wait queue may be non-empty. Releases then take the locked path and grant the
//!   permit directly to the oldest waiter instead of returning it to the counter, so capacity is
//!   handed out in registration order and new arrivals cannot steal an already granted slot. The
//!   counter is zero while this sentinel stands: waiters only register after observing exhaustion,
//!   and grants bypass the counter.
//!
//! With neither sentinel installed, acquire and release are single lock-free operations on `state`.
//! Wait-queue mutations always hold the queue lock. A registration must install or observe
//! `WAITING` before joining the queue, so every subsequent release takes the locked path. If a
//! release wins that transition, acquisition retries instead of registering against a plain count.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::waker_batch::WakerBatch;
use crate::mpsc::SendError;
use crate::mpsc::TrySendError;

pub struct Semaphore {
    state: AtomicUsize,
    waiters: Mutex<WaitList<Waiter>>,
}

const CLOSED: usize = usize::MAX;
const WAITING: usize = usize::MAX - 1;

struct Waiter {
    granted: bool,
    waker: Option<Waker>,
}

impl Semaphore {
    pub fn new(available: usize) -> Self {
        Self {
            state: AtomicUsize::new(available),
            waiters: Mutex::new(WaitList::new()),
        }
    }

    pub fn try_acquire(&self) -> Result<Capacity<'_>, TrySendError<()>> {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state == CLOSED {
                return Err(TrySendError::Disconnected(()));
            }
            if state == WAITING || state == 0 {
                return Err(TrySendError::Full(()));
            }
            match self.state.compare_exchange_weak(
                state,
                state - 1,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(Capacity { semaphore: self }),
                Err(actual) => state = actual,
            }
        }
    }

    /// Acquires one permit asynchronously, waiting in registration order when the semaphore
    /// is exhausted.
    pub fn acquire(&self) -> Acquire<'_> {
        Acquire {
            semaphore: self,
            waiter: None,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.state.load(Ordering::Acquire) == CLOSED
    }

    // Called with the queue locked. A failed installation requires retrying acquisition: a
    // racing sender may consume the returned capacity before a separate recheck can see it.
    fn set_waiting(&self) -> bool {
        matches!(
            self.state
                .compare_exchange(0, WAITING, Ordering::AcqRel, Ordering::Acquire),
            Ok(_) | Err(WAITING)
        )
    }

    // Removes WAITING, keeping whatever count a racing grant restoration left behind.
    fn clear_waiting(&self) {
        let _ = self
            .state
            .compare_exchange(WAITING, 0, Ordering::Release, Ordering::Relaxed);
    }

    pub fn release(&self) {
        // Fast path: with no waiting sender and no close in sight, the permit goes straight
        // back to the counter.
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            if state == WAITING || state == CLOSED {
                break;
            }
            match self.state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => state = actual,
            }
        }
        let wake = self.release_locked(&mut self.waiters.lock());
        if let Some(waker) = wake {
            waker.wake();
        }
    }

    fn release_locked(&self, waiters: &mut WaitList<Waiter>) -> Option<Waker> {
        if self.is_closed() {
            return None;
        }
        if let Some((_, waiter)) = waiters.unlink_first_waiter(|_| true) {
            // Grant ownership before waking; new arrivals cannot steal this capacity.
            waiter.granted = true;
            let waker = waiter.waker.take();
            if waiters.is_empty() {
                self.clear_waiting();
            }
            return waker;
        }
        // The queue is empty: return the permit to the counter. An outstanding grant already
        // owns its capacity. Adding to a plain count is safe because only lock-holding
        // operations install a sentinel, and this operation holds the lock; WAITING itself
        // must be displaced rather than incremented, because WAITING + 1 is CLOSED.
        if self.state.load(Ordering::Relaxed) == WAITING {
            let _displaced =
                self.state
                    .compare_exchange(WAITING, 1, Ordering::Release, Ordering::Relaxed);
            debug_assert_eq!(_displaced, Ok(WAITING));
        } else {
            self.state.fetch_add(1, Ordering::Release);
        }
        None
    }

    pub fn close(&self) -> WakerBatch {
        let mut waiters = self.waiters.lock();
        self.state.store(CLOSED, Ordering::Release);
        let mut wakers = WakerBatch::new();
        while let Some((_, waiter)) = waiters.unlink_first_waiter(|_| true) {
            if let Some(waker) = waiter.waker.take() {
                wakers.push(waker);
            }
        }
        wakers
    }
}

/// Owns one capacity unit until publication transfers it to a queued message.
#[must_use = "dropping the guard releases its capacity"]
pub struct Capacity<'a> {
    semaphore: &'a Semaphore,
}

impl Capacity<'_> {
    pub fn forget(self) {
        std::mem::forget(self);
    }
}

impl Drop for Capacity<'_> {
    fn drop(&mut self) {
        self.semaphore.release();
    }
}

/// An in-flight [`Semaphore::acquire`] operation.
///
/// Dropping the operation removes its wait-queue registration; a capacity grant that already
/// reached the registration is released to the next waiter or returned to the counter.
pub struct Acquire<'a> {
    semaphore: &'a Semaphore,
    waiter: Option<WaiterId>,
}

impl<'a> Acquire<'a> {
    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Capacity<'a>, SendError<()>>> {
        let semaphore = self.semaphore;
        let mut cloned_waker = None;
        let result = loop {
            if self.waiter.is_none() {
                match semaphore.try_acquire() {
                    Ok(capacity) => break Ok(capacity),
                    Err(TrySendError::Disconnected(())) => break Err(SendError::new(())),
                    Err(TrySendError::Full(())) => {}
                }
            }
            let mut waiters = semaphore.waiters.lock();
            if semaphore.is_closed() {
                // Drop removes any remaining registration, including an unused grant.
                break Err(SendError::new(()));
            }
            if let Some(index) = self.waiter {
                let waiter = waiters.waiter_mut(index);
                if waiter.granted {
                    let waiter = waiters.remove_unlinked_waiter(index);
                    self.waiter = None;
                    let capacity = Capacity { semaphore };
                    drop(waiters);
                    drop(waiter);
                    break Ok(capacity);
                }
                if waiter
                    .waker
                    .as_ref()
                    .is_some_and(|w| w.will_wake(cx.waker()))
                {
                    return Poll::Pending;
                }
                if let Some(waker) = cloned_waker.take() {
                    let old = waiter.waker.replace(waker);
                    drop(waiters);
                    drop(old);
                    return Poll::Pending;
                }
            } else {
                if !semaphore.set_waiting() {
                    drop(waiters);
                    continue;
                }
                if let Some(waker) = cloned_waker.take() {
                    self.waiter = Some(waiters.push_back(Waiter {
                        granted: false,
                        waker: Some(waker),
                    }));
                    return Poll::Pending;
                }
            }
            drop(waiters);
            // Clone outside the lock, then recheck capacity and closure before registering.
            cloned_waker = Some(cx.waker().clone());
        };
        // A successful result already owns a guard, so a panicking waker destructor returns
        // capacity even before the caller has constructed its public permit.
        drop(cloned_waker);
        Poll::Ready(result)
    }
}

impl Drop for Acquire<'_> {
    fn drop(&mut self) {
        let Some(index) = self.waiter else { return };
        let semaphore = self.semaphore;
        let (waiter, wake) = {
            let mut waiters = semaphore.waiters.lock();
            waiters.unlink_waiter(index, |_| true);
            let waiter = waiters.remove_unlinked_waiter(index);
            let wake = if waiter.granted {
                semaphore.release_locked(&mut waiters)
            } else {
                if waiters.is_empty() {
                    semaphore.clear_waiting();
                }
                None
            };
            (waiter, wake)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        drop(waiter);
    }
}
