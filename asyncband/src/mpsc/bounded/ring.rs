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

use std::cell::UnsafeCell;
use std::hint::spin_loop;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::task::Poll;
use std::task::Waker;

use super::SEQUENCE_STEP;
use crate::internal::cache_padded::CachePadded;
use crate::internal::mutex::Mutex;

const CLOSED: usize = 1;

pub struct Ring<T> {
    slots: Box<[Slot<T>]>,
    tail: CachePadded<AtomicUsize>,
    // Publication and receiver registration synchronize independently of capacity release.
    receiver_waiting: CachePadded<AtomicBool>,
    receiver: Mutex<Option<Waker>>,
    mask: usize,
}

struct Slot<T> {
    stamp: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

// SAFETY: A successful tail increment gives one producer exclusive access to a slot. That producer
// initializes the value before publishing the next stamp with Release ordering. The single
// consumer reads only after acquiring that stamp and returns capacity before reuse.
unsafe impl<T: Send> Sync for Slot<T> {}

// The ownership transition finishes before user code can unwind, and no stored-value reference is
// exposed.
impl<T> std::panic::UnwindSafe for Slot<T> {}
impl<T> std::panic::RefUnwindSafe for Slot<T> {}

impl<T> Ring<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity <= usize::MAX / 4, "mpsc capacity is too large");
        // Physical storage is rounded up, while Capacity enforces the exact requested limit.
        // A power-of-two ring keeps indexing cheap and continuous across sequence overflow.
        let storage = capacity.next_power_of_two();
        let slots = (0..storage)
            .map(|_| Slot {
                stamp: AtomicUsize::new(CLOSED),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        Self {
            slots,
            tail: CachePadded::new(AtomicUsize::new(0)),
            receiver_waiting: CachePadded::new(AtomicBool::new(false)),
            receiver: Mutex::new(None),
            mask: storage - 1,
        }
    }

    /// Claims the next FIFO position. No payload is touched until `Claim::publish`.
    ///
    /// # Safety
    ///
    /// The caller must already own one capacity permit for this ring, and transfer that permit
    /// to the consumer on publication. The claim must be published without invoking user code.
    pub unsafe fn claim(&self) -> Result<Claim<'_, T>, ()> {
        // A permit may predate the previous use of this slot. Carry earlier claimants'
        // capacity acquires through the sequence so even an old permit observes that read.
        let position = self.tail.fetch_add(SEQUENCE_STEP, Ordering::AcqRel);
        if position & CLOSED != 0 {
            Err(())
        } else {
            Ok(Claim {
                ring: self,
                position,
            })
        }
    }

    /// Pending means the head slot is claimed but not yet published.
    ///
    /// # Safety
    ///
    /// Only the exclusive consumer may call `pop` or `drain`, with its persistent head cursor.
    /// Return one capacity permit after each successful pop, after the value has been read.
    pub unsafe fn pop(&self, head: &mut usize) -> Poll<Option<T>> {
        let index = (*head / SEQUENCE_STEP) & self.mask;
        let slot = &self.slots[index];
        if slot.stamp.load(Ordering::Acquire) == *head {
            // SAFETY: Acquiring the published stamp observes initialization. The consumer owns
            // this cursor exclusively, and capacity is not returned until after reading the value.
            let value = unsafe { (*slot.value.get()).assume_init_read() };
            *head = head.wrapping_add(SEQUENCE_STEP);
            return Poll::Ready(Some(value));
        }
        fence(Ordering::SeqCst);
        if self.tail.load(Ordering::Relaxed) & !CLOSED == *head {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    /// Registers the exclusive receiver, which must retry `pop` before returning Pending.
    pub fn register_receiver(&self, waker: &Waker) {
        let mut receiver = self.receiver.lock();
        let old_waker = if receiver.as_ref().is_some_and(|old| old.will_wake(waker)) {
            None
        } else {
            // Only the receiver registers. Producers can take the old waker while we clone,
            // but cannot install a replacement. Clone/drop callbacks may send into this channel.
            drop(receiver);
            let waker = waker.clone();
            receiver = self.receiver.lock();
            receiver.replace(waker)
        };
        self.receiver_waiting.store(true, Ordering::Relaxed);
        // Paired with the publisher's fence: either it sees this flag, or the receiver's
        // subsequent pop sees its stamp. Taking a waker clears the flag under the same lock,
        // so it cannot erase a newer registration without also taking responsibility for it.
        fence(Ordering::SeqCst);
        drop(receiver);
        drop(old_waker);
    }

    pub fn take_receiver_waker(&self) -> Option<Waker> {
        let mut receiver = self.receiver.lock();
        self.receiver_waiting.store(false, Ordering::Relaxed);
        receiver.take()
    }

    pub fn wake_receiver(&self) {
        if let Some(waker) = self.take_receiver_waker() {
            waker.wake();
        }
    }

    /// Prevents subsequent sends from reserving slots. Already reserved slots still publish.
    pub fn close(&self) -> usize {
        // Failed claims may still advance tail after close. Freeze the drain boundary at the
        // close operation itself; it includes every successful claim and no rejected claims.
        self.tail.fetch_or(CLOSED, Ordering::SeqCst)
    }

    /// Drops all values after closing, including short-lived claims still being published.
    ///
    /// # Safety
    ///
    /// `tail` must be the value returned by the first close, and `head` the consumer's cursor.
    pub unsafe fn drain(&self, head: &mut usize, tail: usize) {
        struct DrainRemaining<'a, T> {
            ring: &'a Ring<T>,
            head: &'a mut usize,
            tail: usize,
        }
        impl<T> Drop for DrainRemaining<'_, T> {
            fn drop(&mut self) {
                // SAFETY: The guard owns the consumer cursor until this closed ring is drained.
                unsafe { self.ring.discard_until(self.head, self.tail) };
            }
        }

        debug_assert_eq!(tail & CLOSED, 0);
        let remaining = DrainRemaining {
            ring: self,
            head,
            tail,
        };
        // SAFETY: The caller guarantees exclusive consumer access. The guard finishes draining
        // if a destructor panics, including messages that own senders and would retain the ring.
        unsafe { self.discard_until(remaining.head, remaining.tail) };
    }

    // The caller owns the cursor and has prevented new claims by closing the ring.
    unsafe fn discard_until(&self, head: &mut usize, tail: usize) {
        let mut backoff = 0;
        while *head != tail {
            let index = (*head / SEQUENCE_STEP) & self.mask;
            let slot = &self.slots[index];
            if slot.stamp.load(Ordering::Acquire) == *head {
                // Advance before running the destructor so unwinding cannot drop a value twice.
                *head = head.wrapping_add(SEQUENCE_STEP);
                // SAFETY: The acquired stamp proves initialization; the cursor claims the value
                // exactly once, and close prevents any producer from reusing its slot.
                unsafe { (*slot.value.get()).assume_init_drop() };
                backoff = 0;
            } else {
                Self::spin(&mut backoff);
            }
        }
    }

    fn spin(step: &mut u32) {
        for _ in 0..(*step).min(6).pow(2) {
            spin_loop();
        }
        *step = (*step).saturating_add(1);
    }
}

// Claims never escape a synchronous send. A public Permit owns capacity without claiming a
// position, so holding or forgetting one cannot leave an unpublished hole in the ring.
pub struct Claim<'a, T> {
    ring: &'a Ring<T>,
    position: usize,
}

impl<T> Claim<'_, T> {
    pub fn publish(self, value: T) {
        let slot = &self.ring.slots[(self.position / SEQUENCE_STEP) & self.ring.mask];
        // SAFETY: Claiming required a capacity permit. Its acquire observes the consumer's
        // completed read on the previous lap; the tail increment grants this producer exclusive
        // access.
        unsafe { (*slot.value.get()).write(value) };
        slot.stamp.store(self.position, Ordering::Release);
        // Paired with receiver registration: either the publisher sees the wait flag or the
        // receiver's second pop observes this publication before it can return Pending.
        fence(Ordering::SeqCst);
        if self.ring.receiver_waiting.load(Ordering::Relaxed)
            && self.ring.receiver_waiting.swap(false, Ordering::Relaxed)
        {
            self.ring.wake_receiver();
        }
    }
}

#[cfg(test)]
#[path = "ring_tests.rs"]
mod tests;
