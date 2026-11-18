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
use std::hint;
use std::mem::MaybeUninit;
use std::panic::RefUnwindSafe;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::internal::cache_padded::CachePadded;
use crate::internal::ring_slots::drop_range;
use crate::spmc::TrySendError;

// The high bit closes publication in `published`, and owns the short consumer claim gate in
// `consumer.position`. The cursor portion always stays in 0..2*capacity.
const FLAG: usize = 1 << (usize::BITS - 1);

struct Producer {
    published: AtomicUsize,
    // Only the unique producer accesses this cache.
    cached_reclaimed: UnsafeCell<usize>,
}

struct Consumer {
    position: AtomicUsize,
    // Only the owner of the claim gate accesses this cache.
    cached_published: UnsafeCell<usize>,
}

/// The producer alone advances publication; consumers alone advance reclamation.
///
/// Consumers serialize reading a value and advancing their cursor. Each side caches the other
/// side's progress and refreshes it only when its cached range is exhausted. Reclamation is
/// published after moving the value out, so no per-slot completion flags are needed.
#[repr(C)]
pub struct Ring<T> {
    producer: CachePadded<Producer>,
    consumer: CachePadded<Consumer>,
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

// SAFETY: There is one producer as required by `try_send`. The consumer gate protects index
// claims, payload reads and the non-atomic publication cache. The unique producer alone accesses
// the reclamation cache. Publication and reclamation cursors order all cross-thread payload
// accesses; reclamation advances only after the value is moved out. T: Send permits these moves.
unsafe impl<T: Send> Sync for Ring<T> {}

// Publication, reclamation and cache updates run no caller code. Discard settles the cursors
// before running destructors, and its claim guard releases them even if one unwinds.
impl<T> RefUnwindSafe for Ring<T> {}

impl<T> Ring<T> {
    pub fn supports(capacity: usize) -> bool {
        // A single slot cannot overlap publication with consumption of a different slot.
        capacity > 1
            && capacity <= 4096
            && size_of::<T>()
                .checked_mul(capacity)
                .is_some_and(|bytes| bytes <= 64 * 1024)
    }

    pub fn new(capacity: usize) -> Self {
        assert!(Self::supports(capacity));
        Self {
            producer: CachePadded::new(Producer {
                published: AtomicUsize::new(0),
                cached_reclaimed: UnsafeCell::new(0),
            }),
            consumer: CachePadded::new(Consumer {
                position: AtomicUsize::new(0),
                cached_published: UnsafeCell::new(0),
            }),
            slots: (0..capacity)
                .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
                .collect(),
        }
    }

    fn advance(&self, position: usize) -> usize {
        if position + 1 == 2 * self.slots.len() {
            0
        } else {
            position + 1
        }
    }

    fn index(&self, position: usize) -> usize {
        if position >= self.slots.len() {
            position - self.slots.len()
        } else {
            position
        }
    }

    /// Publishes a value, or returns its ownership without publishing it.
    ///
    /// # Safety
    ///
    /// There must be exactly one producer. No other producer call may overlap this one.
    pub unsafe fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        // Only this producer advances the cursor bits.
        let next = self.producer.published.load(Ordering::Relaxed) & !FLAG;
        let full_head = if next < self.slots.len() {
            next + self.slots.len()
        } else {
            next - self.slots.len()
        };
        // SAFETY: Only this ring's unique producer accesses its reclamation cache.
        let reclaimed = unsafe { &mut *self.producer.cached_reclaimed.get() };
        if *reclaimed == full_head {
            *reclaimed = self.consumer.position.load(Ordering::SeqCst) & !FLAG;
            if *reclaimed == full_head {
                return Err(
                    if self.producer.published.load(Ordering::SeqCst) & FLAG != 0 {
                        TrySendError::Disconnected(value)
                    } else {
                        TrySendError::Full(value)
                    },
                );
            }
        }
        let slot = &self.slots[self.index(next)];
        // SAFETY: Slots are initially uninitialized. On reuse, the cached reclaimed range was
        // acquired after consumers finished moving its values out. The unique producer cannot
        // overrun it without refreshing the cache. Consumers cannot see this value until
        // publication.
        unsafe { (*slot.get()).write(value) };
        match self.producer.published.compare_exchange(
            next,
            self.advance(next),
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            // A receiver or close may already have taken the value. Do not access the slot.
            Ok(_) => Ok(()),
            Err(closed) => {
                // Only this producer changes the cursor, so a strong CAS fails only when close
                // sets the high bit. This value was never published and remains producer-owned.
                debug_assert_ne!(closed & FLAG, 0);
                // SAFETY: No consumer can claim this unpublished slot.
                let value = unsafe { (*slot.get()).assume_init_read() };
                Err(TrySendError::Disconnected(value))
            }
        }
    }

    fn claim(&self) -> Claim<'_, T> {
        let mut head = self.consumer.position.load(Ordering::SeqCst);
        loop {
            if head & FLAG != 0 {
                hint::spin_loop();
                head = self.consumer.position.load(Ordering::SeqCst);
                continue;
            }
            match self.consumer.position.compare_exchange_weak(
                head,
                head | FLAG,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Claim { ring: self, head },
                Err(current) => head = current,
            }
        }
    }

    pub fn try_recv(&self) -> Option<T> {
        let mut claim = self.claim();
        if !claim.has_value() {
            return None;
        }
        let index = self.index(claim.head);
        let slot = &self.slots[index];
        // SAFETY: This consumer claimed a published index exclusively. The acquired publication
        // cache makes the initialized value visible. The producer cannot reuse the slot until
        // the guard releases the advanced cursor, after this read. Moving T runs no caller code.
        let value = unsafe { (*slot.get()).assume_init_read() };
        claim.head = self.advance(claim.head);
        drop(claim);
        Some(value)
    }

    pub fn is_empty(&self) -> bool {
        !self.claim().has_value()
    }

    pub fn close(&self) {
        // Competes with publication, not with payload initialization. An unpublished value
        // remains owned by its producer, which returns it after its publication CAS fails.
        self.producer.published.fetch_or(FLAG, Ordering::SeqCst);
    }

    /// Drops the remaining values once no receiver remains to wait on the claim gate.
    pub fn discard(&self) {
        let mut claim = self.claim();
        let head = claim.head;
        claim.head = self.producer.published.load(Ordering::SeqCst) & !FLAG;
        // SAFETY: Only the gate owner accesses the publication cache.
        unsafe { *self.consumer.cached_published.get() = claim.head };
        // SAFETY: The gate excludes other consumers and makes the producer see the old head, so it
        // cannot reuse these published slots until the guard releases the advanced cursor. The
        // guard does so even if a destructor unwinds.
        unsafe { drop_range(&self.slots, head, claim.head) };
    }
}

struct Claim<'a, T> {
    ring: &'a Ring<T>,
    head: usize,
}

impl<T> Claim<'_, T> {
    fn has_value(&mut self) -> bool {
        // SAFETY: This guard exclusively owns the consumer gate. An acquiring gate CAS observes
        // the previous owner's released cache update and the payload publication it acquired.
        let cached = unsafe { &mut *self.ring.consumer.cached_published.get() };
        if self.head == *cached {
            // Once the cached range is exhausted, obtain the next published range. The producer
            // cannot lap this fixed head, since it can never get more than capacity slots ahead.
            *cached = self.ring.producer.published.load(Ordering::SeqCst) & !FLAG;
        }
        self.head != *cached
    }
}

impl<T> Drop for Claim<'_, T> {
    fn drop(&mut self) {
        // Also releases the claim gate when an empty observation leaves the cursor unchanged.
        self.ring
            .consumer
            .position
            .store(self.head, Ordering::SeqCst);
    }
}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        self.discard();
    }
}
