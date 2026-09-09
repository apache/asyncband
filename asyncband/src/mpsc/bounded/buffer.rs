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

//! Capacity, position, and publication are separate ownership transitions:
//!
//! - A permit owns capacity, but holds no position until synchronous `push` claims a ticket.
//! - The ticket gives one producer a slot. `READY` publishes its initialized value to the receiver.
//! - The receiver finishes reading before returning capacity. AcqRel ticket increments carry that
//!   reuse ordering even to a producer that acquired its permit on an earlier lap.
//! - Close competes with publication on the slot state. The drain owns `READY` values; a producer
//!   that encounters `CLOSED` owns its unpublished value. Neither waits for the other to resume.
//!
//! Only the non-cloneable receiver advances the read cursor. All endpoints retain the shared
//! allocation, so a publisher's slot stays alive even when receiver drop closes it concurrently.

use std::cell::UnsafeCell;
use std::mem;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;

use crate::internal::cache_padded::CachePadded;

const EMPTY: u8 = 0;
const READY: u8 = 1;
const CLOSED: u8 = 2;
const CLOSED_BIT: usize = 1 << (usize::BITS - 1);

pub struct Buffer<T> {
    storage: Storage<T>,
}

/// Slotted and zero-sized queues share no storage beyond a message count. Splitting them into
/// variants frees the zero-sized queue from a dead ticket, close flag, and slot allocation.
/// Boxing the slotted variant keeps that saving: an unboxed enum reserves room for the larger
/// variant either way. The variant tag sits beside the words every operation already loads,
/// so the dispatch branch is as predictable as the size check it replaces.
enum Storage<T> {
    Slots(Box<Slots<T>>),
    ZeroSized(ZeroSized),
}

struct Slots<T> {
    slots: Box<[Slot<T>]>,
    tail: CachePadded<AtomicUsize>,
    closed: AtomicBool,
}

struct Slot<T> {
    state: AtomicU8,
    value: UnsafeCell<MaybeUninit<T>>,
}

// SAFETY: Capacity and the tail ticket give a producer exclusive ownership of an empty slot.
// Release publication transfers its value to the exclusive consumer. Closing an unpublished
// slot leaves its value with the producer; closing a READY slot transfers it to the drain.
unsafe impl<T: Send> Sync for Slot<T> {}

// No reference to a stored value escapes. Every value is removed from the slot's ownership
// before running a callback or destructor that might panic.
impl<T> std::panic::UnwindSafe for Slot<T> {}
impl<T> std::panic::RefUnwindSafe for Slot<T> {}

/// Queue storage for zero-sized messages, which need no slots, positions, or per-slot flags.
/// Counting them separately also allows every nonzero usize capacity without allocating
/// publication metadata for nonexistent bytes.
///
/// The queue is entirely its length. The count packs a closed flag into its top bit so that
/// publication and close stay atomic: a publication that raced ahead of the flag is included
/// in the drained count, and every later one observes the flag and fails.
struct ZeroSized {
    queued: AtomicUsize,
}

impl ZeroSized {
    fn new() -> Self {
        Self {
            queued: AtomicUsize::new(0),
        }
    }

    /// Accounts for one published message, returning `false` once the queue is closed.
    fn push(&self) -> bool {
        let mut queued = self.queued.load(Ordering::Acquire);
        loop {
            if queued & CLOSED_BIT != 0 {
                return false;
            }
            // The capacity limit keeps the count far below the closed flag bit.
            match self.queued.compare_exchange_weak(
                queued,
                queued + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => queued = actual,
            }
        }
    }

    /// Accounts for one consumed message, returning `false` when the queue was observed empty.
    fn pop(&self) -> bool {
        let queued = self.queued.load(Ordering::Acquire) & !CLOSED_BIT;
        if queued == 0 {
            return false;
        }
        // Only the consumer decrements, and producers can only add: the count observed above
        // is a lower bound, so this cannot wrap.
        self.queued.fetch_sub(1, Ordering::AcqRel);
        true
    }

    /// Stops publication and returns the queue length transferred to the drain.
    fn close(&self) -> usize {
        self.queued.fetch_or(CLOSED_BIT, Ordering::AcqRel) & !CLOSED_BIT
    }
}

impl<T> Slots<T> {
    fn new(capacity: usize) -> Self {
        let slots = (0..capacity.next_power_of_two())
            .map(|_| Slot {
                state: AtomicU8::new(EMPTY),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        Self {
            slots,
            tail: CachePadded::new(AtomicUsize::new(0)),
            closed: AtomicBool::new(false),
        }
    }

    fn slot(&self, position: usize) -> &Slot<T> {
        // Power-of-two storage preserves indexing when the full-width ticket wraps. The
        // semaphore still enforces the exact requested capacity, including non-powers of two.
        &self.slots[position & (self.slots.len() - 1)]
    }

    fn claim(&self) -> Result<usize, ()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(());
        }
        // Closing may race after this check. It marks every physical slot CLOSED, so even a
        // delayed claimant will recover its own value instead of publishing into a dead queue.
        Ok(self.tail.fetch_add(1, Ordering::AcqRel))
    }

    /// Writes and publishes one message into a claimed position.
    ///
    /// # Safety
    ///
    /// Own the position from a claim, backed by a capacity permit, and publish it at most once.
    unsafe fn publish(&self, position: usize, value: T) -> Result<(), T> {
        let slot = self.slot(position);
        // SAFETY: Capacity prevents wrapping over unread slots. AcqRel tail increments carry prior
        // claimants' capacity-acquire edges even when this producer held its permit for a long
        // time. The previous consumer has therefore finished reading before this write.
        unsafe { (*slot.value.get()).write(value) };
        match slot
            .state
            .compare_exchange(EMPTY, READY, Ordering::Release, Ordering::Acquire)
        {
            Ok(_) => Ok(()),
            Err(state) => {
                debug_assert_eq!(state, CLOSED);
                // SAFETY: Close saw an unpublished slot and did not read it. Failed publication
                // leaves exclusive ownership with this producer, including during receiver drop.
                Err(unsafe { (*slot.value.get()).assume_init_read() })
            }
        }
    }

    /// Pending means a producer claimed the head but has not published it yet.
    ///
    /// # Safety
    ///
    /// Only the exclusive consumer may call this, using its persistent cursor.
    unsafe fn pop(&self, head: &mut usize) -> Poll<Option<T>> {
        let slot = self.slot(*head);
        if slot.state.load(Ordering::Acquire) == READY {
            // SAFETY: Publication initialized the value, and only this consumer can read it.
            // Capacity is still held until this method has returned the value to its caller.
            let value = unsafe { (*slot.value.get()).assume_init_read() };
            slot.state.store(EMPTY, Ordering::Release);
            *head = head.wrapping_add(1);
            Poll::Ready(Some(value))
        } else if self.tail.load(Ordering::Acquire) == *head {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    /// Stops new claims and returns the physical slot count the drain must cover: a producer
    /// may have passed the open check but not obtained its ticket yet, and such a late claim
    /// must also find a CLOSED slot.
    fn close(&self) -> usize {
        self.closed.store(true, Ordering::Release);
        self.slots.len()
    }
}

impl<T> Buffer<T> {
    pub fn new(capacity: usize) -> Self {
        let storage = if size_of::<T>() == 0 {
            Storage::ZeroSized(ZeroSized::new())
        } else {
            Storage::Slots(Box::new(Slots::new(capacity)))
        };
        Self { storage }
    }

    /// Writes and publishes one message. Closing may instead return the unsent value.
    ///
    /// # Safety
    ///
    /// Own one capacity permit before calling; release it only after a failed push or after
    /// the consumer reads the published value. No user code runs between claim and publication.
    pub unsafe fn push(&self, value: T) -> Result<(), T> {
        match &self.storage {
            Storage::Slots(slots) => {
                let Ok(position) = slots.claim() else {
                    return Err(value);
                };
                // SAFETY: The caller owns capacity and the ticket assigned this position.
                unsafe { slots.publish(position, value) }
            }
            Storage::ZeroSized(zero_sized) => {
                debug_assert_eq!(size_of::<T>(), 0);
                if zero_sized.push() {
                    mem::forget(value);
                    Ok(())
                } else {
                    Err(value)
                }
            }
        }
    }

    /// Pending means a producer claimed the head but has not published it yet.
    ///
    /// # Safety
    ///
    /// Only the exclusive consumer may call this, using its persistent cursor. Release one
    /// capacity permit after each successful pop, after the value has been read completely.
    pub unsafe fn pop(&self, head: &mut usize) -> Poll<Option<T>> {
        match &self.storage {
            // SAFETY: The caller's guarantee forwards unchanged.
            Storage::Slots(slots) => unsafe { slots.pop(head) },
            Storage::ZeroSized(zero_sized) => {
                debug_assert_eq!(size_of::<T>(), 0);
                if zero_sized.pop() {
                    // SAFETY: A queued value proves that this ZST is inhabited and owns one value.
                    Poll::Ready(Some(unsafe { read_zero_sized() }))
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }

    /// Stops new claims and returns ownership of published values to a drain guard.
    ///
    /// # Safety
    ///
    /// Only the exclusive consumer may close the buffer, once, using its current cursor.
    pub unsafe fn close(&self, head: usize) -> Drain<'_, T> {
        let remaining = match &self.storage {
            Storage::Slots(slots) => slots.close(),
            Storage::ZeroSized(zero_sized) => zero_sized.close(),
        };
        Drain {
            buffer: self,
            position: head,
            remaining,
        }
    }

    #[cfg(test)]
    fn slots(&self) -> &Slots<T> {
        match &self.storage {
            Storage::Slots(slots) => slots,
            Storage::ZeroSized(_) => unreachable!("zero-sized messages have no slots"),
        }
    }
}

/// # Safety
///
/// The caller must own an initialized, inhabited ZST value.
unsafe fn read_zero_sized<T>() -> T {
    // SAFETY: Reading it accesses no bytes; dangling supplies a non-null, correctly aligned
    // pointer, as in a ZST Vec.
    unsafe { NonNull::<T>::dangling().as_ptr().read() }
}

pub struct Drain<'a, T> {
    buffer: &'a Buffer<T>,
    position: usize,
    remaining: usize,
}

impl<T> Iterator for Drain<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        while self.remaining != 0 {
            let position = self.position;
            self.remaining -= 1;
            self.position = self.position.wrapping_add(1);
            match &self.buffer.storage {
                Storage::Slots(slots) => {
                    let slot = slots.slot(position);
                    if slot.state.swap(CLOSED, Ordering::AcqRel) == READY {
                        // SAFETY: The drain won ownership of a published value. The cursor and
                        // state already advanced, so a panicking destructor cannot read twice.
                        return Some(unsafe { (*slot.value.get()).assume_init_read() });
                    }
                    // An unpublished slot stays owned by its producer, which will observe CLOSED
                    // and recover its value. The shared Arc keeps this allocation alive until then.
                }
                // SAFETY: Closing transferred this many initialized ZST values to the drain.
                Storage::ZeroSized(_) => return Some(unsafe { read_zero_sized() }),
            }
        }
        None
    }
}

impl<T> Drop for Drain<'_, T> {
    fn drop(&mut self) {
        struct Remaining<'a, 'b, T>(&'a mut Drain<'b, T>);

        impl<T> Drop for Remaining<'_, '_, T> {
            fn drop(&mut self) {
                for value in self.0.by_ref() {
                    drop(value);
                }
            }
        }

        // A guard inside Drop is necessary: Drop itself is not called again if a payload's
        // destructor panics while this normal drain is running.
        let remaining = Remaining(self);
        for value in remaining.0.by_ref() {
            drop(value);
        }
    }
}

#[cfg(test)]
#[path = "buffer_tests.rs"]
mod tests;
