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
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;

use crate::internal::cache_padded::CachePadded;

const EMPTY: u8 = 0;
const READY: u8 = 1;
const CLOSED: u8 = 2;

pub struct Buffer<T> {
    slots: Box<[Slot<T>]>,
    tail: CachePadded<AtomicUsize>,
    closed: AtomicBool,
}

impl<T> Buffer<T> {
    pub fn new(capacity: usize) -> Self {
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

    /// Writes and publishes one message. Closing may instead return the unsent value.
    ///
    /// # Safety
    ///
    /// Own one capacity permit before calling; release it only after a failed push or after
    /// the consumer reads the published value. No user code runs between claim and publication.
    pub unsafe fn push(&self, value: T) -> Result<(), T> {
        if self.closed.load(Ordering::Acquire) {
            return Err(value);
        }
        // Closing may race after this check. It marks every physical slot CLOSED, so even a
        // delayed claimant will recover its own value instead of publishing into a dead queue.
        let position = self.tail.fetch_add(1, Ordering::AcqRel);
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
    /// Only the exclusive consumer may call this, using its persistent cursor. Release one
    /// capacity permit after each successful pop, after the value has been read completely.
    pub unsafe fn pop(&self, head: &mut usize) -> Poll<Option<T>> {
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

    /// Stops new claims and returns ownership of published values to a drain guard.
    ///
    /// # Safety
    ///
    /// Only the exclusive consumer may close the buffer, once, using its current cursor.
    pub unsafe fn close(&self, head: usize) -> Drain<'_, T> {
        self.closed.store(true, Ordering::Release);
        // Cover every physical slot: a producer may have passed the open check but not yet
        // claimed its ticket. Such a late claim must also find a CLOSED slot.
        Drain {
            buffer: self,
            position: head,
            remaining: self.slots.len(),
        }
    }

    fn slot(&self, position: usize) -> &Slot<T> {
        // Power-of-two storage preserves indexing when the full-width ticket wraps. The
        // semaphore still enforces the exact requested capacity, including non-powers of two.
        &self.slots[position & (self.slots.len() - 1)]
    }
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
            let slot = self.buffer.slot(position);
            if slot.state.swap(CLOSED, Ordering::AcqRel) == READY {
                // SAFETY: The drain won ownership of a published value. The cursor and
                // state already advanced, so a panicking destructor cannot read twice.
                return Some(unsafe { (*slot.value.get()).assume_init_read() });
            }
            // An unpublished slot stays owned by its producer, which will observe CLOSED
            // and recover its value. The shared Arc keeps this allocation alive until then.
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
mod tests {
    use std::sync::atomic::Ordering;
    use std::task::Poll;

    use super::Buffer;

    #[test]
    fn fifo_survives_cursor_wraparound() {
        for capacity in [1, 3, 7] {
            let buffer = Buffer::new(capacity);
            let mut head = usize::MAX - 1;
            // Start near overflow instead of requiring usize::MAX messages to reach it.
            buffer.tail.store(head, Ordering::Relaxed);
            for lap in 0..4 {
                for offset in 0..capacity {
                    // SAFETY: This test owns all capacity and queues at most capacity values.
                    unsafe { buffer.push(lap * capacity + offset) }.unwrap();
                }
                for offset in 0..capacity {
                    // SAFETY: This is the only consumer, and the next batch starts after draining.
                    assert_eq!(
                        unsafe { buffer.pop(&mut head) },
                        Poll::Ready(Some(lap * capacity + offset))
                    );
                }
                // SAFETY: The test still owns the exclusive consumer cursor.
                assert_eq!(unsafe { buffer.pop(&mut head) }, Poll::Ready(None));
            }
        }
    }
}
