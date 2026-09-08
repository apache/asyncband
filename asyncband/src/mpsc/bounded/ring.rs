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

use crate::internal::cache_padded::CachePadded;
use crate::internal::mutex::Mutex;
use crate::mpsc::TrySendError;

pub struct Ring<T> {
    slots: Box<[Slot<T>]>,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    // This flag is usually stable while the consumer advances head. Sharing head
    // for notifications would make every producer track a constantly invalidated cache line.
    receiver_waiting: CachePadded<AtomicBool>,
    receiver: Mutex<Option<Waker>>,
    capacity: usize,
    one_lap: usize,
    mark_bit: usize,
}

struct Slot<T> {
    stamp: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

// SAFETY: A successful tail CAS gives one producer exclusive access to a slot. That producer
// initializes the value before publishing the next stamp with Release ordering. The single
// consumer reads only after acquiring that stamp and publishes the following lap before reuse.
unsafe impl<T: Send> Sync for Slot<T> {}

// The ownership transition finishes before user code can unwind, and no stored-value reference is
// exposed.
impl<T> std::panic::UnwindSafe for Slot<T> {}
impl<T> std::panic::RefUnwindSafe for Slot<T> {}

impl<T> Ring<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity <= usize::MAX / 4, "mpsc capacity is too large");
        let mark_bit = (capacity + 1).next_power_of_two();
        let one_lap = mark_bit * 2;
        let slots = (0..capacity)
            .map(|index| Slot {
                stamp: AtomicUsize::new(index),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        Self {
            slots,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            receiver_waiting: CachePadded::new(AtomicBool::new(false)),
            receiver: Mutex::new(None),
            capacity,
            one_lap,
            mark_bit,
        }
    }

    pub fn try_push(&self, value: T) -> Result<(), TrySendError<T>> {
        let mut tail = self.tail.load(Ordering::Relaxed);
        let mut backoff = 0;
        loop {
            if tail & self.mark_bit != 0 {
                return Err(TrySendError::Disconnected(value));
            }

            let index = tail & (self.mark_bit - 1);
            let slot = &self.slots[index];
            let stamp = slot.stamp.load(Ordering::Acquire);
            if stamp == tail {
                let next_tail = self.advance(tail);
                match self.tail.compare_exchange_weak(
                    tail,
                    next_tail,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: The successful CAS reserved this slot exclusively, and its
                        // matching stamp proves the consumer completed its previous lap.
                        unsafe { (*slot.value.get()).write(value) };
                        slot.stamp.store(tail.wrapping_add(1), Ordering::Release);
                        // Publication precedes the wait check; registration pairs this fence
                        // with a second pop before the receiver is allowed to return Pending.
                        fence(Ordering::SeqCst);
                        if self.receiver_waiting.load(Ordering::Relaxed)
                            && self.receiver_waiting.swap(false, Ordering::Relaxed)
                        {
                            // Claim the notification before locking so concurrent publishers
                            // do not all queue behind the same receiver registration.
                            self.wake_receiver();
                        }
                        return Ok(());
                    }
                    Err(actual) => tail = actual,
                }
            } else if stamp.wrapping_add(self.one_lap) == tail.wrapping_add(1) {
                fence(Ordering::SeqCst);
                if self.head.load(Ordering::Relaxed).wrapping_add(self.one_lap) == tail {
                    return Err(TrySendError::Full(value));
                }
                tail = self.tail.load(Ordering::Relaxed);
            } else {
                let actual = self.tail.load(Ordering::Relaxed);
                if actual == tail {
                    // Reserved but unpublished messages also occupy capacity. In particular, a
                    // capacity-one queue must report Full without waiting for its producer to
                    // publish the slot's stamp.
                    fence(Ordering::SeqCst);
                    if self.head.load(Ordering::Relaxed).wrapping_add(self.one_lap) == tail {
                        return Err(TrySendError::Full(value));
                    }
                }
                tail = actual;
            }
            Self::spin(&mut backoff);
        }
    }

    /// Pending means the head slot is reserved but not published. It is distinct from an empty
    /// queue: later producers may already have completed their sends.
    ///
    /// # Safety
    ///
    /// The caller must serialize all calls to `pop` and `drain` for this queue.
    pub unsafe fn pop(&self) -> Poll<Option<T>> {
        let mut head = self.head.load(Ordering::Relaxed);
        let mut backoff = 0;
        loop {
            let index = head & (self.mark_bit - 1);
            let slot = &self.slots[index];
            let stamp = slot.stamp.load(Ordering::Acquire);
            if stamp == head.wrapping_add(1) {
                let next_head = self.advance(head);
                // SAFETY: Acquiring the matching stamp observes initialization by the producer.
                // There is one consumer, so the value is read exactly once.
                let value = unsafe { (*slot.value.get()).assume_init_read() };
                slot.stamp
                    .store(head.wrapping_add(self.one_lap), Ordering::Release);
                self.head.store(next_head, Ordering::SeqCst);
                return Poll::Ready(Some(value));
            }

            if stamp == head {
                fence(Ordering::SeqCst);
                if self.tail.load(Ordering::Relaxed) & !self.mark_bit == head {
                    return Poll::Ready(None);
                }
            }
            if backoff == 8 {
                return Poll::Pending;
            }
            Self::spin(&mut backoff);
            head = self.head.load(Ordering::Relaxed);
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
    pub fn close(&self) {
        self.tail.fetch_or(self.mark_bit, Ordering::SeqCst);
    }

    /// Drops all values after closing, including values whose publication is still in progress.
    ///
    /// # Safety
    ///
    /// The queue must be closed. The caller must serialize all calls to `pop` and `drain`.
    pub unsafe fn drain(&self) {
        struct DrainRemaining<'a, T> {
            ring: &'a Ring<T>,
            tail: usize,
        }
        impl<T> Drop for DrainRemaining<'_, T> {
            fn drop(&mut self) {
                // SAFETY: The guard is scoped to the exclusive consumer's drain of a closed ring.
                unsafe { self.ring.discard_until(self.tail) };
            }
        }

        let tail = self.tail.load(Ordering::Relaxed);
        debug_assert_ne!(tail & self.mark_bit, 0);
        let remaining = DrainRemaining {
            ring: self,
            tail: tail & !self.mark_bit,
        };
        // SAFETY: The caller guarantees exclusive consumer access. The guard finishes draining if
        // a value's destructor panics, so messages that own senders cannot retain the closed ring.
        unsafe { self.discard_until(remaining.tail) };
    }

    fn advance(&self, position: usize) -> usize {
        let index = position & (self.mark_bit - 1);
        if index + 1 < self.capacity {
            position + 1
        } else {
            let lap = position & !(self.one_lap - 1);
            lap.wrapping_add(self.one_lap)
        }
    }

    // The caller must close the queue and have exclusive consumer access before discarding values.
    unsafe fn discard_until(&self, tail: usize) {
        let mut head = self.head.load(Ordering::Relaxed);
        let mut backoff = 0;
        while head != tail {
            let index = head & (self.mark_bit - 1);
            let slot = &self.slots[index];
            if slot.stamp.load(Ordering::Acquire) == head.wrapping_add(1) {
                let next_head = self.advance(head);
                // Move the head before dropping the value so unwinding cannot drop it twice.
                slot.stamp
                    .store(head.wrapping_add(self.one_lap), Ordering::Release);
                self.head.store(next_head, Ordering::SeqCst);
                // SAFETY: The acquired matching stamp proves the slot contains an initialized
                // value, and advancing the single-consumer head claims it exactly once.
                unsafe { (*slot.value.get()).assume_init_drop() };
                head = next_head;
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

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        self.close();
        // SAFETY: The queue is closed and its exclusive borrow rules out concurrent access.
        unsafe { self.drain() };
    }
}

#[cfg(test)]
#[path = "ring_tests.rs"]
mod tests;
