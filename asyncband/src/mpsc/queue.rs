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
use std::collections::VecDeque;
use std::hint::spin_loop;
use std::mem;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;

use crate::internal::mutex::Mutex;

pub(super) struct UnboundedQueue<T> {
    inner: Mutex<UnboundedInner<T>>,
}

struct UnboundedInner<T> {
    // Storage and receiver liveness share one lock so a send is linearized either before receiver
    // disconnection, with its value in the queue, or after it, with the value returned to sender.
    messages: VecDeque<T>,
    receiver_alive: bool,
}

pub(super) struct UnboundedConsumer<T> {
    local: Mutex<VecDeque<T>>,
}

pub(super) enum PushError<T> {
    Full(T),
    Disconnected(T),
}

impl<T> UnboundedQueue<T> {
    pub(super) const fn new() -> Self {
        Self {
            inner: Mutex::new(UnboundedInner {
                messages: VecDeque::new(),
                receiver_alive: true,
            }),
        }
    }

    pub(super) fn push(&self, value: T) -> Result<(), PushError<T>> {
        let mut inner = self.inner.lock();
        if !inner.receiver_alive {
            return Err(PushError::Disconnected(value));
        }
        inner.messages.push_back(value);
        Ok(())
    }

    pub(super) fn pop(&self, consumer: &UnboundedConsumer<T>) -> Option<T> {
        let mut local = consumer.local.lock();
        if local.is_empty() {
            let mut inner = self.inner.lock();
            mem::swap(&mut *local, &mut inner.messages);
        }
        local.pop_front()
    }

    pub(super) fn disconnect_receiver(&self, consumer: &UnboundedConsumer<T>) {
        let (local, shared) = {
            let mut local = consumer.local.lock();
            let mut inner = self.inner.lock();
            inner.receiver_alive = false;
            (mem::take(&mut *local), mem::take(&mut inner.messages))
        };
        drop((local, shared));
    }
}

impl<T> UnboundedConsumer<T> {
    pub(super) const fn new() -> Self {
        Self {
            local: Mutex::new(VecDeque::new()),
        }
    }
}

pub(super) struct BoundedQueue<T> {
    slots: Box<[Slot<T>]>,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    capacity: usize,
    one_lap: usize,
    mark_bit: usize,
}

#[repr(align(64))]
struct CachePadded<T>(T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct Slot<T> {
    stamp: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

// SAFETY: A successful tail CAS gives one producer exclusive access to a slot. That producer
// initializes the value before publishing the next stamp with Release ordering. The single
// consumer reads only after acquiring that stamp and publishes the following lap before reuse.
unsafe impl<T: Send> Sync for Slot<T> {}

impl<T> BoundedQueue<T> {
    pub(super) fn new(capacity: usize) -> Self {
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
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            capacity,
            one_lap,
            mark_bit,
        }
    }

    pub(super) fn try_push(&self, value: T) -> Result<(), PushError<T>> {
        let mut tail = self.tail.load(Ordering::Relaxed);
        let mut backoff = 0;
        loop {
            if tail & self.mark_bit != 0 {
                return Err(PushError::Disconnected(value));
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
                        return Ok(());
                    }
                    Err(actual) => tail = actual,
                }
            } else if stamp.wrapping_add(self.one_lap) == tail.wrapping_add(1) {
                fence(Ordering::SeqCst);
                if self.head.load(Ordering::Relaxed).wrapping_add(self.one_lap) == tail {
                    return Err(PushError::Full(value));
                }
                tail = self.tail.load(Ordering::Relaxed);
            } else {
                tail = self.tail.load(Ordering::Relaxed);
            }
            Self::spin(&mut backoff);
        }
    }

    pub(super) fn pop(&self) -> Option<T> {
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
                return Some(value);
            }

            if stamp == head {
                fence(Ordering::SeqCst);
                if self.tail.load(Ordering::Relaxed) & !self.mark_bit == head {
                    return None;
                }
            }
            if backoff == 8 {
                return None;
            }
            Self::spin(&mut backoff);
            head = self.head.load(Ordering::Relaxed);
        }
    }

    pub(super) fn disconnect_receiver(&self) {
        let tail = self.tail.fetch_or(self.mark_bit, Ordering::SeqCst) & !self.mark_bit;
        self.discard_until(tail);
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

    fn discard_until(&self, tail: usize) {
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

impl<T> Drop for BoundedQueue<T> {
    fn drop(&mut self) {
        let tail = self.tail.fetch_or(self.mark_bit, Ordering::SeqCst) & !self.mark_bit;
        self.discard_until(tail);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::BoundedQueue;
    use super::PushError;
    use super::UnboundedConsumer;
    use super::UnboundedQueue;

    #[test]
    fn bounded_queue_preserves_capacity_and_fifo_order() {
        let queue = BoundedQueue::new(3);
        for value in 0..3 {
            assert!(queue.try_push(value).is_ok());
        }
        assert!(matches!(queue.try_push(3), Err(PushError::Full(3))));
        for value in 0..3 {
            assert_eq!(queue.pop(), Some(value));
        }
        assert_eq!(queue.pop(), None);

        for value in 3..12 {
            assert!(queue.try_push(value).is_ok());
            assert_eq!(queue.pop(), Some(value));
        }
    }

    #[test]
    fn bounded_queue_coordinates_multiple_producers() {
        let queue = Arc::new(BoundedQueue::new(4));
        let producers: Vec<_> = (0..2)
            .map(|producer| {
                let queue = queue.clone();
                thread::spawn(move || {
                    for offset in 0..32 {
                        let mut value = producer * 32 + offset;
                        loop {
                            match queue.try_push(value) {
                                Ok(()) => break,
                                Err(PushError::Full(returned)) => {
                                    value = returned;
                                    thread::yield_now();
                                }
                                Err(PushError::Disconnected(_)) => panic!("queue disconnected"),
                            }
                        }
                    }
                })
            })
            .collect();

        let mut values = Vec::new();
        while values.len() < 64 {
            if let Some(value) = queue.pop() {
                values.push(value);
            } else {
                thread::yield_now();
            }
        }
        for producer in producers {
            producer.join().unwrap();
        }
        values.sort_unstable();
        assert_eq!(values, (0..64).collect::<Vec<_>>());
    }

    #[test]
    fn unbounded_queue_batches_without_reordering() {
        let queue = UnboundedQueue::new();
        let consumer = UnboundedConsumer::new();
        assert!(queue.push(1).is_ok());
        assert!(queue.push(2).is_ok());
        assert_eq!(queue.pop(&consumer), Some(1));
        assert!(queue.push(3).is_ok());
        assert_eq!(queue.pop(&consumer), Some(2));
        assert_eq!(queue.pop(&consumer), Some(3));
        assert_eq!(queue.pop(&consumer), None);
    }
}
