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
use std::task::Poll;

use crate::internal::mutex::Mutex;

pub struct UnboundedQueue<T> {
    inner: Mutex<UnboundedInner<T>>,
}

struct UnboundedInner<T> {
    // Storage and receiver liveness share one lock so a send is linearized either before receiver
    // disconnection, with its value in the queue, or after it, with the value returned to sender.
    messages: VecDeque<T>,
    receiver_alive: bool,
}

pub struct UnboundedConsumer<T> {
    // Preserve Sync for Send-only values; exclusive consumer access never needs to lock this
    // mutex.
    local: Mutex<VecDeque<T>>,
}

// The consumer never relies on a pinned location for its local queue or queued values.
impl<T> Unpin for UnboundedConsumer<T> {}

pub enum PushError<T> {
    Full(T),
    Disconnected(T),
}

impl<T> UnboundedQueue<T> {
    pub const fn new() -> Self {
        Self {
            inner: Mutex::new(UnboundedInner {
                messages: VecDeque::new(),
                receiver_alive: true,
            }),
        }
    }

    pub fn push(&self, value: T) -> Result<(), PushError<T>> {
        let mut inner = self.inner.lock();
        if !inner.receiver_alive {
            return Err(PushError::Disconnected(value));
        }
        inner.messages.push_back(value);
        Ok(())
    }

    pub fn pop(&self, consumer: &mut UnboundedConsumer<T>) -> Option<T> {
        let local = consumer.local.get_mut();
        if local.is_empty() {
            let mut inner = self.inner.lock();
            mem::swap(local, &mut inner.messages);
        }
        local.pop_front()
    }

    pub fn disconnect_receiver(&self, consumer: &mut UnboundedConsumer<T>) {
        let (local, shared) = {
            let local = consumer.local.get_mut();
            let mut inner = self.inner.lock();
            inner.receiver_alive = false;
            (mem::take(local), mem::take(&mut inner.messages))
        };
        drop((local, shared));
    }
}

impl<T> UnboundedConsumer<T> {
    pub const fn new() -> Self {
        Self {
            local: Mutex::new(VecDeque::new()),
        }
    }
}

pub struct BoundedQueue<T> {
    slots: Box<[Slot<T>]>,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    capacity: usize,
    one_lap: usize,
    mark_bit: usize,
}

// Use conservative architecture estimates, not a guarantee about every CPU's cache line.
// Keep 128 bytes for large ARM/PowerPC lines and adjacent-line prefetching on x86-64,
// 256 bytes for s390x, and at least 64 bytes elsewhere.
#[cfg_attr(target_arch = "s390x", repr(align(256)))]
#[cfg_attr(
    any(
        target_arch = "aarch64",
        target_arch = "arm64ec",
        target_arch = "powerpc64",
        target_arch = "x86_64",
    ),
    repr(align(128))
)]
#[cfg_attr(
    not(any(
        target_arch = "s390x",
        target_arch = "aarch64",
        target_arch = "arm64ec",
        target_arch = "powerpc64",
        target_arch = "x86_64",
    )),
    repr(align(64))
)]
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

// The ownership transition finishes before user code can unwind, and no stored-value reference is
// exposed.
impl<T> std::panic::UnwindSafe for Slot<T> {}
impl<T> std::panic::RefUnwindSafe for Slot<T> {}

impl<T> BoundedQueue<T> {
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
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            capacity,
            one_lap,
            mark_bit,
        }
    }

    pub fn try_push(&self, value: T) -> Result<(), PushError<T>> {
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

    /// Pending means the head slot is reserved but not published. It is distinct from an empty
    /// queue: later producers may already have completed their sends.
    ///
    /// # Safety
    ///
    /// The caller must serialize all calls to `pop` and `disconnect_receiver` for this queue.
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

    /// Closes the queue and drops all remaining values.
    ///
    /// # Safety
    ///
    /// The caller must serialize all calls to `pop` and `disconnect_receiver` for this queue.
    pub unsafe fn disconnect_receiver(&self) {
        let tail = self.tail.fetch_or(self.mark_bit, Ordering::SeqCst) & !self.mark_bit;
        // SAFETY: The caller guarantees exclusive consumer access.
        unsafe { self.discard_until(tail) };
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

impl<T> Drop for BoundedQueue<T> {
    fn drop(&mut self) {
        let tail = self.tail.fetch_or(self.mark_bit, Ordering::SeqCst) & !self.mark_bit;
        // SAFETY: The queue is closed and its exclusive borrow rules out concurrent access.
        unsafe { self.discard_until(tail) };
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Poll;
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
            // SAFETY: This thread is the only consumer.
            assert_eq!(unsafe { queue.pop() }, Poll::Ready(Some(value)));
        }
        // SAFETY: This thread is the only consumer.
        assert_eq!(unsafe { queue.pop() }, Poll::Ready(None));

        for value in 3..12 {
            assert!(queue.try_push(value).is_ok());
            // SAFETY: This thread is the only consumer.
            assert_eq!(unsafe { queue.pop() }, Poll::Ready(Some(value)));
        }
    }

    #[test]
    fn bounded_queue_does_not_report_empty_behind_an_unpublished_head() {
        let queue = BoundedQueue::new(2);

        // Pause a synthetic producer after reserving and initializing slot 0, before publishing
        // its stamp. Another producer can finish sending into slot 1 in the meantime.
        queue.tail.store(1, Ordering::SeqCst);
        let slot = &queue.slots[0];
        // SAFETY: advancing the tail reserved this initially empty slot for the synthetic producer.
        unsafe { (*slot.value.get()).write(1) };
        let later_send = queue.try_push(2);
        // SAFETY: This thread is the only consumer, even while a producer is unpublished.
        let receive = unsafe { queue.pop() };

        // Finish publication before asserting so even a failed assertion can safely drop the queue.
        slot.stamp.store(1, Ordering::Release);
        assert!(later_send.is_ok());
        assert_eq!(receive, Poll::Pending);
        // SAFETY: This thread is the only consumer.
        unsafe {
            assert_eq!(queue.pop(), Poll::Ready(Some(1)));
            assert_eq!(queue.pop(), Poll::Ready(Some(2)));
            assert_eq!(queue.pop(), Poll::Ready(None));
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
            // SAFETY: Worker threads only push; this thread is the only consumer.
            if let Poll::Ready(Some(value)) = unsafe { queue.pop() } {
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
    fn bounded_queue_discards_wrapped_values_once_after_receiver_disconnect() {
        // This has no owning fields, so a buggy second drop remains observable as count == 2
        // instead of invalidating the tracker first.
        struct DropSpy<'a>(&'a AtomicUsize);

        impl<'a> Drop for DropSpy<'a> {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Declare this before `queue` so the counters outlive values held by the queue.
        let drops = [
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        ];
        let queue = BoundedQueue::new(3);

        // Positions: 0, 1, 2 (then tail wraps to 8).
        for counter in &drops[..3] {
            assert!(queue.try_push(DropSpy(counter)).is_ok());
        }

        // Free slot 0, then reuse it on the next lap at position 8.
        // SAFETY: This thread is the only consumer.
        let popped = unsafe { queue.pop() };
        assert!(matches!(popped, Poll::Ready(Some(_))));
        drop(popped);
        assert_eq!(drops[0].load(Ordering::Relaxed), 1);
        assert!(queue.try_push(DropSpy(&drops[3])).is_ok());

        // The pending range is positions 1 -> 2 -> 8 -> 9, not a contiguous integer range.
        assert_eq!(queue.head.load(Ordering::Relaxed), 1);
        assert_eq!(queue.tail.load(Ordering::Relaxed), queue.one_lap + 1);

        // SAFETY: This thread is the only consumer and no pop is in progress.
        unsafe { queue.disconnect_receiver() };

        // `discard_until` must dispose every value exactly once, including position 8.
        for (value, counter) in drops.iter().enumerate() {
            assert_eq!(
                counter.load(Ordering::Relaxed),
                1,
                "value {value} was dropped an unexpected number of times"
            );
        }

        // Queue Drop calls discard_until again; it must see head == tail and not redrop.
        drop(queue);
        for (value, counter) in drops.iter().enumerate() {
            assert_eq!(
                counter.load(Ordering::Relaxed),
                1,
                "value {value} was dropped more than once"
            );
        }
    }

    #[test]
    fn unbounded_queue_batches_without_reordering() {
        let queue = UnboundedQueue::new();
        let mut consumer = UnboundedConsumer::new();
        assert!(queue.push(1).is_ok());
        assert!(queue.push(2).is_ok());
        assert_eq!(queue.pop(&mut consumer), Some(1));
        assert!(queue.push(3).is_ok());
        assert_eq!(queue.pop(&mut consumer), Some(2));
        assert_eq!(queue.pop(&mut consumer), Some(3));
        assert_eq!(queue.pop(&mut consumer), None);
    }
}
