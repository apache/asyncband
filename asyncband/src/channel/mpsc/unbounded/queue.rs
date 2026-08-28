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

// Portions are adapted from crossbeam-channel, copyright (c) 2019 The Crossbeam Project
// Developers, and used under the Apache License, Version 2.0.

//! Segmented storage for the unbounded MPSC channel.
//!
//! The producer-side layout is adapted from the list flavor in `crossbeam-channel`, licensed
//! under Apache-2.0 OR MIT. Unlike that MPMC implementation, this queue has one consumer, so the
//! consumer owns its position and a block can be reclaimed as soon as its final slot is read.

use std::cell::UnsafeCell;
use std::hint;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;

const LAP: usize = 32;
const BLOCK_CAPACITY: usize = LAP - 1;
const SHIFT: usize = 1;
const STEP: usize = 1 << SHIFT;
const CLOSED: usize = 1;

#[repr(align(128))]
struct CachePadded<T>(T);

struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    ready: AtomicBool,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            value: UnsafeCell::new(MaybeUninit::uninit()),
            ready: AtomicBool::new(false),
        }
    }
}

struct Block<T> {
    next: AtomicPtr<Block<T>>,
    slots: [Slot<T>; BLOCK_CAPACITY],
}

impl<T> Block<T> {
    fn new() -> Box<Self> {
        Box::new(Self {
            next: AtomicPtr::new(ptr::null_mut()),
            slots: std::array::from_fn(|_| Slot::new()),
        })
    }
}

struct Position<T> {
    index: AtomicUsize,
    block: AtomicPtr<Block<T>>,
}

/// Producer-owned half of an unbounded MPSC queue.
pub struct Queue<T> {
    tail: CachePadded<Position<T>>,
    _marker: PhantomData<T>,
}

// SAFETY: each producer reserves a distinct slot through `tail.index`. A value placed in a slot is
// only accessed by the single consumer after the producer publishes `ready` with release semantics.
unsafe impl<T: Send> Send for Queue<T> {}
// SAFETY: the producer algorithm coordinates all shared mutation through atomics. The consumer is
// separate and may only read a slot after acquiring its `ready` publication.
unsafe impl<T: Send> Sync for Queue<T> {}

/// Consumer-owned position in an unbounded MPSC queue.
pub struct Consumer<T> {
    index: usize,
    block: *mut Block<T>,
    _marker: PhantomData<T>,
}

// SAFETY: moving the unique consumer moves exclusive ownership of its position. Queue values cross
// the thread boundary only when `T: Send`.
unsafe impl<T: Send> Send for Consumer<T> {}
// SAFETY: shared references cannot pop or close the consumer because both operations require
// exclusive access. Sharing an idle consumer therefore exposes neither its position nor `T`.
unsafe impl<T: Send> Sync for Consumer<T> {}

/// Result of attempting to pop one queue slot.
pub enum Pop<T> {
    /// A published value was removed.
    Value(T),
    /// No producer has reserved the next slot.
    Empty,
    /// A producer reserved the next slot but has not published its value yet.
    Pending,
}

impl<T> Queue<T> {
    /// Creates the producer queue and its unique consumer position.
    pub fn new() -> (Self, Consumer<T>) {
        let block = Box::into_raw(Block::new());
        let queue = Self {
            tail: CachePadded(Position {
                index: AtomicUsize::new(0),
                block: AtomicPtr::new(block),
            }),
            _marker: PhantomData,
        };
        let consumer = Consumer {
            index: 0,
            block,
            _marker: PhantomData,
        };
        (queue, consumer)
    }

    /// Appends a value, or returns it if the consumer has closed the queue.
    pub fn push(&self, value: T) -> Result<(), T> {
        let mut backoff = Backoff::new();
        let mut tail = self.tail.0.index.load(Ordering::Acquire);
        let mut block = self.tail.0.block.load(Ordering::Acquire);
        let mut next_block = None;

        loop {
            if tail & CLOSED != 0 {
                return Err(value);
            }

            let offset = (tail >> SHIFT) % LAP;
            if offset == BLOCK_CAPACITY {
                backoff.snooze();
                tail = self.tail.0.index.load(Ordering::Acquire);
                block = self.tail.0.block.load(Ordering::Acquire);
                continue;
            }

            // The producer that reserves a block's final slot also installs its successor. Doing
            // the allocation before the reservation keeps the boundary transition short.
            if offset + 1 == BLOCK_CAPACITY && next_block.is_none() {
                next_block = Some(Block::new());
            }

            let new_tail = tail.wrapping_add(STEP);
            match self.tail.0.index.compare_exchange_weak(
                tail,
                new_tail,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // SAFETY: a successful reservation gives this producer exclusive ownership of
                    // `block.slots[offset]`. The receiver cannot reclaim the block until this slot
                    // publishes `ready` and is read in FIFO order.
                    unsafe {
                        if offset + 1 == BLOCK_CAPACITY {
                            let next = Box::into_raw(next_block.take().unwrap());
                            (*block).next.store(next, Ordering::Release);
                            self.tail.0.block.store(next, Ordering::Release);

                            // The reserved sentinel index keeps other producers and close cleanup
                            // from entering the next block before both pointers are installed.
                            self.tail.0.index.fetch_add(STEP, Ordering::Release);
                        }

                        let slot = (*block).slots.get_unchecked(offset);
                        (*slot.value.get()).write(value);
                        // This publication joins the notification gate's sequentially consistent
                        // order. If a producer checks the gate before the receiver arms it, the
                        // receiver's subsequent recheck must observe this completed publication.
                        slot.ready.store(true, Ordering::SeqCst);
                    }
                    return Ok(());
                }
                Err(observed) => {
                    tail = observed;
                    block = self.tail.0.block.load(Ordering::Acquire);
                    backoff.spin();
                }
            }
        }
    }

    /// Closes the producer side, waits for already-reserved slots, and discards buffered values.
    pub fn close(&self, consumer: &mut Consumer<T>) {
        let mut backoff = Backoff::new();
        let mut tail = self.tail.0.index.fetch_or(CLOSED, Ordering::SeqCst) | CLOSED;

        // A producer at the sentinel owns the block transition. It reserved before close and must
        // finish installing the next block before cleanup can traverse it.
        while (tail >> SHIFT) % LAP == BLOCK_CAPACITY {
            backoff.snooze();
            tail = self.tail.0.index.load(Ordering::Acquire);
        }

        let mut cleanup = Cleanup {
            queue: self,
            consumer,
            complete: false,
        };
        cleanup.drain();
        cleanup.complete = true;
    }
}

impl<T> Consumer<T> {
    /// Attempts to remove the next value without waiting for a producer to finish publishing it.
    pub fn pop(&mut self, queue: &Queue<T>) -> Pop<T> {
        let tail = queue.tail.0.index.load(Ordering::SeqCst);
        if self.index >> SHIFT == tail >> SHIFT {
            return Pop::Empty;
        }

        let offset = (self.index >> SHIFT) % LAP;
        debug_assert!(offset < BLOCK_CAPACITY);

        // SAFETY: `block` is exclusively owned by this consumer position. A non-empty queue means
        // the corresponding producer reserved this slot; acquiring `ready` publishes its value.
        unsafe {
            let slot = (*self.block).slots.get_unchecked(offset);
            if !slot.ready.load(Ordering::SeqCst) {
                return Pop::Pending;
            }

            let value = (*slot.value.get()).assume_init_read();
            let new_index = self.index.wrapping_add(STEP);

            if offset + 1 == BLOCK_CAPACITY {
                let old = self.block;
                let next = (*old).next.load(Ordering::Acquire);
                debug_assert!(!next.is_null());
                self.block = next;
                self.index = new_index.wrapping_add(STEP);

                // Observing the last slot's publication also observes the producer's earlier next
                // pointer publication. FIFO consumption means every producer using `old` has
                // finished publishing before the consumer reaches this point.
                drop(Box::from_raw(old));
            } else {
                self.index = new_index;
            }

            Pop::Value(value)
        }
    }

    fn finish(&mut self, queue: &Queue<T>) {
        debug_assert!(!self.block.is_null());

        // SAFETY: close prevents new reservations, and cleanup reaches this point only after all
        // reserved values have been read. The current empty block is therefore exclusively owned.
        unsafe { drop(Box::from_raw(self.block)) };
        self.block = ptr::null_mut();
        queue.tail.0.block.store(ptr::null_mut(), Ordering::Release);
    }
}

struct Cleanup<'a, T> {
    queue: &'a Queue<T>,
    consumer: &'a mut Consumer<T>,
    complete: bool,
}

impl<T> Cleanup<'_, T> {
    fn drain(&mut self) {
        let mut backoff = Backoff::new();
        loop {
            match self.consumer.pop(self.queue) {
                Pop::Value(value) => {
                    backoff.reset();
                    drop(value);
                }
                Pop::Pending => backoff.snooze(),
                Pop::Empty => {
                    self.consumer.finish(self.queue);
                    return;
                }
            }
        }
    }
}

impl<T> Drop for Cleanup<'_, T> {
    fn drop(&mut self) {
        if !self.complete {
            // Continue reclaiming if dropping a buffered value unwinds. A second destructor panic
            // follows Rust's usual double-panic behavior and aborts the process.
            self.drain();
        }
    }
}

struct Backoff {
    step: u32,
}

impl Backoff {
    fn new() -> Self {
        Self { step: 0 }
    }

    fn spin(&mut self) {
        let iterations = 1 << self.step.min(6);
        for _ in 0..iterations {
            hint::spin_loop();
        }
        self.step = self.step.saturating_add(1);
    }

    fn snooze(&mut self) {
        if self.step <= 6 {
            self.spin();
        } else {
            thread::yield_now();
            self.step = self.step.saturating_add(1);
        }
    }

    fn reset(&mut self) {
        self.step = 0;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::thread;

    use super::Pop;
    use super::Queue;

    #[test]
    fn crosses_block_boundaries_in_fifo_order() {
        let (queue, mut consumer) = Queue::new();

        for value in 0..1_000 {
            queue.push(value).unwrap();
        }
        for expected in 0..1_000 {
            match consumer.pop(&queue) {
                Pop::Value(value) => assert_eq!(value, expected),
                Pop::Empty | Pop::Pending => panic!("reserved value should be ready"),
            }
        }
        assert!(matches!(consumer.pop(&queue), Pop::Empty));

        queue.close(&mut consumer);
    }

    #[test]
    fn concurrent_producers_preserve_per_producer_order() {
        const PRODUCERS: usize = 4;
        const VALUES: usize = 128;

        let (queue, mut consumer) = Queue::new();
        let queue = Arc::new(queue);
        let start = Arc::new(Barrier::new(PRODUCERS + 1));
        let workers = (0..PRODUCERS)
            .map(|producer| {
                let queue = queue.clone();
                let start = start.clone();
                thread::spawn(move || {
                    start.wait();
                    for sequence in 0..VALUES {
                        queue.push((producer, sequence)).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();

        start.wait();
        let mut next = [0; PRODUCERS];
        while next.iter().sum::<usize>() < PRODUCERS * VALUES {
            match consumer.pop(&queue) {
                Pop::Value((producer, sequence)) => {
                    assert_eq!(sequence, next[producer]);
                    next[producer] += 1;
                }
                Pop::Empty | Pop::Pending => thread::yield_now(),
            }
        }

        for worker in workers {
            worker.join().unwrap();
        }
        queue.close(&mut consumer);
    }

    #[test]
    fn close_drops_or_returns_every_racing_value_once() {
        const PRODUCERS: usize = 4;
        const VALUES: usize = 128;

        struct DropProbe(Arc<AtomicUsize>);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let (queue, mut consumer) = Queue::new();
        let queue = Arc::new(queue);
        let dropped = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(PRODUCERS + 1));
        let workers = (0..PRODUCERS)
            .map(|_| {
                let queue = queue.clone();
                let dropped = dropped.clone();
                let start = start.clone();
                thread::spawn(move || {
                    start.wait();
                    for _ in 0..VALUES {
                        drop(queue.push(DropProbe(dropped.clone())));
                    }
                })
            })
            .collect::<Vec<_>>();

        start.wait();
        queue.close(&mut consumer);
        for worker in workers {
            worker.join().unwrap();
        }

        assert_eq!(dropped.load(Ordering::Relaxed), PRODUCERS * VALUES);
    }
}
