// Portions of this queue are adapted from crossbeam-channel 0.5.16.
// Copyright (c) 2019 The Crossbeam Project Developers
// Asyncband uses the Apache-2.0 license option for the incorporated code.
// The incorporated code has been modified for use in Apache Asyncband.
// Upstream source:
// https://github.com/crossbeam-rs/crossbeam/blob/9b56303b8aa9ff8ec5bbebb9d2da05e034977889/crossbeam-channel/src/flavors/list.rs

//! Producer reservations own distinct slots. Publication transfers each value to the unique
//! consumer; FIFO consumption delays block reclamation until every producer using it is done.

use std::cell::UnsafeCell;
use std::hint;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::panic::RefUnwindSafe;
use std::panic::UnwindSafe;
use std::ptr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;

use super::CachePadded;
use crate::internal::mutex::Mutex;

const BLOCK_BYTES: usize = 32 * 1024;
const SHIFT: usize = 1;
const STEP: usize = 1 << SHIFT;
const CLOSED: usize = 1;

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
    slots: Box<[Slot<T>]>,
}

impl<T> Block<T> {
    // A power-of-two lap preserves slot identity when the wrapping position overflows.
    // Small messages keep the original 31-slot layout; larger messages reduce the allocation.
    const CAPACITY: usize = {
        let slots = BLOCK_BYTES / size_of::<Slot<T>>();
        let mut lap = 2;
        while lap < 32 && lap * 2 - 1 <= slots {
            lap *= 2;
        }
        lap - 1
    };
    const LAP: usize = Self::CAPACITY + 1;

    fn new() -> Box<Self> {
        Box::new(Self {
            next: AtomicPtr::new(ptr::null_mut()),
            slots: (0..Self::CAPACITY).map(|_| Slot::new()).collect(),
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
    initialize: Mutex<()>,
    first: AtomicPtr<Block<T>>,
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
}

impl<T> Queue<T> {
    /// Creates the producer queue and its unique consumer position.
    pub fn new() -> (Self, Consumer<T>) {
        let block = ptr::null_mut();
        let queue = Self {
            tail: CachePadded(Position {
                index: AtomicUsize::new(0),
                block: AtomicPtr::new(block),
            }),
            initialize: Mutex::new(()),
            first: AtomicPtr::new(ptr::null_mut()),
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

            if block.is_null() {
                // Serialize first allocation with close. Ordinary sends never take this mutex.
                let _guard = self.initialize.lock();
                tail = self.tail.0.index.load(Ordering::Acquire);
                if tail & CLOSED != 0 {
                    return Err(value);
                }
                block = self.tail.0.block.load(Ordering::Acquire);
                if block.is_null() {
                    block = Box::into_raw(Block::new());
                    self.first.store(block, Ordering::Release);
                    self.tail.0.block.store(block, Ordering::Release);
                }
            }
            let offset = (tail >> SHIFT) % Block::<T>::LAP;
            if offset == Block::<T>::CAPACITY {
                backoff.snooze();
                tail = self.tail.0.index.load(Ordering::Acquire);
                block = self.tail.0.block.load(Ordering::Acquire);
                continue;
            }

            // The producer that reserves a block's final slot also installs its successor. Doing
            // the allocation before the reservation keeps the boundary transition short.
            if offset + 1 == Block::<T>::CAPACITY && next_block.is_none() {
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
                        if offset + 1 == Block::<T>::CAPACITY {
                            let next = Box::into_raw(next_block.take().unwrap());
                            (*block).next.store(next, Ordering::Release);
                            self.tail.0.block.store(next, Ordering::Release);

                            // The reserved sentinel index keeps other producers and close cleanup
                            // from entering the next block before both pointers are installed.
                            self.tail.0.index.fetch_add(STEP, Ordering::Release);
                        }

                        let slots = &(*block).slots;
                        let slot = slots.get_unchecked(offset);
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
        let mut tail = {
            let _guard = self.initialize.lock();
            self.tail.0.index.fetch_or(CLOSED, Ordering::SeqCst) | CLOSED
        };

        // A producer at the sentinel owns the block transition. It reserved before close and must
        // finish installing the next block before cleanup can traverse it.
        while (tail >> SHIFT) % Block::<T>::LAP == Block::<T>::CAPACITY {
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
    /// Removes the next value, waiting only for a producer that already reserved its slot.
    pub fn pop(&mut self, queue: &Queue<T>) -> Pop<T> {
        if self.block.is_null() {
            if self.index >> SHIFT == queue.tail.0.index.load(Ordering::SeqCst) >> SHIFT {
                return Pop::Empty;
            }
            self.block = queue.first.load(Ordering::Acquire);
        }
        let offset = (self.index >> SHIFT) % Block::<T>::LAP;
        debug_assert!(offset < Block::<T>::CAPACITY);

        // SAFETY: the consumer alone reads slots and frees blocks. A ready slot transfers the
        // initialized value. Reading ready first keeps a draining consumer off the producer's
        // tail cache line; the tail is needed only to distinguish empty from unpublished slots.
        unsafe {
            let slots = &(*self.block).slots;
            let slot = slots.get_unchecked(offset);
            if !slot.ready.load(Ordering::SeqCst) {
                if self.index >> SHIFT == queue.tail.0.index.load(Ordering::SeqCst) >> SHIFT {
                    return Pop::Empty;
                }
                // A later completed send must not be hidden behind an Empty result. Wait for
                // the earlier reservation to publish, as FIFO order requires.
                let mut backoff = Backoff::new();
                while !slot.ready.load(Ordering::Acquire) {
                    backoff.snooze();
                }
            }

            let value = (*slot.value.get()).assume_init_read();
            let new_index = self.index.wrapping_add(STEP);

            if offset + 1 == Block::<T>::CAPACITY {
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
        // An initialized queue may still be empty if close won before the first reservation.
        if self.block.is_null() {
            self.block = queue.first.load(Ordering::Acquire);
        }
        if !self.block.is_null() {
            // SAFETY: close prevents new reservations and every reserved value was consumed.
            unsafe { drop(Box::from_raw(self.block)) };
        }
        self.block = ptr::null_mut();
        queue.tail.0.block.store(ptr::null_mut(), Ordering::Release);
        queue.first.store(ptr::null_mut(), Ordering::Release);
    }
}

struct Cleanup<'a, T> {
    queue: &'a Queue<T>,
    consumer: &'a mut Consumer<T>,
    complete: bool,
}

impl<T> Cleanup<'_, T> {
    fn drain(&mut self) {
        loop {
            match self.consumer.pop(self.queue) {
                Pop::Value(value) => {
                    drop(value);
                }
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
}

// Queue contents are never exposed by shared reference; unwinding does not abandon a reserved
// slot because allocation precedes reservation and publication invokes no user callbacks.
impl<T> RefUnwindSafe for Queue<T> {}
impl<T> UnwindSafe for Queue<T> {}
impl<T> RefUnwindSafe for Consumer<T> {}
impl<T> UnwindSafe for Consumer<T> {}

#[cfg(test)]
mod tests;
