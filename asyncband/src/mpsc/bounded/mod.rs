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

//! Bounded multi-producer, single-consumer channel with capacity backpressure.

#[cfg(target_has_atomic = "64")]
use std::alloc::Layout;
#[cfg(target_has_atomic = "64")]
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::mem;
#[cfg(target_has_atomic = "64")]
use std::mem::ManuallyDrop;
#[cfg(target_has_atomic = "64")]
use std::mem::MaybeUninit;
#[cfg(target_has_atomic = "64")]
use std::ops::Deref;
use std::panic::RefUnwindSafe;
use std::panic::UnwindSafe;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
#[cfg(target_has_atomic = "64")]
use std::sync::atomic::AtomicU32;
#[cfg(target_has_atomic = "64")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::waitlist::WaitList;
use crate::mpsc::TrySendError;

mod receiver;
mod sender;

pub use self::receiver::BoundedReceiver;
pub use self::sender::BoundedSender;
pub use self::sender::Permit;

#[cfg(target_has_atomic = "64")]
#[cfg_attr(any(target_arch = "aarch64", target_arch = "x86_64"), repr(align(128)))]
#[cfg_attr(
    not(any(target_arch = "aarch64", target_arch = "x86_64")),
    repr(align(64))
)]
// Isolate producer-hot and consumer-hot atomic cursors onto separate cache lines so consumer slot
// releases and producer slot claims/publications do not invalidate each other across cores.
pub(super) struct CachePaddedAtomicU64(AtomicU64);

#[cfg(target_has_atomic = "64")]
impl Deref for CachePaddedAtomicU64 {
    type Target = AtomicU64;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(target_has_atomic = "64")]
#[cfg_attr(any(target_arch = "aarch64", target_arch = "x86_64"), repr(align(128)))]
#[cfg_attr(
    not(any(target_arch = "aarch64", target_arch = "x86_64")),
    repr(align(64))
)]
struct CachePaddedAtomicUsize(AtomicUsize);

#[cfg(target_has_atomic = "64")]
impl Deref for CachePaddedAtomicUsize {
    type Target = AtomicUsize;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

const SLOT_BITS: u32 = 31;
pub(super) const NO_SLOT: u32 = (1u32 << SLOT_BITS) - 1;

#[cfg(target_has_atomic = "64")]
const SLOT_MASK: u64 = (1u64 << SLOT_BITS) - 1;

// Keep `(buffer + 1).next_power_of_two()` representable in `usize` and slot indices below
// `NO_SLOT`; larger capacities transparently fall back to `Mutex<State<T>>`.
#[cfg(target_has_atomic = "64")]
const MAX_SLOT_POOL_BUFFER: usize = (1usize << 30) - 1;

// `free_tail` layout: `[63: SEND_WAITERS_BIT | 62..0: monotonic single-consumer push cursor]`.
#[cfg(target_has_atomic = "64")]
const SEND_WAITERS_BIT: u64 = 1u64 << 63;

// `inbox` layout: `[63: RECV_WAITING_BIT | 62: CLOSED_BIT | 61..31: 31-bit tag | 30..0: head
// slot]`.
#[cfg(target_has_atomic = "64")]
const INBOX_TAG_SHIFT: u32 = 31;
#[cfg(target_has_atomic = "64")]
const INBOX_TAG_MASK: u64 = (1u64 << 31) - 1;
#[cfg(target_has_atomic = "64")]
pub(super) const CLOSED_BIT: u64 = 1u64 << 62;
#[cfg(target_has_atomic = "64")]
pub(super) const RECV_WAITING_BIT: u64 = 1u64 << 63;

#[cfg(target_has_atomic = "64")]
fn can_use_slot_pool<T>(buffer: usize) -> bool {
    if size_of::<T>() == 0 || buffer > MAX_SLOT_POOL_BUFFER {
        return false;
    }
    let slot_count = buffer + 1;
    let Some(free_cap) = slot_count.checked_next_power_of_two() else {
        return false;
    };
    Layout::array::<UnsafeCell<MaybeUninit<T>>>(slot_count).is_ok()
        && Layout::array::<AtomicU32>(free_cap).is_ok()
        && Layout::array::<AtomicU32>(slot_count).is_ok()
}

/// Creates a bounded mpsc channel with room for `buffer` queued messages.
///
/// [`BoundedSender::send`] waits for capacity when the buffer is full. Receiving a message releases
/// one slot for a waiting sender. Capacity is granted in the order that pending `send` and
/// `reserve` operations enter the wait queue; new senders cannot take an already granted slot.
///
/// Message storage is preallocated for `buffer` values. Queued messages and outstanding
/// reservations together occupy at most `buffer` capacity units.
///
/// The `try_*` methods do not wait for capacity or messages, but may briefly block on an internal
/// mutex when coordinating parked tasks or fallback capacities.
///
/// # Panics
///
/// Panics if `buffer` is zero or the preallocated message storage exceeds the allocation size
/// limit.
#[track_caller]
pub fn bounded<T>(buffer: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(
        buffer > 0,
        "mpsc bounded channel capacity {buffer} must be nonzero",
    );

    #[cfg(target_has_atomic = "64")]
    if can_use_slot_pool::<T>(buffer) {
        let slot_count = buffer + 1;
        // Allocate uninitialized slots directly on the heap without constructing a `T`-sized
        // temporary on the caller's stack in unoptimized builds.
        let mut slots: Vec<UnsafeCell<MaybeUninit<T>>> = Vec::with_capacity(slot_count);
        // SAFETY: `UnsafeCell<MaybeUninit<T>>` is `#[repr(transparent)]` over `MaybeUninit<T>`,
        // which requires no initialization, and `with_capacity(slot_count)` allocated at least
        // `slot_count` elements.
        unsafe { slots.set_len(slot_count) };

        // `free_ring` holds at most `buffer < free_cap` unpopped slot indices at any time. Slots
        // `0..buffer` start in `free_ring`, while slot `buffer` starts as the receiver's spare
        // slot.
        let free_cap = slot_count.next_power_of_two();
        let mut free_ring = Vec::with_capacity(free_cap);
        for slot in 0..buffer {
            free_ring.push(AtomicU32::new(slot as u32));
        }
        free_ring.resize_with(free_cap, || AtomicU32::new(NO_SLOT));

        let mut next_link = Vec::with_capacity(slot_count);
        next_link.resize_with(slot_count, || AtomicU32::new(NO_SLOT));

        let initial_tail = buffer as u64;
        let shared = Arc::new(Shared {
            pool: UnsafeCell::new(ManuallyDrop::new(Pool {
                slots: slots.into_boxed_slice(),
                free_ring: free_ring.into_boxed_slice(),
                next_link: next_link.into_boxed_slice(),
            })),
            free_mask: free_cap - 1,
            pool_freed: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            has_cancelled: AtomicBool::new(false),
            senders: AtomicUsize::new(1),
            // `BoundedReceiver` holds the initial `+1` pool lease until it finishes draining on
            // drop.
            pool_refs: CachePaddedAtomicUsize(AtomicUsize::new(1)),
            free_head: CachePaddedAtomicU64(AtomicU64::new(0)),
            cached_free_tail: CachePaddedAtomicU64(AtomicU64::new(initial_tail)),
            free_tail: CachePaddedAtomicU64(AtomicU64::new(initial_tail)),
            inbox: CachePaddedAtomicU64(AtomicU64::new(u64::from(NO_SLOT))),
            state: Mutex::new(State {
                locked_available: 0,
                locked_queue: VecDeque::new(),
                cancelled_head: NO_SLOT,
                receiver: true,
                recv_waker: None,
                send_waiters: WaitList::new(),
            }),
        });
        return (
            BoundedSender::new(shared.clone()),
            BoundedReceiver::new(shared, buffer as u32),
        );
    }

    let locked_queue = VecDeque::with_capacity(buffer);
    let shared = Arc::new(Shared {
        #[cfg(target_has_atomic = "64")]
        pool: UnsafeCell::new(ManuallyDrop::new(Pool {
            slots: Box::new([]),
            free_ring: Box::new([]),
            next_link: Box::new([]),
        })),
        #[cfg(target_has_atomic = "64")]
        free_mask: 0,
        #[cfg(target_has_atomic = "64")]
        pool_freed: AtomicBool::new(true),
        closed: AtomicBool::new(false),
        #[cfg(target_has_atomic = "64")]
        has_cancelled: AtomicBool::new(false),
        senders: AtomicUsize::new(1),
        #[cfg(target_has_atomic = "64")]
        pool_refs: CachePaddedAtomicUsize(AtomicUsize::new(0)),
        #[cfg(target_has_atomic = "64")]
        free_head: CachePaddedAtomicU64(AtomicU64::new(0)),
        #[cfg(target_has_atomic = "64")]
        cached_free_tail: CachePaddedAtomicU64(AtomicU64::new(0)),
        #[cfg(target_has_atomic = "64")]
        free_tail: CachePaddedAtomicU64(AtomicU64::new(0)),
        #[cfg(target_has_atomic = "64")]
        inbox: CachePaddedAtomicU64(AtomicU64::new(u64::from(NO_SLOT))),
        state: Mutex::new(State {
            locked_available: buffer,
            locked_queue,
            #[cfg(target_has_atomic = "64")]
            cancelled_head: NO_SLOT,
            receiver: true,
            recv_waker: None,
            send_waiters: WaitList::new(),
        }),
    });
    (
        BoundedSender::new(shared.clone()),
        BoundedReceiver::new(shared, NO_SLOT),
    )
}

#[cfg(target_has_atomic = "64")]
pub(super) struct Pool<T> {
    pub(super) slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    pub(super) free_ring: Box<[AtomicU32]>,
    pub(super) next_link: Box<[AtomicU32]>,
}

// `free_ring` transfers recycled slots from the single receiver (`free_tail`) to producers
// (`free_head` + `cached_free_tail`) on separate cache lines so consumer releases do not contend
// with producer claims. Cold reservation cancellations synchronize through `state`.
struct Shared<T> {
    #[cfg(target_has_atomic = "64")]
    pool: UnsafeCell<ManuallyDrop<Pool<T>>>,
    #[cfg(target_has_atomic = "64")]
    free_mask: usize,
    #[cfg(target_has_atomic = "64")]
    pool_freed: AtomicBool,
    closed: AtomicBool,
    #[cfg(target_has_atomic = "64")]
    has_cancelled: AtomicBool,
    senders: AtomicUsize,
    #[cfg(target_has_atomic = "64")]
    pool_refs: CachePaddedAtomicUsize,
    #[cfg(target_has_atomic = "64")]
    free_head: CachePaddedAtomicU64,
    #[cfg(target_has_atomic = "64")]
    cached_free_tail: CachePaddedAtomicU64,
    #[cfg(target_has_atomic = "64")]
    free_tail: CachePaddedAtomicU64,
    #[cfg(target_has_atomic = "64")]
    inbox: CachePaddedAtomicU64,
    state: Mutex<State<T>>,
}

#[cfg(target_has_atomic = "64")]
impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        // If `Permit`s were leaked with `mem::forget`, `pool_refs` never reached `0`; reclaim the
        // backing storage once the final `Arc<Shared<T>>` handle drops.
        if !*self.pool_freed.get_mut() {
            *self.pool_freed.get_mut() = true;
            // SAFETY: `&mut self` guarantees exclusive access and `pool_freed` was `false`.
            unsafe { ManuallyDrop::drop(self.pool.get_mut()) };
        }
    }
}

// SAFETY: Every slot index is exclusively owned during reads and writes, `pool` is only accessed
// while holding a live `pool_refs` lease, and values only cross threads by value ownership
// transfer synchronized through Release/Acquire operations on `free_tail`, `free_head`, `inbox`,
// or `state`.
unsafe impl<T: Send> Send for Shared<T> {}
unsafe impl<T: Send> Sync for Shared<T> {}
impl<T> UnwindSafe for Shared<T> {}
impl<T> RefUnwindSafe for Shared<T> {}

impl<T> Shared<T> {
    #[inline]
    pub(super) fn use_slot_pool(&self) -> bool {
        #[cfg(target_has_atomic = "64")]
        {
            size_of::<T>() != 0 && self.free_mask != 0
        }
        #[cfg(not(target_has_atomic = "64"))]
        {
            false
        }
    }

    #[inline]
    pub(super) fn is_closed(&self) -> bool {
        #[cfg(target_has_atomic = "64")]
        if self.use_slot_pool() {
            return (self.inbox.load(Ordering::Acquire) & CLOSED_BIT) != 0;
        }
        self.closed.load(Ordering::Acquire)
    }

    pub(super) fn try_send_locked(&self, value: T) -> Result<(), TrySendError<T>> {
        let wake = {
            let mut state = self.state.lock();
            if !state.receiver {
                return Err(TrySendError::Disconnected(value));
            }
            if state.locked_available == 0 {
                return Err(TrySendError::Full(value));
            }
            state.locked_available -= 1;
            state.locked_queue.push_back(value);
            state.recv_waker.take()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        Ok(())
    }

    /// Borrows `Pool<T>`. The caller must currently hold at least one live `pool_refs` lease.
    #[cfg(target_has_atomic = "64")]
    #[inline]
    pub(super) unsafe fn pool(&self) -> &Pool<T> {
        // SAFETY: The caller holds an active `pool_refs` lease, preventing `leave_pool` from
        // dropping `self.pool` until after the caller releases its lease.
        unsafe { &*self.pool.get() }
    }

    /// Acquires a producer `pool_refs` lease if the channel is still open.
    #[cfg(target_has_atomic = "64")]
    #[inline]
    fn try_enter_pool(&self) -> bool {
        if self.is_closed() {
            return false;
        }
        self.pool_refs.fetch_add(1, Ordering::Acquire);
        // Re-check `CLOSED_BIT` after `fetch_add(Acquire)`: if `BoundedReceiver` already closed
        // and dropped `pool_refs` to `0` before our `fetch_add`, `CLOSED_BIT` is guaranteed to be
        // visible here, so we release our transient count without ever dereferencing `self.pool`.
        if self.is_closed() {
            self.leave_pool();
            return false;
        }
        true
    }

    /// Adds a `pool_refs` lease while holding `state` with `state.receiver == true`.
    #[cfg(target_has_atomic = "64")]
    #[inline]
    pub(super) fn retain_pool_while_open(&self) {
        // `BoundedReceiver` holds its `+1` lease until after setting `state.receiver = false`
        // under `state`, so `pool_refs >= 1` is guaranteed here.
        self.pool_refs.fetch_add(1, Ordering::Relaxed);
    }

    /// Releases one `pool_refs` lease, deallocating `Pool<T>` once the receiver and all in-flight
    /// producer operations/permits have exited.
    #[cfg(target_has_atomic = "64")]
    #[inline]
    pub(super) fn leave_pool(&self) {
        if self.pool_refs.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.deallocate_pool_cold();
        }
    }

    #[cfg(target_has_atomic = "64")]
    #[cold]
    fn deallocate_pool_cold(&self) {
        if !self.pool_freed.swap(true, Ordering::AcqRel) {
            // SAFETY: `pool_refs` can only reach `0` after `BoundedReceiver` set `CLOSED_BIT` and
            // released its lease, and after every pre-close lease holder finished accessing
            // `self.pool` with `AcqRel` synchronization. Any late thread that incremented from `0`
            // observes `CLOSED_BIT` in `try_enter_pool` and never dereferences `self.pool`, while
            // `pool_freed` ensures `ManuallyDrop::drop` runs at most once.
            unsafe { ManuallyDrop::drop(&mut *self.pool.get()) };
        }
    }

    /// Caller must hold a `pool_refs` lease.
    #[cfg(target_has_atomic = "64")]
    #[inline]
    unsafe fn try_pop_free_ring(&self) -> Option<u32> {
        // SAFETY: Caller holds a `pool_refs` lease.
        let pool = unsafe { self.pool() };
        let mut head = self.free_head.load(Ordering::Relaxed);
        let mut tail = self.cached_free_tail.load(Ordering::Acquire);
        loop {
            if head >= tail {
                tail = self.free_tail.load(Ordering::Acquire) & !SEND_WAITERS_BIT;
                if head >= tail {
                    return None;
                }
                self.cached_free_tail.store(tail, Ordering::Release);
            }
            // Read the slot index before advancing `free_head`. `AcqRel` on `free_head` ensures
            // later successful claims acquire this claim before publishing and letting the
            // receiver wrap around to overwrite this ring cell.
            let slot = pool.free_ring[(head as usize) & self.free_mask].load(Ordering::Relaxed);
            match self.free_head.compare_exchange_weak(
                head,
                head + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(slot),
                Err(actual) => head = actual,
            }
        }
    }

    /// Caller must hold `state` with `state.receiver == true` (which guarantees a live receiver
    /// `pool_refs` lease).
    #[cfg(target_has_atomic = "64")]
    fn pop_cancelled_locked(&self, state: &mut State<T>) -> Option<u32> {
        let slot = state.cancelled_head;
        if slot == NO_SLOT {
            return None;
        }
        // SAFETY: `state.receiver == true` under `state` guarantees the receiver's `pool_refs`
        // lease is still active.
        let next = unsafe { self.pool() }.next_link[slot as usize].load(Ordering::Relaxed);
        state.cancelled_head = next;
        if next == NO_SLOT {
            self.has_cancelled.store(false, Ordering::Relaxed);
        }
        Some(slot)
    }

    #[cfg(target_has_atomic = "64")]
    #[cold]
    fn try_pop_cancelled_cold(&self) -> Option<u32> {
        let mut state = self.state.lock();
        if !state.receiver {
            return None;
        }
        self.pop_cancelled_locked(&mut state)
    }

    /// Acquires a slot (along with a `pool_refs` lease owned by the caller on `Ok`).
    #[inline]
    pub(super) fn acquire_slot(&self) -> Result<u32, TrySendError<()>> {
        if !self.use_slot_pool() {
            let mut state = self.state.lock();
            return if !state.receiver {
                Err(TrySendError::Disconnected(()))
            } else if state.locked_available == 0 {
                Err(TrySendError::Full(()))
            } else {
                state.locked_available -= 1;
                Ok(NO_SLOT)
            };
        }

        #[cfg(target_has_atomic = "64")]
        {
            if !self.try_enter_pool() {
                return Err(TrySendError::Disconnected(()));
            }
            // SAFETY: `try_enter_pool` acquired a `pool_refs` lease.
            if let Some(slot) = unsafe { self.try_pop_free_ring() } {
                return Ok(slot);
            }
            if self.has_cancelled.load(Ordering::Acquire) {
                if let Some(slot) = self.try_pop_cancelled_cold() {
                    return Ok(slot);
                }
            }
            let closed = self.is_closed();
            self.leave_pool();
            if closed {
                Err(TrySendError::Disconnected(()))
            } else {
                Err(TrySendError::Full(()))
            }
        }
        #[cfg(not(target_has_atomic = "64"))]
        {
            unreachable!("slot pool requires 64-bit atomics")
        }
    }

    /// Tries to pop a slot while holding `state` with `state.receiver == true`; if empty,
    /// atomically sets `SEND_WAITERS_BIT` on `free_tail` so subsequent consumer or cancellation
    /// releases enter `state`.
    pub(super) fn acquire_or_mark_waiting(&self, state: &mut State<T>) -> Option<u32> {
        #[cfg(target_has_atomic = "64")]
        {
            if let Some(slot) = self.pop_cancelled_locked(state) {
                return Some(slot);
            }
            loop {
                // SAFETY: `state.receiver == true` under `state` guarantees the receiver's
                // `pool_refs` lease is active.
                if let Some(slot) = unsafe { self.try_pop_free_ring() } {
                    return Some(slot);
                }
                let tail = self.free_tail.load(Ordering::Acquire);
                let head = self.free_head.load(Ordering::Acquire);
                if (tail & !SEND_WAITERS_BIT) != head {
                    continue;
                }
                if (tail & SEND_WAITERS_BIT) != 0 {
                    return None;
                }
                if self
                    .free_tail
                    .compare_exchange_weak(
                        tail,
                        tail | SEND_WAITERS_BIT,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    return None;
                }
            }
        }
        #[cfg(not(target_has_atomic = "64"))]
        {
            let _ = state;
            unreachable!("slot pool requires 64-bit atomics")
        }
    }

    /// Releases `slot` from the single `BoundedReceiver` (`&mut self`).
    #[cfg(target_has_atomic = "64")]
    #[inline]
    pub(super) fn release_slot_from_receiver(&self, slot: u32) -> Option<Waker> {
        let tail = self.free_tail.load(Ordering::Relaxed);
        if (tail & SEND_WAITERS_BIT) != 0 {
            return self.release_slot_slow(slot);
        }
        // SAFETY: `BoundedReceiver` holds its `pool_refs` lease throughout `&mut self`.
        let pool = unsafe { self.pool() };
        // Only `BoundedReceiver` (`&mut self`) advances `free_tail`, and at most `buffer <
        // free_cap` unpopped slots can exist, so `free_ring[tail & free_mask]` is vacant.
        // Writing it before advancing `free_tail` makes the slot immediately readable
        // without a publication window.
        pool.free_ring[(tail as usize) & self.free_mask].store(slot, Ordering::Relaxed);
        if self
            .free_tail
            .compare_exchange(tail, tail + 1, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            None
        } else {
            // A producer set `SEND_WAITERS_BIT`; `free_tail`'s index did not advance, so hand
            // `slot` directly to `send_waiters` under `state`.
            self.release_slot_slow(slot)
        }
    }

    /// Releases `slot` from a dropped `Permit` (cold cancellation path) and releases its
    /// `pool_refs` lease if in slot-pool mode.
    #[cold]
    pub(super) fn release_slot_from_permit(&self, slot: u32) -> Option<Waker> {
        let wake = self.release_slot_slow(slot);
        #[cfg(target_has_atomic = "64")]
        if self.use_slot_pool() {
            self.leave_pool();
        }
        wake
    }

    #[cold]
    fn release_slot_slow(&self, slot: u32) -> Option<Waker> {
        let mut state = self.state.lock();
        self.release_slot_locked(&mut state, slot)
    }

    pub(super) fn release_slot_locked(&self, state: &mut State<T>, slot: u32) -> Option<Waker> {
        if !state.receiver {
            return None;
        }
        if !self.use_slot_pool() {
            if let Some((_, waiter)) = state.send_waiters.unlink_first_waiter() {
                let Waiter::Waiting(waker) = mem::replace(waiter, Waiter::Granted(NO_SLOT)) else {
                    unreachable!("only waiting senders remain linked");
                };
                return Some(waker);
            }
            state.locked_available += 1;
            return None;
        }

        #[cfg(target_has_atomic = "64")]
        {
            if let Some((_, waiter)) = state.send_waiters.unlink_first_waiter() {
                let Waiter::Waiting(waker) = mem::replace(waiter, Waiter::Granted(slot)) else {
                    unreachable!("only waiting senders remain linked");
                };
                if state.send_waiters.is_empty() {
                    self.free_tail
                        .fetch_and(!SEND_WAITERS_BIT, Ordering::Release);
                }
                return Some(waker);
            }
            self.free_tail
                .fetch_and(!SEND_WAITERS_BIT, Ordering::Release);
            // SAFETY: `state.receiver == true` under `state` guarantees the receiver's `pool_refs`
            // lease is active.
            unsafe { self.pool() }.next_link[slot as usize]
                .store(state.cancelled_head, Ordering::Relaxed);
            state.cancelled_head = slot;
            self.has_cancelled.store(true, Ordering::Release);
            None
        }
        #[cfg(not(target_has_atomic = "64"))]
        {
            let _ = slot;
            unreachable!("slot pool requires 64-bit atomics")
        }
    }

    pub(super) fn clear_send_waiters_bit_if_empty(&self, state: &State<T>) {
        #[cfg(target_has_atomic = "64")]
        if self.use_slot_pool() && state.send_waiters.is_empty() {
            self.free_tail
                .fetch_and(!SEND_WAITERS_BIT, Ordering::Release);
        }
        #[cfg(not(target_has_atomic = "64"))]
        let _ = state;
    }

    /// Pushes an initialized `slot` onto `inbox`, returning `Err(())` if the receiver closed.
    /// Caller must hold a `pool_refs` lease.
    #[cfg(target_has_atomic = "64")]
    #[inline]
    pub(super) unsafe fn publish_slot(&self, slot: u32) -> Result<Option<Waker>, ()> {
        // SAFETY: Caller holds a `pool_refs` lease.
        let slot_next = &unsafe { self.pool() }.next_link[slot as usize];
        let mut old = self.inbox.load(Ordering::Relaxed);
        let mut stored_head = NO_SLOT.wrapping_sub(1);
        loop {
            if (old & CLOSED_BIT) != 0 {
                return Err(());
            }
            let head = (old & SLOT_MASK) as u32;
            if head != stored_head {
                slot_next.store(head, Ordering::Relaxed);
                stored_head = head;
            }
            let tag = ((old >> INBOX_TAG_SHIFT).wrapping_add(1)) & INBOX_TAG_MASK;
            let new = (tag << INBOX_TAG_SHIFT) | u64::from(slot);
            match self
                .inbox
                .compare_exchange_weak(old, new, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => {
                    let wake = if (old & RECV_WAITING_BIT) != 0 {
                        self.take_recv_waker_cold()
                    } else {
                        None
                    };
                    return Ok(wake);
                }
                Err(actual) => old = actual,
            }
        }
    }

    #[cfg(target_has_atomic = "64")]
    #[cold]
    fn take_recv_waker_cold(&self) -> Option<Waker> {
        let mut state = self.state.lock();
        let wake = state.recv_waker.take();
        if wake.is_some() {
            self.inbox.fetch_and(!RECV_WAITING_BIT, Ordering::Release);
        }
        wake
    }

    /// Detaches the entire `inbox` LIFO chain, leaving `NO_SLOT` in its place.
    #[cfg(target_has_atomic = "64")]
    #[inline]
    pub(super) fn detach_inbox(&self) -> u32 {
        let mut old = self.inbox.load(Ordering::Acquire);
        loop {
            let head = (old & SLOT_MASK) as u32;
            if head == NO_SLOT {
                return NO_SLOT;
            }
            let tag = ((old >> INBOX_TAG_SHIFT).wrapping_add(1)) & INBOX_TAG_MASK;
            let new = (old & CLOSED_BIT) | (tag << INBOX_TAG_SHIFT) | u64::from(NO_SLOT);
            match self
                .inbox
                .compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return head,
                Err(actual) => old = actual,
            }
        }
    }

    /// Reverses a non-empty LIFO chain in `next_link` in place, returning the FIFO head.
    /// Caller must hold a `pool_refs` lease.
    #[cfg(target_has_atomic = "64")]
    #[inline]
    pub(super) unsafe fn reverse_msg_chain(&self, lifo_head: u32) -> u32 {
        // SAFETY: Caller holds a `pool_refs` lease.
        let next_link = &unsafe { self.pool() }.next_link;
        let mut prev = NO_SLOT;
        let mut cur = lifo_head;
        while cur != NO_SLOT {
            let slot_next = &next_link[cur as usize];
            let next = slot_next.load(Ordering::Relaxed);
            slot_next.store(prev, Ordering::Relaxed);
            prev = cur;
            cur = next;
        }
        prev
    }

    /// Writes `value` into an uninitialized slot exclusively owned by the caller.
    #[cfg(target_has_atomic = "64")]
    #[inline]
    pub(super) unsafe fn write_slot(&self, slot: u32, value: T) {
        // SAFETY: The caller holds a `pool_refs` lease and exclusive ownership of `slot`, which is
        // currently uninitialized.
        unsafe { (*self.pool().slots[slot as usize].get()).write(value) };
    }

    /// Reads and takes ownership of the initialized value in `slot`, leaving it uninitialized.
    #[cfg(target_has_atomic = "64")]
    #[inline]
    pub(super) unsafe fn read_slot(&self, slot: u32) -> T {
        // SAFETY: The caller holds a `pool_refs` lease and exclusive ownership of `slot`, which
        // contains an initialized `T`.
        unsafe { (*self.pool().slots[slot as usize].get()).assume_init_read() }
    }

    /// Drops the initialized value in `slot` in place, leaving it uninitialized.
    #[cfg(target_has_atomic = "64")]
    #[inline]
    pub(super) unsafe fn drop_slot(&self, slot: u32) {
        // SAFETY: The caller holds a `pool_refs` lease and exclusive ownership of `slot`, which
        // contains an initialized `T`.
        unsafe { (*self.pool().slots[slot as usize].get()).assume_init_drop() };
    }
}

pub(super) struct State<T> {
    pub(super) locked_available: usize,
    pub(super) locked_queue: VecDeque<T>,
    #[cfg(target_has_atomic = "64")]
    pub(super) cancelled_head: u32,
    // True while the receiving endpoint is alive.
    pub(super) receiver: bool,
    pub(super) recv_waker: Option<Waker>,
    pub(super) send_waiters: WaitList<Waiter>,
}

pub(super) enum Waiter {
    Waiting(Waker),
    Granted(u32),
}

#[cfg(target_has_atomic = "64")]
#[inline]
pub(super) const fn inbox_head(word: u64) -> u32 {
    (word & SLOT_MASK) as u32
}

#[cfg(all(test, target_has_atomic = "64"))]
mod tests {
    use super::bounded;

    #[test]
    fn dropping_receiver_releases_slot_pool_while_idle_senders_remain() {
        let (tx, mut rx) = bounded::<[u8; 1024]>(64);
        let held_permit = tx.try_reserve().unwrap();
        tx.try_send([7; 1024]).unwrap();
        assert_eq!(rx.try_recv().unwrap()[0], 7);
        assert!(
            !tx.shared
                .pool_freed
                .load(std::sync::atomic::Ordering::Acquire)
        );

        drop(rx);
        // `held_permit` still holds one lease until it finishes or drops.
        assert!(
            !tx.shared
                .pool_freed
                .load(std::sync::atomic::Ordering::Acquire)
        );
        assert!(held_permit.send([9; 1024]).is_err());
        assert!(
            tx.shared
                .pool_freed
                .load(std::sync::atomic::Ordering::Acquire)
        );
        assert!(tx.is_disconnected());
    }
}
