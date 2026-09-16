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

//! Slot log, waiters, and the receive step shared by the bounded and unbounded SPMC broadcast
//! channels.
//!
//! This is family-private retention and sequencing machinery. The public retention contract is
//! the same as [`crate::broadcast::mpmc`]: lossless fan-out, bounded wait-at-capacity or unbounded
//! growth, and new subscriptions joining at the committed tail. Ring slots, chunk storage, and the
//! `head` / `tail` sequencer stay in this module.
//!
//! The producer is unique (`send` takes `&mut self`). Receivers drain already-published slots
//! without taking the waiter mutex: each slot carries a remaining-reader count, and each
//! subscription keeps its cursor locally. The mutex covers publication, subscribe/unsubscribe, and
//! parking, so a slot's remaining-reader count always matches its consumers and a parking receiver
//! cannot miss a publication.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::error::RecvError;
use super::error::TryRecvError;
use crate::internal::mutex::Mutex;
use crate::internal::wake_all;
use crate::internal::wakerset::WakerSet;
use crate::internal::wakerset::WakerToken;

/// Number of slots in one unbounded log chunk.
pub(super) const CHUNK_LEN: usize = 256;

/// A received value together with whether this receive freed a retained slot.
///
/// A bounded channel wakes the producer from `reclaimed` before it returns `value`, so a panicking
/// `T::clone` cannot strand a waiter on capacity this receive already released.
pub(super) struct Consumed<T> {
    pub value: T,
    pub reclaimed: bool,
}

/// Messages removed from the shared log and waiting to be dropped after waiters are unlocked.
///
/// Keeping the first message out of the `Vec` avoids a heap allocation on the common path where
/// one receive reclaims exactly one message.
pub(super) struct Reclaimed<T> {
    first: Option<T>,
    rest: Vec<T>,
}

impl<T> Reclaimed<T> {
    fn empty() -> Self {
        Self {
            first: None,
            rest: vec![],
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.first.is_none()
    }

    fn push(&mut self, msg: T) {
        if self.first.is_none() {
            self.first = Some(msg);
        } else {
            self.rest.push(msg);
        }
    }
}

/// One published value and the number of subscriptions that still have to consume it.
pub(super) struct Slot<T> {
    msg: UnsafeCell<MaybeUninit<T>>,
    remaining: AtomicUsize,
    /// `true` once the producer has written `msg` and until the last remaining reader takes it.
    ///
    /// `take_msg` clears it before `head` moves past the slot, so the producer cannot reuse the
    /// memory while a reader is still cloning `T`. Teardown also reads it to find leftovers.
    occupied: AtomicBool,
}

// SAFETY: The producer writes `msg` before publishing `tail`. Readers clone `T` only for versions
// they were counted in. The last remaining reader takes `msg` before clearing `occupied` and
// advancing `head`, which is what lets the producer reuse the slot.
unsafe impl<T: Send + Sync> Sync for Slot<T> {}

impl<T> Slot<T> {
    fn empty() -> Self {
        Self {
            msg: UnsafeCell::new(MaybeUninit::uninit()),
            remaining: AtomicUsize::new(0),
            occupied: AtomicBool::new(false),
        }
    }

    /// Writes `msg` for `n` receivers.
    ///
    /// The caller must publish `tail` with `Release` after this returns, and must be the unique
    /// producer for this slot.
    ///
    /// # Safety
    ///
    /// `occupied` must be `false`. `n` must be the number of subscriptions that will consume this
    /// version, and must be greater than zero.
    pub(super) unsafe fn write(&self, msg: T, n: usize) {
        debug_assert!(n > 0);
        debug_assert!(!self.occupied.load(Ordering::Relaxed));
        unsafe {
            (*self.msg.get()).write(msg);
        }
        self.remaining.store(n, Ordering::Relaxed);
        self.occupied.store(true, Ordering::Release);
    }

    /// Clones the published value.
    ///
    /// # Safety
    ///
    /// This slot must currently hold a published message the caller is allowed to read.
    unsafe fn clone_msg(&self) -> T
    where
        T: Clone,
    {
        unsafe { (*self.msg.get()).assume_init_ref().clone() }
    }

    /// Takes the slot's value after the last remaining reader has consumed it.
    ///
    /// # Safety
    ///
    /// The caller must be the last remaining consumer of this slot.
    unsafe fn take_msg(&self) -> T {
        let msg = unsafe { (*self.msg.get()).assume_init_read() };
        self.occupied.store(false, Ordering::Release);
        msg
    }
}

/// Parked receivers, the live subscription count, and the single parked producer.
pub(super) struct State {
    pub waiters: WakerSet,
    pub receiver_count: usize,
    pub producer: Option<Waker>,
}

/// Shared channel state: the slot log plus the waiter mutex.
pub(super) struct Shared<B> {
    pub buffer: B,
    pub head: AtomicU64,
    pub tail: AtomicU64,
    pub senders: AtomicUsize,
    pub producer_waiting: AtomicUsize,
    pub state: Mutex<State>,
}

impl<B> Shared<B> {
    pub(super) fn new(buffer: B) -> Self {
        Self {
            buffer,
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
            senders: AtomicUsize::new(1),
            producer_waiting: AtomicUsize::new(0),
            state: Mutex::new(State {
                waiters: WakerSet::new(),
                receiver_count: 1,
                producer: None,
            }),
        }
    }

    pub(super) fn retained(&self) -> usize {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);
        usize::try_from(tail - head).expect("retained broadcast message count exceeds usize")
    }

    pub(super) fn unread(&self, cursor: u64) -> usize {
        let tail = self.tail.load(Ordering::Acquire);
        debug_assert!(tail >= cursor);
        usize::try_from(tail - cursor).expect("unread broadcast message count exceeds usize")
    }

    /// Next committed version, panicking on overflow.
    pub(super) fn next_tail(tail: u64) -> u64 {
        tail.checked_add(1)
            .expect("broadcast channel version counter overflowed")
    }

    /// Registers a new subscription at the committed tail.
    ///
    /// Publication holds the same lock, so the returned cursor is this subscription's first
    /// counted version.
    pub(super) fn subscribe(&self) -> u64 {
        let mut state = self.state.lock();
        state.receiver_count += 1;
        self.tail.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn set_tail(&self, tail: u64) {
        self.tail.store(tail, Ordering::Relaxed);
        self.head.store(tail, Ordering::Relaxed);
    }
}

/// A random-access published slot, addressed by its committed version.
pub(super) trait SlotStore<T> {
    fn slot(&self, version: u64) -> &Slot<T>;

    /// Moves the lookup start past storage the live window has left behind, releasing it.
    ///
    /// Called after `head` advances. A fixed ring has nothing to release; the unbounded log frees
    /// the storage of chunks now entirely below `head`, keeping both `slot` and the footprint
    /// proportional to the live window rather than to the lifetime message count.
    fn sync_head(&self, _head: u64) {}
}

/// Fixed ring used by the bounded channel.
pub(super) struct BoundedBuffer<T> {
    slots: Box<[Slot<T>]>,
    pub cap: usize,
}

impl<T> BoundedBuffer<T> {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            slots: (0..capacity).map(|_| Slot::empty()).collect(),
            cap: capacity,
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.slots.len()
    }
}

impl<T> Drop for BoundedBuffer<T> {
    fn drop(&mut self) {
        for slot in self.slots.iter() {
            if slot.occupied.load(Ordering::Relaxed) {
                drop(unsafe { slot.take_msg() });
            }
        }
    }
}

impl<T> SlotStore<T> for BoundedBuffer<T> {
    #[inline]
    fn slot(&self, version: u64) -> &Slot<T> {
        &self.slots[(version % self.cap as u64) as usize]
    }
}

/// One growable segment of the unbounded log.
///
/// The header is split from the storage. A lookup walks the `next` chain from
/// [`UnboundedBuffer::head_chunk`], possibly through segments the live window has left behind, so
/// headers live until the channel is dropped; `slots` is released once `head` passes the segment.
pub(super) struct Chunk<T> {
    slots: AtomicPtr<[Slot<T>; CHUNK_LEN]>,
    next: AtomicPtr<Chunk<T>>,
    base: u64,
}

impl<T> Chunk<T> {
    fn new(base: u64) -> Box<Self> {
        let slots = Box::into_raw(Box::new(std::array::from_fn(|_| Slot::empty())));
        Box::new(Self {
            slots: AtomicPtr::new(slots),
            next: AtomicPtr::new(ptr::null_mut()),
            base,
        })
    }

    /// Returns this segment's slot for `version`.
    ///
    /// # Safety
    ///
    /// `version` must belong to this segment and be at or above the channel's `head`, so the
    /// storage cannot have been released.
    #[inline]
    unsafe fn slot(&self, version: u64) -> &Slot<T> {
        let slots = self.slots.load(Ordering::Acquire);
        debug_assert!(!slots.is_null());
        unsafe { &(*slots)[(version - self.base) as usize] }
    }

    /// Releases this segment's message storage, dropping any message it still holds.
    ///
    /// Concurrent callers race on one swap, so the storage is freed exactly once. Only teardown
    /// finds a message here: `head` passes a slot only after its value was taken, so
    /// [`UnboundedBuffer::release_consumed_chunks`] never runs `T::drop`.
    fn release(&self) {
        let slots = self.slots.swap(ptr::null_mut(), Ordering::AcqRel);
        if slots.is_null() {
            return;
        }
        let slots = unsafe { Box::from_raw(slots) };
        for slot in slots.iter() {
            if slot.occupied.load(Ordering::Relaxed) {
                drop(unsafe { slot.take_msg() });
            }
        }
    }
}

/// Linked chunks used by the unbounded channel.
///
/// The producer appends chunks without moving earlier slots, so receivers can drain without a
/// publication lock. Storage is released as `head` advances past a chunk; the headers stay
/// allocated until the channel is dropped.
pub(super) struct UnboundedBuffer<T> {
    /// First chunk ever allocated. Never moves; `Drop` walks from here.
    root: AtomicPtr<Chunk<T>>,
    /// First chunk that may still hold a live message. Lookup starts here.
    head_chunk: AtomicPtr<Chunk<T>>,
    tail_chunk: AtomicPtr<Chunk<T>>,
    _marker: std::marker::PhantomData<Slot<T>>,
}

impl<T> UnboundedBuffer<T> {
    pub(super) fn new() -> Self {
        let chunk = Box::into_raw(Chunk::new(0));
        Self {
            root: AtomicPtr::new(chunk),
            head_chunk: AtomicPtr::new(chunk),
            tail_chunk: AtomicPtr::new(chunk),
            _marker: std::marker::PhantomData,
        }
    }

    /// Ensures the chunk that holds `version` exists and returns that slot.
    ///
    /// The caller is the unique producer and must not publish `tail` past this version until this
    /// returns. `version` is at or above `tail`, so its chunk still owns its storage.
    pub(super) fn slot_for_publish(&self, version: u64) -> &Slot<T> {
        loop {
            let chunk = self.tail_chunk.load(Ordering::Acquire);
            debug_assert!(!chunk.is_null());
            let current = unsafe { &*chunk };
            if version < current.base + CHUNK_LEN as u64 {
                debug_assert!(version >= current.base);
                return unsafe { current.slot(version) };
            }

            let next_base = current.base + CHUNK_LEN as u64;
            let raw = Box::into_raw(Chunk::new(next_base));
            current.next.store(raw, Ordering::Release);
            self.tail_chunk.store(raw, Ordering::Release);
        }
    }

    /// Releases the message storage of every chunk the live window has left behind.
    ///
    /// A receive only addresses versions at or above `head`, so a chunk entirely below it can
    /// release its storage even while receivers walk past its header. Freeing the header instead
    /// would race with that walk.
    fn release_consumed_chunks(&self, head: u64) {
        loop {
            let chunk = self.head_chunk.load(Ordering::Acquire);
            debug_assert!(!chunk.is_null());
            let current = unsafe { &*chunk };
            let next = current.next.load(Ordering::Acquire);
            if next.is_null() || current.base + CHUNK_LEN as u64 > head {
                return;
            }
            current.release();
            self.head_chunk.store(next, Ordering::Release);
        }
    }

    /// The number of message slots this buffer currently keeps allocated.
    #[cfg(test)]
    pub(super) fn allocated_slots(&self) -> usize {
        let mut n = 0;
        let mut chunk = self.root.load(Ordering::Acquire);
        while !chunk.is_null() {
            let current = unsafe { &*chunk };
            if !current.slots.load(Ordering::Acquire).is_null() {
                n += CHUNK_LEN;
            }
            chunk = current.next.load(Ordering::Acquire);
        }
        n
    }
}

impl<T> Drop for UnboundedBuffer<T> {
    fn drop(&mut self) {
        let mut chunk = self.root.load(Ordering::Relaxed);
        while !chunk.is_null() {
            let boxed = unsafe { Box::from_raw(chunk) };
            boxed.release();
            chunk = boxed.next.load(Ordering::Relaxed);
        }
    }
}

impl<T> SlotStore<T> for UnboundedBuffer<T> {
    #[inline]
    fn slot(&self, version: u64) -> &Slot<T> {
        let mut chunk = self.head_chunk.load(Ordering::Acquire);
        loop {
            debug_assert!(!chunk.is_null());
            let current = unsafe { &*chunk };
            if version < current.base + CHUNK_LEN as u64 {
                debug_assert!(version >= current.base);
                return unsafe { current.slot(version) };
            }
            chunk = current.next.load(Ordering::Acquire);
        }
    }

    fn sync_head(&self, head: u64) {
        self.release_consumed_chunks(head);
    }
}

/// Advances `head` past `version`, whose value the caller has just taken.
///
/// Slots are released in version order: a subscription consumes in cursor order and a dropped one
/// reclaims in increasing order, so `remaining` can only reach zero at `version` once it has at
/// every earlier version. One `fetch_max` therefore does what a scan over the log would, and no
/// caller has to address a version the live window may already have passed.
fn release_head<T, B: SlotStore<T>>(shared: &Shared<B>, version: u64) {
    let next = version + 1;
    let head = shared.head.fetch_max(next, Ordering::AcqRel).max(next);
    shared.buffer.sync_head(head);
}

/// Consumes the message at `cursor` and advances the cursor.
///
/// A subscription that is the last remaining reader takes the slot value without cloning. Any
/// other reader clones `T` with the waiter mutex not held. The cursor advances before that clone
/// so a panicking `T::clone` still consumes the subscription's claim; a drop guard releases the
/// remaining count if the clone unwinds.
pub(super) fn consume<T: Clone, B: SlotStore<T>>(
    shared: &Shared<B>,
    cursor: &mut u64,
) -> Consumed<T> {
    let version = *cursor;
    debug_assert!(version < shared.tail.load(Ordering::Acquire));
    let slot = shared.buffer.slot(version);
    *cursor = version + 1;

    if slot.remaining.load(Ordering::Acquire) == 1 {
        let value = unsafe { slot.take_msg() };
        slot.remaining.store(0, Ordering::Release);
        release_head(shared, version);
        return Consumed {
            value,
            reclaimed: true,
        };
    }

    struct RemainingGuard<'a, T, B: SlotStore<T>> {
        slot: &'a Slot<T>,
        shared: &'a Shared<B>,
        version: u64,
        armed: bool,
    }

    impl<T, B: SlotStore<T>> Drop for RemainingGuard<'_, T, B> {
        fn drop(&mut self) {
            if !self.armed {
                return;
            }
            if self.slot.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                drop(unsafe { self.slot.take_msg() });
                release_head(self.shared, self.version);
                // Another reader may have advanced past this slot while the clone was running, so
                // the panicking receive can be the one that frees capacity. The caller is
                // unwinding and will not wake anyone, so wake from here.
                wake_producer(take_producer_on_reclaim(self.shared, true));
            }
        }
    }

    let mut guard = RemainingGuard {
        slot,
        shared,
        version,
        armed: true,
    };
    let value = unsafe { slot.clone_msg() };
    let last = slot.remaining.fetch_sub(1, Ordering::AcqRel) == 1;
    guard.armed = false;
    if last {
        drop(unsafe { slot.take_msg() });
        release_head(shared, version);
    }
    Consumed {
        value,
        reclaimed: last,
    }
}

fn reclaim_range<T, B: SlotStore<T>>(shared: &Shared<B>, start: u64, end: u64) -> Reclaimed<T> {
    let mut reclaimed = Reclaimed::empty();
    let mut released = None;
    let mut version = start;
    while version < end {
        let slot = shared.buffer.slot(version);
        if slot.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            reclaimed.push(unsafe { slot.take_msg() });
            released = Some(version);
        }
        version += 1;
    }
    if let Some(version) = released {
        release_head(shared, version);
    }
    reclaimed
}

/// Takes the parked producer if a reclaim may have freed capacity.
///
/// Skipping the lock keeps the common receive off the waiter mutex, but this reclaim stored `head`
/// before loading `producer_waiting` while a parking producer does the opposite. Release/acquire
/// does not order a store against a later load of another location, so without the fence — and its
/// counterpart in the send path — both sides can read stale values and the producer parks on
/// capacity this reclaim already released.
pub(super) fn take_producer_on_reclaim<B>(shared: &Shared<B>, reclaimed: bool) -> Option<Waker> {
    if !reclaimed {
        return None;
    }

    fence(Ordering::SeqCst);
    if shared.producer_waiting.load(Ordering::Acquire) == 0 {
        return None;
    }

    let mut state = shared.state.lock();
    state.producer.take()
}

/// Wakes the parked producer, if any, with the channel already unlocked.
pub(super) fn wake_producer(producer: Option<Waker>) {
    if let Some(waker) = producer {
        waker.wake();
    }
}

/// Drops a subscription, reclaiming every unread slot it still held.
pub(super) fn drop_subscription<T, B: SlotStore<T>>(
    shared: &Shared<B>,
    cursor: u64,
) -> (Reclaimed<T>, Option<Waker>) {
    let mut state = shared.state.lock();
    state.receiver_count -= 1;
    let last = state.receiver_count == 0;
    let tail = shared.tail.load(Ordering::Acquire);

    if last {
        // Take slot values before the producer can observe `receiver_count == 0` and reuse them.
        let reclaimed = reclaim_range(shared, cursor, tail);
        shared.producer_waiting.store(0, Ordering::Release);
        let producer = state.producer.take();
        drop(state);
        return (reclaimed, producer);
    }
    drop(state);

    let reclaimed = reclaim_range(shared, cursor, tail);
    let producer = take_producer_on_reclaim(shared, !reclaimed.is_empty());
    (reclaimed, producer)
}

/// Wakes every parked receiver so it can observe the channel's disconnected state.
pub(super) fn disconnect<B>(shared: &Shared<B>) {
    let wakers = {
        let mut state = shared.state.lock();
        state.waiters.take_all()
    };
    wake_all(wakers);
}

/// Releases a cancelled receive's waker registration, dropping the waker unlocked.
pub(super) fn unregister<B>(shared: &Shared<B>, cursor: u64, token: &mut Option<WakerToken>) {
    let mut state = shared.state.lock();
    if cursor < shared.tail.load(Ordering::Relaxed) || shared.senders.load(Ordering::Acquire) == 0 {
        *token = None;
        return;
    }

    let waker = state.waiters.unregister(token);
    drop(state);
    drop(waker);
}

/// Receives without waiting.
pub(super) fn try_receive<T: Clone, B: SlotStore<T>>(
    shared: &Shared<B>,
    cursor: &mut u64,
) -> Result<Consumed<T>, TryRecvError> {
    if *cursor < shared.tail.load(Ordering::Acquire) {
        return Ok(consume(shared, cursor));
    }
    if shared.senders.load(Ordering::Acquire) == 0 {
        Err(TryRecvError::Disconnected)
    } else {
        Err(TryRecvError::Empty)
    }
}

/// The one poll step behind `recv` on both channels.
///
/// The ready path does not take the waiter mutex. Parking takes it, and so do publication and the
/// sender's disconnect, so a registration made here is visible to whichever happens next.
pub(super) fn poll_receive<T: Clone, B: SlotStore<T>>(
    shared: &Shared<B>,
    cursor: &mut u64,
    token: &mut Option<WakerToken>,
    cx: &mut Context<'_>,
) -> Poll<Result<Consumed<T>, RecvError>> {
    if *cursor < shared.tail.load(Ordering::Acquire) {
        *token = None;
        return Poll::Ready(Ok(consume(shared, cursor)));
    }

    let mut state = shared.state.lock();
    if *cursor < shared.tail.load(Ordering::Acquire) {
        *token = None;
        drop(state);
        return Poll::Ready(Ok(consume(shared, cursor)));
    }
    if shared.senders.load(Ordering::Acquire) == 0 {
        *token = None;
        return Poll::Ready(Err(RecvError::Disconnected));
    }

    // A disconnect racing this registration still wakes it: the sender clears `senders` before
    // taking this lock to drain the wait set.
    let retired_waker = state.waiters.register(token, cx.waker());
    drop(state);
    drop(retired_waker);
    Poll::Pending
}

/// Publishes `tail` after writing a slot.
pub(super) fn commit_publish(tail: &AtomicU64, next: u64) {
    tail.store(next, Ordering::Release);
}

/// Advances `head` and `tail` together when nothing can read the message.
///
/// `tail` moves first so a concurrent `retained` never observes `head` ahead of it. `fetch_max`
/// keeps `head` monotonic against a reclaim that is releasing slots at the same time.
pub(super) fn commit_discard(head: &AtomicU64, tail: &AtomicU64, next: u64) {
    tail.store(next, Ordering::Release);
    head.fetch_max(next, Ordering::AcqRel);
}
