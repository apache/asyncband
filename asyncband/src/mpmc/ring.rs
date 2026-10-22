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

// Portions of the slot-stamp algorithm are adapted from the array flavor of crossbeam-channel
// 0.5.17, which implements Dmitry Vyukov's bounded MPMC queue, and the backoff helper follows
// crossbeam-utils 0.8.23.
// Copyright (c) 2019 The Crossbeam Project Developers
// Asyncband uses the upstream project's Apache-2.0 license option for the incorporated portions.
// Asyncband adds reservations that hold capacity without occupying a slot, grants capacity to
// waiting senders in queue order, and parks receivers in its own wait list.
// Upstream sources:
// https://github.com/crossbeam-rs/crossbeam/blob/2920c984290229ab4e0ca0452ef09e48a82063f3/crossbeam-channel/src/flavors/array.rs
// https://github.com/crossbeam-rs/crossbeam/blob/2920c984290229ab4e0ca0452ef09e48a82063f3/crossbeam-utils/src/backoff.rs

use std::cell::UnsafeCell;
use std::fmt;
use std::future::poll_fn;
use std::hint;
use std::marker::PhantomPinned;
use std::mem;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread;

use crate::internal::atomic_u64::AtomicU64;
use crate::internal::cache_padded::CachePadded;
use crate::internal::mutex::Mutex;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::waker_batch::WakerBatch;
use crate::mpmc::RecvError;
use crate::mpmc::SendError;
use crate::mpmc::TryRecvError;
use crate::mpmc::TrySendError;
use crate::mpmc::waiters::RecvWaiter;
use crate::mpmc::waiters::remove_waiter;

// Positions are 64 bits on every target. Targets without native 64-bit atomics protect each cursor
// and stamp with a lock instead, which is slower but keeps the same algorithm.
type AtomicIndex = AtomicU64;
type Index = u64;

/// The largest capacity whose tail keeps enough lap bits that an exchange prepared from an earlier
/// tail can succeed only after at least 2^40 sends.
const MAX_CAPACITY: usize = (1 << 21) - 1;

/// The bounded queue's shared state.
///
/// Values move through a ring of stamped slots without a lock. A position packs a slot index into
/// its low bits and a lap count into its high bits. A slot's stamp equals the position that may
/// write it next, or that position plus one once its value is published. Senders advance `tail` and
/// receivers advance `head`, so each cursor is written by one side only.
///
/// Between the index and the lap, `tail` carries `mark_bit` once either side disconnects, the
/// capacity that permits and grants to waiting senders hold without a slot, and `queued_bit` while
/// senders wait. Every exchange on `tail` therefore compares the reservation state it was computed
/// from. The other values it depends on, `head` and the slot stamps, only advance, so reading them
/// early can only understate free capacity. An exchange prepared from an earlier tail succeeds only
/// once the lap count wraps, which takes 2^54 sends at a capacity of 64 and at least 2^40 sends up
/// to `MAX_CAPACITY`.
pub struct Shared<T> {
    head: CachePadded<AtomicIndex>,
    tail: CachePadded<AtomicIndex>,
    send_waiters: CachePadded<SendWaiters>,
    recv_waiters: CachePadded<RecvWaiters>,
    slots: Box<[Slot<T>]>,
    mark_bit: Index,
    /// The shift of the reserved count, which fills the bits between `mark_bit` and `queued_bit`.
    count_shift: u32,
    queued_bit: Index,
    one_lap: Index,
    sender_count: AtomicUsize,
    receiver_count: AtomicUsize,
}

// SAFETY: Values move into and out of slots by value and are never shared by reference. Each stamp
// hands its slot to exactly one sender or receiver at a time, so sharing the queue only requires
// `T: Send`.
unsafe impl<T: Send> Sync for Shared<T> {}

struct Slot<T> {
    stamp: AtomicIndex,
    value: UnsafeCell<MaybeUninit<T>>,
}

struct SendWaiters {
    /// Mirrors `queued_bit`, so receivers check for queued senders without loading `tail`, which
    /// senders write on every send.
    queued: AtomicBool,
    /// Counts sends with a reservation that began while senders were queued and have not yet
    /// checked for capacity to grant. Receivers leave grants to these sends.
    handoffs: AtomicUsize,
    state: Mutex<SendState>,
}

struct SendState {
    waiters: WaitList<SendWaiter>,
    /// Set when the last receiver disconnects. The waiter storage is replaced then, so earlier
    /// waiter IDs no longer refer to it.
    closed: bool,
}

/// A sender waiting for capacity. A grant removes the node and sets the flag it points to.
struct SendWaiter {
    waker: Waker,
    /// Points into the pinned `Reserve` that owns this node. It is written only under the
    /// wait-queue lock, and the `Reserve` removes its node under that lock before it is dropped.
    granted: NonNull<AtomicBool>,
}

// SAFETY: `granted` is dereferenced only under the wait-queue lock while its `Reserve` is alive,
// and `AtomicBool` is `Sync`.
unsafe impl Send for SendWaiter {}

struct RecvWaiters {
    /// Whether a receiver is linked in `state`, so senders can skip the lock otherwise.
    linked: AtomicBool,
    state: Mutex<RecvState>,
}

struct RecvState {
    waiters: WaitList<RecvWaiter>,
    /// Set when the last sender disconnects. The waiter storage is replaced then, so earlier
    /// waiter IDs no longer refer to it.
    closed: bool,
}

/// Why a send without a reservation did not claim a slot.
enum Unclaimed<T> {
    /// No unreserved capacity remains, or queued senders own the capacity that frees next.
    Full(T),
    Disconnected(T),
}

/// What a receiver would find without taking a value.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Peek {
    /// A value is published, or a sender has claimed a slot and is writing it.
    Ready,
    Empty,
    Disconnected,
}

/// Returns `mark_bit`, the unit of the reserved count, `queued_bit`, and `one_lap` for a capacity.
///
/// From the low bits up, a tail holds the slot index, `mark_bit`, a reserved count as wide as the
/// index, `queued_bit`, and the lap count.
fn tail_layout(capacity: usize) -> (Index, Index, Index, Index) {
    assert!(
        capacity <= MAX_CAPACITY,
        "mpmc bounded queue capacity is too large"
    );
    let mark_bit = (capacity as Index + 1).next_power_of_two();
    let count_unit = mark_bit << 1;
    let queued_bit = count_unit * mark_bit;
    (mark_bit, count_unit, queued_bit, queued_bit << 1)
}

impl<T> Shared<T> {
    pub fn new(capacity: usize) -> Self {
        let (mark_bit, count_unit, queued_bit, one_lap) = tail_layout(capacity);
        let slots = (0..capacity)
            .map(|index| Slot {
                stamp: AtomicIndex::new(index as Index),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        Self {
            head: CachePadded::new(AtomicIndex::new(0)),
            tail: CachePadded::new(AtomicIndex::new(0)),
            send_waiters: CachePadded::new(SendWaiters {
                queued: AtomicBool::new(false),
                handoffs: AtomicUsize::new(0),
                state: Mutex::new(SendState {
                    waiters: WaitList::new(),
                    closed: false,
                }),
            }),
            recv_waiters: CachePadded::new(RecvWaiters {
                linked: AtomicBool::new(false),
                state: Mutex::new(RecvState {
                    waiters: WaitList::new(),
                    closed: false,
                }),
            }),
            slots,
            mark_bit,
            count_shift: count_unit.trailing_zeros(),
            queued_bit,
            one_lap,
            sender_count: AtomicUsize::new(1),
            receiver_count: AtomicUsize::new(1),
        }
    }

    pub fn clone_sender(&self) {
        self.sender_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn drop_sender(&self) {
        if self.sender_count.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        self.tail.fetch_or(self.mark_bit, Ordering::SeqCst);
        let mut waiters = {
            let mut state = self.recv_waiters.state.lock();
            state.closed = true;
            self.recv_waiters.linked.store(false, Ordering::SeqCst);
            // Disconnection invalidates every receiver waiter ID. Move the storage out so both
            // notification and reclamation happen without holding the lock.
            mem::replace(&mut state.waiters, WaitList::new())
        };
        while let Some(waker) = waiters.notify_one() {
            waker.wake();
        }
    }

    pub fn clone_receiver(&self) {
        self.receiver_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn drop_receiver(&self) {
        // AcqRel orders every other receiver's slot accesses before the discard below.
        if self.receiver_count.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        let tail = self.tail.fetch_or(self.mark_bit, Ordering::SeqCst);
        let mut waiters = {
            let mut state = self.send_waiters.state.lock();
            state.closed = true;
            // As in `drop_sender`, waiter IDs no longer refer to the replaced storage.
            mem::replace(&mut state.waiters, WaitList::new())
        };
        // Notify blocked senders before destroying buffered values, whose destructors may panic.
        // Granted senders were already woken, and their nodes removed.
        while let Some((id, _)) = waiters.unlink_first_waiter(|_| true) {
            waiters.remove_unlinked_waiter(id).waker.wake();
        }
        drop(waiters);
        self.discard_values(self.position(tail));
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        match self.push_unreserved(value) {
            Ok(()) => {
                self.notify_receiver();
                Ok(())
            }
            Err(Unclaimed::Full(value)) => Err(TrySendError::Full(value)),
            Err(Unclaimed::Disconnected(value)) => Err(TrySendError::Disconnected(value)),
        }
    }

    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        let value = match self.try_send(value) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Disconnected(value)) => return Err(SendError::new(value)),
            Err(TrySendError::Full(value)) => value,
        };
        match self.reserve().await {
            Ok(permit) => permit.send(value),
            Err(_) => Err(SendError::new(value)),
        }
    }

    pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        self.reserve_unqueued()?;
        Ok(Permit { shared: self })
    }

    pub async fn reserve(&self) -> Result<Permit<'_, T>, SendError<()>> {
        // The async state pins `reserve`, which a queued node points into.
        let mut reserve = Reserve::new(self);
        poll_fn(|cx| reserve.poll(cx)).await
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.pop()
    }

    pub async fn recv(&self) -> Result<T, RecvError> {
        match self.pop() {
            Ok(value) => return Ok(value),
            Err(TryRecvError::Disconnected) => return Err(RecvError::Disconnected),
            Err(TryRecvError::Empty) => {}
        }
        let mut recv = Recv {
            shared: self,
            waiter: None,
        };
        poll_fn(|cx| recv.poll(cx)).await
    }

    /// Reports whether either side has disconnected. The tail exchanges are authoritative.
    fn is_disconnected(&self) -> bool {
        self.tail.load(Ordering::Relaxed) & self.mark_bit != 0
    }

    /// Strips the mark bit and the reservation state from a tail.
    fn position(&self, tail: Index) -> Index {
        tail & !(self.one_lap - self.mark_bit)
    }

    /// Replaces the position in a tail, keeping its mark bit and reservation state.
    fn with_position(&self, tail: Index, position: Index) -> Index {
        (tail & (self.one_lap - self.mark_bit)) | position
    }

    /// Returns the capacity that permits and grants hold, according to a tail.
    fn reserved(&self, tail: Index) -> usize {
        ((tail & (self.queued_bit - 1)) >> self.count_shift) as usize
    }

    /// Returns one unit of the reserved count in `tail`.
    fn count_unit(&self) -> Index {
        1 << self.count_shift
    }

    fn slot(&self, position: Index) -> &Slot<T> {
        &self.slots[(position & (self.mark_bit - 1)) as usize]
    }

    fn next_position(&self, position: Index) -> Index {
        let index = position & (self.mark_bit - 1);
        if index + 1 < self.slots.len() as Index {
            position + 1
        } else {
            // Wrap to the first slot of the next lap.
            (position & !(self.one_lap - 1)).wrapping_add(self.one_lap)
        }
    }

    /// Counts the values between `head` and the tail position.
    ///
    /// Returns `None` if `head` was read after receivers passed the tail position.
    fn queued_len(&self, tail: Index, head: Index) -> Option<usize> {
        let capacity = self.slots.len() as Index;
        let laps = tail.wrapping_sub(head).wrapping_add(capacity) / self.one_lap;
        let len = (laps * capacity + (tail & (self.mark_bit - 1)))
            .checked_sub(head & (self.mark_bit - 1))?;
        (laps <= 1 && len <= capacity).then_some(len as usize)
    }

    /// Reserves one unit of capacity for a sender while none is queued.
    fn reserve_unqueued(&self) -> Result<(), TrySendError<()>> {
        let mut backoff = Backoff::new();
        let mut tail = self.tail.load(Ordering::Relaxed);
        loop {
            if tail & self.mark_bit != 0 {
                return Err(TrySendError::Disconnected(()));
            }
            if tail & self.queued_bit != 0 {
                // Queued senders own released capacity first.
                return Err(TrySendError::Full(()));
            }
            // Reading `head` after `tail` can only overstate the queued values.
            match self.queued_len(self.position(tail), self.head.load(Ordering::SeqCst)) {
                Some(len) if len + self.reserved(tail) < self.slots.len() => {}
                Some(_) => return Err(TrySendError::Full(())),
                None => {
                    // Receivers passed this tail, so the exchange below would fail.
                    backoff.spin();
                    tail = self.tail.load(Ordering::Relaxed);
                    continue;
                }
            }
            match self.tail.compare_exchange_weak(
                tail,
                tail + self.count_unit(),
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(current) => {
                    tail = current;
                    backoff.spin();
                }
            }
        }
    }

    /// Reports whether capacity remains for a grant, with the wait-queue lock held while senders
    /// are queued.
    fn can_grant(&self) -> bool {
        // While senders are queued, only grants under this lock add to the reserved count, and
        // sends without a reservation cannot claim slots, so capacity seen free stays free.
        loop {
            let tail = self.tail.load(Ordering::SeqCst);
            if let Some(len) =
                self.queued_len(self.position(tail), self.head.load(Ordering::SeqCst))
            {
                return len + self.reserved(tail) < self.slots.len();
            }
            // Receivers passed this tail after it was loaded.
            hint::spin_loop();
        }
    }

    /// Queues a sender, then grants it capacity if some is free.
    fn enqueue_sender(
        &self,
        state: &mut SendState,
        waiter: &mut Option<WaiterId>,
        waker: &Waker,
        granted: NonNull<AtomicBool>,
        wakers: &mut WakerBatch,
    ) {
        if state.waiters.is_empty() {
            // From here on, sends and reservations without a place in the queue leave freed
            // capacity to it. Those prepared from an earlier tail fail their exchange.
            self.send_waiters.queued.store(true, Ordering::SeqCst);
            self.tail.fetch_or(self.queued_bit, Ordering::SeqCst);
        }
        *waiter = Some(state.waiters.push_back(SendWaiter {
            waker: waker.clone(),
            granted,
        }));
        // Pairs with `grant_after_receive` and `release_reservation`: either they observe queued
        // senders, or this check observes the capacity they freed.
        self.grant_queued(state, wakers);
    }

    /// Grants free capacity to queued senders in order, and clears `queued_bit` once none remain.
    fn grant_queued(&self, state: &mut SendState, wakers: &mut WakerBatch) {
        while !state.waiters.is_empty() && self.can_grant() {
            let (id, _) = state
                .waiters
                .unlink_first_waiter(|_| true)
                .expect("the wait queue is not empty");
            let SendWaiter { waker, granted } = state.waiters.remove_unlinked_waiter(id);
            // Count the grant before the sender can observe it and spend it. Granting the last
            // queued sender clears `queued_bit` in the same update.
            let update = if state.waiters.is_empty() {
                self.send_waiters.queued.store(false, Ordering::Relaxed);
                self.count_unit().wrapping_sub(self.queued_bit)
            } else {
                self.count_unit()
            };
            self.tail.fetch_add(update, Ordering::SeqCst);
            // SAFETY: The node was queued, so its `Reserve` is alive, and it cannot be dropped
            // while this lock is held.
            unsafe { granted.as_ref() }.store(true, Ordering::Release);
            wakers.push(waker);
        }
        if state.waiters.is_empty() && self.tail.load(Ordering::Relaxed) & self.queued_bit != 0 {
            // A cancelled waiter emptied the queue.
            self.send_waiters.queued.store(false, Ordering::Relaxed);
            self.tail.fetch_and(!self.queued_bit, Ordering::SeqCst);
        }
    }

    /// Runs `grant_queued` under the wait-queue lock and wakes the granted senders.
    fn grant_queued_locked(&self) {
        let mut wakers = WakerBatch::new();
        {
            let mut state = self.send_waiters.state.lock();
            if !state.closed {
                self.grant_queued(&mut state, &mut wakers);
            }
        }
        wakers.wake_all();
    }

    /// Hands capacity freed by a receive to the first queued sender.
    fn grant_after_receive(&self) {
        // The head exchange in `pop` and these loads are sequentially consistent, as are the
        // `queued` store and the capacity check in `enqueue_sender`, and the end of a handoff and
        // the capacity check in `finish_handoff`. A handoff in progress grants the freed capacity.
        let senders = &self.send_waiters;
        if senders.queued.load(Ordering::SeqCst) && senders.handoffs.load(Ordering::SeqCst) == 0 {
            self.grant_queued_locked();
        }
    }

    /// Ends a handoff that a send with a reservation began while senders were queued, then grants
    /// capacity that receivers freed meanwhile.
    fn finish_handoff(&self) {
        let senders = &self.send_waiters;
        senders.handoffs.fetch_sub(1, Ordering::SeqCst);
        if senders.queued.load(Ordering::SeqCst) {
            self.grant_queued_locked();
        }
    }

    /// Returns capacity held by an unused permit to queued senders, or makes it available again.
    fn release_reservation(&self) {
        // Either this update observes `queued_bit`, or the capacity check after setting it
        // observes this update.
        let tail = self.tail.fetch_sub(self.count_unit(), Ordering::SeqCst);
        if tail & self.queued_bit != 0 {
            self.grant_queued_locked();
        }
    }

    /// Pushes a value using capacity a reservation holds, then notifies a waiting receiver.
    ///
    /// Returns the value if the last receiver disconnected first.
    fn publish_reserved(&self, value: T) -> Result<(), T> {
        // While senders are queued, this send takes over granting capacity that receivers free
        // until it finishes. Otherwise, a receiver that grants and wakes every queued sender itself
        // can fall behind them, which keeps them queued and makes every send wait.
        let handoff = self.send_waiters.queued.load(Ordering::SeqCst);
        if handoff {
            self.send_waiters.handoffs.fetch_add(1, Ordering::SeqCst);
        }
        let result = self.push_reserved(value);
        if result.is_ok() {
            self.notify_receiver();
        }
        if handoff {
            self.finish_handoff();
        }
        result
    }

    /// Claims a free slot without a reservation, leaving room for reserved capacity.
    fn push_unreserved(&self, value: T) -> Result<(), Unclaimed<T>> {
        let mut backoff = Backoff::new();
        let mut tail = self.tail.load(Ordering::Relaxed);
        loop {
            if tail & self.mark_bit != 0 {
                return Err(Unclaimed::Disconnected(value));
            }
            if tail & self.queued_bit != 0 {
                // Queued senders own released capacity first.
                return Err(Unclaimed::Full(value));
            }
            let position = self.position(tail);
            let slot = self.slot(position);
            let stamp = slot.stamp.load(Ordering::Acquire);
            if stamp == position {
                let reserved = self.reserved(tail);
                if reserved != 0 {
                    match self.queued_len(position, self.head.load(Ordering::SeqCst)) {
                        Some(len) if len + reserved >= self.slots.len() => {
                            return Err(Unclaimed::Full(value));
                        }
                        Some(_) => {}
                        None => {
                            // Receivers passed this tail, so the exchange below would fail.
                            backoff.spin();
                            tail = self.tail.load(Ordering::Relaxed);
                            continue;
                        }
                    }
                }
                // The exchange fails if the reservation state changed after the load.
                match self.tail.compare_exchange_weak(
                    tail,
                    self.with_position(tail, self.next_position(position)),
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: The exchange claimed this free slot. No receiver reads it until
                        // the stamp below publishes the value.
                        unsafe { slot.value.get().write(MaybeUninit::new(value)) };
                        slot.stamp.store(position + 1, Ordering::Release);
                        return Ok(());
                    }
                    Err(current) => {
                        tail = current;
                        backoff.spin();
                    }
                }
            } else if stamp.wrapping_add(self.one_lap) == position + 1 {
                // This slot still holds the previous lap's value. A receiver that has not stored
                // the head exchange yet is still receiving, so reporting full linearizes before it.
                if self.head.load(Ordering::SeqCst).wrapping_add(self.one_lap) == position {
                    return Err(Unclaimed::Full(value));
                }
                // A receiver has claimed that value and is still reading it.
                backoff.snooze();
                tail = self.tail.load(Ordering::Relaxed);
            } else {
                // Another sender advanced `tail` after it was loaded.
                backoff.spin();
                tail = self.tail.load(Ordering::Relaxed);
            }
        }
    }

    /// Claims a slot with capacity a reservation holds, returning the reservation in the same
    /// exchange.
    fn push_reserved(&self, value: T) -> Result<(), T> {
        let mut backoff = Backoff::new();
        let mut tail = self.tail.load(Ordering::Relaxed);
        loop {
            if tail & self.mark_bit != 0 {
                return Err(value);
            }
            let position = self.position(tail);
            let slot = self.slot(position);
            let stamp = slot.stamp.load(Ordering::Acquire);
            if stamp == position {
                // The queued value takes over the capacity, so the free capacity is unchanged.
                match self.tail.compare_exchange_weak(
                    tail,
                    self.with_position(tail, self.next_position(position)) - self.count_unit(),
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: As in `push_unreserved`.
                        unsafe { slot.value.get().write(MaybeUninit::new(value)) };
                        slot.stamp.store(position + 1, Ordering::Release);
                        return Ok(());
                    }
                    Err(current) => {
                        tail = current;
                        backoff.spin();
                    }
                }
            } else if stamp.wrapping_add(self.one_lap) == position + 1 {
                // A receiver is still reading this slot's previous value. The reservation rules
                // out a previous value that no receiver has claimed.
                backoff.snooze();
                tail = self.tail.load(Ordering::Relaxed);
            } else {
                // Another sender advanced `tail` after it was loaded.
                backoff.spin();
                tail = self.tail.load(Ordering::Relaxed);
            }
        }
    }

    fn pop(&self) -> Result<T, TryRecvError> {
        let mut backoff = Backoff::new();
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            let slot = self.slot(head);
            let stamp = slot.stamp.load(Ordering::Acquire);
            if stamp == head + 1 {
                match self.head.compare_exchange_weak(
                    head,
                    self.next_position(head),
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: The exchange claimed this published slot. No sender writes it
                        // again until the stamp below frees it for the next lap.
                        let value = unsafe { slot.value.get().read().assume_init() };
                        slot.stamp
                            .store(head.wrapping_add(self.one_lap), Ordering::Release);
                        self.grant_after_receive();
                        return Ok(value);
                    }
                    Err(current) => {
                        head = current;
                        backoff.spin();
                    }
                }
            } else if stamp == head {
                // Reporting empty before a sender's tail exchange is visible linearizes before
                // that send.
                let tail = self.tail.load(Ordering::SeqCst);
                if self.position(tail) == head {
                    return Err(if tail & self.mark_bit == 0 {
                        TryRecvError::Empty
                    } else {
                        TryRecvError::Disconnected
                    });
                }
                // A sender has claimed this slot and is still writing it.
                backoff.snooze();
                head = self.head.load(Ordering::Relaxed);
            } else if stamp.wrapping_add(self.one_lap) == head + 1 {
                // Another receiver is still reading this slot's previous value.
                backoff.snooze();
                head = self.head.load(Ordering::Relaxed);
            } else {
                // Another receiver advanced `head` after it was loaded.
                backoff.spin();
                head = self.head.load(Ordering::Relaxed);
            }
        }
    }

    fn peek(&self) -> Peek {
        let head = self.head.load(Ordering::SeqCst);
        let tail = self.tail.load(Ordering::SeqCst);
        if self.position(tail) != head {
            Peek::Ready
        } else if tail & self.mark_bit == 0 {
            Peek::Empty
        } else {
            Peek::Disconnected
        }
    }

    fn notify_receiver(&self) {
        // The tail exchange in a push and this load are sequentially consistent, as are a
        // receiver's registration and its recheck in `Recv::poll`. Either this load observes the
        // registration, or the recheck observes the claimed slot.
        if !self.recv_waiters.linked.load(Ordering::SeqCst) {
            return;
        }
        let waker = {
            let mut state = self.recv_waiters.state.lock();
            let waker = state.waiters.notify_one();
            self.recv_waiters
                .linked
                .store(!state.waiters.is_empty(), Ordering::SeqCst);
            waker
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Drops buffered values once the last receiver has disconnected the queue.
    ///
    /// `tail` is the final tail position. Senders that claimed a slot before the disconnection may
    /// still be writing it.
    fn discard_values(&self, tail: Index) {
        // If a destructor panics, the guard drops the remaining values while unwinding.
        struct Discard<'a, T> {
            shared: &'a Shared<T>,
            tail: Index,
        }

        impl<T> Drop for Discard<'_, T> {
            fn drop(&mut self) {
                while self.shared.drop_next_value(self.tail) {}
            }
        }

        let discard = Discard { shared: self, tail };
        while self.drop_next_value(tail) {}
        drop(discard);
    }

    /// Drops the value at `head` once it is published, and returns whether one remained.
    ///
    /// Only the last receiver calls this, so `head` cannot change concurrently.
    fn drop_next_value(&self, tail: Index) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        if head == tail {
            return false;
        }
        let slot = self.slot(head);
        let mut backoff = Backoff::new();
        while slot.stamp.load(Ordering::Acquire) != head + 1 {
            backoff.snooze();
        }
        // SAFETY: The stamp shows a published value, and the last receiver is its only reader.
        let value = unsafe { slot.value.get().read().assume_init() };
        slot.stamp
            .store(head.wrapping_add(self.one_lap), Ordering::Relaxed);
        self.head.store(self.next_position(head), Ordering::Relaxed);
        drop(value);
        true
    }
}

impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        // The last receiver discards buffered values; this only reclaims values it could not.
        let tail = *self.tail.get_mut();
        let tail = self.position(tail);
        while self.drop_next_value(tail) {}
    }
}

/// Capacity reserved for one value on a bounded MPMC queue.
///
/// A permit does not keep any receiver alive or claim message order. Dropping it without sending
/// passes its capacity to the next waiting sender or makes it available again.
#[must_use = "dropping the permit releases its reserved capacity"]
pub struct Permit<'a, T> {
    shared: &'a Shared<T>,
}

impl<T> fmt::Debug for Permit<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Permit").finish_non_exhaustive()
    }
}

impl<T> Permit<'_, T> {
    /// Enqueues a value using the reserved capacity without waiting for space.
    ///
    /// If the last receiver has been dropped, the error returns ownership of the value.
    pub fn send(self, value: T) -> Result<(), SendError<T>> {
        let shared = self.shared;
        mem::forget(self);
        // Claiming the slot returns the reservation. Once the last receiver has disconnected, the
        // capacity no longer matters.
        shared.publish_reserved(value).map_err(SendError::new)
    }
}

impl<T> Drop for Permit<'_, T> {
    fn drop(&mut self) {
        self.shared.release_reservation();
    }
}

struct Reserve<'a, T> {
    shared: &'a Shared<T>,
    waiter: Option<WaiterId>,
    /// Set by a grant, which also removes the queued node.
    granted: AtomicBool,
    /// The queued node points at `granted`, so this future must not move while queued.
    _pinned: PhantomPinned,
}

impl<'a, T> Reserve<'a, T> {
    fn new(shared: &'a Shared<T>) -> Self {
        Self {
            shared,
            waiter: None,
            granted: AtomicBool::new(false),
            _pinned: PhantomPinned,
        }
    }

    /// Takes a grant, whose capacity passes to the returned permit.
    fn take_grant(&mut self) -> Permit<'a, T> {
        // The grant already removed the node.
        self.waiter = None;
        self.granted.store(false, Ordering::Relaxed);
        Permit {
            shared: self.shared,
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Permit<'a, T>, SendError<()>>> {
        let shared = self.shared;
        // A queued node or a grant stays until `drop` handles it under the lock, because a grant
        // may still write to `granted` before the last receiver closes the wait queue.
        if shared.is_disconnected() {
            return Poll::Ready(Err(SendError::new(())));
        }
        // Pairs with the release store in `grant_queued`, so claiming a grant needs no lock.
        if self.granted.load(Ordering::Acquire) {
            return Poll::Ready(Ok(self.take_grant()));
        }
        let mut wakers = WakerBatch::new();
        let (outcome, retired) = {
            let mut state = shared.send_waiters.state.lock();
            if state.closed {
                // The closed storage that held this waiter has been replaced.
                self.waiter = None;
                return Poll::Ready(Err(SendError::new(())));
            }
            let mut retired = None;
            let outcome = match self.waiter {
                // A grant may have arrived after the first check.
                _ if self.granted.load(Ordering::Relaxed) => Some(self.take_grant()),
                // Reserving free capacity directly avoids marking senders queued, which would turn
                // concurrent sends away. Trying only under the lock keeps waiting senders from
                // contending on the cursors. A disconnection fails this attempt, and closing the
                // queue then wakes this sender.
                None if state.waiters.is_empty() && shared.reserve_unqueued().is_ok() => {
                    Some(Permit { shared })
                }
                None => {
                    let granted = NonNull::from(&self.granted);
                    shared.enqueue_sender(
                        &mut state,
                        &mut self.waiter,
                        cx.waker(),
                        granted,
                        &mut wakers,
                    );
                    // The enqueue grants this sender if capacity was already free.
                    self.granted
                        .load(Ordering::Relaxed)
                        .then(|| self.take_grant())
                }
                Some(id) => {
                    let waiter = state.waiters.waiter_mut(id);
                    if !waiter.waker.will_wake(cx.waker()) {
                        retired = Some(mem::replace(&mut waiter.waker, cx.waker().clone()));
                    }
                    None
                }
            };
            (outcome, retired)
        };
        wakers.wake_all();
        drop(retired);
        match outcome {
            Some(permit) => Poll::Ready(Ok(permit)),
            None => Poll::Pending,
        }
    }
}

impl<T> Drop for Reserve<'_, T> {
    fn drop(&mut self) {
        if self.waiter.is_none() && !self.granted.load(Ordering::Relaxed) {
            return;
        }
        let shared = self.shared;
        let mut wakers = WakerBatch::new();
        let retired = {
            let mut state = shared.send_waiters.state.lock();
            if state.closed {
                return;
            }
            let retired = if self.granted.load(Ordering::Relaxed) {
                // Return the granted capacity, which passes to the next queued sender below or
                // becomes available again.
                shared.tail.fetch_sub(shared.count_unit(), Ordering::SeqCst);
                None
            } else {
                self.waiter
                    .take()
                    .map(|id| remove_waiter(&mut state.waiters, id))
            };
            shared.grant_queued(&mut state, &mut wakers);
            retired
        };
        wakers.wake_all();
        drop(retired);
    }
}

struct Recv<'a, T> {
    shared: &'a Shared<T>,
    waiter: Option<WaiterId>,
}

impl<T> Recv<'_, T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        loop {
            match self.shared.peek() {
                Peek::Ready => {}
                Peek::Disconnected => {
                    self.unregister();
                    return Poll::Ready(Err(RecvError::Disconnected));
                }
                Peek::Empty => {
                    if !self.register(cx.waker()) {
                        // The last sender has disconnected, which the next peek observes.
                        continue;
                    }
                    if self.shared.peek() == Peek::Empty {
                        return Poll::Pending;
                    }
                }
            }
            // Leave the wait queue before taking a value. Otherwise, a sender could assign its
            // notification to this receiver after it took an earlier value.
            self.unregister();
            match self.shared.pop() {
                Ok(value) => return Poll::Ready(Ok(value)),
                Err(TryRecvError::Disconnected) => {
                    return Poll::Ready(Err(RecvError::Disconnected));
                }
                // Another receiver took the value first.
                Err(TryRecvError::Empty) => {}
            }
        }
    }

    /// Registers this receiver's waker, or refreshes it without losing its place.
    ///
    /// Returns `false` once the last sender has disconnected.
    fn register(&mut self, waker: &Waker) -> bool {
        let receivers = &self.shared.recv_waiters;
        let retired = {
            let mut state = receivers.state.lock();
            if state.closed {
                self.waiter = None;
                return false;
            }
            let retired = state.waiters.register(&mut self.waiter, waker);
            receivers.linked.store(true, Ordering::SeqCst);
            retired
        };
        drop(retired);
        true
    }

    /// Leaves the wait queue, consuming any notification this receiver received.
    fn unregister(&mut self) {
        let Some(id) = self.waiter.take() else {
            return;
        };
        let receivers = &self.shared.recv_waiters;
        let retired = {
            let mut state = receivers.state.lock();
            if state.closed {
                return;
            }
            let retired = remove_waiter(&mut state.waiters, id);
            receivers
                .linked
                .store(!state.waiters.is_empty(), Ordering::SeqCst);
            retired
        };
        drop(retired);
    }
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        let Some(id) = self.waiter.take() else {
            return;
        };
        let receivers = &self.shared.recv_waiters;
        let (retired, waker) = {
            let mut state = receivers.state.lock();
            if state.closed {
                return;
            }
            let retired = remove_waiter(&mut state.waiters, id);
            // Hand an unconsumed notification to the next receiver while a value still waits.
            let waker =
                if matches!(retired, RecvWaiter::Notified) && self.shared.peek() == Peek::Ready {
                    state.waiters.notify_one()
                } else {
                    None
                };
            receivers
                .linked
                .store(!state.waiters.is_empty(), Ordering::SeqCst);
            (retired, waker)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        drop(retired);
    }
}

/// Spins briefly, then yields, while another thread finishes a short operation on a slot or cursor.
struct Backoff {
    spins: u32,
    snoozes: u32,
}

impl Backoff {
    const SPIN_LIMIT: u32 = 6;

    fn new() -> Self {
        Self {
            spins: 0,
            snoozes: 0,
        }
    }

    /// Backs off after losing a race on a cursor, without yielding.
    fn spin(&mut self) {
        for _ in 0..1 << self.spins {
            hint::spin_loop();
        }
        self.spins = (self.spins + 1).min(Self::SPIN_LIMIT);
    }

    /// Waits for another thread to finish writing or reading a slot.
    fn snooze(&mut self) {
        if self.snoozes <= Self::SPIN_LIMIT {
            for _ in 0..1 << self.snoozes {
                hint::spin_loop();
            }
            self.snoozes += 1;
        } else {
            thread::yield_now();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::panic;
    use std::panic::AssertUnwindSafe;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::Wake;

    use super::*;

    struct CountWakes(AtomicUsize);

    impl Wake for CountWakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn wake_counter() -> (Waker, Arc<CountWakes>) {
        let wakes = Arc::new(CountWakes(AtomicUsize::new(0)));
        (Waker::from(wakes.clone()), wakes)
    }

    /// Returns the capacity held by permits and grants, and whether senders are queued.
    fn reservation_state<T>(shared: &Shared<T>) -> (usize, bool) {
        let tail = shared.tail.load(Ordering::Relaxed);
        (shared.reserved(tail), tail & shared.queued_bit != 0)
    }

    struct CountDrops(Arc<AtomicUsize>);

    impl Drop for CountDrops {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn positions_wrap_across_laps_in_fifo_order() {
        let shared = Shared::new(3);
        let mut next = 0;
        for lap in 0..20 {
            // Leave a varying number of values queued so every slot wraps at a different offset.
            for _ in 0..lap % 3 + 1 {
                shared.try_send(next).unwrap();
                next += 1;
            }
            for expected in next - (lap % 3 + 1)..next {
                assert_eq!(shared.try_recv(), Ok(expected));
            }
        }
        for value in 0..3 {
            shared.try_send(value).unwrap();
        }
        assert_eq!(shared.try_send(3), Err(TrySendError::Full(3)));
    }

    #[test]
    #[should_panic(expected = "mpmc bounded queue capacity is too large")]
    fn capacity_above_the_maximum_panics() {
        let _ = Shared::<()>::new(MAX_CAPACITY + 1);
    }

    #[test]
    fn every_allowed_capacity_needs_2_40_sends_to_wrap_the_tail() {
        // A capacity just above a power of two leaves the fewest sends per wrap.
        for capacity in [
            1,
            2,
            63,
            64,
            65,
            1 << 16,
            (1 << 20) - 1,
            1 << 20,
            MAX_CAPACITY,
        ] {
            let (.., one_lap) = tail_layout(capacity);
            let laps = 1u128 << (Index::BITS - one_lap.trailing_zeros());
            assert!(laps * capacity as u128 >= 1 << 40, "capacity {capacity}");
        }
    }

    #[test]
    fn last_receiver_drops_buffered_values_across_the_wrap() {
        let drops = Arc::new(AtomicUsize::new(0));
        let shared = Shared::new(2);
        shared.try_send(CountDrops(drops.clone())).unwrap();
        shared.try_send(CountDrops(drops.clone())).unwrap();
        drop(shared.try_recv().unwrap());
        shared.try_send(CountDrops(drops.clone())).unwrap();
        assert_eq!(drops.load(Ordering::Relaxed), 1);

        shared.drop_receiver();
        assert_eq!(drops.load(Ordering::Relaxed), 3);
        assert!(matches!(
            shared.try_send(CountDrops(drops.clone())),
            Err(TrySendError::Disconnected(_))
        ));
        assert_eq!(drops.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn discard_continues_after_a_destructor_panics() {
        struct PanicOnDrop;

        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("value destructor panicked");
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let shared = Shared::new(3);
        shared.try_send((None, Some(PanicOnDrop))).unwrap();
        shared
            .try_send((Some(CountDrops(drops.clone())), None))
            .unwrap();
        shared
            .try_send((Some(CountDrops(drops.clone())), None))
            .unwrap();

        let result = panic::catch_unwind(AssertUnwindSafe(|| shared.drop_receiver()));
        assert!(result.is_err());
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        drop(shared);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn receiver_that_takes_a_silently_published_value_leaves_the_wait_queue() {
        let shared = Shared::new(2);
        shared.clone_receiver();
        let (first_waker, first_wakes) = wake_counter();
        let (second_waker, second_wakes) = wake_counter();
        let mut first = Recv {
            shared: &shared,
            waiter: None,
        };
        let mut second = Recv {
            shared: &shared,
            waiter: None,
        };
        assert!(
            first
                .poll(&mut Context::from_waker(&first_waker))
                .is_pending()
        );
        assert!(
            second
                .poll(&mut Context::from_waker(&second_waker))
                .is_pending()
        );

        // A sender that read the wait-queue flag before either registration publishes silently.
        assert!(matches!(shared.push_unreserved(1), Ok(())));
        // A spurious poll finds that value. Its receiver must leave the queue before taking it, so
        // the next notification goes to the receiver that is still waiting.
        assert_eq!(
            first.poll(&mut Context::from_waker(&first_waker)),
            Poll::Ready(Ok(1))
        );
        shared.try_send(2).unwrap();
        assert_eq!(first_wakes.0.load(Ordering::Relaxed), 0);
        assert_eq!(second_wakes.0.load(Ordering::Relaxed), 1);
        assert_eq!(
            second.poll(&mut Context::from_waker(&second_waker)),
            Poll::Ready(Ok(2))
        );
    }

    #[test]
    fn cancelled_reservation_keeps_capacity_exact() {
        let shared = Shared::new(1);
        let permit = shared.try_reserve().unwrap();
        let (waker, wakes) = wake_counter();
        let mut reserve = pin!(shared.reserve());
        assert!(
            reserve
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(permit);
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
        let Poll::Ready(Ok(granted)) = reserve.as_mut().poll(&mut Context::from_waker(&waker))
        else {
            panic!("the released capacity must be granted to the waiting reservation");
        };
        assert!(matches!(shared.try_send(1), Err(TrySendError::Full(1))));
        granted.send(2).unwrap();
        assert_eq!(shared.try_recv(), Ok(2));
        shared.try_send(3).unwrap();
        assert_eq!(shared.try_recv(), Ok(3));
    }

    #[test]
    fn reservations_and_grants_keep_capacity_exact_across_laps() {
        let shared = Shared::new(2);
        let (waker, wakes) = wake_counter();
        for round in 0..6 {
            let permit = shared.try_reserve().unwrap();
            shared.try_send(round).unwrap();
            assert!(matches!(shared.try_send(9), Err(TrySendError::Full(9))));
            let mut reserve = pin!(shared.reserve());
            assert!(
                reserve
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            // The queued reservation owns the capacity the next receive frees.
            assert_eq!(shared.try_recv(), Ok(round));
            assert!(matches!(shared.try_send(9), Err(TrySendError::Full(9))));
            let Poll::Ready(Ok(granted)) = reserve.as_mut().poll(&mut Context::from_waker(&waker))
            else {
                panic!("the freed capacity must be granted to the queued reservation");
            };
            drop(permit);
            granted.send(round + 10).unwrap();
            assert_eq!(reservation_state(&shared), (0, false));
            assert_eq!(shared.try_recv(), Ok(round + 10));
        }
        assert_eq!(wakes.0.load(Ordering::Relaxed), 6);
        for value in 0..2 {
            shared.try_send(value).unwrap();
        }
        assert!(matches!(shared.try_send(2), Err(TrySendError::Full(2))));
    }

    #[test]
    fn send_in_a_handoff_grants_capacity_that_a_receive_freed() {
        let shared = Shared::new(1);
        shared.try_send(1).unwrap();
        let (waker, wakes) = wake_counter();
        let mut reserve = pin!(shared.reserve());
        assert!(
            reserve
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        // A send with a reservation is between its claim and its grant check.
        shared.send_waiters.handoffs.fetch_add(1, Ordering::SeqCst);
        assert_eq!(shared.try_recv(), Ok(1));
        assert_eq!(
            wakes.0.load(Ordering::Relaxed),
            0,
            "the receive left the grant"
        );
        shared.finish_handoff();
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
        let Poll::Ready(Ok(granted)) = reserve.as_mut().poll(&mut Context::from_waker(&waker))
        else {
            panic!("the handoff must grant the capacity the receive freed");
        };
        granted.send(2).unwrap();
        assert_eq!(shared.try_recv(), Ok(2));
        assert_eq!(reservation_state(&shared), (0, false));
    }

    #[test]
    fn stalled_send_cannot_claim_capacity_that_a_later_permit_holds() {
        // Enough reservations to wrap any counter narrower than 25 bits run while the send stalls.
        const RESERVATIONS: usize = if cfg!(miri) { 4 } else { 1 << 24 };

        let shared = Shared::new(1);
        // The send loads the tail and finds the only slot free, then stalls before its exchange.
        let stale = shared.tail.load(Ordering::Relaxed);
        for _ in 0..RESERVATIONS {
            drop(shared.try_reserve().unwrap());
        }
        let permit = shared.try_reserve().unwrap();
        let position = shared.position(stale);
        let claim = shared.tail.compare_exchange(
            stale,
            shared.with_position(stale, shared.next_position(position)),
            Ordering::SeqCst,
            Ordering::Relaxed,
        );
        assert!(claim.is_err(), "the permit holds the only capacity");
        permit.send(1).unwrap();
        assert_eq!(shared.try_recv(), Ok(1));
        assert_eq!(reservation_state(&shared), (0, false));
    }

    #[test]
    fn threads_mixing_reservations_and_direct_sends_deliver_every_value_once() {
        // Alternating permits and direct sends keeps reservations changing while slots are claimed.
        const THREADS: usize = 3;
        const VALUES: usize = if cfg!(miri) { 16 } else { 20_000 };

        let shared = Shared::new(2);
        let received = thread::scope(|scope| {
            for producer in 0..THREADS {
                let shared = &shared;
                scope.spawn(move || {
                    for value in producer * VALUES..(producer + 1) * VALUES {
                        if value % 2 == 0 {
                            let mut value = value;
                            while let Err(error) = shared.try_send(value) {
                                let TrySendError::Full(returned) = error else {
                                    unreachable!("receivers outlive the producers");
                                };
                                value = returned;
                                thread::yield_now();
                            }
                        } else {
                            let permit = loop {
                                match shared.try_reserve() {
                                    Ok(permit) => break permit,
                                    Err(TrySendError::Full(())) => thread::yield_now(),
                                    Err(TrySendError::Disconnected(())) => unreachable!(),
                                }
                            };
                            permit.send(value).unwrap();
                        }
                    }
                });
            }
            let consumers = (0..THREADS)
                .map(|_| {
                    let shared = &shared;
                    scope.spawn(move || {
                        let mut values = Vec::new();
                        while values.len() < VALUES {
                            match shared.try_recv() {
                                Ok(value) => values.push(value),
                                Err(TryRecvError::Empty) => thread::yield_now(),
                                Err(TryRecvError::Disconnected) => unreachable!(),
                            }
                        }
                        values
                    })
                })
                .collect::<Vec<_>>();
            consumers
                .into_iter()
                .flat_map(|consumer| consumer.join().unwrap())
                .collect::<Vec<_>>()
        });
        let mut received = received;
        received.sort_unstable();
        assert_eq!(received, (0..THREADS * VALUES).collect::<Vec<_>>());
        assert_eq!(reservation_state(&shared), (0, false));
    }

    #[test]
    fn threads_deliver_every_value_exactly_once_through_a_small_ring() {
        const THREADS: usize = 3;
        const VALUES: usize = if cfg!(miri) { 20 } else { 20_000 };

        let shared = Shared::new(2);
        let received = thread::scope(|scope| {
            for producer in 0..THREADS {
                let shared = &shared;
                scope.spawn(move || {
                    for value in producer * VALUES..(producer + 1) * VALUES {
                        let mut value = value;
                        loop {
                            match shared.try_send(value) {
                                Ok(()) => break,
                                Err(TrySendError::Full(returned)) => {
                                    value = returned;
                                    thread::yield_now();
                                }
                                Err(TrySendError::Disconnected(_)) => unreachable!(),
                            }
                        }
                    }
                });
            }
            let consumers = (0..THREADS)
                .map(|_| {
                    let shared = &shared;
                    scope.spawn(move || {
                        let mut values = Vec::new();
                        while values.len() < VALUES {
                            match shared.try_recv() {
                                Ok(value) => values.push(value),
                                Err(TryRecvError::Empty) => thread::yield_now(),
                                Err(TryRecvError::Disconnected) => unreachable!(),
                            }
                        }
                        values
                    })
                })
                .collect::<Vec<_>>();
            consumers
                .into_iter()
                .flat_map(|consumer| consumer.join().unwrap())
                .collect::<Vec<_>>()
        });
        let mut received = received;
        received.sort_unstable();
        assert_eq!(received, (0..THREADS * VALUES).collect::<Vec<_>>());
    }
}
