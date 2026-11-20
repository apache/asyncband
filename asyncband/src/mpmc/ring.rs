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

// The bounded ring's cursor design derives in part from crossbeam-channel 0.5.17's array flavor.
// Copyright (c) 2019 The Crossbeam Project Developers
// Asyncband uses the upstream project's Apache-2.0 license option for the incorporated portions.
// Asyncband serializes completed transfers on each side, caches peer progress, and accounts for
// reservations under the producer lock instead of using per-slot stamps or packed capacity bits.
// Upstream source:
// https://github.com/crossbeam-rs/crossbeam/blob/2920c984290229ab4e0ca0452ef09e48a82063f3/crossbeam-channel/src/flavors/array.rs

use std::cell::UnsafeCell;
use std::future::poll_fn;
use std::mem;
use std::mem::MaybeUninit;
use std::panic::RefUnwindSafe;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::cache_padded::CachePadded;
use crate::internal::mutex::Mutex;
use crate::internal::retry_waiter::RetryWaiter;
use crate::internal::ring_slots::drop_range;
use crate::internal::update_waker;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::waker_batch::WakerBatch;
use crate::mpmc::RecvError;
use crate::mpmc::SendError;
use crate::mpmc::TryRecvError;
use crate::mpmc::TrySendError;

/// Bounded storage with separate production and consumption critical sections.
///
/// Each cursor denotes completed payload transfers, not claims. Its owner holds the corresponding
/// lock from the first peer-progress check through the payload move and cursor publication.
/// Cursors wrap at twice the capacity; neither side can lap a peer snapshot before refreshing it.
/// There are no suspended claims outside the locks and no slot stamps or generation assumptions.
pub struct Shared<T> {
    producer: CachePadded<Mutex<Producer>>,
    consumer: CachePadded<Mutex<Consumer>>,
    recv_waiters: Mutex<WaitList<RetryWaiter>>,
    published: CachePadded<AtomicUsize>,
    reclaimed: CachePadded<AtomicUsize>,
    send_waiting: AtomicBool,
    recv_waiting: AtomicBool,
    senders_closed: AtomicBool,
    receivers_closed: AtomicBool,
    sender_count: AtomicUsize,
    receiver_count: AtomicUsize,
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

struct Producer {
    cursor: usize,
    cached_reclaimed: usize,
    // All permits and detached grants are counted here; only the producer lock changes it.
    reserved: usize,
    waiters: WaitList<SendWaiter>,
}

struct Consumer {
    cursor: usize,
    cached_published: usize,
}

enum SendWaiter {
    Waiting(Waker),
    Granted,
}

// SAFETY: Producer and consumer locks each serialize their side's slot accesses. Publication makes
// initialized values visible; reclamation permits reuse only after the value has been moved out.
// Peer snapshots are acquired while holding the owning lock. Values move by ownership, not by
// shared reference, so T only needs to be Send.
unsafe impl<T: Send> Sync for Shared<T> {}

// Payload transfers and state transitions run no caller code. Callbacks and message destruction
// happen after the associated state is settled and locks are released.
impl<T> RefUnwindSafe for Shared<T> {}

impl<T> Shared<T> {
    pub fn supports(capacity: usize) -> bool {
        capacity > 1
            && capacity <= 4096
            && size_of::<T>()
                .checked_mul(capacity)
                .is_some_and(|bytes| bytes <= 64 * 1024)
    }

    pub fn new(capacity: usize) -> Self {
        assert!((1..=4096).contains(&capacity));
        Self {
            producer: CachePadded::new(Mutex::new(Producer {
                cursor: 0,
                cached_reclaimed: 0,
                reserved: 0,
                waiters: WaitList::new(),
            })),
            consumer: CachePadded::new(Mutex::new(Consumer {
                cursor: 0,
                cached_published: 0,
            })),
            recv_waiters: Mutex::new(WaitList::new()),
            published: CachePadded::new(AtomicUsize::new(0)),
            reclaimed: CachePadded::new(AtomicUsize::new(0)),
            send_waiting: AtomicBool::new(false),
            recv_waiting: AtomicBool::new(false),
            senders_closed: AtomicBool::new(false),
            receivers_closed: AtomicBool::new(false),
            sender_count: AtomicUsize::new(1),
            receiver_count: AtomicUsize::new(1),
            slots: (0..capacity)
                .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
                .collect(),
        }
    }

    fn advance(&self, cursor: usize) -> usize {
        if cursor + 1 == 2 * self.slots.len() {
            0
        } else {
            cursor + 1
        }
    }

    fn index(&self, cursor: usize) -> usize {
        if cursor >= self.slots.len() {
            cursor - self.slots.len()
        } else {
            cursor
        }
    }

    fn queued(&self, producer: &Producer) -> usize {
        if producer.cursor >= producer.cached_reclaimed {
            producer.cursor - producer.cached_reclaimed
        } else {
            2 * self.slots.len() - producer.cached_reclaimed + producer.cursor
        }
    }

    fn has_capacity(&self, producer: &mut Producer) -> bool {
        if self.queued(producer) + producer.reserved == self.slots.len() {
            // Also the capacity recheck after sender registration. The matching consumer cursor
            // publication precedes its SeqCst send_waiting check.
            producer.cached_reclaimed = self.reclaimed.load(Ordering::SeqCst);
        }
        self.queued(producer) + producer.reserved < self.slots.len()
    }

    fn has_value(&self, consumer: &mut Consumer) -> bool {
        if consumer.cursor == consumer.cached_published {
            // Also the message recheck after receiver registration.
            consumer.cached_published = self.published.load(Ordering::SeqCst);
        }
        consumer.cursor != consumer.cached_published
    }

    /// Requires the producer lock and one unreserved capacity unit.
    fn publish(&self, producer: &mut Producer, value: T) {
        let slot = &self.slots[self.index(producer.cursor)];
        // SAFETY: Initially slots are uninitialized. On reuse, the cached reclaimed range was
        // acquired after consumers finished reading it. Held reservations cannot be spent by
        // other producers, and no sender can advance this cursor while its lock is held.
        unsafe { (*slot.get()).write(value) };
        producer.cursor = self.advance(producer.cursor);
        self.published.store(producer.cursor, Ordering::SeqCst);
    }

    fn pop(&self, consumer: &mut Consumer) -> Option<T> {
        if !self.has_value(consumer) {
            return None;
        }
        let slot = &self.slots[self.index(consumer.cursor)];
        // SAFETY: The acquired publication covers this initialized slot. Only this consumer
        // owns its cursor, and reclamation is published after the move, before allowing reuse.
        let value = unsafe { (*slot.get()).assume_init_read() };
        consumer.cursor = self.advance(consumer.cursor);
        self.reclaimed.store(consumer.cursor, Ordering::SeqCst);
        Some(value)
    }

    pub fn clone_sender(&self) {
        self.sender_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn drop_sender(&self) {
        if self.sender_count.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        let mut waiters = {
            let mut waiters = self.recv_waiters.lock();
            self.senders_closed.store(true, Ordering::SeqCst);
            self.recv_waiting.store(false, Ordering::SeqCst);
            mem::replace(&mut *waiters, WaitList::new())
        };
        while let Some(waker) = waiters.notify_one() {
            waker.wake();
        }
    }

    pub fn clone_receiver(&self) {
        self.receiver_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn drop_receiver(&self) {
        if self.receiver_count.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        let mut waiters = {
            let mut producer = self.producer.lock();
            // All earlier sends have finished publication; later ones observe the close.
            self.receivers_closed.store(true, Ordering::SeqCst);
            self.send_waiting.store(false, Ordering::SeqCst);
            mem::replace(&mut producer.waiters, WaitList::new())
        };
        while let Some((id, _)) = waiters.unlink_first_waiter() {
            let SendWaiter::Waiting(waker) = waiters.remove_unlinked_waiter(id) else {
                unreachable!("granted waiters are detached");
            };
            waker.wake();
        }
        drop(waiters);
        self.discard();
    }

    fn discard(&self) {
        let (head, tail) = {
            let mut consumer = self.consumer.lock();
            let tail = self.published.load(Ordering::SeqCst);
            consumer.cached_published = tail;
            (mem::replace(&mut consumer.cursor, tail), tail)
        };
        // SAFETY: The acquired publication covers this range, and the consumer cursor has passed
        // it. Reclamation stays behind it, so no sender can reuse these slots.
        unsafe { drop_range(&self.slots, head, tail) };
    }

    pub fn is_disconnected(&self) -> bool {
        // A live caller keeps its own side open.
        self.senders_closed.load(Ordering::Acquire) || self.receivers_closed.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        !self.has_value(&mut self.consumer.lock())
    }

    fn peek(&self) -> Result<(), TryRecvError> {
        let mut consumer = self.consumer.lock();
        if self.has_value(&mut consumer) {
            Ok(())
        } else if self.senders_closed.load(Ordering::Acquire) {
            // Closing is independent of the cursor lock. Acquire the close before rechecking
            // publication so the final sender's messages cannot be mistaken for a drained queue.
            if self.has_value(&mut consumer) {
                Ok(())
            } else {
                Err(TryRecvError::Disconnected)
            }
        } else {
            Err(TryRecvError::Empty)
        }
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        {
            let mut producer = self.producer.lock();
            if self.receivers_closed.load(Ordering::Relaxed) {
                return Err(TrySendError::Disconnected(value));
            }
            if !producer.waiters.is_empty() || !self.has_capacity(&mut producer) {
                return Err(TrySendError::Full(value));
            }
            self.publish(&mut producer, value);
        }
        self.notify_receiver();
        Ok(())
    }

    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        let value = match self.try_send(value) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Disconnected(value)) => return Err(SendError::new(value)),
            Err(TrySendError::Full(value)) => value,
        };
        let mut value = Some(value);
        // Drop the reservation before the caller's unsent value on cancellation.
        let mut reserve = Reserve {
            shared: self,
            waiter: None,
        };
        let result = poll_fn(|cx| {
            reserve.poll_acquire(cx, |producer| {
                producer.reserved -= 1;
                self.publish(producer, value.take().expect("pending send owns its value"));
            })
        })
        .await;
        match result {
            Ok(()) => {
                self.notify_receiver();
                Ok(())
            }
            Err(_) => Err(SendError::new(
                value.take().expect("failed send owns its value"),
            )),
        }
    }

    pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        let mut producer = self.producer.lock();
        if self.receivers_closed.load(Ordering::Relaxed) {
            return Err(TrySendError::Disconnected(()));
        }
        if !producer.waiters.is_empty() || !self.has_capacity(&mut producer) {
            return Err(TrySendError::Full(()));
        }
        producer.reserved += 1;
        Ok(Permit { shared: self })
    }

    pub async fn reserve(&self) -> Result<Permit<'_, T>, SendError<()>> {
        let mut reserve = Reserve {
            shared: self,
            waiter: None,
        };
        poll_fn(|cx| reserve.poll_acquire(cx, |_| Permit { shared: self })).await
    }

    fn grant_waiters(&self, producer: &mut Producer, wakers: &mut WakerBatch) {
        while !producer.waiters.is_empty() && self.has_capacity(producer) {
            let (_, waiter) = producer
                .waiters
                .unlink_first_waiter()
                .expect("wait queue is not empty");
            let SendWaiter::Waiting(waker) = mem::replace(waiter, SendWaiter::Granted) else {
                unreachable!("granted waiters are detached");
            };
            producer.reserved += 1;
            wakers.push(waker);
        }
        self.send_waiting
            .store(!producer.waiters.is_empty(), Ordering::SeqCst);
    }

    fn notify_sender(&self) {
        if self.send_waiting.load(Ordering::SeqCst) {
            let mut wakers = WakerBatch::new();
            {
                let mut producer = self.producer.lock();
                if !self.receivers_closed.load(Ordering::Relaxed) {
                    self.grant_waiters(&mut producer, &mut wakers);
                }
            }
            wakers.wake_all();
        }
    }

    fn notify_receiver(&self) {
        if self.recv_waiting.load(Ordering::SeqCst) {
            let wake = {
                let mut waiters = self.recv_waiters.lock();
                let wake = waiters.notify_one();
                self.recv_waiting
                    .store(!waiters.is_empty(), Ordering::SeqCst);
                wake
            };
            if let Some(waker) = wake {
                waker.wake();
            }
        }
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let value = {
            let mut consumer = self.consumer.lock();
            match self.pop(&mut consumer) {
                Some(value) => value,
                None if self.senders_closed.load(Ordering::Acquire) => {
                    self.pop(&mut consumer).ok_or(TryRecvError::Disconnected)?
                }
                None => return Err(TryRecvError::Empty),
            }
        };
        self.notify_sender();
        Ok(value)
    }

    pub async fn recv(&self) -> Result<T, RecvError> {
        match self.try_recv() {
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
}

impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        self.discard();
    }
}

pub struct Permit<'a, T> {
    shared: &'a Shared<T>,
}

impl<T> Permit<'_, T> {
    pub fn send(self, value: T) -> Result<(), SendError<T>> {
        let shared = self.shared;
        {
            let mut producer = shared.producer.lock();
            if shared.receivers_closed.load(Ordering::Relaxed) {
                return Err(SendError::new(value));
            }
            producer.reserved -= 1;
            shared.publish(&mut producer, value);
            mem::forget(self);
        }
        shared.notify_receiver();
        Ok(())
    }
}

impl<T> Drop for Permit<'_, T> {
    fn drop(&mut self) {
        let mut wakers = WakerBatch::new();
        {
            let mut producer = self.shared.producer.lock();
            if self.shared.receivers_closed.load(Ordering::Relaxed) {
                return;
            }
            producer.reserved -= 1;
            self.shared.grant_waiters(&mut producer, &mut wakers);
        }
        wakers.wake_all();
    }
}

struct Reserve<'a, T> {
    shared: &'a Shared<T>,
    waiter: Option<WaiterId>,
}

impl<T> Reserve<'_, T> {
    // The callback is internal capacity consumption, never caller-provided code.
    fn poll_acquire<R>(
        &mut self,
        cx: &mut Context<'_>,
        acquired: impl FnOnce(&mut Producer) -> R,
    ) -> Poll<Result<R, SendError<()>>> {
        let mut producer = self.shared.producer.lock();
        if self.shared.receivers_closed.load(Ordering::Relaxed) {
            self.waiter = None;
            return Poll::Ready(Err(SendError::new(())));
        }
        let mut retired = None;
        let mut wakers = WakerBatch::new();
        let granted = self
            .waiter
            .is_some_and(|id| matches!(producer.waiters.waiter_mut(id), SendWaiter::Granted));
        let outcome = if granted {
            producer.waiters.remove(self.waiter.take().unwrap());
            Some(acquired(&mut producer))
        } else if self.waiter.is_none()
            && producer.waiters.is_empty()
            && self.shared.has_capacity(&mut producer)
        {
            producer.reserved += 1;
            Some(acquired(&mut producer))
        } else {
            if let Some(id) = self.waiter {
                let SendWaiter::Waiting(waker) = producer.waiters.waiter_mut(id) else {
                    unreachable!("grants were handled above");
                };
                retired = update_waker(waker, cx.waker());
            } else {
                self.waiter = Some(
                    producer
                        .waiters
                        .push_back(SendWaiter::Waiting(cx.waker().clone())),
                );
            }
            // Publish registration before the SeqCst reclaimed-cursor recheck in grant_waiters.
            self.shared.send_waiting.store(true, Ordering::SeqCst);
            self.shared.grant_waiters(&mut producer, &mut wakers);
            let id = self.waiter.expect("pending acquisition is registered");
            if matches!(producer.waiters.waiter_mut(id), SendWaiter::Granted) {
                producer.waiters.remove(id);
                self.waiter = None;
                Some(acquired(&mut producer))
            } else {
                None
            }
        };
        drop(producer);
        wakers.wake_all();
        drop(retired);
        match outcome {
            Some(value) => Poll::Ready(Ok(value)),
            None => Poll::Pending,
        }
    }
}

impl<T> Drop for Reserve<'_, T> {
    fn drop(&mut self) {
        let Some(id) = self.waiter.take() else { return };
        let mut wakers = WakerBatch::new();
        let retired = {
            let mut producer = self.shared.producer.lock();
            if self.shared.receivers_closed.load(Ordering::Relaxed) {
                return;
            }
            let retired = producer.waiters.remove(id);
            if matches!(retired, SendWaiter::Granted) {
                producer.reserved -= 1;
            }
            self.shared.grant_waiters(&mut producer, &mut wakers);
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
                Ok(()) => {}
                Err(TryRecvError::Disconnected) => {
                    self.unregister();
                    return Poll::Ready(Err(RecvError::Disconnected));
                }
                Err(TryRecvError::Empty) => {
                    if !self.register(cx.waker()) {
                        continue;
                    }
                    if self.shared.peek() == Err(TryRecvError::Empty) {
                        return Poll::Pending;
                    }
                }
            }
            // Leave the wait queue before consuming. No notification may be assigned to this
            // future after it takes another value. Neither hot-path lock is nested in the other.
            self.unregister();
            match self.shared.try_recv() {
                Ok(value) => return Poll::Ready(Ok(value)),
                Err(TryRecvError::Disconnected) => {
                    return Poll::Ready(Err(RecvError::Disconnected));
                }
                Err(TryRecvError::Empty) => {}
            }
        }
    }

    fn register(&mut self, waker: &Waker) -> bool {
        let retired = {
            let mut waiters = self.shared.recv_waiters.lock();
            if self.shared.senders_closed.load(Ordering::Relaxed) {
                self.waiter = None;
                return false;
            }
            let retired = waiters.register(&mut self.waiter, waker);
            self.shared.recv_waiting.store(true, Ordering::SeqCst);
            retired
        };
        drop(retired);
        true
    }

    fn unregister(&mut self) {
        let Some(id) = self.waiter.take() else { return };
        let retired = {
            let mut waiters = self.shared.recv_waiters.lock();
            if self.shared.senders_closed.load(Ordering::Relaxed) {
                return;
            }
            let retired = waiters.remove(id);
            self.shared
                .recv_waiting
                .store(!waiters.is_empty(), Ordering::SeqCst);
            retired
        };
        drop(retired);
    }
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        let Some(id) = self.waiter.take() else { return };
        let (retired, wake) = {
            let mut waiters = self.shared.recv_waiters.lock();
            if self.shared.senders_closed.load(Ordering::Relaxed) {
                return;
            }
            let retired = waiters.remove(id);
            let wake = if matches!(retired, RetryWaiter::Notified)
                && self.shared.has_value(&mut self.shared.consumer.lock())
            {
                waiters.notify_one()
            } else {
                None
            };
            self.shared
                .recv_waiting
                .store(!waiters.is_empty(), Ordering::SeqCst);
            (retired, wake)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        drop(retired);
    }
}

#[cfg(test)]
mod tests;
