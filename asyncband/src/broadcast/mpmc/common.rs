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

//! Storage, cursors, and the receive step shared by the bounded and unbounded MPMC broadcast
//! channels.
//!
//! Both channels retain the same committed backlog and reclaim it the same way; they differ only
//! in what a producer does when that backlog is large. That difference stays in the two channel
//! modules, and so does each channel's own `Recv` future, so neither contract is hidden behind a
//! shared abstraction. What lives here is the state and the one poll step whose waker protocol is
//! subtle enough that a second copy would be a second thing to keep correct.

use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use super::error::RecvError;
use super::error::TryRecvError;
use crate::internal::arena::Arena;
use crate::internal::arena::SlotId;
use crate::internal::mutex::Mutex;
use crate::internal::waitset::WaitSet;
use crate::internal::waitset::WakerToken;
use crate::internal::waitset::wake_all;

/// Retained capacity below which an elastic backlog is never shrunk back.
pub const MIN_RETAINED_CAPACITY: usize = 64;

/// A received message together with the retained prefix that the receive released.
///
/// The two travel together because the caller has to act on both with the channel unlocked, and a
/// bounded channel has to hand the released capacity back before it touches the payload.
pub type Received<T> = (Arc<T>, Reclaimed<T>);

/// Messages removed from the shared buffer and waiting to be dropped after it is unlocked.
///
/// Keeping the first message out of the `Vec` avoids a heap allocation on the common path where
/// one receive reclaims exactly one message.
pub struct Reclaimed<T> {
    first: Option<Arc<T>>,
    rest: Vec<Arc<T>>,
}

impl<T> Reclaimed<T> {
    fn empty() -> Self {
        Self {
            first: None,
            rest: vec![],
        }
    }

    fn first(&self) -> Option<&Arc<T>> {
        self.first.as_ref()
    }

    pub fn is_empty(&self) -> bool {
        self.first.is_none()
    }

    /// The number of retained messages this reclaim released.
    ///
    /// A bounded channel turns this into the capacity it hands back to blocked producers.
    pub fn len(&self) -> usize {
        usize::from(self.first.is_some()) + self.rest.len()
    }
}

/// How a backlog manages the allocation behind its retained messages.
enum Retention {
    /// Grow on demand, and return a burst allocation once a later cycle stays small.
    Elastic {
        /// The largest backlog retained since the buffer was last empty.
        peak_len: usize,
    },
    /// Allocated once for the requested capacity and never shrunk.
    Fixed,
}

/// The committed backlog: every message whose version falls in `[head, tail)`, plus one cursor for
/// each active subscription.
///
/// This is the retention and sequencing machinery that stays private to the channel families.
/// `tail` is the single sequencer: it only advances while the channel lock is held, and a message
/// is placed in `buffer` in the same critical section, so a later publication can never become
/// visible ahead of an earlier one.
pub struct Backlog<T> {
    /// Messages whose versions are in the range `[head, tail)`.
    ///
    /// Each message is held behind an `Arc` so the receive path can move the payload out of the
    /// critical section. Cloning the `Arc` under the lock keeps `T::clone` — and, for reclaimed
    /// messages, `T::drop` — outside it, which matters because both are arbitrary user code that
    /// may call back into this channel.
    buffer: VecDeque<Arc<T>>,
    /// The version of the first message in `buffer`.
    head: u64,
    /// The number of active receivers whose cursor equals `head`.
    head_receivers: usize,
    /// The next message version to assign.
    tail: u64,
    /// Cursor for each active receiver.
    receivers: Arena<u64>,
    retention: Retention,
}

impl<T> Backlog<T> {
    /// A backlog that grows on demand and gives burst allocations back.
    pub fn elastic() -> Self {
        Self::new(VecDeque::new(), Retention::Elastic { peak_len: 0 })
    }

    /// A backlog preallocated for `capacity` retained messages that never shrinks.
    pub fn fixed(capacity: usize) -> Self {
        Self::new(VecDeque::with_capacity(capacity), Retention::Fixed)
    }

    fn new(buffer: VecDeque<Arc<T>>, retention: Retention) -> Self {
        Self {
            buffer,
            head: 0,
            head_receivers: 0,
            tail: 0,
            receivers: Arena::new(),
            retention,
        }
    }

    /// The number of messages the channel currently retains.
    ///
    /// This is the shared backlog kept alive by the slowest active subscription, and it is what a
    /// bounded channel measures its capacity against.
    pub fn retained(&self) -> usize {
        self.buffer.len()
    }

    /// Whether any subscription is active.
    ///
    /// A channel with none retains nothing, so a bounded producer never waits on one.
    pub fn has_receivers(&self) -> bool {
        !self.receivers.is_empty()
    }

    /// The number of messages the subscription registered as `key` can still read.
    pub fn unread(&self, key: SlotId) -> usize {
        let head = *self
            .receivers
            .get(key)
            .expect("active broadcast receiver must be registered");
        usize::try_from(self.tail - head).expect("unread broadcast message count exceeds usize")
    }

    /// Advances the committed tail.
    ///
    /// # Panics
    ///
    /// Panics if the message version counter overflows.
    fn advance_tail(&mut self) {
        self.tail = self
            .tail
            .checked_add(1)
            .expect("broadcast channel version counter overflowed");
    }

    /// Advances the committed tail for a message no subscription can read.
    ///
    /// `head` moves with it so the invariant that `buffer` covers versions `[head, tail)` still
    /// holds without buffering anything. The buffer is already drained when the last receiver was
    /// dropped, so there is nothing to clear here. The caller keeps the payload and drops it after
    /// releasing the channel lock.
    pub fn publish_discarded(&mut self) {
        debug_assert!(!self.has_receivers());
        debug_assert!(self.buffer.is_empty());
        debug_assert_eq!(self.head_receivers, 0);
        self.advance_tail();
        self.head = self.tail;
    }

    /// Advances the committed tail and retains `msg` for every currently active subscription.
    ///
    /// Returns the message when no subscription can read it, so the caller drops it after
    /// releasing the channel lock rather than running `T::drop` inside the critical section.
    ///
    /// # Panics
    ///
    /// Panics if the message version counter overflows.
    #[must_use = "drop the unretained message after releasing the channel lock"]
    pub fn publish(&mut self, msg: Arc<T>) -> Option<Arc<T>> {
        if !self.has_receivers() {
            self.publish_discarded();
            return Some(msg);
        }

        self.publish_retained(msg);
        None
    }

    /// Advances the committed tail and retains `msg`.
    ///
    /// The caller must already have established that a subscription is active, which is what a
    /// bounded channel does anyway to decide between rejecting and discarding.
    ///
    /// # Panics
    ///
    /// Panics if the message version counter overflows.
    pub fn publish_retained(&mut self, msg: Arc<T>) {
        debug_assert!(self.has_receivers());
        self.advance_tail();
        self.buffer.push_back(msg);
        if let Retention::Elastic { peak_len } = &mut self.retention {
            *peak_len = (*peak_len).max(self.buffer.len());
        }
    }

    fn insert_receiver(&mut self, head: u64) -> SlotId {
        if head == self.head {
            self.head_receivers += 1;
        }

        self.receivers.insert(head)
    }

    /// Registers a new subscription at the committed tail.
    ///
    /// A new cursor never lowers `retained()`, so this can never release capacity.
    pub fn subscribe(&mut self) -> SlotId {
        let head = self.tail;
        self.insert_receiver(head)
    }

    pub fn remove_receiver(&mut self, key: SlotId) -> Reclaimed<T> {
        let head = self.receivers.remove(key);

        if head == self.head {
            self.release_head_receiver()
        } else {
            Reclaimed::empty()
        }
    }

    fn release_head_receiver(&mut self) -> Reclaimed<T> {
        self.head_receivers -= 1;

        if self.head_receivers == 0 {
            self.reclaim_consumed()
        } else {
            Reclaimed::empty()
        }
    }

    pub fn receive(&mut self, key: SlotId) -> Option<Received<T>> {
        let head = {
            let cursor = self
                .receivers
                .get_mut(key)
                .expect("active broadcast receiver must be registered");
            if *cursor >= self.tail {
                return None;
            }
            let head = *cursor;
            *cursor += 1;
            head
        };

        debug_assert!(head >= self.head);
        let offset = (head - self.head) as usize;
        let msg = self.buffer[offset].clone();
        let reclaimed = if head == self.head {
            self.release_head_receiver()
        } else {
            Reclaimed::empty()
        };
        // A reclaim triggered by this receive always begins with this receiver's own message: the
        // reclaim path runs only for a cursor sitting at `head`, so the first slot drained is
        // `msg`. `take_msg` relies on this to recognize that it owns the payload.
        debug_assert!(
            reclaimed
                .first()
                .is_none_or(|first| Arc::ptr_eq(first, &msg))
        );
        Some((msg, reclaimed))
    }

    /// Advances `head` to the slowest active cursor and hands the released prefix to the caller.
    ///
    /// `buffer` shrinks here and grows only in [`Backlog::publish`], so this is the one place
    /// `retained()` can fall. A bounded channel therefore accounts for released capacity at
    /// exactly the two call sites that reach this: [`Backlog::receive`] and
    /// [`Backlog::remove_receiver`].
    fn reclaim_consumed(&mut self) -> Reclaimed<T> {
        let mut next_head = self.tail;
        let mut head_receivers = 0;

        for head in self.receivers.values() {
            if *head < next_head {
                next_head = *head;
                head_receivers = 1;
            } else if *head == next_head {
                head_receivers += 1;
            }
        }

        debug_assert!(next_head >= self.head);
        let consumed = usize::try_from(next_head - self.head)
            .expect("retained broadcast message count exceeds usize");
        // Move reclaimed messages out so their Drop impls run after the channel is unlocked. Keep
        // the first one separate so the usual one-message reclaim does not allocate another buffer.
        let first = if consumed == 0 {
            None
        } else {
            self.buffer.pop_front()
        };
        // Reclaiming exactly one message is the overwhelmingly common case — a cursor advances by
        // one at a time — so skip building a `Drain` that would yield nothing.
        let rest = if consumed > 1 {
            self.buffer.drain(..consumed - 1).collect()
        } else {
            vec![]
        };
        let reclaimed = Reclaimed { first, rest };
        debug_assert_eq!(reclaimed.len(), consumed);

        self.head = next_head;
        self.head_receivers = head_receivers;
        self.shrink_buffer();
        reclaimed
    }

    /// Returns the allocation grown for a stalled receiver once that backlog is behind us.
    ///
    /// Without this, a single burst pins its peak allocation for the lifetime of the channel.
    /// The decision is deliberately made only when the buffer drains completely, and against the
    /// peak of the cycle that just ended rather than the current length: a channel that repeatedly
    /// fills and drains keeps a peak as large as its bursts, so it holds its allocation instead of
    /// reallocating on every cycle. Only once a full cycle stays small does the buffer give the
    /// memory back.
    ///
    /// A fixed backlog keeps the allocation it was built with, which is the whole point of asking
    /// for a capacity up front.
    fn shrink_buffer(&mut self) {
        let Retention::Elastic { peak_len } = &mut self.retention else {
            return;
        };

        if !self.buffer.is_empty() {
            return;
        }

        let peak = mem::take(peak_len);
        let capacity = self.buffer.capacity();
        if capacity > MIN_RETAINED_CAPACITY && peak <= capacity / 4 {
            self.buffer.shrink_to(MIN_RETAINED_CAPACITY.max(peak * 2));
        }
    }

    #[cfg(test)]
    pub fn buffer_capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Doctors the sequencer so a test can reach the overflow guard in `publish`.
    #[cfg(test)]
    pub fn set_tail(&mut self, tail: u64) {
        self.tail = tail;
    }
}

/// Buffer, receiver cursors, and parked receivers, all under one lock.
///
/// The wait set lives beside the backlog so that publishing a message and draining the waiters
/// happen in one critical section. That is what makes the park path race-free: a receiver that
/// finds no message and then registers still holds this lock, so a concurrent send cannot slip
/// between the two steps and skip the wake-up.
pub struct Inner<T> {
    pub log: Backlog<T>,
    pub waiters: WaitSet,
}

impl<T> Inner<T> {
    /// Wraps `log` in the channel lock and registers the subscription every constructor hands out
    /// alongside its first sender.
    pub fn with_first_subscription(mut log: Backlog<T>) -> (Mutex<Self>, SlotId) {
        let key = log.subscribe();
        let inner = Mutex::new(Self {
            log,
            waiters: WaitSet::new(),
        });
        (inner, key)
    }
}

/// Wakes every parked receiver so it can observe the channel's disconnected state.
///
/// Both families call this from the last sender's `Drop`.
pub fn disconnect<T>(inner: &Mutex<Inner<T>>) {
    let wakers = {
        let mut inner = inner.lock();
        inner.waiters.drain()
    };
    wake_all(wakers);
}

/// Releases a cancelled receive's waker registration, dropping the waker unlocked.
pub fn unregister<T>(inner: &Mutex<Inner<T>>, token: &mut Option<WakerToken>) {
    let waker = {
        let mut inner = inner.lock();
        inner.waiters.unregister(token)
    };
    drop(waker);
}

/// Receives without waiting, yielding the message and the prefix the receive released.
///
/// The caller owns what happens next: a bounded channel hands the released count back to blocked
/// producers before it touches the payload.
pub fn try_receive<T>(
    inner: &Mutex<Inner<T>>,
    senders: &AtomicUsize,
    key: SlotId,
) -> Result<Received<T>, TryRecvError> {
    // Check this receiver's cursor while holding `inner` before observing the sender count.
    // Senders append messages under the same lock before they can be dropped, so an empty result
    // here means this receiver has no unread buffered message.
    let mut inner = inner.lock();
    match inner.log.receive(key) {
        Some(received) => Ok(received),
        None if senders.load(Ordering::Acquire) == 0 => Err(TryRecvError::Disconnected),
        None => Err(TryRecvError::Empty),
    }
}

/// The one poll step behind `recv` on both channels.
///
/// Buffered messages and repeated polls with the same task waker require no clone. If the pending
/// path needs a new waker, this releases the lock, clones, and repeats the full state check before
/// registration. Senders publish messages and drain waiters under the same lock, so the recheck
/// cannot miss a send, disconnection, or state change made by a reentrant clone callback. The loop
/// executes at most twice.
pub fn poll_receive<T>(
    inner: &Mutex<Inner<T>>,
    senders: &AtomicUsize,
    key: SlotId,
    token: &mut Option<WakerToken>,
    cx: &mut Context<'_>,
) -> Poll<Result<Received<T>, RecvError>> {
    let mut prepared_waker = None;
    loop {
        let mut guard = inner.lock();

        match guard.log.receive(key) {
            Some(received) => {
                drop(guard);
                drop(prepared_waker);
                // Clearing the token without unregistering is safe, and it is what keeps the
                // ready path off a second lock acquisition. A message can only become readable
                // through a publish, and a publish drains the wait set in the same critical
                // section that made the message visible — so any registration this future still
                // held was already taken by that drain, and the token is stale. `Drop` reads the
                // cleared token and skips its own lock for the same reason.
                *token = None;
                return Poll::Ready(Ok(received));
            }
            None => {
                if senders.load(Ordering::Acquire) == 0 {
                    *token = None;
                    drop(guard);
                    drop(prepared_waker);
                    return Poll::Ready(Err(RecvError::Disconnected));
                }

                if prepared_waker.is_none() && guard.waiters.will_wake(token, cx.waker()) {
                    return Poll::Pending;
                }
                let Some(waker) = prepared_waker.take() else {
                    drop(guard);
                    prepared_waker = Some(cx.waker().clone());
                    continue;
                };
                let retired_waker = guard.waiters.register(token, waker);
                drop(guard);
                drop(retired_waker);
                return Poll::Pending;
            }
        }
    }
}

/// Drops the reclaimed backlog, then yields the received message, both with the channel unlocked.
///
/// A non-empty backlog means this receive drained `msg` from the buffer, so once the backlog is
/// dropped this receive holds the only reference and the payload can be moved out instead of
/// cloned. A channel with a single receiver therefore never clones a payload.
///
/// Ownership is decided from that bookkeeping rather than by probing the reference count. An
/// [`Arc::try_unwrap`] on every receive would fail under fan-out, and its failed compare-exchange
/// writes to a cache line that every receiver draining the message shares.
///
/// This runs `T::clone` and `T::drop`, either of which may panic, so a bounded channel must
/// already have released the reclaimed capacity before calling it.
pub fn take_msg<T: Clone>(msg: Arc<T>, reclaimed: Reclaimed<T>) -> T {
    let sole_owner = !reclaimed.is_empty();
    drop(reclaimed);

    if !sole_owner {
        return (*msg).clone();
    }

    // Another receiver can still hold an in-flight reference to the same message, so the clone
    // remains the fallback.
    Arc::try_unwrap(msg).unwrap_or_else(|msg| (*msg).clone())
}
