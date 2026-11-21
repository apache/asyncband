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

//! Storage, cursors, and receiving logic shared by the bounded and unbounded MPMC broadcast
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
use std::task::Waker;

use crate::broadcast::mpmc::error::RecvError;
use crate::broadcast::mpmc::error::TryRecvError;
use crate::internal::arena::Arena;
use crate::internal::arena::SlotId;
use crate::internal::mutex::Mutex;
use crate::internal::wakerset::WakerSet;
use crate::internal::wakerset::WakerToken;

/// Retained capacity below which an elastic backlog is never shrunk back.
pub const MIN_RETAINED_CAPACITY: usize = 64;

/// A received message together with whether advancing the cursor vacated the head slot.
///
/// The two travel together because the caller acts on both with the channel unlocked: a bounded
/// channel releases the freed slot before touching the payload, and an owning receive uses
/// `reclaimed` to decide whether it can unwrap the `Arc` instead of cloning.
pub struct Received<T> {
    pub msg: Arc<T>,
    pub reclaimed: bool,
}

/// Messages removed from the shared buffer when a lagging subscription is dropped, waiting to be
/// dropped after the channel is unlocked.
///
/// Keeping the first message out of the `Vec` avoids a heap allocation when removing a
/// subscription reclaims only one message.
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

    #[cfg(test)]
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

/// A retained message together with the number of receiver cursors positioned at its version.
///
/// While a cursor sits on a version the message stays readable, so once the count reaches zero the
/// head can advance past the slot. Tracking the count per slot is what lets reclaim release the
/// invisible prefix directly instead of scanning every subscription for the slowest cursor.
struct Slot<T> {
    msg: Arc<T>,
    /// The number of receivers whose next read is this message.
    cursors: usize,
}

/// The committed backlog: every message whose version falls in `[head, tail)`, plus one cursor for
/// each active subscription.
///
/// This is the retention and sequencing machinery that stays private to the channel families.
/// `tail` is the single sequencer: it only advances while the channel lock is held, and a message
/// is placed in `buffer` in the same critical section, so a later publication can never become
/// visible ahead of an earlier one.
pub struct Backlog<T> {
    /// Messages whose versions are in the range `[head, tail)`, each with its cursor count.
    ///
    /// Each message is held behind an `Arc` so a receiver can move the payload out of the
    /// critical section. Cloning the `Arc` under the lock keeps `T::clone` — and, for reclaimed
    /// messages, `T::drop` — outside it, which matters because both are arbitrary user code that
    /// may call back into this channel.
    buffer: VecDeque<Slot<T>>,
    /// The version of the first message in `buffer`.
    head: u64,
    /// The number of receivers whose cursor equals `tail`.
    ///
    /// Caught-up cursors have no buffered slot to count against, so they are tallied here. Every
    /// cursor is counted exactly once: either in one slot's `cursors` or in `at_tail`.
    at_tail: usize,
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

    fn new(buffer: VecDeque<Slot<T>>, retention: Retention) -> Self {
        Self {
            buffer,
            head: 0,
            at_tail: 0,
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

    /// Publishes one version, taking the payload only if a subscription can read it.
    ///
    /// The caller owns the payload through the overflow check and drops any unretained value
    /// after unlocking. Conversion to an Arc is deferred until the value will be retained.
    pub fn publish<P>(&mut self, payload: &mut Option<P>, into_msg: impl FnOnce(P) -> Arc<T>) {
        self.tail = self
            .tail
            .checked_add(1)
            .expect("broadcast channel version counter overflowed");
        if !self.has_receivers() {
            debug_assert!(self.buffer.is_empty());
            debug_assert_eq!(self.at_tail, 0);
            self.head = self.tail;
            return;
        }

        let msg = into_msg(payload.take().expect("a publication must have a payload"));
        // Every cursor that was caught up now has this message as its next read.
        let cursors = mem::take(&mut self.at_tail);
        self.buffer.push_back(Slot { msg, cursors });
        if let Retention::Elastic { peak_len } = &mut self.retention {
            *peak_len = (*peak_len).max(self.buffer.len());
        }
    }

    /// Registers a new subscription at the committed tail.
    ///
    /// A new cursor never lowers `retained()`, so this can never release capacity.
    pub fn subscribe(&mut self) -> SlotId {
        self.at_tail += 1;
        self.receivers.insert(self.tail)
    }

    pub fn remove_receiver(&mut self, key: SlotId) -> Reclaimed<T> {
        let cursor = self.receivers.remove(key);
        if cursor == self.tail {
            self.at_tail -= 1;
            return Reclaimed::empty();
        }

        let offset = (cursor - self.head) as usize;
        let slot = &mut self.buffer[offset];
        slot.cursors -= 1;
        if offset == 0 && slot.cursors == 0 {
            self.reclaim_vacated()
        } else {
            Reclaimed::empty()
        }
    }

    pub fn receive(&mut self, key: SlotId) -> Option<Received<T>> {
        let version = {
            let cursor = self
                .receivers
                .get_mut(key)
                .expect("active broadcast receiver must be registered");
            if *cursor >= self.tail {
                return None;
            }
            let version = *cursor;
            *cursor += 1;
            version
        };

        debug_assert!(version >= self.head);
        let offset = (version - self.head) as usize;
        // Count the cursor at its next version before it leaves this one, so a reclaim triggered
        // by leaving `head` stops at the message this receiver reads next.
        if version + 1 == self.tail {
            self.at_tail += 1;
        } else {
            self.buffer[offset + 1].cursors += 1;
        }

        let slot = &mut self.buffer[offset];
        slot.cursors -= 1;
        let received = if offset == 0 && slot.cursors == 0 {
            // Moving the cursor onto `version + 1` above guarantees the next slot, if any, has a
            // non-zero cursor count. A receive therefore vacates only `head` itself and can hand
            // the slot's `Arc` straight to the caller without touching its reference count.
            let msg = self.buffer.pop_front().expect("head slot must exist").msg;
            self.head += 1;
            debug_assert!(self.head <= self.tail);
            self.shrink_buffer();
            Received {
                msg,
                reclaimed: true,
            }
        } else {
            Received {
                msg: slot.msg.clone(),
                reclaimed: false,
            }
        };
        Some(received)
    }

    /// Releases the prefix no cursor can still read when a subscription is removed at `head`, and
    /// hands the messages to the caller to drop outside the channel lock.
    ///
    /// The head slot's cursor count has just reached zero, so every message up to the next counted
    /// slot is invisible: each cursor is counted in exactly one place, and none of those places is
    /// a slot being popped. Popping the zero-count prefix replaces the old scan over every
    /// subscription for the slowest cursor, so advancing the head costs the messages released
    /// instead of the receivers subscribed.
    fn reclaim_vacated(&mut self) -> Reclaimed<T> {
        debug_assert!(self.buffer.front().is_some_and(|slot| slot.cursors == 0));

        // Move reclaimed messages out so their Drop impls run after the channel is unlocked. Keep
        // the first one separate so reclaiming a single message does not allocate, and skip
        // building a `Drain` that would yield nothing. A bulk reclaim counts the zero-count prefix
        // up front so it moves out in one drain instead of growing a vector geometrically, which
        // measured 15% slower for a 32-message backlog.
        let first = self.buffer.pop_front().map(|slot| slot.msg);
        let extra = self
            .buffer
            .iter()
            .take_while(|slot| slot.cursors == 0)
            .count();
        let rest = if extra == 0 {
            vec![]
        } else {
            self.buffer.drain(..extra).map(|slot| slot.msg).collect()
        };
        let reclaimed = Reclaimed { first, rest };

        self.head += reclaimed.len() as u64;
        debug_assert!(self.head <= self.tail);
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
        assert!(self.buffer.is_empty() && !self.has_receivers());
        self.head = tail;
        self.tail = tail;
    }

    /// Asserts the cursor accounting invariants: every cursor is counted exactly once, either in
    /// one slot's `cursors` or in `at_tail`, and `buffer` covers exactly `[head, tail)`.
    #[cfg(test)]
    pub fn assert_cursor_accounting(&self) {
        let in_slots: usize = self.buffer.iter().map(|slot| slot.cursors).sum();
        assert_eq!(in_slots + self.at_tail, self.receivers.len());
        assert_eq!(self.tail - self.head, self.buffer.len() as u64);
    }
}

/// Buffer, receiver cursors, and parked channel waiters, all under one lock.
///
/// Checking the backlog and registering a waker share this lock with publishing and detaching
/// registrations, so neither messages nor freed capacity can slip between a check and
/// registration.
pub struct Inner<T, S = ()> {
    pub log: Backlog<T>,
    pub waiters: WakerSet,
    pub send_waiters: S,
}

impl<T, S> Inner<T, S> {
    /// Wraps `log` in the channel lock and registers the subscription every constructor hands out
    /// alongside its first sender.
    pub fn with_first_subscription(mut log: Backlog<T>, send_waiters: S) -> (Mutex<Self>, SlotId) {
        let key = log.subscribe();
        let inner = Mutex::new(Self {
            log,
            waiters: WakerSet::new(),
            send_waiters,
        });
        (inner, key)
    }

    /// Receives without waiting while holding the channel lock.
    #[inline]
    pub fn try_receive(
        &mut self,
        senders: &AtomicUsize,
        key: SlotId,
    ) -> Result<Received<T>, TryRecvError> {
        // Check this receiver's cursor while holding `inner` before observing the sender count.
        // Senders append messages under the same lock before they can be dropped, so an empty
        // result here means this receiver has no unread buffered message.
        match self.log.receive(key) {
            Some(received) => Ok(received),
            None if senders.load(Ordering::Acquire) == 0 => Err(TryRecvError::Disconnected),
            None => Err(TryRecvError::Empty),
        }
    }

    /// Polls the shared receiving logic while holding the channel lock, returning any replaced
    /// waker for unlocked destruction.
    #[inline]
    #[must_use = "drop the returned waker after releasing the channel lock"]
    pub fn poll_receive(
        &mut self,
        senders: &AtomicUsize,
        key: SlotId,
        token: &mut Option<WakerToken>,
        cx: &mut Context<'_>,
    ) -> (Poll<Result<Received<T>, RecvError>>, Option<Waker>) {
        match self.log.receive(key) {
            Some(received) => {
                *token = None;
                (Poll::Ready(Ok(received)), None)
            }
            None if senders.load(Ordering::Acquire) == 0 => {
                *token = None;
                (Poll::Ready(Err(RecvError::Disconnected)), None)
            }
            None => {
                let retired_waker = self.waiters.register(token, cx.waker());
                (Poll::Pending, retired_waker)
            }
        }
    }
}

/// Wakes every parked receiver so it can observe the channel's disconnected state.
///
/// Both families call this from the last sender's `Drop`.
pub fn disconnect<T, S>(inner: &Mutex<Inner<T, S>>) {
    let wakers = mem::take(&mut inner.lock().waiters);
    wakers.wake_all();
}

/// Removes a cancelled `recv` future's waker registration, dropping the waker after unlocking.
pub fn unregister<T, S>(
    inner: &Mutex<Inner<T, S>>,
    senders: &AtomicUsize,
    key: SlotId,
    token: &mut Option<WakerToken>,
) {
    let mut inner = inner.lock();
    if inner.log.unread(key) != 0 || senders.load(Ordering::Acquire) == 0 {
        // Publication or disconnection detached this registration under the channel lock.
        *token = None;
        return;
    }

    let waker = inner.waiters.unregister(token);
    drop(inner);
    drop(waker);
}

/// Receives without waiting on an unbounded channel.
pub fn try_receive<T>(
    inner: &Mutex<Inner<T>>,
    senders: &AtomicUsize,
    key: SlotId,
) -> Result<Received<T>, TryRecvError> {
    inner.lock().try_receive(senders, key)
}

/// Polls the shared receiving logic on an unbounded channel.
pub fn poll_receive<T>(
    inner: &Mutex<Inner<T>>,
    senders: &AtomicUsize,
    key: SlotId,
    token: &mut Option<WakerToken>,
    cx: &mut Context<'_>,
) -> Poll<Result<Received<T>, RecvError>> {
    let (poll, retired_waker) = inner.lock().poll_receive(senders, key, token, cx);
    drop(retired_waker);
    poll
}

/// Yields the received message owned with the channel unlocked.
///
/// When `received.reclaimed` is true, advancing the cursor popped the message from `buffer`, so
/// the caller may hold the only remaining reference and move the payload out instead of cloning
/// it.
///
/// Ownership is decided from that bookkeeping rather than by probing the reference count on every
/// receive. Calling [`Arc::unwrap_or_clone`] while the slot is still retained would fail under
/// fan-out, and its failed compare-exchange writes to a cache line that every receiver draining
/// the message shares.
///
/// This may run `T::clone` and `T::drop`, either of which can panic, so a bounded channel must
/// already have released the reclaimed capacity before calling it.
pub fn take_msg<T: Clone>(received: Received<T>) -> T {
    if !received.reclaimed {
        return (*received.msg).clone();
    }

    // Another receiver can still hold an in-flight reference to the same message, or an `Arc` it
    // kept from a shared receive, so the clone remains the fallback.
    Arc::unwrap_or_clone(received.msg)
}

#[cfg(test)]
pub mod tests {
    use std::cell::Cell;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::rc::Rc;

    use super::*;

    pub struct InspectOnDrop(Box<dyn Fn()>);

    impl Drop for InspectOnDrop {
        fn drop(&mut self) {
            (self.0)();
        }
    }

    pub fn assert_overflow_drops_unlocked(
        inspect: impl Fn() -> Option<usize> + 'static,
        send: impl FnOnce(InspectOnDrop),
    ) {
        let observed = Rc::new(Cell::new(None));
        let result = observed.clone();
        let payload = InspectOnDrop(Box::new(move || result.set(inspect())));
        let overflow = catch_unwind(AssertUnwindSafe(|| send(payload)));
        assert!(overflow.is_err());
        assert_eq!(
            observed.get(),
            Some(0),
            "destructor could not inspect the backlog"
        );
    }

    fn publish(log: &mut Backlog<u64>, value: u64) -> Option<u64> {
        let mut payload = Some(value);
        log.publish(&mut payload, Arc::new);
        payload
    }

    /// Drives every cursor-count mutation point — subscribing, publishing, receiving, and dropping
    /// — and checks the accounting after each step.
    #[test]
    fn cursor_accounting_holds_across_subscribe_receive_and_drop() {
        let mut log = Backlog::<u64>::elastic();

        let a = log.subscribe();
        let b = log.subscribe();
        log.assert_cursor_accounting();

        // The first publication picks up the caught-up cursors; a subscription registered
        // mid-backlog starts at the tail and counts against the next publication.
        assert!(publish(&mut log, 1).is_none());
        let c = log.subscribe();
        assert!(publish(&mut log, 2).is_none());
        assert!(publish(&mut log, 3).is_none());
        log.assert_cursor_accounting();

        assert_eq!(*log.receive(a).unwrap().msg, 1);
        log.assert_cursor_accounting();

        // Dropping a subscription parked mid-backlog only decrements that slot and reclaims
        // nothing.
        assert!(log.remove_receiver(c).is_empty());
        log.assert_cursor_accounting();

        // `a` drains to the tail, leaving zero-count slots behind the lagging `b`.
        assert_eq!(*log.receive(a).unwrap().msg, 2);
        assert_eq!(*log.receive(a).unwrap().msg, 3);
        log.assert_cursor_accounting();
        assert!(log.receive(a).is_none());

        // Dropping a caught-up subscription reclaims nothing either.
        let d = log.subscribe();
        assert!(log.remove_receiver(d).is_empty());
        log.assert_cursor_accounting();

        // Dropping the lagging subscription releases the whole prefix it held back in one step.
        assert_eq!(log.remove_receiver(b).len(), 3);
        log.assert_cursor_accounting();

        // A call to `receive` that catches up to the tail releases the head slot and moves the
        // cursor back to `at_tail`.
        assert!(publish(&mut log, 4).is_none());
        let received = log.receive(a).unwrap();
        assert_eq!(*received.msg, 4);
        assert!(received.reclaimed);
        log.assert_cursor_accounting();

        // With no subscription left, a publication is discarded and nothing is counted.
        assert!(log.remove_receiver(a).is_empty());
        assert_eq!(publish(&mut log, 5), Some(5));
        log.assert_cursor_accounting();
    }
}
