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

//! Retained backlog buffer and per-slot cursor accounting for bounded broadcast channels.

use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;

use crate::broadcast::mpmc::common::Received;
use crate::internal::arena::Arena;
use crate::internal::arena::SlotId;

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
/// This is the retention and sequencing machinery for the bounded channel. `tail` is the single
/// sequencer: it only advances while the channel lock is held, and a message is placed in `buffer`
/// in the same critical section, so a later publication can never become visible ahead of an
/// earlier one.
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
}

impl<T> Backlog<T> {
    /// A backlog preallocated for `capacity` retained messages that never shrinks.
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(capacity),
            head: 0,
            at_tail: 0,
            tail: 0,
            receivers: Arena::new(),
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
        reclaimed
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
