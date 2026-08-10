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

//! Bounded multicast rings modeled after the LMAX Disruptor sequencer.
//!
//! Unlike a competing-consumer MPMC queue, every subscriber observes every published sequence.
//! Producers reserve ring sequences, write their slots, and publish only the highest contiguous
//! range. Subscriber cursors gate wrap-around, so unread slots are never overwritten.
//!
//! This async-oriented implementation parks tasks with wakers. It intentionally does not expose
//! busy-spin or blocking wait strategies.
//!
//! ~~~
//! use asyncband::channel::disruptor;
//!
//! let capacity = disruptor::Capacity::new(16).unwrap();
//! let (mut publisher, mut subscriber) = disruptor::single_producer::channel(capacity);
//! assert_eq!(publisher.try_publish("event"), Ok(0));
//! assert_eq!(subscriber.try_recv(), Ok((0, "event")));
//! ~~~

use std::cell::Cell;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

pub use crate::channel::RecvError;
pub use crate::channel::SendError;
pub use crate::channel::TryRecvError;
pub use crate::channel::TrySendError;
use crate::channel::wait::WaitQueue;
use crate::channel::wait::wake_all;
use crate::internal::mutex::Mutex;

/// Marker for the single-producer sequencer.
#[doc(hidden)]
pub struct SingleProducer(PhantomData<Cell<()>>);

/// Marker for the multi-producer sequencer.
#[doc(hidden)]
pub struct MultiProducer;

/// A validated non-zero power-of-two ring capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Capacity(NonZeroUsize);

impl Capacity {
    /// Creates a capacity when the value is a non-zero power of two.
    pub const fn new(value: usize) -> Option<Self> {
        if value.is_power_of_two() {
            match NonZeroUsize::new(value) {
                Some(value) => Some(Self(value)),
                None => None,
            }
        } else {
            None
        }
    }

    /// Returns the capacity as a usize.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// A ring publisher parameterized by its producer sequencer.
#[doc(hidden)]
pub struct Publisher<T, Mode> {
    shared: Arc<Shared<T>>,
    marker: PhantomData<Mode>,
}

/// A multicast subscriber that gates ring wrap-around.
#[doc(hidden)]
pub struct Subscriber<T> {
    shared: Arc<Shared<T>>,
    id: u64,
    cursor: u64,
}

struct Shared<T> {
    capacity: usize,
    mask: usize,
    slots: Box<[Mutex<Slot<T>>]>,
    state: Mutex<State>,
}

struct Slot<T> {
    sequence: Option<u64>,
    value: Option<Arc<T>>,
}

struct State {
    next_claim: u64,
    published: u64,
    available: Box<[Option<u64>]>,
    publishers: usize,
    subscribers: HashMap<u64, u64>,
    next_subscriber_id: u64,
    publish_waiters: WaitQueue,
    recv_waiters: WaitQueue,
}

type TryRecvArc<T> = Result<(u64, Arc<T>), TryRecvError>;

fn channel_with_capacity<T, Mode>(capacity: Capacity) -> (Publisher<T, Mode>, Subscriber<T>) {
    let capacity = capacity.get();
    let mut subscribers = HashMap::new();
    subscribers.insert(0, 0);
    let slots = (0..capacity)
        .map(|_| {
            Mutex::new(Slot {
                sequence: None,
                value: None,
            })
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let shared = Arc::new(Shared {
        capacity,
        mask: capacity - 1,
        slots,
        state: Mutex::new(State {
            next_claim: 0,
            published: 0,
            available: vec![None; capacity].into_boxed_slice(),
            publishers: 1,
            subscribers,
            next_subscriber_id: 1,
            publish_waiters: WaitQueue::default(),
            recv_waiters: WaitQueue::default(),
        }),
    });
    (
        Publisher {
            shared: shared.clone(),
            marker: PhantomData,
        },
        Subscriber {
            shared,
            id: 0,
            cursor: 0,
        },
    )
}

impl<T, Mode> fmt::Debug for Publisher<T, Mode> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Publisher")
            .field("capacity", &self.shared.capacity)
            .field("cursor", &self.cursor())
            .field("remaining_capacity", &self.remaining_capacity())
            .finish()
    }
}

impl<T> Clone for Publisher<T, MultiProducer> {
    fn clone(&self) -> Self {
        let mut state = self.shared.state.lock();
        state.publishers = state
            .publishers
            .checked_add(1)
            .expect("disruptor publisher count overflow");
        drop(state);
        Self {
            shared: self.shared.clone(),
            marker: PhantomData,
        }
    }
}

impl<T, Mode> Publisher<T, Mode> {
    /// Creates a subscriber starting at the highest contiguous published sequence.
    pub fn subscribe(&self) -> Subscriber<T> {
        let mut state = self.shared.state.lock();
        let id = state.allocate_subscriber_id();
        let cursor = state.published;
        state.subscribers.insert(id, cursor);
        drop(state);
        Subscriber {
            shared: self.shared.clone(),
            id,
            cursor,
        }
    }

    /// Returns the next sequence after the highest contiguous publication.
    pub fn cursor(&self) -> u64 {
        self.shared.state.lock().published
    }

    /// Returns the number of sequences that may currently be reserved.
    pub fn remaining_capacity(&self) -> usize {
        let state = self.shared.state.lock();
        state.remaining_capacity(self.shared.capacity)
    }
}

macro_rules! publish_methods {
    ($mode:ty, $this:ident, $($receiver:tt)+) => {
        impl<T> Publisher<T, $mode> {
            /// Publishes a value, waiting until every subscriber has released its ring slot.
            pub async fn publish($($receiver)+, value: T) -> Result<u64, SendError<T>> {
                Publish {
                    shared: &$this.shared,
                    value: Some(value),
                    waiter: None,
                    completed: false,
                }
                .await
            }

            /// Attempts to reserve and publish a value without waiting.
            pub fn try_publish($($receiver)+, value: T) -> Result<u64, TrySendError<T>> {
                $this.shared.try_publish(value)
            }
        }
    };
}

publish_methods!(SingleProducer, self, &mut self);
publish_methods!(MultiProducer, self, &self);

impl<T, Mode> Drop for Publisher<T, Mode> {
    fn drop(&mut self) {
        let wakers = {
            let mut state = self.shared.state.lock();
            state.publishers -= 1;
            if state.publishers == 0 {
                state.recv_waiters.take_all()
            } else {
                Vec::new()
            }
        };
        wake_all(wakers);
    }
}

impl<T> Clone for Subscriber<T> {
    fn clone(&self) -> Self {
        let mut state = self.shared.state.lock();
        let id = state.allocate_subscriber_id();
        state.subscribers.insert(id, self.cursor);
        drop(state);
        Self {
            shared: self.shared.clone(),
            id,
            cursor: self.cursor,
        }
    }
}

impl<T> fmt::Debug for Subscriber<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscriber")
            .field("cursor", &self.cursor)
            .field("disconnected", &self.is_disconnected())
            .finish()
    }
}

impl<T: Clone> Subscriber<T> {
    /// Receives the next published sequence and value.
    pub async fn recv(&mut self) -> Result<(u64, T), RecvError> {
        Recv {
            subscriber: self,
            waiter: None,
            completed: false,
        }
        .await
    }

    /// Attempts to receive the next published sequence and value without waiting.
    pub fn try_recv(&mut self) -> Result<(u64, T), TryRecvError> {
        self.try_recv_arc()
            .map(|(sequence, value)| (sequence, (*value).clone()))
    }
}

impl<T> Subscriber<T> {
    /// Returns the next sequence this subscriber will receive.
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Returns true if every publisher has been dropped.
    pub fn is_disconnected(&self) -> bool {
        self.shared.state.lock().publishers == 0
    }

    fn try_recv_arc(&mut self) -> TryRecvArc<T> {
        let (result, wakers) = {
            let mut state = self.shared.state.lock();
            if self.cursor < state.published {
                let sequence = self.cursor;
                let slot = self.shared.slots[sequence as usize & self.shared.mask].lock();
                assert_eq!(
                    slot.sequence,
                    Some(sequence),
                    "a published disruptor sequence must occupy its gated slot"
                );
                let value = slot
                    .value
                    .as_ref()
                    .expect("a published disruptor slot contains a value")
                    .clone();
                drop(slot);
                self.cursor += 1;
                state.subscribers.insert(self.id, self.cursor);
                (Ok((sequence, value)), state.publish_waiters.take_all())
            } else if state.publishers == 0 {
                (Err(TryRecvError::Disconnected), Vec::new())
            } else {
                (Err(TryRecvError::Empty), Vec::new())
            }
        };
        wake_all(wakers);
        result
    }
}

impl<T> Drop for Subscriber<T> {
    fn drop(&mut self) {
        let wakers = {
            let mut state = self.shared.state.lock();
            state.subscribers.remove(&self.id);
            state.publish_waiters.take_all()
        };
        wake_all(wakers);
    }
}

impl<T> Shared<T> {
    fn try_publish(&self, value: T) -> Result<u64, TrySendError<T>> {
        let sequence = {
            let mut state = self.state.lock();
            if state.subscribers.is_empty() {
                return Err(TrySendError::Disconnected(value));
            }
            if state.remaining_capacity(self.capacity) == 0 {
                return Err(TrySendError::Full(value));
            }
            state.claim()
        };
        self.finish_publish(sequence, value);
        Ok(sequence)
    }

    fn finish_publish(&self, sequence: u64, value: T) {
        let index = sequence as usize & self.mask;
        let replaced = {
            let mut slot = self.slots[index].lock();
            let replaced = slot.value.replace(Arc::new(value));
            slot.sequence = Some(sequence);
            replaced
        };

        let wakers = {
            let mut state = self.state.lock();
            state.available[index] = Some(sequence);
            let before = state.published;
            while state.available[state.published as usize & self.mask] == Some(state.published) {
                state.published += 1;
            }
            if state.published != before {
                state.recv_waiters.take_all()
            } else {
                Vec::new()
            }
        };
        wake_all(wakers);
        drop(replaced);
    }
}

impl State {
    fn claim(&mut self) -> u64 {
        let sequence = self.next_claim;
        self.next_claim = self
            .next_claim
            .checked_add(1)
            .expect("disruptor sequence overflow");
        sequence
    }

    fn remaining_capacity(&self, capacity: usize) -> usize {
        let gating = self
            .subscribers
            .values()
            .copied()
            .min()
            .unwrap_or(self.next_claim);
        let used = usize::try_from(self.next_claim - gating)
            .expect("the gated disruptor range fits in memory");
        capacity - used
    }

    fn allocate_subscriber_id(&mut self) -> u64 {
        loop {
            let id = self.next_subscriber_id;
            self.next_subscriber_id = self.next_subscriber_id.wrapping_add(1);
            if !self.subscribers.contains_key(&id) {
                return id;
            }
        }
    }
}

struct Publish<'a, T> {
    shared: &'a Shared<T>,
    value: Option<T>,
    waiter: Option<u64>,
    completed: bool,
}

impl<T> Unpin for Publish<'_, T> {}

impl<T> Future for Publish<'_, T> {
    type Output = Result<u64, SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let mut retired_wakers = Vec::new();
        let sequence = {
            let mut state = this.shared.state.lock();
            if state.subscribers.is_empty() {
                retired_wakers.extend(state.publish_waiters.remove(&mut this.waiter));
                this.completed = true;
                return Poll::Ready(Err(SendError::new(
                    this.value
                        .take()
                        .expect("an incomplete publication owns its value"),
                )));
            }
            if state.remaining_capacity(this.shared.capacity) == 0 {
                retired_wakers.extend(state.publish_waiters.register(&mut this.waiter, cx.waker()));
                return Poll::Pending;
            }
            retired_wakers.extend(state.publish_waiters.remove(&mut this.waiter));
            state.claim()
        };

        drop(retired_wakers);
        this.shared.finish_publish(
            sequence,
            this.value
                .take()
                .expect("an incomplete publication owns its value"),
        );
        this.completed = true;
        Poll::Ready(Ok(sequence))
    }
}

impl<T> Drop for Publish<'_, T> {
    fn drop(&mut self) {
        if !self.completed {
            let retired_waker = {
                let mut state = self.shared.state.lock();
                state.publish_waiters.remove(&mut self.waiter)
            };
            drop(retired_waker);
        }
    }
}

struct Recv<'a, T> {
    subscriber: &'a mut Subscriber<T>,
    waiter: Option<u64>,
    completed: bool,
}

impl<T: Clone> Future for Recv<'_, T> {
    type Output = Result<(u64, T), RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let result = this.subscriber.try_recv_arc();
        let poll = match result {
            Ok((sequence, value)) => {
                this.waiter = None;
                Poll::Ready(Ok((sequence, (*value).clone())))
            }
            Err(TryRecvError::Disconnected) => {
                this.waiter = None;
                Poll::Ready(Err(RecvError::Disconnected))
            }
            Err(TryRecvError::Empty) => {
                let mut state = this.subscriber.shared.state.lock();
                let retired_waker =
                    if this.subscriber.cursor < state.published || state.publishers == 0 {
                        this.waiter = None;
                        drop(state);
                        cx.waker().wake_by_ref();
                        None
                    } else {
                        state.recv_waiters.register(&mut this.waiter, cx.waker())
                    };
                drop(retired_waker);
                Poll::Pending
            }
        };
        if poll.is_ready() {
            this.completed = true;
        }
        poll
    }
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        if !self.completed {
            let retired_waker = {
                let mut state = self.subscriber.shared.state.lock();
                state.recv_waiters.remove(&mut self.waiter)
            };
            drop(retired_waker);
        }
    }
}

/// A ring with one statically enforced publisher.
pub mod single_producer {
    /// The single publisher endpoint.
    pub type Publisher<T> = super::Publisher<T, super::SingleProducer>;
    /// A multicast subscriber endpoint.
    pub type Subscriber<T> = super::Subscriber<T>;

    /// Creates a single-producer Disruptor ring.
    pub fn channel<T>(capacity: super::Capacity) -> (Publisher<T>, Subscriber<T>) {
        super::channel_with_capacity(capacity)
    }
}

/// A ring with cloneable concurrent publishers.
pub mod multi_producer {
    /// A concurrent publisher endpoint.
    pub type Publisher<T> = super::Publisher<T, super::MultiProducer>;
    /// A multicast subscriber endpoint.
    pub type Subscriber<T> = super::Subscriber<T>;

    /// Creates a multi-producer Disruptor ring.
    pub fn channel<T>(capacity: super::Capacity) -> (Publisher<T>, Subscriber<T>) {
        super::channel_with_capacity(capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::MultiProducer;
    use super::channel_with_capacity;
    use crate::channel::TryRecvError;

    #[test]
    fn multi_producer_only_exposes_contiguous_publications() {
        let capacity = super::Capacity::new(4).unwrap();
        let (publisher, mut subscriber) = channel_with_capacity::<_, MultiProducer>(capacity);
        let first = {
            let mut state = publisher.shared.state.lock();
            state.claim()
        };
        let second = {
            let mut state = publisher.shared.state.lock();
            state.claim()
        };

        publisher.shared.finish_publish(second, 2);
        assert_eq!(publisher.cursor(), 0);
        assert_eq!(subscriber.try_recv(), Err(TryRecvError::Empty));

        publisher.shared.finish_publish(first, 1);
        assert_eq!(publisher.cursor(), 2);
        assert_eq!(subscriber.try_recv(), Ok((0, 1)));
        assert_eq!(subscriber.try_recv(), Ok((1, 2)));
    }
}
