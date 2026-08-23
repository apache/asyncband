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

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::channel::error::RecvError;
use crate::channel::error::SendError;
use crate::channel::error::TryRecvError;
use crate::channel::error::TrySendError;
use crate::internal::arena::Arena;
use crate::internal::arena::SlotId;
use crate::internal::mutex::Mutex;
use crate::internal::waitset::WaitSet;
use crate::internal::waitset::WakerToken;

struct Reclaimed<T> {
    first: Option<Arc<T>>,
    // Boxing the uncommon multi-value tail keeps the zero/one-value hot path to two words.
    #[allow(clippy::box_collection)]
    _rest: Option<Box<Vec<Arc<T>>>>,
}

impl<T> Reclaimed<T> {
    fn none() -> Self {
        Self {
            first: None,
            _rest: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.first.is_none()
    }

    fn first(&self) -> Option<&Arc<T>> {
        self.first.as_ref()
    }
}

#[derive(Clone, Copy)]
enum Retention {
    Bounded(usize),
    Unbounded,
}

pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    debug_assert!(capacity > 0);
    channel(Retention::Bounded(capacity))
}

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    channel(Retention::Unbounded)
}

fn channel<T>(retention: Retention) -> (Sender<T>, Receiver<T>) {
    let mut receivers = Receivers::new();
    let key = receivers.insert(0);
    let buffer = match retention {
        Retention::Bounded(capacity) => VecDeque::with_capacity(capacity),
        Retention::Unbounded => VecDeque::new(),
    };
    let shared = Arc::new(Shared {
        retention,
        inner: Mutex::new(Inner {
            buffer,
            head: 0,
            head_receivers: 1,
            tail: 0,
            receivers,
            send_waiters: WaitSet::new(),
            recv_waiters: WaitSet::new(),
        }),
        senders: AtomicUsize::new(1),
    });
    (
        Sender {
            shared: shared.clone(),
        },
        Receiver {
            shared,
            key,
            cursor: 0,
        },
    )
}

struct Shared<T> {
    retention: Retention,
    inner: Mutex<Inner<T>>,
    senders: AtomicUsize,
}

struct Inner<T> {
    /// Messages with sequence numbers in `[head, tail)`.
    ///
    /// `Arc` lets cursor bookkeeping finish under the lock while user-defined `Clone` and `Drop`
    /// implementations run after unlocking.
    buffer: VecDeque<Arc<T>>,
    head: u64,
    /// Number of live subscriptions currently equal to `head`.
    ///
    /// Only the last one to advance scans all live cursors and reclaims the common prefix.
    head_receivers: usize,
    tail: u64,
    receivers: Receivers,
    send_waiters: WaitSet,
    recv_waiters: WaitSet,
}

/// Stable receiver keys backed by a dense cursor list.
///
/// The dense list keeps reclaim scans proportional to the number of live subscriptions instead of
/// the arena's historical high-water mark. Arena slots map stable endpoint keys into that list.
struct Receivers {
    slots: Arena<usize>,
    active: Vec<ReceiverCursor>,
}

struct ReceiverCursor {
    key: SlotId,
    sequence: u64,
}

impl Receivers {
    fn new() -> Self {
        Self {
            slots: Arena::new(),
            active: Vec::new(),
        }
    }

    fn insert(&mut self, sequence: u64) -> SlotId {
        let active_index = self.active.len();
        let key = self.slots.insert(active_index);
        self.active.push(ReceiverCursor { key, sequence });
        key
    }

    fn set_sequence(&mut self, key: SlotId, sequence: u64) {
        let active_index = *self
            .slots
            .get(key)
            .expect("active broadcast receiver must be registered");
        let receiver = &mut self.active[active_index];
        debug_assert_eq!(receiver.sequence + 1, sequence);
        receiver.sequence = sequence;
    }

    fn remove(&mut self, key: SlotId) -> u64 {
        let active_index = self.slots.remove(key);
        let removed = self.active.swap_remove(active_index);
        debug_assert_eq!(removed.key, key);
        if let Some(moved) = self.active.get(active_index) {
            *self
                .slots
                .get_mut(moved.key)
                .expect("active broadcast receiver must be registered") = active_index;
        }
        removed.sequence
    }

    fn len(&self) -> usize {
        self.active.len()
    }

    fn is_empty(&self) -> bool {
        self.active.is_empty()
    }

    fn sequences(&self) -> impl Iterator<Item = u64> + '_ {
        self.active.iter().map(|receiver| receiver.sequence)
    }
}

impl<T> Inner<T> {
    fn insert_receiver(&mut self, cursor: u64) -> SlotId {
        if cursor == self.head {
            self.head_receivers += 1;
        }
        self.receivers.insert(cursor)
    }

    fn remove_receiver(&mut self, key: SlotId) -> Reclaimed<T> {
        let cursor = self.receivers.remove(key);
        if cursor == self.head {
            self.release_head_receiver()
        } else {
            Reclaimed::none()
        }
    }

    fn receive(&mut self, key: SlotId, cursor: u64) -> Option<(Arc<T>, Reclaimed<T>)> {
        if cursor == self.tail {
            return None;
        }

        debug_assert!(cursor >= self.head);
        let offset = usize::try_from(cursor - self.head)
            .expect("retained broadcast message count exceeds usize");
        let value = self.buffer[offset].clone();
        self.receivers.set_sequence(key, cursor + 1);
        let reclaimed = if cursor == self.head {
            self.release_head_receiver()
        } else {
            Reclaimed::none()
        };
        debug_assert!(
            reclaimed
                .first()
                .is_none_or(|first| Arc::ptr_eq(first, &value))
        );
        Some((value, reclaimed))
    }

    fn release_head_receiver(&mut self) -> Reclaimed<T> {
        self.head_receivers -= 1;
        if self.head_receivers == 0 {
            self.reclaim_consumed()
        } else {
            Reclaimed::none()
        }
    }

    fn reclaim_consumed(&mut self) -> Reclaimed<T> {
        let mut next_head = self.tail;
        let mut head_receivers = 0;
        for cursor in self.receivers.sequences() {
            if cursor < next_head {
                next_head = cursor;
                head_receivers = 1;
            } else if cursor == next_head {
                head_receivers += 1;
            }
        }

        let consumed = usize::try_from(next_head - self.head)
            .expect("retained broadcast message count exceeds usize");
        let first = (consumed > 0).then(|| {
            self.buffer
                .pop_front()
                .expect("a reclaimed broadcast range must be buffered")
        });
        let rest = if consumed > 1 {
            Some(Box::new(self.buffer.drain(..consumed - 1).collect()))
        } else {
            None
        };
        self.head = next_head;
        self.head_receivers = head_receivers;
        Reclaimed { first, _rest: rest }
    }
}

pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let mut senders = self.shared.senders.load(Ordering::Relaxed);
        loop {
            let next = senders
                .checked_add(1)
                .expect("broadcast sender count overflow");
            match self.shared.senders.compare_exchange_weak(
                senders,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => senders = actual,
            }
        }
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.shared.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            let wakers = {
                let mut inner = self.shared.inner.lock();
                inner.recv_waiters.take_wakers()
            };
            wake_all(wakers);
        }
    }
}

impl<T> Sender<T> {
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        Send {
            sender: self,
            value: Some(value),
            registration: None,
        }
        .await
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let wakers = {
            let mut inner = self.shared.inner.lock();
            if inner.receivers.is_empty() {
                return Err(TrySendError::Disconnected(value));
            }
            let Retention::Bounded(capacity) = self.shared.retention else {
                unreachable!("try_send is only used by bounded broadcast endpoints")
            };
            if inner.buffer.len() == capacity {
                return Err(TrySendError::Full(value));
            }
            append(&mut inner, value);
            (!inner.recv_waiters.is_empty()).then(|| inner.recv_waiters.take_wakers())
        };
        if let Some(wakers) = wakers {
            wake_all(wakers);
        }
        Ok(())
    }

    pub fn send_unbounded(&self, value: T) -> Result<(), SendError<T>> {
        let wakers = {
            let mut inner = self.shared.inner.lock();
            if inner.receivers.is_empty() {
                return Err(SendError::new(value));
            }
            debug_assert!(matches!(self.shared.retention, Retention::Unbounded));
            append(&mut inner, value);
            (!inner.recv_waiters.is_empty()).then(|| inner.recv_waiters.take_wakers())
        };
        if let Some(wakers) = wakers {
            wake_all(wakers);
        }
        Ok(())
    }

    pub fn subscribe(&self) -> Receiver<T> {
        let (key, cursor) = {
            let mut inner = self.shared.inner.lock();
            let cursor = inner.tail;
            (inner.insert_receiver(cursor), cursor)
        };
        Receiver {
            shared: self.shared.clone(),
            key,
            cursor,
        }
    }

    pub fn receiver_count(&self) -> usize {
        self.shared.inner.lock().receivers.len()
    }

    pub fn buffer_len(&self) -> usize {
        self.shared.inner.lock().buffer.len()
    }

    fn cancel_send(&self, token: &mut Option<WakerToken>) {
        let waker = {
            let mut inner = self.shared.inner.lock();
            inner.send_waiters.unregister_waker(token)
        };
        drop(waker);
    }
}

fn append<T>(inner: &mut Inner<T>, value: T) {
    inner.tail = inner
        .tail
        .checked_add(1)
        .expect("broadcast sequence overflow");
    inner.buffer.push_back(Arc::new(value));
}

pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    key: SlotId,
    // Keep the owning endpoint's hot cursor local; the registry copy exists only for gating.
    cursor: u64,
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let (reclaimed, wakers) = {
            let mut inner = self.shared.inner.lock();
            let reclaimed = inner.remove_receiver(self.key);
            let wakers = (!reclaimed.is_empty() && !inner.send_waiters.is_empty())
                .then(|| inner.send_waiters.take_wakers());
            (reclaimed, wakers)
        };
        drop(reclaimed);
        if let Some(wakers) = wakers {
            wake_all(wakers);
        }
    }
}

impl<T: Clone> Receiver<T> {
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        Recv {
            receiver: self,
            registration: None,
        }
        .await
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let (result, wakers) = {
            let mut inner = self.shared.inner.lock();
            if let Some((value, reclaimed)) = inner.receive(self.key, self.cursor) {
                self.cursor += 1;
                let wakers = (!reclaimed.is_empty() && !inner.send_waiters.is_empty())
                    .then(|| inner.send_waiters.take_wakers());
                (Ok((value, reclaimed)), wakers)
            } else if self.shared.senders.load(Ordering::Acquire) == 0 {
                (Err(TryRecvError::Disconnected), None)
            } else {
                (Err(TryRecvError::Empty), None)
            }
        };
        if let Some(wakers) = wakers {
            wake_all(wakers);
        }
        let (value, reclaimed) = result?;
        Ok(take_value(value, reclaimed))
    }
}

impl<T> Receiver<T> {
    pub fn len(&self) -> usize {
        let inner = self.shared.inner.lock();
        usize::try_from(inner.tail - self.cursor)
            .expect("unread broadcast message count exceeds usize")
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_disconnected(&self) -> bool {
        self.shared.senders.load(Ordering::Acquire) == 0
    }

    fn cancel_recv(&self, token: &mut Option<WakerToken>) {
        let waker = {
            let mut inner = self.shared.inner.lock();
            inner.recv_waiters.unregister_waker(token)
        };
        drop(waker);
    }
}

fn take_value<T: Clone>(value: Arc<T>, reclaimed: Reclaimed<T>) -> T {
    // Reclaiming this sequence means `value` becomes uniquely owned after the drained buffer
    // references are dropped. The common single-subscription path can therefore move T out.
    let reclaimed_value = !reclaimed.is_empty();
    drop(reclaimed);
    if reclaimed_value {
        match Arc::try_unwrap(value) {
            Ok(value) => value,
            Err(value) => (*value).clone(),
        }
    } else {
        (*value).clone()
    }
}

struct Send<'a, T> {
    sender: &'a Sender<T>,
    value: Option<T>,
    registration: Option<WakerToken>,
}

impl<T> Unpin for Send<'_, T> {}

impl<T> Future for Send<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let (poll, retired_waker, wake_receivers) = {
            let mut inner = this.sender.shared.inner.lock();
            if inner.receivers.is_empty() {
                let retired_waker = inner.send_waiters.unregister_waker(&mut this.registration);
                (
                    Poll::Ready(Err(SendError::new(
                        this.value
                            .take()
                            .expect("an incomplete send owns its value"),
                    ))),
                    retired_waker,
                    None,
                )
            } else {
                let Retention::Bounded(capacity) = this.sender.shared.retention else {
                    unreachable!("async send is only used by bounded broadcast endpoints")
                };
                if inner.buffer.len() == capacity {
                    let retired_waker = inner
                        .send_waiters
                        .register_waker(&mut this.registration, cx);
                    (Poll::Pending, retired_waker, None)
                } else {
                    let retired_waker = inner.send_waiters.unregister_waker(&mut this.registration);
                    append(
                        &mut inner,
                        this.value
                            .take()
                            .expect("an incomplete send owns its value"),
                    );
                    let wake_receivers =
                        (!inner.recv_waiters.is_empty()).then(|| inner.recv_waiters.take_wakers());
                    (Poll::Ready(Ok(())), retired_waker, wake_receivers)
                }
            }
        };
        drop(retired_waker);
        if let Some(wakers) = wake_receivers {
            wake_all(wakers);
        }
        poll
    }
}

impl<T> Drop for Send<'_, T> {
    fn drop(&mut self) {
        if self.registration.is_some() {
            self.sender.cancel_send(&mut self.registration);
        }
    }
}

struct Recv<'a, T> {
    receiver: &'a mut Receiver<T>,
    registration: Option<WakerToken>,
}

impl<T: Clone> Future for Recv<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let (poll, retired_waker, wake_senders) = {
            let mut inner = this.receiver.shared.inner.lock();
            if let Some((value, reclaimed)) = inner.receive(this.receiver.key, this.receiver.cursor)
            {
                let retired_waker = inner.recv_waiters.unregister_waker(&mut this.registration);
                this.receiver.cursor += 1;
                let wake_senders = (!reclaimed.is_empty() && !inner.send_waiters.is_empty())
                    .then(|| inner.send_waiters.take_wakers());
                (
                    Poll::Ready(Ok((value, reclaimed))),
                    retired_waker,
                    wake_senders,
                )
            } else if this.receiver.shared.senders.load(Ordering::Acquire) == 0 {
                let retired_waker = inner.recv_waiters.unregister_waker(&mut this.registration);
                (
                    Poll::Ready(Err(RecvError::Disconnected)),
                    retired_waker,
                    None,
                )
            } else {
                let retired_waker = inner
                    .recv_waiters
                    .register_waker(&mut this.registration, cx);
                (Poll::Pending, retired_waker, None)
            }
        };
        drop(retired_waker);
        if let Some(wakers) = wake_senders {
            wake_all(wakers);
        }
        match poll {
            Poll::Ready(Ok((value, reclaimed))) => Poll::Ready(Ok(take_value(value, reclaimed))),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        if self.registration.is_some() {
            self.receiver.cancel_recv(&mut self.registration);
        }
    }
}

fn wake_all(wakers: impl Iterator<Item = Waker>) {
    // Every parked subscription observes a publication. Backpressured senders also wake as a set
    // so cancellation of one selected future cannot strand newly available capacity.
    for waker in wakers {
        waker.wake();
    }
}
