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

use std::cell::Cell;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::channel::FullBehavior;
use crate::channel::RecvError;
use crate::channel::SendError;
use crate::channel::SendOutcome;
use crate::channel::TryRecvError;
use crate::channel::TrySendError;
use crate::channel::wait::WaitQueue;
use crate::channel::wait::wake_all;
use crate::internal::mutex::Mutex;

/// Marker for an endpoint that cannot be cloned or shared concurrently.
#[doc(hidden)]
pub struct Single(PhantomData<Cell<()>>);

/// Marker for a cloneable, concurrently shared endpoint.
#[doc(hidden)]
pub struct Multiple;

#[derive(Debug, Clone, Copy)]
pub(super) enum QueueKind {
    Rendezvous,
    Bounded(usize),
    Unbounded,
}

/// A sending endpoint parameterized by its producer and consumer cardinalities.
#[doc(hidden)]
pub struct Sender<T, Producer, Consumer> {
    core: Arc<Core<T>>,
    producer: PhantomData<Producer>,
    consumer: PhantomData<fn() -> Consumer>,
}

/// A receiving endpoint parameterized by its producer and consumer cardinalities.
#[doc(hidden)]
pub struct Receiver<T, Producer, Consumer> {
    core: Arc<Core<T>>,
    producer: PhantomData<fn() -> Producer>,
    consumer: PhantomData<Consumer>,
}

pub(super) fn channel<T, Producer, Consumer>(
    kind: QueueKind,
) -> (
    Sender<T, Producer, Consumer>,
    Receiver<T, Producer, Consumer>,
) {
    debug_assert!(!matches!(kind, QueueKind::Bounded(0)));
    let core = Arc::new(Core::new(kind));
    (
        Sender {
            core: core.clone(),
            producer: PhantomData,
            consumer: PhantomData,
        },
        Receiver {
            core,
            producer: PhantomData,
            consumer: PhantomData,
        },
    )
}

impl<T, Producer, Consumer> fmt::Debug for Sender<T, Producer, Consumer> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("capacity", &self.capacity())
            .field("len", &self.len())
            .field("disconnected", &self.is_disconnected())
            .finish()
    }
}

impl<T, Producer, Consumer> fmt::Debug for Receiver<T, Producer, Consumer> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("capacity", &self.capacity())
            .field("len", &self.len())
            .field("disconnected", &self.is_disconnected())
            .finish()
    }
}

impl<T, Consumer> Clone for Sender<T, Multiple, Consumer> {
    fn clone(&self) -> Self {
        self.core.add_sender();
        Self {
            core: self.core.clone(),
            producer: PhantomData,
            consumer: PhantomData,
        }
    }
}

impl<T, Producer> Clone for Receiver<T, Producer, Multiple> {
    fn clone(&self) -> Self {
        self.core.add_receiver();
        Self {
            core: self.core.clone(),
            producer: PhantomData,
            consumer: PhantomData,
        }
    }
}

impl<T, Producer, Consumer> Drop for Sender<T, Producer, Consumer> {
    fn drop(&mut self) {
        self.core.drop_sender();
    }
}

impl<T, Producer, Consumer> Drop for Receiver<T, Producer, Consumer> {
    fn drop(&mut self) {
        self.core.drop_receiver();
    }
}

macro_rules! sender_methods {
    ($mode:ty, $this:ident, $($receiver:tt)+) => {
        impl<T, Consumer> Sender<T, $mode, Consumer> {
            /// Sends a value, waiting for capacity when the channel is full.
            pub async fn send($($receiver)+, value: T) -> Result<(), SendError<T>> {
                Send {
                    core: &$this.core,
                    value: Some(value),
                    waiter: None,
                    rendezvous_id: None,
                    completed: false,
                }
                .await
            }

            /// Attempts to send a value without waiting.
            pub fn try_send($($receiver)+, value: T) -> Result<(), TrySendError<T>> {
                $this.core.try_send(value).map(|_| ())
            }

            /// Sends a value by explicitly replacing one buffered value when full.
            ///
            /// Rendezvous channels cannot replace a value and return Full unless a receiver is
            /// already waiting. Unbounded channels never replace a value.
            pub fn force_send(
                $($receiver)+,
                value: T,
                behavior: FullBehavior,
            ) -> Result<SendOutcome<T>, TrySendError<T>> {
                $this.core.force_send(value, behavior)
            }
        }
    };
}

sender_methods!(Single, self, &mut self);
sender_methods!(Multiple, self, &self);

macro_rules! receiver_methods {
    ($mode:ty, $this:ident, $($receiver:tt)+) => {
        impl<T, Producer> Receiver<T, Producer, $mode> {
            /// Receives the next value, waiting while the connected channel is empty.
            pub async fn recv($($receiver)+) -> Result<T, RecvError> {
                Recv {
                    core: &$this.core,
                    waiter: None,
                    completed: false,
                }
                .await
            }

            /// Attempts to receive the next value without waiting.
            pub fn try_recv($($receiver)+) -> Result<T, TryRecvError> {
                $this.core.try_recv()
            }
        }
    };
}

receiver_methods!(Single, self, &mut self);
receiver_methods!(Multiple, self, &self);

impl<T, Producer, Consumer> Sender<T, Producer, Consumer> {
    /// Returns the configured buffer capacity, or None for an unbounded channel.
    pub fn capacity(&self) -> Option<usize> {
        self.core.capacity()
    }

    /// Returns the number of accepted values waiting in the buffer.
    pub fn len(&self) -> usize {
        self.core.len()
    }

    /// Returns true if no accepted value is waiting in the buffer.
    pub fn is_empty(&self) -> bool {
        self.core.len() == 0
    }

    /// Returns true if no receivers remain.
    pub fn is_disconnected(&self) -> bool {
        self.core.receiver_count() == 0
    }
}

impl<T, Producer, Consumer> Receiver<T, Producer, Consumer> {
    /// Returns the configured buffer capacity, or None for an unbounded channel.
    pub fn capacity(&self) -> Option<usize> {
        self.core.capacity()
    }

    /// Returns the number of accepted values waiting in the buffer.
    pub fn len(&self) -> usize {
        self.core.len()
    }

    /// Returns true if no accepted value is waiting in the buffer.
    pub fn is_empty(&self) -> bool {
        self.core.len() == 0
    }

    /// Returns true if no senders remain.
    pub fn is_disconnected(&self) -> bool {
        self.core.sender_count() == 0
    }
}

struct Core<T> {
    kind: QueueKind,
    state: Mutex<State<T>>,
}

struct State<T> {
    queue: VecDeque<T>,
    senders: usize,
    receivers: usize,
    send_waiters: WaitQueue,
    recv_waiters: WaitQueue,
    rendezvous_sends: VecDeque<PendingSend<T>>,
    next_rendezvous_id: u64,
}

struct PendingSend<T> {
    id: u64,
    value: T,
    waker: Waker,
}

impl<T> Core<T> {
    fn new(kind: QueueKind) -> Self {
        Self {
            kind,
            state: Mutex::new(State {
                queue: VecDeque::new(),
                senders: 1,
                receivers: 1,
                send_waiters: WaitQueue::default(),
                recv_waiters: WaitQueue::default(),
                rendezvous_sends: VecDeque::new(),
                next_rendezvous_id: 0,
            }),
        }
    }

    fn capacity(&self) -> Option<usize> {
        match self.kind {
            QueueKind::Rendezvous => Some(0),
            QueueKind::Bounded(capacity) => Some(capacity),
            QueueKind::Unbounded => None,
        }
    }

    fn len(&self) -> usize {
        self.state.lock().queue.len()
    }

    fn sender_count(&self) -> usize {
        self.state.lock().senders
    }

    fn receiver_count(&self) -> usize {
        self.state.lock().receivers
    }

    fn add_sender(&self) {
        let mut state = self.state.lock();
        state.senders = state
            .senders
            .checked_add(1)
            .expect("channel sender count overflow");
    }

    fn add_receiver(&self) {
        let mut state = self.state.lock();
        state.receivers = state
            .receivers
            .checked_add(1)
            .expect("channel receiver count overflow");
    }

    fn drop_sender(&self) {
        let wakers = {
            let mut state = self.state.lock();
            state.senders -= 1;
            if state.senders == 0 {
                state.recv_waiters.take_all()
            } else {
                Vec::new()
            }
        };
        wake_all(wakers);
    }

    fn drop_receiver(&self) {
        let wakers = {
            let mut state = self.state.lock();
            state.receivers -= 1;
            if state.receivers == 0 {
                let mut wakers = state.send_waiters.take_all();
                wakers.extend(
                    state
                        .rendezvous_sends
                        .iter()
                        .map(|pending| pending.waker.clone()),
                );
                wakers
            } else {
                Vec::new()
            }
        };
        wake_all(wakers);
    }

    fn try_send(&self, value: T) -> Result<SendOutcome<T>, TrySendError<T>> {
        let (result, wakers) = {
            let mut state = self.state.lock();
            if state.receivers == 0 {
                return Err(TrySendError::Disconnected(value));
            }

            match self.kind {
                QueueKind::Rendezvous => {
                    let wakers = state.recv_waiters.take_all();
                    if wakers.is_empty() {
                        return Err(TrySendError::Full(value));
                    }
                    state.queue.push_back(value);
                    (Ok(SendOutcome::Sent), wakers)
                }
                QueueKind::Bounded(capacity) if state.queue.len() >= capacity => {
                    return Err(TrySendError::Full(value));
                }
                QueueKind::Bounded(_) | QueueKind::Unbounded => {
                    state.queue.push_back(value);
                    (Ok(SendOutcome::Sent), state.recv_waiters.take_all())
                }
            }
        };

        wake_all(wakers);
        result
    }

    fn force_send(
        &self,
        value: T,
        behavior: FullBehavior,
    ) -> Result<SendOutcome<T>, TrySendError<T>> {
        let (result, wakers) = {
            let mut state = self.state.lock();
            if state.receivers == 0 {
                return Err(TrySendError::Disconnected(value));
            }

            match self.kind {
                QueueKind::Rendezvous => {
                    let wakers = state.recv_waiters.take_all();
                    if wakers.is_empty() {
                        return Err(TrySendError::Full(value));
                    }
                    state.queue.push_back(value);
                    (Ok(SendOutcome::Sent), wakers)
                }
                QueueKind::Unbounded => {
                    state.queue.push_back(value);
                    (Ok(SendOutcome::Sent), state.recv_waiters.take_all())
                }
                QueueKind::Bounded(capacity) if state.queue.len() < capacity => {
                    state.queue.push_back(value);
                    (Ok(SendOutcome::Sent), state.recv_waiters.take_all())
                }
                QueueKind::Bounded(_) => {
                    let replaced = match behavior {
                        FullBehavior::DropOldest => state.queue.pop_front(),
                        FullBehavior::DropNewest => state.queue.pop_back(),
                    }
                    .expect("a full bounded channel has a buffered value");
                    state.queue.push_back(value);
                    (
                        Ok(SendOutcome::Replaced(replaced)),
                        state.recv_waiters.take_all(),
                    )
                }
            }
        };

        wake_all(wakers);
        result
    }

    fn try_recv(&self) -> Result<T, TryRecvError> {
        let (result, wakers) = {
            let mut state = self.state.lock();

            if let Some(value) = state.queue.pop_front() {
                let waker = match self.kind {
                    QueueKind::Rendezvous => Vec::new(),
                    QueueKind::Bounded(_) | QueueKind::Unbounded => state.send_waiters.take_all(),
                };
                (Ok(value), waker)
            } else if matches!(self.kind, QueueKind::Rendezvous) {
                if let Some(pending) = state.rendezvous_sends.pop_front() {
                    (Ok(pending.value), vec![pending.waker])
                } else if state.senders == 0 {
                    (Err(TryRecvError::Disconnected), Vec::new())
                } else {
                    (Err(TryRecvError::Empty), Vec::new())
                }
            } else if state.senders == 0 {
                (Err(TryRecvError::Disconnected), Vec::new())
            } else {
                (Err(TryRecvError::Empty), Vec::new())
            }
        };

        wake_all(wakers);
        result
    }

    fn poll_send(
        &self,
        value: &mut Option<T>,
        waiter: &mut Option<u64>,
        rendezvous_id: &mut Option<u64>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), SendError<T>>> {
        let mut wake_receivers = Vec::new();
        let mut retired_wakers = Vec::new();
        let poll = {
            let mut state = self.state.lock();

            if matches!(self.kind, QueueKind::Rendezvous) {
                if let Some(id) = *rendezvous_id {
                    if state
                        .rendezvous_sends
                        .iter()
                        .all(|pending| pending.id != id)
                    {
                        return Poll::Ready(Ok(()));
                    }
                }
            }

            if state.receivers == 0 {
                retired_wakers.extend(state.send_waiters.remove(waiter));
                if let Some(id) = rendezvous_id.take() {
                    let index = state
                        .rendezvous_sends
                        .iter()
                        .position(|pending| pending.id == id)
                        .expect("a pending rendezvous send owns its value");
                    let pending = state
                        .rendezvous_sends
                        .remove(index)
                        .expect("the rendezvous send index exists");
                    let PendingSend {
                        value: pending_value,
                        waker,
                        ..
                    } = pending;
                    retired_wakers.push(waker);
                    *value = Some(pending_value);
                }
                return Poll::Ready(Err(SendError::new(
                    value.take().expect("an incomplete send owns its value"),
                )));
            }

            match self.kind {
                QueueKind::Rendezvous => {
                    if let Some(id) = *rendezvous_id {
                        if let Some(pending) = state
                            .rendezvous_sends
                            .iter_mut()
                            .find(|pending| pending.id == id)
                        {
                            if !pending.waker.will_wake(cx.waker()) {
                                retired_wakers.push(std::mem::replace(
                                    &mut pending.waker,
                                    cx.waker().clone(),
                                ));
                            }
                            Poll::Pending
                        } else {
                            Poll::Ready(Ok(()))
                        }
                    } else {
                        let id = state.allocate_rendezvous_id();
                        state.rendezvous_sends.push_back(PendingSend {
                            id,
                            value: value.take().expect("an incomplete send owns its value"),
                            waker: cx.waker().clone(),
                        });
                        *rendezvous_id = Some(id);
                        wake_receivers = state.recv_waiters.take_all();
                        Poll::Pending
                    }
                }
                QueueKind::Bounded(capacity) if state.queue.len() >= capacity => {
                    retired_wakers.extend(state.send_waiters.register(waiter, cx.waker()));
                    Poll::Pending
                }
                QueueKind::Bounded(_) | QueueKind::Unbounded => {
                    retired_wakers.extend(state.send_waiters.remove(waiter));
                    state
                        .queue
                        .push_back(value.take().expect("an incomplete send owns its value"));
                    wake_receivers = state.recv_waiters.take_all();
                    Poll::Ready(Ok(()))
                }
            }
        };

        drop(retired_wakers);
        wake_all(wake_receivers);
        poll
    }

    fn poll_recv(
        &self,
        waiter: &mut Option<u64>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<T, RecvError>> {
        let mut wake_senders = Vec::new();
        let mut retired_wakers = Vec::new();
        let poll = {
            let mut state = self.state.lock();

            if let Some(value) = state.queue.pop_front() {
                retired_wakers.extend(state.recv_waiters.remove(waiter));
                if !matches!(self.kind, QueueKind::Rendezvous) {
                    wake_senders = state.send_waiters.take_all();
                }
                Poll::Ready(Ok(value))
            } else if matches!(self.kind, QueueKind::Rendezvous) {
                if let Some(pending) = state.rendezvous_sends.pop_front() {
                    retired_wakers.extend(state.recv_waiters.remove(waiter));
                    wake_senders.push(pending.waker);
                    Poll::Ready(Ok(pending.value))
                } else if state.senders == 0 {
                    retired_wakers.extend(state.recv_waiters.remove(waiter));
                    Poll::Ready(Err(RecvError::Disconnected))
                } else {
                    retired_wakers.extend(state.recv_waiters.register(waiter, cx.waker()));
                    Poll::Pending
                }
            } else if state.senders == 0 {
                retired_wakers.extend(state.recv_waiters.remove(waiter));
                Poll::Ready(Err(RecvError::Disconnected))
            } else {
                retired_wakers.extend(state.recv_waiters.register(waiter, cx.waker()));
                Poll::Pending
            }
        };

        drop(retired_wakers);
        wake_all(wake_senders);
        poll
    }

    fn cancel_send(&self, waiter: &mut Option<u64>, rendezvous_id: &mut Option<u64>) {
        let (retired_waker, pending) = {
            let mut state = self.state.lock();
            let retired_waker = state.send_waiters.remove(waiter);
            let pending = if let Some(id) = rendezvous_id.take() {
                state
                    .rendezvous_sends
                    .iter()
                    .position(|pending| pending.id == id)
                    .and_then(|index| state.rendezvous_sends.remove(index))
            } else {
                None
            };
            (retired_waker, pending)
        };
        drop(retired_waker);
        drop(pending);
    }

    fn cancel_recv(&self, waiter: &mut Option<u64>) {
        let retired_waker = {
            let mut state = self.state.lock();
            state.recv_waiters.remove(waiter)
        };
        drop(retired_waker);
    }
}

impl<T> State<T> {
    fn allocate_rendezvous_id(&mut self) -> u64 {
        loop {
            let id = self.next_rendezvous_id;
            self.next_rendezvous_id = self.next_rendezvous_id.wrapping_add(1);
            if self.rendezvous_sends.iter().all(|pending| pending.id != id) {
                return id;
            }
        }
    }
}

struct Send<'a, T> {
    core: &'a Core<T>,
    value: Option<T>,
    waiter: Option<u64>,
    rendezvous_id: Option<u64>,
    completed: bool,
}

impl<T> Unpin for Send<'_, T> {}

impl<T> Future for Send<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let poll = this.core.poll_send(
            &mut this.value,
            &mut this.waiter,
            &mut this.rendezvous_id,
            cx,
        );
        if poll.is_ready() {
            this.completed = true;
        }
        poll
    }
}

impl<T> Drop for Send<'_, T> {
    fn drop(&mut self) {
        if !self.completed {
            self.core
                .cancel_send(&mut self.waiter, &mut self.rendezvous_id);
        }
    }
}

struct Recv<'a, T> {
    core: &'a Core<T>,
    waiter: Option<u64>,
    completed: bool,
}

impl<T> Future for Recv<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let poll = self.core.poll_recv(&mut self.waiter, cx);
        if poll.is_ready() {
            self.completed = true;
        }
        poll
    }
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        if !self.completed {
            self.core.cancel_recv(&mut self.waiter);
        }
    }
}
