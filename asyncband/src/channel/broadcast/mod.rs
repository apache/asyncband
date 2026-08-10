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

//! Multi-producer, multi-consumer channels where every receiver observes every retained value.
//!
//! Retention is selected by constructor rather than hidden behind a generic configuration:
//!
//! * [overflow] overwrites the oldest value and reports lag to slow receivers.
//! * [backpressure] gates producers on the slowest receiver.
//! * [unbounded] grows as needed and reclaims values observed by every receiver.
//!
//! ~~~
//! use std::num::NonZeroUsize;
//!
//! use asyncband::channel::broadcast;
//!
//! let (tx, mut rx) = broadcast::overflow::channel(NonZeroUsize::new(16).unwrap());
//! tx.send("event").unwrap();
//! assert_eq!(rx.try_recv(), Ok("event"));
//! ~~~

use std::any::type_name;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::channel::SendError;
use crate::channel::TrySendError;
use crate::channel::wait::WaitQueue;
use crate::channel::wait::wake_all;
use crate::internal::mutex::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InternalRecvError {
    Lagged(u64),
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InternalTryRecvError {
    Empty,
    Lagged(u64),
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Retention {
    Overflow(usize),
    Backpressure(usize),
    Unbounded,
}

/// The overflow retention policy.
#[doc(hidden)]
pub struct Overflow;

/// The backpressure retention policy.
#[doc(hidden)]
pub struct Backpressure;

/// The unbounded retention policy.
#[doc(hidden)]
pub struct Unbounded;

/// A sending endpoint whose concrete retention policy is chosen by its constructor module.
#[doc(hidden)]
pub struct Sender<T, Policy> {
    shared: Arc<Shared<T>>,
    policy: PhantomData<Policy>,
}

/// A receiving endpoint whose concrete retention policy is chosen by its constructor module.
#[doc(hidden)]
pub struct Receiver<T, Policy> {
    shared: Arc<Shared<T>>,
    id: u64,
    cursor: u64,
    policy: PhantomData<Policy>,
}

struct Shared<T> {
    retention: Retention,
    state: Mutex<State<T>>,
}

struct State<T> {
    log: VecDeque<Arc<T>>,
    base: u64,
    tail: u64,
    senders: usize,
    receivers: HashMap<u64, u64>,
    next_receiver_id: u64,
    recv_waiters: WaitQueue,
    send_waiters: WaitQueue,
}

fn channel<T, Policy>(retention: Retention) -> (Sender<T, Policy>, Receiver<T, Policy>) {
    let mut receivers = HashMap::new();
    receivers.insert(0, 0);
    let shared = Arc::new(Shared {
        retention,
        state: Mutex::new(State {
            log: VecDeque::new(),
            base: 0,
            tail: 0,
            senders: 1,
            receivers,
            next_receiver_id: 1,
            recv_waiters: WaitQueue::default(),
            send_waiters: WaitQueue::default(),
        }),
    });
    (
        Sender {
            shared: shared.clone(),
            policy: PhantomData,
        },
        Receiver {
            shared,
            id: 0,
            cursor: 0,
            policy: PhantomData,
        },
    )
}

impl<T, Policy> Clone for Sender<T, Policy> {
    fn clone(&self) -> Self {
        let mut state = self.shared.state.lock();
        state.senders = state
            .senders
            .checked_add(1)
            .expect("broadcast sender count overflow");
        drop(state);
        Self {
            shared: self.shared.clone(),
            policy: PhantomData,
        }
    }
}

impl<T, Policy> fmt::Debug for Sender<T, Policy> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("capacity", &self.capacity())
            .field("len", &self.len())
            .field("receiver_count", &self.receiver_count())
            .finish()
    }
}

impl<T, Policy> Sender<T, Policy> {
    fn try_send_internal(&self, value: T) -> Result<usize, TrySendError<T>> {
        let (receivers, wakers, replaced) = {
            let mut state = self.shared.state.lock();
            if state.receivers.is_empty() {
                return Err(TrySendError::Disconnected(value));
            }
            if matches!(
                self.shared.retention,
                Retention::Backpressure(capacity) if state.log.len() >= capacity
            ) {
                return Err(TrySendError::Full(value));
            }
            let replaced = state.push(self.shared.retention, value);
            (
                state.receivers.len(),
                state.recv_waiters.take_all(),
                replaced,
            )
        };
        wake_all(wakers);
        drop(replaced);
        Ok(receivers)
    }

    fn send_nonblocking(&self, value: T) -> Result<usize, SendError<T>> {
        match self.try_send_internal(value) {
            Ok(receivers) => Ok(receivers),
            Err(TrySendError::Disconnected(value)) => Err(SendError::new(value)),
            Err(TrySendError::Full(_)) => {
                unreachable!("nonblocking broadcast retention cannot become full")
            }
        }
    }

    /// Creates a receiver that starts after the latest published value.
    pub fn subscribe(&self) -> Receiver<T, Policy> {
        let mut state = self.shared.state.lock();
        let id = state.allocate_receiver_id();
        let cursor = state.tail;
        state.receivers.insert(id, cursor);
        drop(state);
        Receiver {
            shared: self.shared.clone(),
            id,
            cursor,
            policy: PhantomData,
        }
    }

    /// Returns the configured capacity, or None for unbounded retention.
    pub fn capacity(&self) -> Option<usize> {
        match self.shared.retention {
            Retention::Overflow(capacity) | Retention::Backpressure(capacity) => Some(capacity),
            Retention::Unbounded => None,
        }
    }

    /// Returns the number of values currently retained.
    pub fn len(&self) -> usize {
        self.shared.state.lock().log.len()
    }

    /// Returns true if no value is currently retained.
    pub fn is_empty(&self) -> bool {
        self.shared.state.lock().log.is_empty()
    }

    /// Returns the number of active receivers.
    pub fn receiver_count(&self) -> usize {
        self.shared.state.lock().receivers.len()
    }
}

impl<T> Sender<T, Overflow> {
    /// Sends a value, overwriting the oldest retained value when necessary.
    pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
        self.send_nonblocking(value)
    }

    /// Attempts to send a value without waiting.
    ///
    /// This policy never returns [`TrySendError::Full`].
    pub fn try_send(&self, value: T) -> Result<usize, TrySendError<T>> {
        self.try_send_internal(value)
    }
}

impl<T> Sender<T, Backpressure> {
    /// Sends a value, waiting until every receiver leaves room in the retained window.
    pub async fn send(&self, value: T) -> Result<usize, SendError<T>> {
        Send {
            shared: &self.shared,
            value: Some(value),
            waiter: None,
            completed: false,
        }
        .await
    }

    /// Attempts to send a value without waiting.
    pub fn try_send(&self, value: T) -> Result<usize, TrySendError<T>> {
        self.try_send_internal(value)
    }
}

impl<T> Sender<T, Unbounded> {
    /// Sends a value without waiting.
    pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
        self.send_nonblocking(value)
    }

    /// Attempts to send a value without waiting.
    ///
    /// This policy never returns [`TrySendError::Full`].
    pub fn try_send(&self, value: T) -> Result<usize, TrySendError<T>> {
        self.try_send_internal(value)
    }
}

impl<T, Policy> Drop for Sender<T, Policy> {
    fn drop(&mut self) {
        let wakers = {
            let mut state = self.shared.state.lock();
            state.senders -= 1;
            if state.senders == 0 {
                state.recv_waiters.take_all()
            } else {
                Vec::new()
            }
        };
        wake_all(wakers);
    }
}

impl<T, Policy> Clone for Receiver<T, Policy> {
    fn clone(&self) -> Self {
        let mut state = self.shared.state.lock();
        let id = state.allocate_receiver_id();
        state.receivers.insert(id, self.cursor);
        drop(state);
        Self {
            shared: self.shared.clone(),
            id,
            cursor: self.cursor,
            policy: PhantomData,
        }
    }
}

impl<T, Policy> fmt::Debug for Receiver<T, Policy> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("cursor", &self.cursor)
            .field("disconnected", &self.is_disconnected())
            .finish()
    }
}

impl<T: Clone, Policy> Receiver<T, Policy> {
    async fn recv_internal(&mut self) -> Result<T, InternalRecvError> {
        Recv {
            receiver: self,
            waiter: None,
            completed: false,
        }
        .await
    }

    fn try_recv_internal(&mut self) -> Result<T, InternalTryRecvError> {
        self.try_recv_arc().map(|value| (*value).clone())
    }
}

impl<T: Clone> Receiver<T, Overflow> {
    /// Receives the next retained value.
    pub async fn recv(&mut self) -> Result<T, overflow::RecvError> {
        self.recv_internal().await.map_err(|error| match error {
            InternalRecvError::Lagged(count) => overflow::RecvError::Lagged(count),
            InternalRecvError::Disconnected => overflow::RecvError::Disconnected,
        })
    }

    /// Attempts to receive the next retained value without waiting.
    pub fn try_recv(&mut self) -> Result<T, overflow::TryRecvError> {
        self.try_recv_internal().map_err(|error| match error {
            InternalTryRecvError::Empty => overflow::TryRecvError::Empty,
            InternalTryRecvError::Lagged(count) => overflow::TryRecvError::Lagged(count),
            InternalTryRecvError::Disconnected => overflow::TryRecvError::Disconnected,
        })
    }
}

macro_rules! impl_lossless_receiver {
    ($policy:ty) => {
        impl<T: Clone> Receiver<T, $policy> {
            /// Receives the next retained value.
            pub async fn recv(&mut self) -> Result<T, crate::channel::RecvError> {
                self.recv_internal().await.map_err(|error| match error {
                    InternalRecvError::Disconnected => crate::channel::RecvError::Disconnected,
                    InternalRecvError::Lagged(_) => {
                        unreachable!("a lossless broadcast receiver cannot lag")
                    }
                })
            }

            /// Attempts to receive the next retained value without waiting.
            pub fn try_recv(&mut self) -> Result<T, crate::channel::TryRecvError> {
                self.try_recv_internal().map_err(|error| match error {
                    InternalTryRecvError::Empty => crate::channel::TryRecvError::Empty,
                    InternalTryRecvError::Disconnected => {
                        crate::channel::TryRecvError::Disconnected
                    }
                    InternalTryRecvError::Lagged(_) => {
                        unreachable!("a lossless broadcast receiver cannot lag")
                    }
                })
            }
        }
    };
}

impl_lossless_receiver!(Backpressure);
impl_lossless_receiver!(Unbounded);

impl<T, Policy> Receiver<T, Policy> {
    /// Creates a receiver that starts after the latest published value.
    pub fn resubscribe(&self) -> Self {
        let mut state = self.shared.state.lock();
        let id = state.allocate_receiver_id();
        let cursor = state.tail;
        state.receivers.insert(id, cursor);
        drop(state);
        Self {
            shared: self.shared.clone(),
            id,
            cursor,
            policy: PhantomData,
        }
    }

    /// Returns true if every sender has been dropped.
    pub fn is_disconnected(&self) -> bool {
        self.shared.state.lock().senders == 0
    }

    fn try_recv_arc(&mut self) -> Result<Arc<T>, InternalTryRecvError> {
        let (result, wakers, reclaimed) = {
            let mut state = self.shared.state.lock();
            if self.cursor < state.base {
                let missed = state.base - self.cursor;
                self.cursor = state.base;
                state.receivers.insert(self.id, self.cursor);
                (
                    Err(InternalTryRecvError::Lagged(missed)),
                    Vec::new(),
                    Vec::new(),
                )
            } else if self.cursor < state.tail {
                let index = usize::try_from(self.cursor - state.base)
                    .expect("the retained broadcast range fits in memory");
                let value = state.log[index].clone();
                self.cursor += 1;
                state.receivers.insert(self.id, self.cursor);
                let reclaimed = state.reclaim(self.shared.retention);
                (Ok(value), state.send_waiters.take_all(), reclaimed)
            } else if state.senders == 0 {
                (
                    Err(InternalTryRecvError::Disconnected),
                    Vec::new(),
                    Vec::new(),
                )
            } else {
                (Err(InternalTryRecvError::Empty), Vec::new(), Vec::new())
            }
        };
        wake_all(wakers);
        drop(reclaimed);
        result
    }
}

impl<T, Policy> Drop for Receiver<T, Policy> {
    fn drop(&mut self) {
        let (wakers, reclaimed) = {
            let mut state = self.shared.state.lock();
            state.receivers.remove(&self.id);
            let reclaimed = state.reclaim(self.shared.retention);
            (state.send_waiters.take_all(), reclaimed)
        };
        wake_all(wakers);
        drop(reclaimed);
    }
}

impl<T> State<T> {
    fn push(&mut self, retention: Retention, value: T) -> Option<Arc<T>> {
        let next_tail = self
            .tail
            .checked_add(1)
            .expect("broadcast sequence overflow");
        let mut replaced = None;
        if let Retention::Overflow(capacity) = retention {
            if self.log.len() == capacity {
                replaced = self.log.pop_front();
                self.base += 1;
            }
        }
        self.log.push_back(Arc::new(value));
        self.tail = next_tail;
        replaced
    }

    fn reclaim(&mut self, retention: Retention) -> Vec<Arc<T>> {
        if matches!(retention, Retention::Overflow(_)) {
            return Vec::new();
        }
        let retain_from = self.receivers.values().copied().min().unwrap_or(self.tail);
        let mut reclaimed = Vec::new();
        while self.base < retain_from {
            reclaimed.push(
                self.log
                    .pop_front()
                    .expect("the retained broadcast prefix exists"),
            );
            self.base += 1;
        }
        reclaimed
    }

    fn allocate_receiver_id(&mut self) -> u64 {
        loop {
            let id = self.next_receiver_id;
            self.next_receiver_id = self.next_receiver_id.wrapping_add(1);
            if !self.receivers.contains_key(&id) {
                return id;
            }
        }
    }
}

struct Send<'a, T> {
    shared: &'a Shared<T>,
    value: Option<T>,
    waiter: Option<u64>,
    completed: bool,
}

impl<T> Unpin for Send<'_, T> {}

impl<T> Future for Send<'_, T> {
    type Output = Result<usize, SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let mut recv_wakers = Vec::new();
        let mut retired_wakers = Vec::new();
        let mut replaced = None;
        let poll = {
            let mut state = this.shared.state.lock();
            if state.receivers.is_empty() {
                retired_wakers.extend(state.send_waiters.remove(&mut this.waiter));
                Poll::Ready(Err(SendError::new(
                    this.value
                        .take()
                        .expect("an incomplete send owns its value"),
                )))
            } else if matches!(
                this.shared.retention,
                Retention::Backpressure(capacity) if state.log.len() >= capacity
            ) {
                retired_wakers.extend(state.send_waiters.register(&mut this.waiter, cx.waker()));
                Poll::Pending
            } else {
                retired_wakers.extend(state.send_waiters.remove(&mut this.waiter));
                replaced = state.push(
                    this.shared.retention,
                    this.value
                        .take()
                        .expect("an incomplete send owns its value"),
                );
                recv_wakers = state.recv_waiters.take_all();
                Poll::Ready(Ok(state.receivers.len()))
            }
        };
        drop(retired_wakers);
        wake_all(recv_wakers);
        drop(replaced);
        if poll.is_ready() {
            this.completed = true;
        }
        poll
    }
}

impl<T> Drop for Send<'_, T> {
    fn drop(&mut self) {
        if !self.completed {
            let retired_waker = {
                let mut state = self.shared.state.lock();
                state.send_waiters.remove(&mut self.waiter)
            };
            drop(retired_waker);
        }
    }
}

struct Recv<'a, T, Policy> {
    receiver: &'a mut Receiver<T, Policy>,
    waiter: Option<u64>,
    completed: bool,
}

impl<T: Clone, Policy> Future for Recv<'_, T, Policy> {
    type Output = Result<T, InternalRecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let result = this.receiver.try_recv_arc();
        let poll = match result {
            Ok(value) => {
                this.waiter = None;
                Poll::Ready(Ok((*value).clone()))
            }
            Err(InternalTryRecvError::Lagged(count)) => {
                this.waiter = None;
                Poll::Ready(Err(InternalRecvError::Lagged(count)))
            }
            Err(InternalTryRecvError::Disconnected) => {
                this.waiter = None;
                Poll::Ready(Err(InternalRecvError::Disconnected))
            }
            Err(InternalTryRecvError::Empty) => {
                let mut state = this.receiver.shared.state.lock();
                let retired_waker = if this.receiver.cursor < state.tail || state.senders == 0 {
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

impl<T, Policy> Drop for Recv<'_, T, Policy> {
    fn drop(&mut self) {
        if !self.completed {
            let retired_waker = {
                let mut state = self.receiver.shared.state.lock();
                state.recv_waiters.remove(&mut self.waiter)
            };
            drop(retired_waker);
        }
    }
}

/// Bounded broadcast that overwrites the oldest value for slow receivers.
pub mod overflow {
    use std::fmt;
    use std::num::NonZeroUsize;

    use super::Retention;
    pub use crate::channel::SendError;
    pub use crate::channel::TrySendError;

    /// An error returned when an overflow receiver cannot produce the next value.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum RecvError {
        /// The receiver missed the contained number of values.
        Lagged(u64),
        /// Every sender has been dropped and no retained value remains.
        Disconnected,
    }

    impl fmt::Display for RecvError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Lagged(count) => write!(f, "receiver lagged by {count} values"),
                Self::Disconnected => f.write_str("receiving on a closed broadcast channel"),
            }
        }
    }

    impl std::error::Error for RecvError {}

    /// An error returned by a non-waiting overflow receive.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum TryRecvError {
        /// No value is currently available, but the channel is connected.
        Empty,
        /// The receiver missed the contained number of values.
        Lagged(u64),
        /// Every sender has been dropped and no retained value remains.
        Disconnected,
    }

    impl fmt::Display for TryRecvError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Empty => f.write_str("receiving on an empty broadcast channel"),
                Self::Lagged(count) => write!(f, "receiver lagged by {count} values"),
                Self::Disconnected => f.write_str("receiving on a closed broadcast channel"),
            }
        }
    }

    impl std::error::Error for TryRecvError {}

    /// A sender for overflow retention.
    pub type Sender<T> = super::Sender<T, super::Overflow>;
    /// A receiver for overflow retention.
    pub type Receiver<T> = super::Receiver<T, super::Overflow>;

    /// Creates a bounded overflow broadcast channel.
    pub fn channel<T>(capacity: NonZeroUsize) -> (Sender<T>, Receiver<T>) {
        super::channel::<T, super::Overflow>(Retention::Overflow(capacity.get()))
    }
}

/// Bounded broadcast that waits for the slowest receiver.
pub mod backpressure {
    use std::num::NonZeroUsize;

    use super::Retention;
    pub use crate::channel::RecvError;
    pub use crate::channel::SendError;
    pub use crate::channel::TryRecvError;
    pub use crate::channel::TrySendError;

    /// A sender for backpressure retention.
    pub type Sender<T> = super::Sender<T, super::Backpressure>;
    /// A receiver for backpressure retention.
    pub type Receiver<T> = super::Receiver<T, super::Backpressure>;

    /// Creates a bounded backpressure broadcast channel.
    pub fn channel<T>(capacity: NonZeroUsize) -> (Sender<T>, Receiver<T>) {
        super::channel::<T, super::Backpressure>(Retention::Backpressure(capacity.get()))
    }
}

/// Unbounded broadcast that reclaims values after every receiver advances.
pub mod unbounded {
    use super::Retention;
    pub use crate::channel::RecvError;
    pub use crate::channel::SendError;
    pub use crate::channel::TryRecvError;
    pub use crate::channel::TrySendError;

    /// A sender for unbounded retention.
    pub type Sender<T> = super::Sender<T, super::Unbounded>;
    /// A receiver for unbounded retention.
    pub type Receiver<T> = super::Receiver<T, super::Unbounded>;

    /// Creates an unbounded broadcast channel.
    pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
        super::channel::<T, super::Unbounded>(Retention::Unbounded)
    }
}

impl<T> fmt::Debug for Send<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Send")
            .field("value", &format_args!("{}(..)", type_name::<T>()))
            .finish()
    }
}
