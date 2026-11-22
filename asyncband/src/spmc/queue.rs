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
use std::future::poll_fn;
use std::mem;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::retry_waiter::RetryWaiter;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::spmc::RecvError;
use crate::spmc::SendError;
use crate::spmc::TryRecvError;
use crate::spmc::TrySendError;

pub struct Shared<T> {
    state: Mutex<State<T>>,
}

/// Values, endpoint state, and both waiter queues share one lock, so each transition and the
/// waiter it selects are decided together. Wake callbacks and waker or value destructors run
/// outside the lock, because they may reenter the queue.
struct State<T> {
    values: VecDeque<T>,
    capacity: Option<usize>,
    sender_alive: bool,
    receivers: usize,
    recv_waiters: WaitList<RetryWaiter>,
    send_waiters: WaitList<RetryWaiter>,
}

impl<T> State<T> {
    fn has_capacity(&self) -> bool {
        self.capacity
            .is_none_or(|capacity| self.values.len() < capacity)
    }

    /// Queues a value and selects a waiting receiver's waker.
    fn push(&mut self, value: T) -> Option<Waker> {
        self.values.push_back(value);
        self.recv_waiters.notify_one()
    }

    /// Takes the next value and selects a waiting sender's waker.
    fn pop(&mut self) -> Result<(T, Option<Waker>), TryRecvError> {
        if let Some(value) = self.values.pop_front() {
            // Unbounded queues never block senders, so their sender queue is always empty.
            Ok((value, self.send_waiters.notify_one()))
        } else if !self.sender_alive {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }
}

impl<T> Shared<T> {
    pub fn bounded(capacity: usize) -> Self {
        Self::new(Some(capacity))
    }

    pub fn unbounded() -> Self {
        Self::new(None)
    }

    fn new(capacity: Option<usize>) -> Self {
        Self {
            state: Mutex::new(State {
                values: VecDeque::new(),
                capacity,
                sender_alive: true,
                receivers: 1,
                recv_waiters: WaitList::new(),
                send_waiters: WaitList::new(),
            }),
        }
    }

    pub fn drop_sender(&self) {
        let mut waiters = {
            let mut state = self.state.lock();
            state.sender_alive = false;
            // Disconnection invalidates every receiver waiter ID. Move the storage out so both
            // notification and reclamation happen without holding the queue lock.
            mem::replace(&mut state.recv_waiters, WaitList::new())
        };
        while let Some(waker) = waiters.notify_one() {
            waker.wake();
        }
    }

    pub fn clone_receiver(&self) {
        self.state.lock().receivers += 1;
    }

    pub fn has_sender(&self) -> bool {
        self.state.lock().sender_alive
    }

    pub fn has_receivers(&self) -> bool {
        self.state.lock().receivers != 0
    }

    pub fn is_empty(&self) -> bool {
        self.state.lock().values.is_empty()
    }

    pub fn drop_receiver(&self) {
        let (discarded, mut waiters) = {
            let mut state = self.state.lock();
            state.receivers -= 1;
            if state.receivers != 0 {
                return;
            }
            (
                mem::take(&mut state.values),
                mem::replace(&mut state.send_waiters, WaitList::new()),
            )
        };
        // Notify blocked senders before destroying buffered values, whose destructors may panic.
        while let Some(waker) = waiters.notify_one() {
            waker.wake();
        }
        drop(discarded);
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let waker = {
            let mut state = self.state.lock();
            if state.receivers == 0 {
                return Err(TrySendError::Disconnected(value));
            }
            if !state.has_capacity() {
                return Err(TrySendError::Full(value));
            }
            state.push(value)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        let value = match self.try_send(value) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Disconnected(value)) => return Err(SendError::new(value)),
            Err(TrySendError::Full(value)) => value,
        };
        let mut send = Send {
            shared: self,
            waiter: None,
            value: Some(value),
        };
        poll_fn(|cx| send.poll(cx)).await
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let (value, waker) = self.state.lock().pop()?;
        if let Some(waker) = waker {
            waker.wake();
        }
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

struct Send<'a, T> {
    shared: &'a Shared<T>,
    waiter: Option<WaiterId>,
    value: Option<T>,
}

impl<T> Send<'_, T> {
    fn take_value(&mut self) -> T {
        self.value.take().expect("pending send must own its value")
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), SendError<T>>> {
        let mut state = self.shared.state.lock();
        let outcome = if state.receivers == 0 {
            self.waiter = None;
            Err(self.take_value())
        } else if state.has_capacity() {
            Ok(state.push(self.take_value()))
        } else {
            let retired = state.send_waiters.register(&mut self.waiter, cx.waker());
            drop(state);
            drop(retired);
            return Poll::Pending;
        };
        let retired = self.waiter.take().map(|id| state.send_waiters.remove(id));
        drop(state);
        let result = outcome
            .map(|waker| {
                if let Some(waker) = waker {
                    waker.wake();
                }
            })
            .map_err(SendError::new);
        drop(retired);
        Poll::Ready(result)
    }
}

impl<T> Drop for Send<'_, T> {
    fn drop(&mut self) {
        let Some(id) = self.waiter.take() else {
            return;
        };
        let retired = {
            let mut state = self.shared.state.lock();
            if state.receivers == 0 {
                return;
            }
            state.send_waiters.remove(id)
        };
        drop(retired);
    }
}

struct Recv<'a, T> {
    shared: &'a Shared<T>,
    waiter: Option<WaiterId>,
}

impl<T> Recv<'_, T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        let mut state = self.shared.state.lock();
        if !state.sender_alive {
            // Buffered values remain readable after the waiter storage has been detached.
            self.waiter = None;
        }
        let outcome = match state.pop() {
            Ok(popped) => Ok(popped),
            Err(TryRecvError::Disconnected) => Err(RecvError::Disconnected),
            Err(TryRecvError::Empty) => {
                let retired = state.recv_waiters.register(&mut self.waiter, cx.waker());
                drop(state);
                drop(retired);
                return Poll::Pending;
            }
        };
        let retired = self.waiter.take().map(|id| state.recv_waiters.remove(id));
        drop(state);
        let result = outcome.map(|(value, waker)| {
            if let Some(waker) = waker {
                waker.wake();
            }
            value
        });
        drop(retired);
        Poll::Ready(result)
    }
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        let Some(id) = self.waiter.take() else {
            return;
        };
        let (retired, waker) = {
            let mut state = self.shared.state.lock();
            if !state.sender_alive {
                return;
            }
            let retired = state.recv_waiters.remove(id);
            // Hand an unconsumed notification to the next receiver while a value still waits.
            let waker = if matches!(retired, RetryWaiter::Notified) && !state.values.is_empty() {
                state.recv_waiters.notify_one()
            } else {
                None
            };
            (retired, waker)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        drop(retired);
    }
}
