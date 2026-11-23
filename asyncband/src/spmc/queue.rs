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
use crate::internal::register_waker;
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

/// Values, endpoint state, receiver waiters, and the single sender waker share one lock, so each
/// transition and the task it wakes are decided together. Wake callbacks and waker or value
/// destructors run outside the lock, because they may reenter the queue.
struct State<T> {
    values: VecDeque<T>,
    capacity: Option<usize>,
    sender_alive: bool,
    receivers: usize,
    recv_waiters: WaitList<RetryWaiter>,
    send_waker: Option<Waker>,
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

    /// Takes the next value and wakes the blocked sender, if any.
    fn pop(&mut self) -> Result<(T, Option<Waker>), TryRecvError> {
        if let Some(value) = self.values.pop_front() {
            // Unbounded queues never block the sender, so `send_waker` is always `None`.
            Ok((value, self.send_waker.take()))
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
                send_waker: None,
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
        let (discarded, waker) = {
            let mut state = self.state.lock();
            state.receivers -= 1;
            if state.receivers != 0 {
                return;
            }
            (mem::take(&mut state.values), state.send_waker.take())
        };
        // Notify a blocked sender before destroying buffered values, whose destructors may panic.
        if let Some(waker) = waker {
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
        match self.reserve().await {
            Ok(()) => self.send_reserved(value),
            Err(_) => Err(SendError::new(value)),
        }
    }

    pub fn try_reserve(&self) -> Result<(), TrySendError<()>> {
        let state = self.state.lock();
        if state.receivers == 0 {
            Err(TrySendError::Disconnected(()))
        } else if !state.has_capacity() {
            Err(TrySendError::Full(()))
        } else {
            Ok(())
        }
    }

    pub async fn reserve(&self) -> Result<(), SendError<()>> {
        let mut reserve = Reserve {
            shared: self,
            registered: false,
        };
        poll_fn(|cx| reserve.poll(cx)).await
    }

    pub fn send_reserved(&self, value: T) -> Result<(), SendError<T>> {
        let waker = {
            let mut state = self.state.lock();
            if state.receivers == 0 {
                return Err(SendError::new(value));
            }
            debug_assert!(
                state.has_capacity(),
                "exclusive sender borrow preserves reserved capacity"
            );
            state.push(value)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
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

/// Waits for capacity on the single `BoundedSender`, whose exclusive `&mut self` borrow guarantees
/// that at most one `Reserve` future exists for the queue at a time.
struct Reserve<'a, T> {
    shared: &'a Shared<T>,
    registered: bool,
}

impl<T> Reserve<'_, T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), SendError<()>>> {
        let mut state = self.shared.state.lock();
        let outcome = if state.receivers == 0 {
            self.registered = false;
            Err(SendError::new(()))
        } else if state.has_capacity() {
            Ok(())
        } else {
            let retired = register_waker(&mut state.send_waker, cx.waker());
            self.registered = true;
            drop(state);
            drop(retired);
            return Poll::Pending;
        };
        let retired = if self.registered {
            self.registered = false;
            state.send_waker.take()
        } else {
            None
        };
        drop(state);
        drop(retired);
        Poll::Ready(outcome)
    }
}

impl<T> Drop for Reserve<'_, T> {
    fn drop(&mut self) {
        if !self.registered {
            return;
        }
        let retired = self.shared.state.lock().send_waker.take();
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
