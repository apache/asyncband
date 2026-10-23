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

use crate::internal::mutex::Mutex;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::mpmc::RecvError;
use crate::mpmc::SendError;
use crate::mpmc::TryRecvError;
use crate::mpmc::waiters::RecvWaiter;
use crate::mpmc::waiters::remove_waiter;

/// The unbounded queue's shared state.
pub struct Shared<T> {
    state: Mutex<State<T>>,
}

/// Values, endpoint counts, and the receiver wait queue share one lock, so each transition and the
/// waiter it selects are decided together. Wake callbacks and waker or value destructors run
/// outside the lock, because they may reenter the queue.
struct State<T> {
    values: VecDeque<T>,
    senders: usize,
    receivers: usize,
    recv_waiters: WaitList<RecvWaiter>,
}

impl<T> State<T> {
    fn pop(&mut self) -> Result<T, TryRecvError> {
        if let Some(value) = self.values.pop_front() {
            Ok(value)
        } else if self.senders == 0 {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }
}

impl<T> Shared<T> {
    pub fn unbounded() -> Self {
        Self {
            state: Mutex::new(State {
                values: VecDeque::new(),
                senders: 1,
                receivers: 1,
                recv_waiters: WaitList::new(),
            }),
        }
    }

    pub fn clone_sender(&self) {
        self.state.lock().senders += 1;
    }

    pub fn drop_sender(&self) {
        let mut waiters = {
            let mut state = self.state.lock();
            state.senders -= 1;
            if state.senders != 0 {
                return;
            }
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

    pub fn drop_receiver(&self) {
        let discarded = {
            let mut state = self.state.lock();
            state.receivers -= 1;
            if state.receivers != 0 {
                return;
            }
            mem::take(&mut state.values)
        };
        drop(discarded);
    }

    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let waker = {
            let mut state = self.state.lock();
            if state.receivers == 0 {
                return Err(SendError::new(value));
            }
            state.values.push_back(value);
            state.recv_waiters.notify_one()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.state.lock().pop()
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

struct Recv<'a, T> {
    shared: &'a Shared<T>,
    waiter: Option<WaiterId>,
}

impl<T> Recv<'_, T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        let mut state = self.shared.state.lock();
        if state.senders == 0 {
            // Buffered values remain readable after the waiter storage has been detached.
            self.waiter = None;
        }
        let outcome = match state.pop() {
            Ok(value) => Ok(value),
            Err(TryRecvError::Disconnected) => Err(RecvError::Disconnected),
            Err(TryRecvError::Empty) => {
                let retired = state.recv_waiters.register(&mut self.waiter, cx.waker());
                drop(state);
                drop(retired);
                return Poll::Pending;
            }
        };
        let retired = self
            .waiter
            .take()
            .map(|id| remove_waiter(&mut state.recv_waiters, id));
        drop(state);
        drop(retired);
        Poll::Ready(outcome)
    }
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        let Some(id) = self.waiter.take() else {
            return;
        };
        let (retired, waker) = {
            let mut state = self.shared.state.lock();
            if state.senders == 0 {
                return;
            }
            let retired = remove_waiter(&mut state.recv_waiters, id);
            // Hand an unconsumed notification to the next receiver while a value still waits.
            let waker = if matches!(retired, RecvWaiter::Notified) && !state.values.is_empty() {
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
