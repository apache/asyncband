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
use std::future::Future;
use std::future::poll_fn;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use super::RecvError;
use super::SendError;
use super::TryRecvError;
use super::TrySendError;
use crate::internal::mutex::Mutex;
use crate::internal::semaphore::Acquire;
use crate::internal::semaphore::Semaphore;

pub(super) struct Shared<T> {
    state: Mutex<State<T>>,
    recv_waiters: Semaphore,
    send_waiters: Semaphore,
    capacity: Option<usize>,
}

struct State<T> {
    values: VecDeque<T>,
    senders: usize,
    receivers: usize,
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
                senders: 1,
                receivers: 1,
            }),
            recv_waiters: Semaphore::new(0),
            send_waiters: Semaphore::new(0),
            capacity,
        }
    }

    pub fn clone_sender(&self) {
        let mut state = self.state.lock();
        state.senders = state
            .senders
            .checked_add(1)
            .expect("mpmc sender count overflow");
    }

    pub fn drop_sender(&self) {
        let is_last = {
            let mut state = self.state.lock();
            state.senders -= 1;
            state.senders == 0
        };
        if is_last {
            self.recv_waiters.notify_all();
        }
    }

    pub fn clone_receiver(&self) {
        let mut state = self.state.lock();
        state.receivers = state
            .receivers
            .checked_add(1)
            .expect("mpmc receiver count overflow");
    }

    pub fn drop_receiver(&self) {
        let discarded = {
            let mut state = self.state.lock();
            state.receivers -= 1;
            (state.receivers == 0).then(|| std::mem::take(&mut state.values))
        };
        if discarded.is_some() {
            self.send_waiters.notify_all();
        }
        drop(discarded);
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        {
            let mut state = self.state.lock();
            if state.receivers == 0 {
                return Err(TrySendError::Disconnected(value));
            }
            if self
                .capacity
                .is_some_and(|capacity| state.values.len() >= capacity)
            {
                return Err(TrySendError::Full(value));
            }
            state.values.push_back(value);
        }
        self.recv_waiters.release_if_nonempty(1);
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
            value: Some(value),
            acquire: self.send_waiters.poll_acquire(1),
        };
        poll_fn(|cx| send.poll(cx)).await
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let value = {
            let mut state = self.state.lock();
            match state.values.pop_front() {
                Some(value) => value,
                None if state.senders == 0 => return Err(TryRecvError::Disconnected),
                None => return Err(TryRecvError::Empty),
            }
        };
        if self.capacity.is_some() {
            self.send_waiters.release_if_nonempty(1);
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
            acquire: self.recv_waiters.poll_acquire(1),
        };
        poll_fn(|cx| recv.poll(cx)).await
    }
}

struct Send<'a, T> {
    shared: &'a Shared<T>,
    value: Option<T>,
    acquire: Acquire<'a>,
}

impl<T> Send<'_, T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), SendError<T>>> {
        let mut value = self.value.take().expect("pending send must own its value");
        loop {
            let notified = Pin::new(&mut self.acquire).poll(cx);
            value = match self.shared.try_send(value) {
                Ok(()) => return Poll::Ready(Ok(())),
                Err(TrySendError::Disconnected(value)) => {
                    return Poll::Ready(Err(SendError::new(value)));
                }
                Err(TrySendError::Full(value)) => value,
            };
            if notified.is_ready() {
                self.acquire = self.shared.send_waiters.poll_acquire(1);
            } else {
                self.value = Some(value);
                return Poll::Pending;
            }
        }
    }
}

struct Recv<'a, T> {
    shared: &'a Shared<T>,
    acquire: Acquire<'a>,
}

impl<T> Recv<'_, T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        loop {
            let notified = Pin::new(&mut self.acquire).poll(cx);
            match self.shared.try_recv() {
                Ok(value) => return Poll::Ready(Ok(value)),
                Err(TryRecvError::Disconnected) => {
                    return Poll::Ready(Err(RecvError::Disconnected));
                }
                Err(TryRecvError::Empty) if notified.is_ready() => {
                    self.acquire = self.shared.recv_waiters.poll_acquire(1);
                }
                Err(TryRecvError::Empty) => return Poll::Pending,
            }
        }
    }
}
