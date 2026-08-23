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

//! Shared storage for competing multi-consumer queues.

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::error::RecvError;
use super::error::SendError;
use super::error::TryRecvError;
use super::error::TrySendError;
use crate::internal::mutex::Mutex;
use crate::internal::waitset::WaitSet;
use crate::internal::waitset::WakerToken;

#[derive(Clone, Copy)]
enum Capacity {
    Bounded(usize),
    Unbounded,
}

pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    debug_assert!(capacity > 0);
    channel(Capacity::Bounded(capacity))
}

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    channel(Capacity::Unbounded)
}

fn channel<T>(capacity: Capacity) -> (Sender<T>, Receiver<T>) {
    let queue = match capacity {
        Capacity::Bounded(capacity) => VecDeque::with_capacity(capacity),
        Capacity::Unbounded => VecDeque::new(),
    };
    let shared = Arc::new(Shared {
        capacity,
        state: Mutex::new(State {
            queue,
            senders: 1,
            receivers: 1,
            send_waiters: WaitSet::new(),
            recv_waiters: WaitSet::new(),
        }),
    });
    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared },
    )
}

struct Shared<T> {
    capacity: Capacity,
    state: Mutex<State<T>>,
}

struct State<T> {
    queue: VecDeque<T>,
    senders: usize,
    receivers: usize,
    send_waiters: WaitSet,
    recv_waiters: WaitSet,
}

pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let mut state = self.shared.state.lock();
        state.senders = state
            .senders
            .checked_add(1)
            .expect("queue sender count overflow");
        drop(state);
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
        let wakers = {
            let mut state = self.shared.state.lock();
            state.senders -= 1;
            (state.senders == 0).then(|| state.recv_waiters.take_wakers())
        };
        if let Some(wakers) = wakers {
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
            let mut state = self.shared.state.lock();
            if state.receivers == 0 {
                return Err(TrySendError::Disconnected(value));
            }
            let Capacity::Bounded(capacity) = self.shared.capacity else {
                unreachable!("try_send is only used by bounded queue endpoints")
            };
            if state.queue.len() == capacity {
                return Err(TrySendError::Full(value));
            }
            state.queue.push_back(value);
            (!state.recv_waiters.is_empty()).then(|| state.recv_waiters.take_wakers())
        };
        if let Some(wakers) = wakers {
            wake_all(wakers);
        }
        Ok(())
    }

    pub fn send_unbounded(&self, value: T) -> Result<(), SendError<T>> {
        let wakers = {
            let mut state = self.shared.state.lock();
            if state.receivers == 0 {
                return Err(SendError::new(value));
            }
            debug_assert!(matches!(self.shared.capacity, Capacity::Unbounded));
            state.queue.push_back(value);
            (!state.recv_waiters.is_empty()).then(|| state.recv_waiters.take_wakers())
        };
        if let Some(wakers) = wakers {
            wake_all(wakers);
        }
        Ok(())
    }

    fn cancel_send(&self, registration: &mut Option<WakerToken>) {
        let waker = {
            let mut state = self.shared.state.lock();
            state.send_waiters.unregister_waker(registration)
        };
        drop(waker);
    }
}

pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        let mut state = self.shared.state.lock();
        state.receivers = state
            .receivers
            .checked_add(1)
            .expect("queue receiver count overflow");
        drop(state);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let (wakers, queued) = {
            let mut state = self.shared.state.lock();
            state.receivers -= 1;
            if state.receivers == 0 {
                (
                    Some(state.send_waiters.take_wakers()),
                    mem::take(&mut state.queue),
                )
            } else {
                (None, VecDeque::new())
            }
        };
        drop(queued);
        if let Some(wakers) = wakers {
            wake_all(wakers);
        }
    }
}

impl<T> Receiver<T> {
    pub async fn recv(&self) -> Result<T, RecvError> {
        Recv {
            receiver: self,
            registration: None,
        }
        .await
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let (result, wakers) = {
            let mut state = self.shared.state.lock();
            if let Some(value) = state.queue.pop_front() {
                let wakers = (matches!(self.shared.capacity, Capacity::Bounded(_))
                    && !state.send_waiters.is_empty())
                .then(|| state.send_waiters.take_wakers());
                (Ok(value), wakers)
            } else if state.senders == 0 {
                (Err(TryRecvError::Disconnected), None)
            } else {
                (Err(TryRecvError::Empty), None)
            }
        };
        if let Some(wakers) = wakers {
            wake_all(wakers);
        }
        result
    }

    fn cancel_recv(&self, registration: &mut Option<WakerToken>) {
        let waker = {
            let mut state = self.shared.state.lock();
            state.recv_waiters.unregister_waker(registration)
        };
        drop(waker);
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
            let mut state = this.sender.shared.state.lock();
            if state.receivers == 0 {
                let retired_waker = state.send_waiters.unregister_waker(&mut this.registration);
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
                let Capacity::Bounded(capacity) = this.sender.shared.capacity else {
                    unreachable!("async send is only used by bounded queue endpoints")
                };
                if state.queue.len() == capacity {
                    let retired_waker = state
                        .send_waiters
                        .register_waker(&mut this.registration, cx);
                    (Poll::Pending, retired_waker, None)
                } else {
                    let retired_waker = state.send_waiters.unregister_waker(&mut this.registration);
                    state.queue.push_back(
                        this.value
                            .take()
                            .expect("an incomplete send owns its value"),
                    );
                    let wake_receivers =
                        (!state.recv_waiters.is_empty()).then(|| state.recv_waiters.take_wakers());
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
    receiver: &'a Receiver<T>,
    registration: Option<WakerToken>,
}

impl<T> Future for Recv<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let (poll, retired_waker, wake_senders) = {
            let mut state = this.receiver.shared.state.lock();
            if let Some(value) = state.queue.pop_front() {
                let retired_waker = state.recv_waiters.unregister_waker(&mut this.registration);
                let wake_senders = (matches!(this.receiver.shared.capacity, Capacity::Bounded(_))
                    && !state.send_waiters.is_empty())
                .then(|| state.send_waiters.take_wakers());
                (Poll::Ready(Ok(value)), retired_waker, wake_senders)
            } else if state.senders == 0 {
                let retired_waker = state.recv_waiters.unregister_waker(&mut this.registration);
                (
                    Poll::Ready(Err(RecvError::Disconnected)),
                    retired_waker,
                    None,
                )
            } else {
                let retired_waker = state
                    .recv_waiters
                    .register_waker(&mut this.registration, cx);
                (Poll::Pending, retired_waker, None)
            }
        };
        drop(retired_waker);
        if let Some(wakers) = wake_senders {
            wake_all(wakers);
        }
        poll
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
    // A wake is not a reservation. Waking every contender prevents available work or capacity
    // from being stranded if the first selected future is cancelled before it polls again.
    for waker in wakers {
        waker.wake();
    }
}
