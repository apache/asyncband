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

use super::RecvError;
use super::SendError;
use super::TryRecvError;
use super::TrySendError;
use crate::internal::mutex::Mutex;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::wake_all;

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
    recv_waiters: WaitList<Waiter>,
    send_waiters: WaitList<Waiter>,
}

impl<T> State<T> {
    fn has_capacity(&self) -> bool {
        self.capacity
            .is_none_or(|capacity| self.values.len() < capacity)
    }

    /// Queues a value and selects the receiver to wake.
    fn push(&mut self, value: T) -> Option<Waker> {
        self.values.push_back(value);
        self.recv_waiters.notify_one()
    }

    /// Takes the next value and selects the sender to wake.
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

/// A pending receive or bounded send.
///
/// Notification makes a waiter runnable; it does not reserve a value or slot. The detached node
/// remains owned by its future until it retries or is dropped.
enum Waiter {
    Waiting(Waker),
    Notified,
}

impl WaitList<Waiter> {
    fn notify_one(&mut self) -> Option<Waker> {
        let (_, waiter) = self.unlink_first_waiter(|_| true)?;
        let Waiter::Waiting(waker) = mem::replace(waiter, Waiter::Notified) else {
            unreachable!("only waiting operations remain linked");
        };
        Some(waker)
    }

    fn remove_waiter(&mut self, id: WaiterId) -> Waiter {
        // Unlinking is idempotent, so notified waiters are removed the same way as linked ones.
        self.unlink_waiter(id, |_| true);
        self.remove_unlinked_waiter(id)
    }

    /// Queues a blocked operation or refreshes the waker of a queued one.
    ///
    /// A notified operation that still found no value or slot queues again at the back.
    #[must_use = "drop the replaced waker after releasing the queue lock"]
    fn register(&mut self, id: &mut Option<WaiterId>, current: &Waker) -> Option<Waker> {
        if let Some(queued) = *id {
            if let Waiter::Waiting(waker) = self.waiter_mut(queued) {
                if waker.will_wake(current) {
                    return None;
                }
                return Some(mem::replace(waker, current.clone()));
            }
        }
        let waker = current.clone();
        if let Some(notified) = id.take() {
            // The notification already took this node's waker, so nothing is retired.
            self.remove_waiter(notified);
        }
        *id = Some(self.push_back(Waiter::Waiting(waker)));
        None
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
        wake_all(std::iter::from_fn(|| waiters.notify_one()));
    }

    pub fn clone_receiver(&self) {
        self.state.lock().receivers += 1;
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
        // Release blocked senders before destroying buffered values. Local ownership still drops
        // the values if a wake callback unwinds.
        wake_all(std::iter::from_fn(|| waiters.notify_one()));
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
    // `Drop` passes an unconsumed notification on before this value is destroyed, because its
    // destructor may depend on another blocked sender making progress.
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
        let retired = self
            .waiter
            .take()
            .map(|id| state.send_waiters.remove_waiter(id));
        drop(state);
        // Deliver the notification before running waker destructors, which may panic.
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
        let (retired, waker) = {
            let mut state = self.shared.state.lock();
            if state.receivers == 0 {
                return;
            }
            let retired = state.send_waiters.remove_waiter(id);
            // Hand an unconsumed notification to the next sender while the slot is still free.
            let waker = if matches!(retired, Waiter::Notified) && state.has_capacity() {
                state.send_waiters.notify_one()
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
        let retired = self
            .waiter
            .take()
            .map(|id| state.recv_waiters.remove_waiter(id));
        drop(state);
        // Deliver the notification before running waker destructors, which may panic.
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
            let retired = state.recv_waiters.remove_waiter(id);
            // Hand an unconsumed notification to the next receiver while a value still waits.
            let waker = if matches!(retired, Waiter::Notified) && !state.values.is_empty() {
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
