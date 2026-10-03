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
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::RecvError;
use super::SendError;
use super::TryRecvError;
use super::TrySendError;
use crate::internal::mutex::Mutex;
use crate::internal::register_waker;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;

pub struct Shared<T> {
    state: Mutex<State<T>>,
}

/// The only capability that can append values. Exclusive borrowing also limits the channel to
/// one pending `send` future, including inside this module.
pub struct Producer<T> {
    shared: Arc<Shared<T>>,
}

pub fn channel<T>(initial_capacity: usize) -> (Producer<T>, Arc<Shared<T>>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            values: VecDeque::with_capacity(initial_capacity),
            sender_alive: true,
            receivers: 1,
            recv_waiters: WaitList::new(),
            send_waker: None,
        }),
    });
    let producer = Producer {
        shared: shared.clone(),
    };
    (producer, shared)
}

/// Consumers serialize removal with publication and waiter registration. Wake callbacks and
/// waker or value destructors run outside this lock, because they may reenter the queue.
struct State<T> {
    values: VecDeque<T>,
    sender_alive: bool,
    receivers: usize,
    recv_waiters: WaitList<Waiter>,
    // Only the bounded producer can wait for capacity. Once a consumer frees a slot, no other
    // producer can take it, so notification needs neither a queue nor a capacity grant.
    send_waker: Option<Waker>,
}

impl<T> State<T> {
    /// Queues a value and selects a waiting receiver's waker.
    fn push(&mut self, value: T) -> Option<Waker> {
        self.values.push_back(value);
        self.recv_waiters.notify_one()
    }

    /// Takes the next value and selects a waiting sender's waker.
    fn pop(&mut self) -> Result<(T, Option<Waker>), TryRecvError> {
        if let Some(value) = self.values.pop_front() {
            Ok((value, self.send_waker.take()))
        } else if !self.sender_alive {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }
}

/// Notification state for a pending `recv` future.
///
/// Notification wakes the waiting task; it does not reserve a value.
/// The future retains its waiter ID so it can reclaim the detached node when polled again or
/// dropped. Disconnection clears the waiter storage instead.
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

    /// Registers a pending operation's waker or refreshes an existing registration.
    ///
    /// If the future finds no value after notification, its waiter rejoins the queue.
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
        if let Some(notified) = id.take() {
            // The notification already took this node's waker, so nothing is retired.
            self.remove_waiter(notified);
        }
        *id = Some(self.push_back(Waiter::Waiting(current.clone())));
        None
    }
}

impl<T> Producer<T> {
    pub fn try_send(&mut self, value: T, capacity: usize) -> Result<(), TrySendError<T>> {
        let waker = {
            let mut state = self.shared.state.lock();
            if state.receivers == 0 {
                return Err(TrySendError::Disconnected(value));
            }
            if state.values.len() == capacity {
                return Err(TrySendError::Full(value));
            }
            state.push(value)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    pub fn send_unbounded(&mut self, value: T) -> Result<(), SendError<T>> {
        let waker = {
            let mut state = self.shared.state.lock();
            if state.receivers == 0 {
                return Err(SendError::new(value));
            }
            state.push(value)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    pub async fn send(&mut self, value: T, capacity: usize) -> Result<(), SendError<T>> {
        let value = match self.try_send(value, capacity) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Disconnected(value)) => return Err(SendError::new(value)),
            Err(TrySendError::Full(value)) => value,
        };
        let mut send = Send {
            shared: &self.shared,
            capacity,
            registered: false,
            value: Some(value),
        };
        poll_fn(|cx| send.poll(cx)).await
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        let (mut waiters, retired) = {
            let mut state = self.shared.state.lock();
            state.sender_alive = false;
            // Disconnection invalidates every receiver waiter ID. Move the storage out so both
            // notification and reclamation happen without holding the queue lock.
            let waiters = mem::replace(&mut state.recv_waiters, WaitList::new());
            // An explicitly forgotten `send` future may have left a registration behind.
            (waiters, state.send_waker.take())
        };
        while let Some(waker) = waiters.notify_one() {
            waker.wake();
        }
        drop(retired);
    }
}

impl<T> Shared<T> {
    pub fn clone_receiver(&self) {
        self.state.lock().receivers += 1;
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
        // Notify the producer before destroying buffered values, whose destructors may panic.
        if let Some(waker) = waker {
            waker.wake();
        }
        drop(discarded);
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let (value, waker) = self.state.lock().pop()?;
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(value)
    }

    pub async fn recv(&self) -> Result<T, RecvError> {
        let mut recv = Recv {
            shared: self,
            waiter: None,
        };
        poll_fn(|cx| recv.poll(cx)).await
    }
}

struct Send<'a, T> {
    // `Producer::send` retains the exclusive producer borrow for this future's lifetime.
    shared: &'a Shared<T>,
    capacity: usize,
    registered: bool,
    value: Option<T>,
}

impl<T> Send<'_, T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), SendError<T>>> {
        let mut state = self.shared.state.lock();
        if state.receivers != 0 && state.values.len() == self.capacity {
            let retired = register_waker(&mut state.send_waker, cx.waker());
            self.registered = true;
            drop(state);
            drop(retired);
            return Poll::Pending;
        }
        let value = self.value.take().expect("pending send must own its value");
        let outcome = if state.receivers == 0 {
            Err(value)
        } else {
            Ok(state.push(value))
        };
        // A consumer that frees capacity, or the last receiver that disconnects, takes the
        // registration. With no competing producer, readiness cannot be stolen before this poll.
        debug_assert!(state.send_waker.is_none());
        self.registered = false;
        drop(state);
        let result = outcome
            .map(|waker| {
                if let Some(waker) = waker {
                    waker.wake();
                }
            })
            .map_err(SendError::new);
        Poll::Ready(result)
    }
}

impl<T> Drop for Send<'_, T> {
    fn drop(&mut self) {
        if self.registered {
            // Remove the registration before dropping `value`, without running either destructor
            // under the lock. No other producer can have replaced this future's registration.
            let retired = self.shared.state.lock().send_waker.take();
            drop(retired);
        }
    }
}

struct Recv<'a, T> {
    shared: &'a Shared<T>,
    waiter: Option<WaiterId>,
}

impl<T> Recv<'_, T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        let mut state = self.shared.state.lock();
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
        let retired = self.waiter.take().and_then(|id| {
            // Disconnection detaches waiter storage, but buffered values remain readable.
            state
                .sender_alive
                .then(|| state.recv_waiters.remove_waiter(id))
        });
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
