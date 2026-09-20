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
use crate::internal::waker_batch::WakerBatch;

pub struct Shared<T> {
    state: Mutex<State<T>>,
}

/// Values, endpoint counts, and both waiter queues share one lock, so each transition and the
/// waiter it selects are decided together. Wake callbacks and waker or value destructors run
/// outside the lock, because they may reenter the queue.
struct State<T> {
    values: VecDeque<T>,
    capacity: Option<usize>,
    senders: usize,
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
        notify_one(&mut self.recv_waiters)
    }

    /// Takes the next value and selects the sender to wake.
    fn pop(&mut self) -> Result<(T, Option<Waker>), TryRecvError> {
        if let Some(value) = self.values.pop_front() {
            // Unbounded queues never block senders, so their sender queue is always empty.
            Ok((value, notify_one(&mut self.send_waiters)))
        } else if self.senders == 0 {
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

fn notify_one(waiters: &mut WaitList<Waiter>) -> Option<Waker> {
    let (_, waiter) = waiters.unlink_first_waiter(|_| true)?;
    let Waiter::Waiting(waker) = mem::replace(waiter, Waiter::Notified) else {
        unreachable!("only waiting operations remain linked");
    };
    Some(waker)
}

fn notify_all(waiters: &mut WaitList<Waiter>, wakers: &mut WakerBatch) {
    while let Some(waker) = notify_one(waiters) {
        wakers.push(waker);
    }
}

fn remove_waiter(waiters: &mut WaitList<Waiter>, id: WaiterId) -> Waiter {
    // Unlinking is idempotent, so notified waiters are removed the same way as linked ones.
    waiters.unlink_waiter(id, |_| true);
    waiters.remove_unlinked_waiter(id)
}

fn wake(waker: Option<Waker>) {
    if let Some(waker) = waker {
        waker.wake();
    }
}

/// Queues a blocked operation or refreshes the waker of a queued one.
///
/// A notified operation that still found no value or slot queues again at the back.
#[must_use = "drop the replaced waker after releasing the queue lock"]
fn register(
    waiters: &mut WaitList<Waiter>,
    id: &mut Option<WaiterId>,
    current: &Waker,
) -> Option<Waker> {
    if let Some(queued) = *id {
        if let Waiter::Waiting(waker) = waiters.waiter_mut(queued) {
            if waker.will_wake(current) {
                return None;
            }
            return Some(mem::replace(waker, current.clone()));
        }
    }
    let waker = current.clone();
    if let Some(notified) = id.take() {
        // The notification already took this node's waker, so nothing is retired.
        remove_waiter(waiters, notified);
    }
    *id = Some(waiters.push_back(Waiter::Waiting(waker)));
    None
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
                senders: 1,
                receivers: 1,
                recv_waiters: WaitList::new(),
                send_waiters: WaitList::new(),
            }),
        }
    }

    pub fn clone_sender(&self) {
        self.state.lock().senders += 1;
    }

    pub fn drop_sender(&self) {
        let mut wakers = WakerBatch::new();
        {
            let mut state = self.state.lock();
            state.senders -= 1;
            if state.senders != 0 {
                return;
            }
            // Woken receivers drain buffered values before they observe disconnection.
            notify_all(&mut state.recv_waiters, &mut wakers);
        }
        wake_all(&mut wakers);
    }

    pub fn clone_receiver(&self) {
        self.state.lock().receivers += 1;
    }

    pub fn drop_receiver(&self) {
        let mut wakers = WakerBatch::new();
        let discarded = {
            let mut state = self.state.lock();
            state.receivers -= 1;
            if state.receivers != 0 {
                return;
            }
            notify_all(&mut state.send_waiters, &mut wakers);
            mem::take(&mut state.values)
        };
        // Release blocked senders before destroying buffered values. Local ownership still drops
        // the values if a wake callback unwinds.
        wake_all(&mut wakers);
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
        wake(waker);
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
        wake(waker);
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
            Err(self.take_value())
        } else if state.has_capacity() {
            Ok(state.push(self.take_value()))
        } else {
            let retired = register(&mut state.send_waiters, &mut self.waiter, cx.waker());
            drop(state);
            drop(retired);
            return Poll::Pending;
        };
        let retired = self
            .waiter
            .take()
            .map(|id| remove_waiter(&mut state.send_waiters, id));
        drop(state);
        // Deliver the notification before running waker destructors, which may panic.
        let result = outcome.map(wake).map_err(SendError::new);
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
            let retired = remove_waiter(&mut state.send_waiters, id);
            // Hand an unconsumed notification to the next sender while the slot is still free.
            let waker = if matches!(retired, Waiter::Notified) && state.has_capacity() {
                notify_one(&mut state.send_waiters)
            } else {
                None
            };
            (retired, waker)
        };
        wake(waker);
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
        let outcome = match state.pop() {
            Ok(popped) => Ok(popped),
            Err(TryRecvError::Disconnected) => Err(RecvError::Disconnected),
            Err(TryRecvError::Empty) => {
                let retired = register(&mut state.recv_waiters, &mut self.waiter, cx.waker());
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
        // Deliver the notification before running waker destructors, which may panic.
        let result = outcome.map(|(value, waker)| {
            wake(waker);
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
            let retired = remove_waiter(&mut state.recv_waiters, id);
            // Hand an unconsumed notification to the next receiver while a value still waits.
            let waker = if matches!(retired, Waiter::Notified) && !state.values.is_empty() {
                notify_one(&mut state.recv_waiters)
            } else {
                None
            };
            (retired, waker)
        };
        wake(waker);
        drop(retired);
    }
}
