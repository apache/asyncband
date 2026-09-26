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
use std::fmt;
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

/// Values, endpoint counts, and both waiter queues share one lock, so each transition and the
/// waiter it selects are decided together. Wake callbacks and waker or value destructors run
/// outside the lock, because they may reenter the queue.
struct State<T> {
    values: VecDeque<T>,
    // None is an unbounded queue. In a bounded queue, each slot belongs to available,
    // a queued value, a Permit, or a detached waiter with a grant.
    available: Option<usize>,
    senders: usize,
    receivers: usize,
    recv_waiters: WaitList<RecvWaiter>,
    send_waiters: WaitList<SendWaiter>,
}

impl<T> State<T> {
    fn has_capacity(&self) -> bool {
        self.available.is_none_or(|available| available != 0)
    }

    fn acquire(&mut self) -> Result<(), TrySendError<()>> {
        if self.receivers == 0 {
            return Err(TrySendError::Disconnected(()));
        }
        if !self.has_capacity() {
            return Err(TrySendError::Full(()));
        }
        if let Some(available) = &mut self.available {
            *available -= 1;
        }
        Ok(())
    }

    fn release(&mut self) -> Option<Waker> {
        if self.receivers == 0 {
            return None;
        }
        let Some(available) = &mut self.available else {
            return None;
        };
        if let Some(waker) = self.send_waiters.grant_one() {
            return Some(waker);
        }
        *available += 1;
        None
    }

    /// Queues a value and selects the receiver to wake.
    fn push(&mut self, value: T) -> Option<Waker> {
        self.values.push_back(value);
        self.recv_waiters.notify_one()
    }

    /// Takes the next value and grants its capacity to the oldest waiting sender.
    fn pop(&mut self) -> Result<(T, Option<Waker>), TryRecvError> {
        if let Some(value) = self.values.pop_front() {
            Ok((value, self.release()))
        } else if self.senders == 0 {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }
}

/// A receiver notification makes the task runnable without reserving a value.
enum RecvWaiter {
    Waiting(Waker),
    Notified,
}

/// A grant transfers capacity to a detached sender waiter until it claims or cancels it.
enum SendWaiter {
    Waiting(Waker),
    Granted,
}

fn remove_waiter<W>(waiters: &mut WaitList<W>, id: WaiterId) -> W {
    // Unlinking is idempotent, so detached waiters are removed the same way as linked ones.
    waiters.unlink_waiter(id, |_| true);
    waiters.remove_unlinked_waiter(id)
}

impl WaitList<RecvWaiter> {
    fn notify_one(&mut self) -> Option<Waker> {
        let (_, waiter) = self.unlink_first_waiter(|_| true)?;
        let RecvWaiter::Waiting(waker) = mem::replace(waiter, RecvWaiter::Notified) else {
            unreachable!("only waiting operations remain linked");
        };
        Some(waker)
    }

    /// Queues a blocked operation or refreshes the waker of a queued one.
    ///
    /// A notified receive that still found no value queues again at the back.
    #[must_use = "drop the replaced waker after releasing the queue lock"]
    fn register(&mut self, id: &mut Option<WaiterId>, current: &Waker) -> Option<Waker> {
        if let Some(queued) = *id {
            if let RecvWaiter::Waiting(waker) = self.waiter_mut(queued) {
                if waker.will_wake(current) {
                    return None;
                }
                return Some(mem::replace(waker, current.clone()));
            }
        }
        let waker = current.clone();
        if let Some(notified) = id.take() {
            // The notification already took this node's waker, so nothing is retired.
            remove_waiter(self, notified);
        }
        *id = Some(self.push_back(RecvWaiter::Waiting(waker)));
        None
    }
}

impl WaitList<SendWaiter> {
    /// Queues a blocked sender or refreshes its waker without losing its place.
    #[must_use = "drop the replaced waker after releasing the queue lock"]
    fn register_waiter(&mut self, id: &mut Option<WaiterId>, current: &Waker) -> Option<Waker> {
        if let Some(queued) = *id {
            let SendWaiter::Waiting(waker) = self.waiter_mut(queued) else {
                unreachable!("a granted waiter must be claimed before registration");
            };
            if waker.will_wake(current) {
                return None;
            }
            return Some(mem::replace(waker, current.clone()));
        }
        *id = Some(self.push_back(SendWaiter::Waiting(current.clone())));
        None
    }

    fn grant_one(&mut self) -> Option<Waker> {
        let (_, waiter) = self.unlink_first_waiter(|_| true)?;
        let SendWaiter::Waiting(waker) = mem::replace(waiter, SendWaiter::Granted) else {
            unreachable!("only waiting operations remain linked");
        };
        Some(waker)
    }

    fn take_waiting_waker(&mut self) -> Option<Waker> {
        let (id, _) = self.unlink_first_waiter(|_| true)?;
        let SendWaiter::Waiting(waker) = self.remove_unlinked_waiter(id) else {
            unreachable!("only waiting operations remain linked");
        };
        Some(waker)
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
                available: capacity,
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
        wake_all(std::iter::from_fn(|| waiters.take_waiting_waker()));
        drop(discarded);
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let waker = {
            let mut state = self.state.lock();
            match state.acquire() {
                Ok(()) => {}
                Err(TrySendError::Full(())) => return Err(TrySendError::Full(value)),
                Err(TrySendError::Disconnected(())) => {
                    return Err(TrySendError::Disconnected(value));
                }
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
            Ok(permit) => permit.send(value),
            Err(_) => Err(SendError::new(value)),
        }
    }

    pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        self.state.lock().acquire()?;
        Ok(Permit { shared: self })
    }

    pub async fn reserve(&self) -> Result<Permit<'_, T>, SendError<()>> {
        let mut reserve = Reserve {
            shared: self,
            waiter: None,
        };
        poll_fn(|cx| reserve.poll(cx)).await
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

/// Capacity reserved for one value on a bounded MPMC queue.
///
/// A permit does not keep any receiver alive or claim message order. Dropping it without sending
/// passes its capacity to the next waiting sender or makes it available again.
#[must_use = "dropping the permit releases its reserved capacity"]
pub struct Permit<'a, T> {
    shared: &'a Shared<T>,
}

impl<T> fmt::Debug for Permit<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Permit").finish_non_exhaustive()
    }
}

impl<T> Permit<'_, T> {
    /// Enqueues a value using the reserved capacity without waiting for space.
    ///
    /// If the last receiver has been dropped, the error returns ownership of the value.
    pub fn send(self, value: T) -> Result<(), SendError<T>> {
        let waker = {
            let mut state = self.shared.state.lock();
            if state.receivers == 0 {
                return Err(SendError::new(value));
            }
            let waker = state.push(value);
            // The queued value now owns this capacity, including if waking a receiver panics.
            mem::forget(self);
            waker
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }
}

impl<T> Drop for Permit<'_, T> {
    fn drop(&mut self) {
        let waker = self.shared.state.lock().release();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

struct Reserve<'a, T> {
    shared: &'a Shared<T>,
    waiter: Option<WaiterId>,
}

impl<'a, T> Reserve<'a, T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Permit<'a, T>, SendError<()>>> {
        let mut state = self.shared.state.lock();
        // The last receiver may have dropped while this reservation was waiting or granted.
        if state.receivers == 0 {
            self.waiter = None;
            return Poll::Ready(Err(SendError::new(())));
        }
        // A previous poll queued this reservation; a released slot may now belong to it.
        if let Some(id) = self.waiter {
            if matches!(state.send_waiters.waiter_mut(id), SendWaiter::Granted) {
                let retired = state.send_waiters.remove_unlinked_waiter(id);
                self.waiter = None;
                drop(state);
                drop(retired);
                return Poll::Ready(Ok(Permit {
                    shared: self.shared,
                }));
            }
        } else if state.has_capacity() {
            // This reservation has not queued yet and can claim unassigned capacity immediately.
            if let Some(available) = &mut state.available {
                *available -= 1;
            }
            drop(state);
            return Poll::Ready(Ok(Permit {
                shared: self.shared,
            }));
        }
        // Otherwise, wait for a slot or refresh the waker of the queued reservation.
        let retired = state
            .send_waiters
            .register_waiter(&mut self.waiter, cx.waker());
        drop(state);
        drop(retired);
        Poll::Pending
    }
}

impl<T> Drop for Reserve<'_, T> {
    fn drop(&mut self) {
        let Some(id) = self.waiter.take() else {
            return;
        };
        let (retired, waker) = {
            let mut state = self.shared.state.lock();
            if state.receivers == 0 {
                return;
            }
            let retired = remove_waiter(&mut state.send_waiters, id);
            // A grant already owns a slot, so returning it does not require a capacity check.
            let waker = if matches!(retired, SendWaiter::Granted) {
                state.release()
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
        if state.senders == 0 {
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
            .map(|id| remove_waiter(&mut state.recv_waiters, id));
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
