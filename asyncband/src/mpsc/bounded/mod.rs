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

//! A bounded multi-producer, single-consumer queue for sending values between asynchronous
//! tasks with backpressure control.

use std::collections::VecDeque;
use std::fmt;
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
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::wake_all;
use crate::internal::waker_batch::WakerBatch;

/// Creates a bounded mpsc channel with room for `buffer` queued messages.
///
/// [`BoundedSender::send`] waits for capacity when the buffer is full. Receiving a message releases
/// one slot for a waiting sender. Capacity is granted in the order that pending sends and
/// reservations enter the wait queue; new senders cannot take an already granted slot.
///
/// # Panics
///
/// Panics if `buffer` is zero or the preallocated message buffer exceeds the allocation size
/// limit. There is no additional channel-specific capacity limit.
#[track_caller]
pub fn bounded<T>(buffer: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(buffer > 0, "mpsc bounded channel requires buffer > 0");
    let state = Arc::new(Mutex::new(State {
        queue: VecDeque::with_capacity(buffer),
        available: buffer,
        receiver_open: true,
        senders: 1,
        receiver_waker: None,
        waiters: WaitList::new(),
    }));
    let sender = BoundedSender {
        state: state.clone(),
    };
    let receiver = BoundedReceiver { state };
    (sender, receiver)
}

// All transitions happen under one lock. Capacity belongs to exactly one of: `available`, a
// queued message, a live Permit, or a granted waiter. Waker callbacks and payload destruction
// run after unlocking; neither a pending send nor a public Permit owns a queue position.
struct State<T> {
    queue: VecDeque<T>,
    available: usize,
    receiver_open: bool,
    senders: usize,
    receiver_waker: Option<Waker>,
    waiters: WaitList<Waiter>,
}

impl<T> State<T> {
    fn acquire(&mut self) -> Result<(), TrySendError<()>> {
        if !self.receiver_open {
            Err(TrySendError::Disconnected(()))
        } else if self.available == 0 {
            Err(TrySendError::Full(()))
        } else {
            self.available -= 1;
            Ok(())
        }
    }

    fn release(&mut self) -> Option<Waker> {
        if let Some((_, waiter)) = self.waiters.unlink_first_waiter(|_| true) {
            // Keep the detached node until its future claims or cancels this grant.
            waiter.granted = true;
            return waiter.waker.take();
        }
        self.available += 1;
        None
    }

    fn pop(&mut self) -> Result<(T, Option<Waker>), TryRecvError> {
        if let Some(value) = self.queue.pop_front() {
            Ok((value, self.release()))
        } else if self.senders == 0 {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }
}

struct Waiter {
    granted: bool,
    waker: Option<Waker>,
}

struct Reservation<'a, T> {
    sender: &'a BoundedSender<T>,
    index: Option<WaiterId>,
}

impl<'a, T> Reservation<'a, T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Permit<'a, T>, SendError<()>>> {
        let mut cloned_waker = None;
        loop {
            let mut state = self.sender.state.lock();
            if !state.receiver_open {
                // Drop removes any remaining registration, including an unused grant.
                return Poll::Ready(Err(SendError::new(())));
            }
            if let Some(index) = self.index {
                let waiter = state.waiters.waiter_mut(index);
                if waiter.granted {
                    let waiter = state.waiters.remove_unlinked_waiter(index);
                    self.index = None;
                    let permit = Permit {
                        sender: Some(self.sender),
                    };
                    drop(state);
                    drop(waiter);
                    // A clone callback may have freed capacity. Establish ownership before
                    // dropping the unused clone, whose destructor can also run user code.
                    drop(cloned_waker);
                    return Poll::Ready(Ok(permit));
                }
                if waiter
                    .waker
                    .as_ref()
                    .is_some_and(|w| w.will_wake(cx.waker()))
                {
                    return Poll::Pending;
                }
                if let Some(waker) = cloned_waker.take() {
                    let old = waiter.waker.replace(waker);
                    drop(state);
                    drop(old);
                    return Poll::Pending;
                }
            } else if state.available != 0 {
                state.available -= 1;
                let permit = Permit {
                    sender: Some(self.sender),
                };
                drop(state);
                drop(cloned_waker);
                return Poll::Ready(Ok(permit));
            } else if let Some(waker) = cloned_waker.take() {
                self.index = Some(state.waiters.push_back(Waiter {
                    granted: false,
                    waker: Some(waker),
                }));
                return Poll::Pending;
            }
            drop(state);
            // Clone outside the lock, then recheck capacity and closure before registering.
            cloned_waker = Some(cx.waker().clone());
        }
    }
}

impl<T> Drop for Reservation<'_, T> {
    fn drop(&mut self) {
        let Some(index) = self.index else { return };
        let (waiter, wake) = {
            let mut state = self.sender.state.lock();
            state.waiters.unlink_waiter(index, |_| true);
            let waiter = state.waiters.remove_unlinked_waiter(index);
            let wake = if waiter.granted {
                state.release()
            } else {
                None
            };
            (waiter, wake)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        drop(waiter);
    }
}

/// The sending endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`] function.
pub struct BoundedSender<T> {
    state: Arc<Mutex<State<T>>>,
}

impl<T> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        self.state.lock().senders += 1;
        BoundedSender {
            state: self.state.clone(),
        }
    }
}

impl<T> fmt::Debug for BoundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedSender").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedSender<T> {
    fn drop(&mut self) {
        let wake = {
            let mut state = self.state.lock();
            state.senders -= 1;
            if state.senders == 0 {
                state.receiver_waker.take()
            } else {
                None
            }
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

impl<T> BoundedSender<T> {
    /// Sends a message, waiting until the channel has capacity when necessary.
    ///
    /// If the receiver has been dropped, the returned error contains `value`.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending `send` loses its place waiting for capacity and drops `value`; a call
    /// that has returned `Pending` has not sent the message. Use [`Self::try_send`] when the
    /// caller must retain ownership if capacity is unavailable, or [`Self::reserve`] to wait for
    /// capacity before constructing the message.
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        // Publish directly so a ready payload does not travel through try_send's large error
        // return value. Capacity and publication still share one critical section.
        {
            let mut state = self.state.lock();
            match state.acquire() {
                Ok(()) => {
                    state.queue.push_back(value);
                    let wake = state.receiver_waker.take();
                    drop(state);
                    if let Some(waker) = wake {
                        waker.wake();
                    }
                    return Ok(());
                }
                Err(TrySendError::Disconnected(())) => return Err(SendError::new(value)),
                Err(TrySendError::Full(())) => {}
            }
        }
        match self.reserve().await {
            Ok(permit) => permit.send(value),
            Err(_) => Err(SendError::new(value)),
        }
    }

    /// Reserves capacity for one message before constructing it.
    ///
    /// A successful reservation returns a [`Permit`]. Dropping the permit releases capacity;
    /// [`Permit::send`] publishes a value without waiting for space. Reservations do not establish
    /// message order: other producers may send while a permit is held.
    ///
    /// Returns `SendError(())` if the receiver has been dropped. A permit obtained earlier does
    /// not keep the receiver alive; sending with it can still return the unsent value on
    /// disconnect.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending reservation loses its place in the wait queue. If capacity has already
    /// been granted, it is released to the next waiter or made available to a new sender.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (tx, mut rx) = asyncband::mpsc::bounded(1);
    /// let permit = tx.reserve().await.unwrap();
    /// let message = String::from("constructed after capacity became available");
    /// permit.send(message).unwrap();
    /// assert_eq!(
    ///     rx.recv().await.unwrap(),
    ///     "constructed after capacity became available"
    /// );
    /// # }
    /// ```
    pub async fn reserve(&self) -> Result<Permit<'_, T>, SendError<()>> {
        let mut reservation = Reservation {
            sender: self,
            index: None,
        };
        poll_fn(|cx| reservation.poll(cx)).await
    }

    /// Reserves capacity for one message without waiting.
    ///
    /// Returns [`TrySendError::Full`] if queued messages and outstanding permits occupy the
    /// buffer, or [`TrySendError::Disconnected`] if the receiver has been dropped.
    pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        self.state.lock().acquire()?;
        Ok(Permit { sender: Some(self) })
    }

    /// Attempts to send a message without waiting for capacity.
    ///
    /// A full buffer returns [`TrySendError::Full`], while a dropped receiver returns
    /// [`TrySendError::Disconnected`]. Both errors return ownership of the unsent value.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpsc::TrySendError;
    /// use asyncband::mpsc::bounded;
    ///
    /// let (tx, mut rx) = bounded(1);
    /// tx.try_send(10).unwrap();
    /// assert_eq!(tx.try_send(20), Err(TrySendError::Full(20)));
    ///
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// tx.try_send(20).unwrap();
    /// drop(rx);
    /// assert_eq!(tx.try_send(30), Err(TrySendError::Disconnected(30)));
    /// ```
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let mut state = self.state.lock();
        match state.acquire() {
            Ok(()) => {
                state.queue.push_back(value);
                let wake = state.receiver_waker.take();
                drop(state);
                if let Some(waker) = wake {
                    waker.wake();
                }
                Ok(())
            }
            Err(TrySendError::Full(())) => Err(TrySendError::Full(value)),
            Err(TrySendError::Disconnected(())) => Err(TrySendError::Disconnected(value)),
        }
    }
}

/// Capacity reserved for one message on a bounded channel.
///
/// Created by [`BoundedSender::reserve`] or [`BoundedSender::try_reserve`]. Holding a permit
/// reduces available capacity but does not prevent other messages from being received. Dropping
/// it without sending releases capacity and notifies a waiting sender.
#[must_use = "dropping the permit releases its reserved capacity"]
pub struct Permit<'a, T> {
    sender: Option<&'a BoundedSender<T>>,
}

impl<T> fmt::Debug for Permit<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Permit").finish_non_exhaustive()
    }
}

impl<T> Permit<'_, T> {
    /// Publishes a message using this reservation, without waiting for capacity.
    ///
    /// If the receiver has been dropped, the returned error contains the unsent value.
    pub fn send(mut self, value: T) -> Result<(), SendError<T>> {
        let mut state = self.sender.unwrap().state.lock();
        if !state.receiver_open {
            return Err(SendError::new(value));
        }
        state.queue.push_back(value);
        // The queued message owns the capacity before any wake callback can panic.
        self.sender = None;
        let wake = state.receiver_waker.take();
        drop(state);
        if let Some(waker) = wake {
            waker.wake();
        }
        Ok(())
    }
}

impl<T> Drop for Permit<'_, T> {
    fn drop(&mut self) {
        if let Some(sender) = self.sender {
            let wake = sender.state.lock().release();
            if let Some(waker) = wake {
                waker.wake();
            }
        }
    }
}

/// The receiving endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`] function.
pub struct BoundedReceiver<T> {
    state: Arc<Mutex<State<T>>>,
}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedReceiver<T> {
    fn drop(&mut self) {
        let (queue, receiver_waker, wakers) = {
            let mut state = self.state.lock();
            state.receiver_open = false;
            let queue = mem::take(&mut state.queue);
            state.available += queue.len();
            let receiver_waker = state.receiver_waker.take();
            let mut wakers = WakerBatch::new();
            while let Some((_, waiter)) = state.waiters.unlink_first_waiter(|_| true) {
                if let Some(waker) = waiter.waker.take() {
                    wakers.push(waker);
                }
            }
            (queue, receiver_waker, wakers)
        };
        // Local ownership also drains the queue if a wake or waker destructor unwinds.
        wake_all(wakers.into_iter());
        drop(receiver_waker);
        drop(queue);
    }
}

impl<T> BoundedReceiver<T> {
    /// Attempts to receive the next queued value without waiting for a new message.
    ///
    /// Receiving a value frees one buffer slot. An empty channel returns [`TryRecvError::Empty`]
    /// while at least one sender remains, or [`TryRecvError::Disconnected`] after every sender has
    /// been dropped and all queued values have been consumed.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpsc::TryRecvError;
    /// use asyncband::mpsc::bounded;
    ///
    /// let (tx, mut rx) = bounded(2);
    /// tx.try_send("first").unwrap();
    /// tx.try_send("second").unwrap();
    ///
    /// assert_eq!(rx.try_recv(), Ok("first"));
    /// assert_eq!(rx.try_recv(), Ok("second"));
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    /// drop(tx);
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    /// ```
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let (value, wake) = self.state.lock().pop()?;
        if let Some(waker) = wake {
            waker.wake();
        }
        Ok(value)
    }

    /// Waits for and receives the next value, freeing one buffer slot.
    ///
    /// If no value is queued, this method waits until a sender adds one or the last sender is
    /// dropped. It returns [`RecvError::Disconnected`] only after all senders are gone and the
    /// buffer has been drained.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending `recv` does not remove a message from the channel. A later receive
    /// operation can still observe the next queued value, so `recv` may safely be raced with other
    /// futures in a selection construct.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mpsc;
    /// let (tx, mut rx) = mpsc::bounded(2);
    ///
    /// tx.send("first").await.unwrap();
    /// tx.send("second").await.unwrap();
    /// drop(tx);
    ///
    /// assert_eq!(rx.recv().await, Ok("first"));
    /// assert_eq!(rx.recv().await, Ok("second"));
    /// assert_eq!(rx.recv().await, Err(mpsc::RecvError::Disconnected));
    /// # }
    /// ```
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        let mut cloned_waker = None;
        loop {
            let mut state = self.state.lock();
            match state.pop() {
                Ok((value, wake)) => {
                    drop(state);
                    if let Some(waker) = wake {
                        waker.wake();
                    }
                    return Poll::Ready(Ok(value));
                }
                Err(TryRecvError::Disconnected) => {
                    let old = state.receiver_waker.take();
                    drop(state);
                    drop(old);
                    return Poll::Ready(Err(RecvError::Disconnected));
                }
                Err(TryRecvError::Empty) => {}
            }
            if state
                .receiver_waker
                .as_ref()
                .is_some_and(|w| w.will_wake(cx.waker()))
            {
                return Poll::Pending;
            }
            if let Some(waker) = cloned_waker.take() {
                let old = state.receiver_waker.replace(waker);
                drop(state);
                drop(old);
                return Poll::Pending;
            }
            drop(state);
            cloned_waker = Some(cx.waker().clone());
        }
    }
}
