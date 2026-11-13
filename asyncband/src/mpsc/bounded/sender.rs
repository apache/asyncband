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

use std::fmt;
use std::future::poll_fn;
use std::mem;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::internal::mutex::Mutex;
use crate::internal::register_waker;
use crate::internal::waitlist::WaiterId;
use crate::mpsc::SendError;
use crate::mpsc::TrySendError;
use crate::mpsc::bounded::State;
use crate::mpsc::bounded::Waiter;

/// The sending endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`](crate::mpsc::bounded) function.
pub struct BoundedSender<T> {
    shared: Arc<Mutex<State<T>>>,
}

impl<T> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        self.shared.lock().senders += 1;
        BoundedSender {
            shared: self.shared.clone(),
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
            let mut state = self.shared.lock();
            state.senders -= 1;
            if state.senders == 0 {
                state.recv_waker.take()
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
    pub(super) fn new(shared: Arc<Mutex<State<T>>>) -> Self {
        Self { shared }
    }

    /// Sends a value, waiting for capacity when the queue is full.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] containing the unsent value if the receiver has been dropped.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending `send` future loses its place waiting for capacity and drops `value`;
    /// a future that has returned `Pending` has not sent the message. Use
    /// [`try_send`](Self::try_send) when the caller must retain ownership if capacity is
    /// unavailable, or [`reserve`](Self::reserve) to wait for capacity before constructing the
    /// message.
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

    /// Reserves capacity for one message before the caller supplies its value.
    ///
    /// The returned [`Permit`] holds one capacity slot. Dropping it releases that slot;
    /// [`Permit::send`] publishes a value without waiting for space. Holding a permit does not
    /// establish message order or keep a receiver alive.
    ///
    /// # Errors
    ///
    /// Returns `SendError(())` if the receiver has been dropped. A successful reservation
    /// can still fail to send if disconnection happens before [`Permit::send`].
    ///
    /// # Cancellation
    ///
    /// Dropping a pending `reserve` future loses its place in the wait queue. If capacity has
    /// already been granted, it is released to the next waiter or made available to a new sender.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mpsc;
    ///
    /// let (tx, mut rx) = mpsc::bounded(1);
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
        let mut reserve = Reserve {
            shared: &self.shared,
            waiter: None,
        };
        poll_fn(|cx| reserve.poll(cx)).await
    }

    /// Reserves capacity for one message without waiting.
    ///
    /// Dropping the returned [`Permit`] releases its capacity. See [`reserve`](Self::reserve)
    /// for reservation and message ordering.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] if capacity is unavailable, or
    /// [`TrySendError::Disconnected`] if the receiver has been dropped. Errors carry `()`
    /// because no message has been supplied.
    pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        self.shared.lock().acquire()?;
        Ok(Permit {
            shared: &self.shared,
        })
    }

    /// Sends a value without waiting for capacity.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] if capacity is unavailable, or
    /// [`TrySendError::Disconnected`] if the receiver has been dropped. Both errors contain the
    /// unsent value; nothing is enqueued on error.
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
        let mut state = self.shared.lock();
        match state.acquire() {
            Ok(()) => {
                state.queue.push_back(value);
                let wake = state.recv_waker.take();
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

    /// Returns `true` if the receiver has been dropped.
    ///
    /// Once this returns `true`, every later [`send`](Self::send), [`try_send`](Self::try_send),
    /// [`reserve`](Self::reserve) or [`try_reserve`](Self::try_reserve) fails with a disconnection
    /// error, as does [`Permit::send`] on a permit reserved earlier. A `false` result can be
    /// outdated as soon as it is returned, because the receiver may be dropped concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpsc::bounded;
    ///
    /// let (tx, rx) = bounded::<u8>(1);
    /// assert!(!tx.is_disconnected());
    /// drop(rx);
    /// assert!(tx.is_disconnected());
    /// ```
    pub fn is_disconnected(&self) -> bool {
        !self.shared.lock().receiver
    }
}

/// Capacity reserved for one message on a bounded channel.
///
/// Created by [`BoundedSender::reserve`] or [`BoundedSender::try_reserve`]. Holding a permit
/// reduces available capacity but does not prevent other messages from being received. Dropping
/// it without sending releases capacity and notifies a waiting sender.
#[must_use = "dropping the permit releases its reserved capacity"]
pub struct Permit<'a, T> {
    shared: &'a Mutex<State<T>>,
}

impl<T> fmt::Debug for Permit<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Permit").finish_non_exhaustive()
    }
}

impl<T> Permit<'_, T> {
    /// Publishes a value using this reserved capacity without waiting for space.
    ///
    /// This consumes the permit, including when sending fails.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] containing the unsent value if the receiver has been dropped.
    pub fn send(self, value: T) -> Result<(), SendError<T>> {
        let mut state = self.shared.lock();
        if !state.receiver {
            return Err(SendError::new(value));
        }
        state.queue.push_back(value);
        // The queued message now owns capacity.
        mem::forget(self);
        let wake = state.recv_waker.take();
        drop(state);
        if let Some(waker) = wake {
            waker.wake();
        }
        Ok(())
    }
}

impl<T> Drop for Permit<'_, T> {
    fn drop(&mut self) {
        let wake = self.shared.lock().release();
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

struct Reserve<'a, T> {
    shared: &'a Mutex<State<T>>,
    waiter: Option<WaiterId>,
}

impl<'a, T> Reserve<'a, T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Permit<'a, T>, SendError<()>>> {
        let mut state = self.shared.lock();
        if !state.receiver {
            self.waiter = None;
            return Poll::Ready(Err(SendError::new(())));
        }
        if let Some(index) = self.waiter {
            let waiter = state.send_waiters.waiter_mut(index);
            if waiter.grant {
                let waiter = state.send_waiters.remove_unlinked_waiter(index);
                self.waiter = None;
                let permit = Permit {
                    shared: self.shared,
                };
                drop(state);
                drop(waiter);
                return Poll::Ready(Ok(permit));
            }
            let old = register_waker(&mut waiter.waker, cx.waker());
            drop(state);
            drop(old);
            return Poll::Pending;
        }
        if state.available != 0 {
            state.available -= 1;
            let permit = Permit {
                shared: self.shared,
            };
            drop(state);
            return Poll::Ready(Ok(permit));
        }
        self.waiter = Some(state.send_waiters.push_back(Waiter {
            grant: false,
            waker: Some(cx.waker().clone()),
        }));
        Poll::Pending
    }
}

impl<T> Drop for Reserve<'_, T> {
    fn drop(&mut self) {
        let Some(index) = self.waiter else { return };
        let (waiter, wake) = {
            let mut state = self.shared.lock();
            if !state.receiver {
                return;
            }
            state.send_waiters.unlink_waiter(index, |_| true);
            let waiter = state.send_waiters.remove_unlinked_waiter(index);
            let wake = if waiter.grant { state.release() } else { None };
            (waiter, wake)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        drop(waiter);
    }
}
