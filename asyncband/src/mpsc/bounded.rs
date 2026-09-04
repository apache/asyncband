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

use std::fmt;
use std::future::Future;
use std::future::poll_fn;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use super::RecvError;
use super::SendError;
use super::TryRecvError;
use super::TrySendError;
use super::queue::BoundedQueue;
use super::queue::PushError;
use crate::internal::atomic_waker::AtomicWaker;
use crate::internal::semaphore::Acquire;
use crate::internal::semaphore::Semaphore;

/// Creates a bounded mpsc channel with room for `buffer` queued messages.
///
/// [`BoundedSender::send`] waits for capacity when the buffer is full. Receiving a message releases
/// one slot for a waiting sender.
///
/// # Panics
///
/// Panics if `buffer` is zero.
#[track_caller]
pub fn bounded<T>(buffer: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(buffer > 0, "mpsc bounded channel requires buffer > 0");
    let state = Arc::new(BoundedState {
        queue: BoundedQueue::new(buffer),
        senders: AtomicUsize::new(1),
        tx_permits: Semaphore::new(0),
        rx_waker: AtomicWaker::new(),
    });
    let sender = BoundedSender {
        state: state.clone(),
    };
    let receiver = BoundedReceiver { state };
    (sender, receiver)
}

struct BoundedState<T> {
    queue: BoundedQueue<T>,
    senders: AtomicUsize,
    tx_permits: Semaphore,
    rx_waker: AtomicWaker,
}

/// The sending endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`] function.
pub struct BoundedSender<T> {
    state: Arc<BoundedState<T>>,
}

impl<T> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        self.state.senders.fetch_add(1, Ordering::Release);
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
        match self.state.senders.fetch_sub(1, Ordering::AcqRel) {
            1 => {
                // Wake the receiver so it can observe the channel's disconnected state.
                self.state.rx_waker.wake();
            }
            _ => {
                // there are still other senders left, do nothing
            }
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
    /// caller must retain ownership if capacity is unavailable.
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        let value = match self.try_send(value) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Disconnected(value)) => return Err(SendError::new(value)),
            Err(TrySendError::Full(value)) => value,
        };

        struct SendState<'a, T> {
            sender: &'a BoundedSender<T>,
            value: Option<T>,
            acquire: Acquire<'a>,
        }

        impl<T> SendState<'_, T> {
            fn poll_send(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), SendError<T>>> {
                let mut value = match self.value.take() {
                    Some(value) => value,
                    None => return Poll::Ready(Ok(())),
                };

                loop {
                    let poll = pin!(&mut self.acquire).poll(cx);

                    value = match self.sender.try_send(value) {
                        Ok(()) => return Poll::Ready(Ok(())),
                        Err(TrySendError::Disconnected(value)) => {
                            return Poll::Ready(Err(SendError::new(value)));
                        }
                        Err(TrySendError::Full(value)) => value,
                    };

                    if poll.is_ready() {
                        self.acquire = self.sender.state.tx_permits.poll_acquire(1);
                    } else {
                        self.value = Some(value);
                        return Poll::Pending;
                    }
                }
            }
        }

        let acquire = self.state.tx_permits.poll_acquire(1);
        let mut send = SendState {
            sender: self,
            value: Some(value),
            acquire,
        };
        poll_fn(|cx| send.poll_send(cx)).await
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
        match self.state.queue.try_push(value) {
            Ok(()) => {
                self.state.rx_waker.wake();

                Ok(())
            }
            Err(PushError::Full(value)) => Err(TrySendError::Full(value)),
            Err(PushError::Disconnected(value)) => Err(TrySendError::Disconnected(value)),
        }
    }
}

/// The receiving endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`] function.
pub struct BoundedReceiver<T> {
    state: Arc<BoundedState<T>>,
}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedReceiver<T> {
    fn drop(&mut self) {
        self.state.queue.disconnect_receiver();
        self.state.tx_permits.notify_all();
    }
}

impl<T> BoundedReceiver<T> {
    /// Attempts to receive the next queued value without waiting.
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
        if let Some(value) = self.state.queue.pop() {
            self.state.tx_permits.release_if_nonempty(1);
            Ok(value)
        } else if self.state.senders.load(Ordering::Acquire) == 0 {
            // The final sender can enqueue between the first empty observation and decrementing
            // the sender count, so check the queue again before reporting disconnection.
            if let Some(value) = self.state.queue.pop() {
                self.state.tx_permits.release_if_nonempty(1);
                Ok(value)
            } else {
                Err(TryRecvError::Disconnected)
            }
        } else {
            Err(TryRecvError::Empty)
        }
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
        match self.try_recv() {
            Ok(v) => Poll::Ready(Ok(v)),
            Err(TryRecvError::Disconnected) => Poll::Ready(Err(RecvError::Disconnected)),
            Err(TryRecvError::Empty) => {
                self.state.rx_waker.register(cx.waker());

                match self.try_recv() {
                    Ok(v) => Poll::Ready(Ok(v)),
                    Err(TryRecvError::Disconnected) => Poll::Ready(Err(RecvError::Disconnected)),
                    Err(TryRecvError::Empty) => Poll::Pending,
                }
            }
        }
    }
}
