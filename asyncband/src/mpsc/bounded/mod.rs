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
use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::ready;

use self::ring::Ring;
use self::waiters::SendWaiters;
use super::RecvError;
use super::SendError;
use super::TryRecvError;
use super::TrySendError;

// Ring owns capacity, publication, and waiting for the head slot. SendWaiters only schedules
// retries after receiving frees capacity; a notification does not reserve a slot.
mod ring;
mod waiters;

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
    let state = Arc::new(Shared {
        buffer: Ring::new(buffer),
        senders: AtomicUsize::new(1),
        send_waiters: SendWaiters::new(),
    });
    let sender = BoundedSender {
        state: state.clone(),
    };
    let receiver = BoundedReceiver { state };
    (sender, receiver)
}

struct Shared<T> {
    buffer: Ring<T>,
    senders: AtomicUsize,
    send_waiters: SendWaiters,
}

/// The sending endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`] function.
pub struct BoundedSender<T> {
    state: Arc<Shared<T>>,
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
        if self.state.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.state.buffer.wake_receiver();
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
        let mut waiter = self.state.send_waiters.waiter();
        let mut value = Some(value);
        poll_fn(|cx| {
            let message = value.take().expect("send polled after completion");
            let message = match self.try_send(message) {
                Ok(()) => {
                    waiter.finish();
                    return Poll::Ready(Ok(()));
                }
                Err(TrySendError::Disconnected(message)) => {
                    waiter.finish();
                    return Poll::Ready(Err(SendError::new(message)));
                }
                Err(TrySendError::Full(message)) => message,
            };
            waiter.register(cx.waker());
            match self.try_send(message) {
                Ok(()) => {
                    waiter.finish();
                    Poll::Ready(Ok(()))
                }
                Err(TrySendError::Disconnected(message)) => {
                    waiter.finish();
                    Poll::Ready(Err(SendError::new(message)))
                }
                Err(TrySendError::Full(message)) => {
                    value = Some(message);
                    Poll::Pending
                }
            }
        })
        .await
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
        self.state.buffer.try_push(value)
    }
}

/// The receiving endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`] function.
pub struct BoundedReceiver<T> {
    state: Arc<Shared<T>>,
}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedReceiver<T> {
    fn drop(&mut self) {
        struct DrainOnDrop<'a, T>(&'a Ring<T>);
        impl<T> Drop for DrainOnDrop<'_, T> {
            fn drop(&mut self) {
                // SAFETY: This guard lives only within the exclusive receiver's drop, after close.
                unsafe { self.0.drain() };
            }
        }

        self.state.buffer.close();
        let drain = DrainOnDrop(&self.state.buffer);
        // A registered waker may own a sender; release it to break that ownership cycle.
        let receiver_waker = self.state.buffer.take_receiver_waker();
        // Complete notifications before dropping messages. Either kind of callback may panic;
        // the drain guard still releases buffered values if a wake or waker drop unwinds.
        self.state.send_waiters.notify_all();
        drop(receiver_waker);
        drop(drain);
    }
}

impl<T> BoundedReceiver<T> {
    /// Attempts to receive the next queued value without waiting for a new message.
    ///
    /// A producer already publishing a queued message may delay this call until publication
    /// finishes. Use [`Self::recv`] to yield asynchronously while publication is in progress.
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
        loop {
            if let Poll::Ready(result) = self.try_recv_once() {
                return result;
            }
            std::thread::yield_now();
        }
    }

    fn try_recv_once(&mut self) -> Poll<Result<T, TryRecvError>> {
        // SAFETY: Only this non-cloneable receiver consumes the queue, through exclusive borrows.
        let value = if let Some(value) = ready!(unsafe { self.state.buffer.pop() }) {
            value
        } else if self.state.senders.load(Ordering::Acquire) == 0 {
            // The final sender can enqueue between the first empty observation and decrementing
            // the sender count, so check the queue again before reporting disconnection.
            // SAFETY: The exclusive receiver borrow still guarantees a single consumer.
            let Some(value) = ready!(unsafe { self.state.buffer.pop() }) else {
                return Poll::Ready(Err(TryRecvError::Disconnected));
            };
            value
        } else {
            return Poll::Ready(Err(TryRecvError::Empty));
        };
        self.state.send_waiters.notify_one();
        Poll::Ready(Ok(value))
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
        match self.try_recv_once() {
            Poll::Ready(Ok(v)) => Poll::Ready(Ok(v)),
            Poll::Ready(Err(TryRecvError::Disconnected)) => {
                Poll::Ready(Err(RecvError::Disconnected))
            }
            Poll::Pending | Poll::Ready(Err(TryRecvError::Empty)) => {
                self.state.buffer.register_receiver(cx.waker());

                match self.try_recv_once() {
                    Poll::Ready(Ok(v)) => Poll::Ready(Ok(v)),
                    Poll::Ready(Err(TryRecvError::Disconnected)) => {
                        Poll::Ready(Err(RecvError::Disconnected))
                    }
                    Poll::Pending | Poll::Ready(Err(TryRecvError::Empty)) => Poll::Pending,
                }
            }
        }
    }
}
