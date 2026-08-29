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
use crate::internal::atomic_waker::AtomicWaker;
use crate::internal::semaphore::Acquire;
use crate::internal::semaphore::Semaphore;

/// Creates a bounded mpsc channel for communicating between asynchronous
/// tasks with backpressure.
///
/// A `send` on this channel will wait if the buffer of the channel is full until a
/// `recv` is called on the receiver, which will consume the message and
/// free up space in the buffer.
#[track_caller]
pub fn bounded<T>(buffer: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(buffer > 0, "mpsc bounded channel requires buffer > 0");
    let state = Arc::new(BoundedState {
        senders: AtomicUsize::new(1),
        tx_permits: Semaphore::new(0),
        rx_waker: AtomicWaker::new(),
    });
    let (sender, receiver) = std::sync::mpsc::sync_channel(buffer);
    let sender = BoundedSender {
        state: state.clone(),
        sender: Some(sender),
    };
    let receiver = BoundedReceiver {
        state: state.clone(),
        receiver: Some(receiver),
    };
    (sender, receiver)
}

struct BoundedState {
    senders: AtomicUsize,
    tx_permits: Semaphore,
    rx_waker: AtomicWaker,
}

/// Send values to the associated [`BoundedReceiver`].
///
/// Instances are created by the [`bounded`] function.
pub struct BoundedSender<T> {
    state: Arc<BoundedState>,
    sender: Option<std::sync::mpsc::SyncSender<T>>,
}

impl<T> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        self.state.senders.fetch_add(1, Ordering::Release);
        BoundedSender {
            state: self.state.clone(),
            sender: self.sender.clone(),
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
        // Dropping the final underlying sender disconnects the channel.
        drop(self.sender.take());

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
    /// Attempts to send a message to the associated receiver.
    ///
    /// This method will wait if the buffer of the channel is full until a `recv` is called on the
    /// receiver, which will consume the message and free up space in the buffer.
    ///
    /// If the receiver has been dropped, this function returns an error. The error includes
    /// the value passed to `send`.
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

    /// Attempts to send a message to the associated receiver without waiting.
    ///
    /// This method returns the [`Full`] error if the buffer of the channel is full.
    ///
    /// This method returns the [`Disconnected`] error if the receiver has been dropped.
    ///
    /// [`Full`]: TrySendError::Full
    /// [`Disconnected`]: TrySendError::Disconnected
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mpsc::TrySendError;
    /// use asyncband::mpsc::bounded;
    /// let (tx, mut rx) = bounded::<i32>(1);
    ///
    /// tx.try_send(1).unwrap();
    /// assert_eq!(tx.try_send(2), Err(TrySendError::Full(2)));
    ///
    /// drop(rx);
    /// assert_eq!(tx.try_send(3), Err(TrySendError::Disconnected(3)));
    /// # }
    /// ```
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        // SAFETY: The sender is guaranteed to be non-null before dropped.
        let sender = self.sender.as_ref().unwrap();
        match sender.try_send(value) {
            Ok(()) => {
                self.state.rx_waker.wake();

                Ok(())
            }
            Err(std::sync::mpsc::TrySendError::Full(value)) => Err(TrySendError::Full(value)),
            Err(std::sync::mpsc::TrySendError::Disconnected(value)) => {
                Err(TrySendError::Disconnected(value))
            }
        }
    }
}

/// Receives values from the associated [`BoundedSender`].
///
/// Instances are created by the [`bounded`] function.
pub struct BoundedReceiver<T> {
    state: Arc<BoundedState>,
    receiver: Option<std::sync::mpsc::Receiver<T>>,
}

/// The only `!Sync` field `receiver` is protected by `&mut self` in `recv` and `try_recv`.
/// That is, `BoundedReceiver` can only be accessed by one thread at a time.
unsafe impl<T: Send> Sync for BoundedReceiver<T> {}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedReceiver<T> {
    fn drop(&mut self) {
        drop(self.receiver.take());
        self.state.tx_permits.notify_all();
    }
}

impl<T> BoundedReceiver<T> {
    /// Tries to receive the next value for this receiver and frees up a space in the buffer if
    /// successful.
    ///
    /// This method returns the [`Empty`] error if the channel is currently
    /// empty, but there are still outstanding [senders].
    ///
    /// This method returns the [`Disconnected`] error if the channel is
    /// currently empty, and there are no outstanding [senders].
    ///
    /// [`Empty`]: TryRecvError::Empty
    /// [`Disconnected`]: TryRecvError::Disconnected
    /// [senders]: BoundedSender
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mpsc;
    /// use asyncband::mpsc::TryRecvError;
    /// let (tx, mut rx) = mpsc::bounded(2);
    ///
    /// tx.send("hello").await.unwrap();
    ///
    /// assert_eq!(Ok("hello"), rx.try_recv());
    /// assert_eq!(Err(TryRecvError::Empty), rx.try_recv());
    ///
    /// tx.send("hello").await.unwrap();
    /// drop(tx);
    ///
    /// assert_eq!(Ok("hello"), rx.try_recv());
    /// assert_eq!(Err(TryRecvError::Disconnected), rx.try_recv());
    /// # }
    /// ```
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        // SAFETY: The receiver is guaranteed to be non-null before dropped.
        let receiver = self.receiver.as_ref().unwrap();
        match receiver.try_recv() {
            Ok(v) => {
                self.state.tx_permits.release_if_nonempty(1);
                Ok(v)
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => Err(TryRecvError::Disconnected),
            Err(std::sync::mpsc::TryRecvError::Empty) => Err(TryRecvError::Empty),
        }
    }

    /// Receives the next value for this receiver and frees up a space in the buffer if successful.
    ///
    /// This method returns `Err(RecvError::Disconnected)` after all senders have been dropped and
    /// no buffered messages remain. At that point, this `Receiver` can never receive another
    /// value.
    ///
    /// If the buffer is empty while a sender remains, this method sleeps until a message is sent or
    /// the final sender is dropped.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a `select` statement
    /// and some other branch completes first, it is guaranteed that no messages were received
    /// on this channel.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mpsc;
    /// let (tx, mut rx) = mpsc::bounded(1);
    ///
    /// tokio::spawn(async move {
    ///     tx.send("hello").await.unwrap();
    /// });
    ///
    /// assert_eq!(Ok("hello"), rx.recv().await);
    /// assert_eq!(Err(mpsc::RecvError::Disconnected), rx.recv().await);
    /// # }
    /// ```
    ///
    /// Values are buffered if the channel has enough capacity:
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mpsc;
    /// let (tx, mut rx) = mpsc::bounded(2);
    ///
    /// tx.send("hello").await.unwrap();
    /// tx.send("world").await.unwrap();
    ///
    /// assert_eq!(Ok("hello"), rx.recv().await);
    /// assert_eq!(Ok("world"), rx.recv().await);
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
