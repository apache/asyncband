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

//! An unbounded multi-producer, single-consumer queue for sending values between asynchronous
//! tasks.

use std::fmt;
use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use super::RecvError;
use super::SendError;
use super::TryRecvError;
use crate::internal::atomic_waker::AtomicWaker;

/// Creates an unbounded mpsc channel for communicating between asynchronous
/// tasks without backpressure.
///
/// A `send` on this channel will always succeed as long as the receiver is alive.
/// If the receiver falls behind, messages will be arbitrarily buffered.
///
/// Note that the amount of available system memory is an implicit bound to
/// the channel. Using an `unbounded` channel has the ability of causing the
/// process to run out of memory. In this case, the process will be aborted.
pub fn unbounded<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let state = Arc::new(UnboundedState {
        senders: AtomicUsize::new(1),
        rx_waker: AtomicWaker::new(),
    });
    let (sender, receiver) = std::sync::mpsc::channel();
    let sender = UnboundedSender {
        state: state.clone(),
        sender: Some(sender),
    };
    let receiver = UnboundedReceiver {
        state: state.clone(),
        receiver,
    };
    (sender, receiver)
}

struct UnboundedState {
    senders: AtomicUsize,
    rx_waker: AtomicWaker,
}

/// Send values to the associated [`UnboundedReceiver`].
///
/// Instances are created by the [`unbounded`] function.
pub struct UnboundedSender<T> {
    state: Arc<UnboundedState>,
    sender: Option<std::sync::mpsc::Sender<T>>,
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        self.state.senders.fetch_add(1, Ordering::Release);
        UnboundedSender {
            state: self.state.clone(),
            sender: self.sender.clone(),
        }
    }
}

impl<T> fmt::Debug for UnboundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedSender").finish_non_exhaustive()
    }
}

impl<T> Drop for UnboundedSender<T> {
    fn drop(&mut self) {
        // Drop the sender; this disconnects the receiver if it is the last sender.
        drop(self.sender.take());

        match self.state.senders.fetch_sub(1, Ordering::AcqRel) {
            1 => {
                // If this is the last sender, we need to wake up the receiver so it can
                // observe the disconnected state.
                self.state.rx_waker.wake();
            }
            _ => {
                // there are still other senders left, do nothing
            }
        }
    }
}

impl<T> UnboundedSender<T> {
    /// Attempts to send a message without blocking.
    ///
    /// This method is not marked async because sending a message to an unbounded channel
    /// never requires any form of waiting. Because of this, the `send` method can be
    /// used in both synchronous and asynchronous code without problems.
    ///
    /// If the receiver has been dropped, this function returns an error. The error includes
    /// the value passed to `send`.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        // SAFETY: The sender is guaranteed to be non-null before dropped.
        let sender = self.sender.as_ref().unwrap();
        sender.send(value).map_err(|err| SendError::new(err.0))?;

        self.state.rx_waker.wake();

        Ok(())
    }
}

/// Receive values from the associated [`UnboundedSender`].
///
/// Instances are created by the [`unbounded`] function.
pub struct UnboundedReceiver<T> {
    state: Arc<UnboundedState>,
    receiver: std::sync::mpsc::Receiver<T>,
}

/// The only `!Sync` field `receiver` is protected by `&mut self` in `recv` and `try_recv`.
/// That is, `UnboundedReceiver` can only be accessed by one thread at a time.
unsafe impl<T: Send> Sync for UnboundedReceiver<T> {}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedReceiver").finish_non_exhaustive()
    }
}

impl<T> UnboundedReceiver<T> {
    /// Tries to receive the next value for this receiver.
    ///
    /// This method returns the [`Empty`] error if the channel is currently
    /// empty, but there are still outstanding [senders].
    ///
    /// This method returns the [`Disconnected`] error if the channel is
    /// currently empty, and there are no outstanding [senders].
    ///
    /// [`Empty`]: TryRecvError::Empty
    /// [`Disconnected`]: TryRecvError::Disconnected
    /// [senders]: UnboundedSender
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mpsc;
    /// use asyncband::mpsc::TryRecvError;
    /// let (tx, mut rx) = mpsc::unbounded();
    ///
    /// tx.send("hello").unwrap();
    ///
    /// assert_eq!(Ok("hello"), rx.try_recv());
    /// assert_eq!(Err(TryRecvError::Empty), rx.try_recv());
    ///
    /// tx.send("hello").unwrap();
    /// drop(tx);
    ///
    /// assert_eq!(Ok("hello"), rx.try_recv());
    /// assert_eq!(Err(TryRecvError::Disconnected), rx.try_recv());
    /// # }
    /// ```
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        match self.receiver.try_recv() {
            Ok(v) => Ok(v),
            Err(std::sync::mpsc::TryRecvError::Disconnected) => Err(TryRecvError::Disconnected),
            Err(std::sync::mpsc::TryRecvError::Empty) => Err(TryRecvError::Empty),
        }
    }

    /// Receives the next value for this receiver.
    ///
    /// This method returns `Err(RecvError::Disconnected)` after every sender has disconnected and
    /// no messages remain in the channel's buffer. At that point, this `Receiver` can never receive
    /// another value.
    ///
    /// If the buffer is empty while a sender remains, this method sleeps until a message is sent or
    /// every sender disconnects.
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
    /// let (tx, mut rx) = mpsc::unbounded();
    ///
    /// tokio::spawn(async move {
    ///     tx.send("hello").unwrap();
    /// });
    ///
    /// assert_eq!(Ok("hello"), rx.recv().await);
    /// assert_eq!(Err(mpsc::RecvError::Disconnected), rx.recv().await);
    /// # }
    /// ```
    ///
    /// Values are buffered:
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mpsc;
    /// let (tx, mut rx) = mpsc::unbounded();
    ///
    /// tx.send("hello").unwrap();
    /// tx.send("world").unwrap();
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
