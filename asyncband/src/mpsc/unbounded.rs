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

/// Creates an unbounded mpsc channel whose send operation never waits for capacity.
///
/// While the receiver is alive, each send appends its value immediately. Pending messages can
/// therefore grow with producer demand and are limited only by successful memory allocation. Use a
/// bounded channel or external admission control when producers may outpace the receiver.
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

/// The sending endpoint of an unbounded mpsc channel.
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

impl<T> UnboundedSender<T> {
    /// Enqueues a message without waiting for capacity.
    ///
    /// This operation is synchronous because the channel has no capacity limit. If the receiver has
    /// been dropped, the returned error contains `value`.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        // INVARIANT: A shared borrow of the endpoint cannot overlap its destructor.
        let sender = self.sender.as_ref().unwrap();
        sender.send(value).map_err(|err| SendError::new(err.0))?;

        self.state.rx_waker.wake();

        Ok(())
    }
}

/// The receiving endpoint of an unbounded mpsc channel.
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
    /// Attempts to receive the next queued value without waiting.
    ///
    /// An empty channel returns [`TryRecvError::Empty`] while at least one sender remains, or
    /// [`TryRecvError::Disconnected`] after every sender has been dropped and all queued values
    /// have been consumed.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpsc::TryRecvError;
    /// use asyncband::mpsc::unbounded;
    ///
    /// let (tx, mut rx) = unbounded();
    /// tx.send("first").unwrap();
    /// tx.send("second").unwrap();
    ///
    /// assert_eq!(rx.try_recv(), Ok("first"));
    /// assert_eq!(rx.try_recv(), Ok("second"));
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    /// drop(tx);
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    /// ```
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        match self.receiver.try_recv() {
            Ok(v) => Ok(v),
            Err(std::sync::mpsc::TryRecvError::Disconnected) => Err(TryRecvError::Disconnected),
            Err(std::sync::mpsc::TryRecvError::Empty) => Err(TryRecvError::Empty),
        }
    }

    /// Waits for and receives the next value.
    ///
    /// If no value is queued, this method waits until a sender adds one or the last sender is
    /// dropped. It returns [`RecvError::Disconnected`] only after all senders are gone and the
    /// queue has been drained.
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
    /// let (tx, mut rx) = mpsc::unbounded();
    ///
    /// tx.send("first").unwrap();
    /// tx.send("second").unwrap();
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
