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
use super::queue::PushError;
use super::queue::UnboundedConsumer;
use super::queue::UnboundedQueue;
use crate::internal::atomic_waker::AtomicWaker;

/// Creates an unbounded mpsc channel whose send operation never waits for capacity.
///
/// While the receiver is alive, each send appends its value immediately. Pending messages can
/// therefore grow with producer demand and are limited only by successful memory allocation. Use a
/// bounded channel or external admission control when producers may outpace the receiver.
pub fn unbounded<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let state = Arc::new(UnboundedState {
        queue: UnboundedQueue::new(),
        senders: AtomicUsize::new(1),
        rx_waker: AtomicWaker::new(),
    });
    let sender = UnboundedSender {
        state: state.clone(),
    };
    let receiver = UnboundedReceiver {
        state,
        consumer: UnboundedConsumer::new(),
    };
    (sender, receiver)
}

struct UnboundedState<T> {
    queue: UnboundedQueue<T>,
    senders: AtomicUsize,
    rx_waker: AtomicWaker,
}

/// The sending endpoint of an unbounded mpsc channel.
///
/// Instances are created by the [`unbounded`] function.
pub struct UnboundedSender<T> {
    state: Arc<UnboundedState<T>>,
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        self.state.senders.fetch_add(1, Ordering::Release);
        UnboundedSender {
            state: self.state.clone(),
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
        match self.state.queue.push(value) {
            Ok(()) => {}
            Err(PushError::Disconnected(value)) => return Err(SendError::new(value)),
            Err(PushError::Full(_)) => unreachable!("unbounded queue cannot be full"),
        }

        self.state.rx_waker.wake();

        Ok(())
    }
}

/// The receiving endpoint of an unbounded mpsc channel.
///
/// Instances are created by the [`unbounded`] function.
pub struct UnboundedReceiver<T> {
    state: Arc<UnboundedState<T>>,
    consumer: UnboundedConsumer<T>,
}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for UnboundedReceiver<T> {
    fn drop(&mut self) {
        self.state.queue.disconnect_receiver(&self.consumer);
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
        if let Some(value) = self.state.queue.pop(&self.consumer) {
            Ok(value)
        } else if self.state.senders.load(Ordering::Acquire) == 0 {
            // The final sender can enqueue between the first empty observation and decrementing
            // the sender count, so check the queue again before reporting disconnection.
            self.state
                .queue
                .pop(&self.consumer)
                .ok_or(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
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
