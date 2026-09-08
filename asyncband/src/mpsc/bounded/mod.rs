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
use std::mem;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::ready;

use self::capacity::Capacity;
use self::ring::Ring;
use super::RecvError;
use super::SendError;
use super::TryRecvError;
use super::TrySendError;

// Capacity accounts for permits and queued messages. Ring owns FIFO publication; the receiver
// alone advances its read cursor. A public reservation does not claim a position in the ring.
mod capacity;
mod ring;

// The low bit marks closure; reservation and publication cursors advance in matching units.
const SEQUENCE_STEP: usize = 2;

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
        capacity: Capacity::new(buffer),
    });
    let sender = BoundedSender {
        state: state.clone(),
    };
    let receiver = BoundedReceiver { state, head: 0 };
    (sender, receiver)
}

struct Shared<T> {
    buffer: Ring<T>,
    senders: AtomicUsize,
    capacity: Capacity,
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
    /// caller must retain ownership if capacity is unavailable, or [`Self::reserve`] to wait for
    /// capacity before constructing the message.
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
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
    /// Dropping a pending reservation removes its wait registration without consuming capacity.
    /// Notifications grant a retry, so a new sender may acquire capacity before a woken waiter.
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
        match self.try_reserve() {
            Ok(permit) => return Ok(permit),
            Err(TrySendError::Disconnected(())) => return Err(SendError::new(())),
            Err(TrySendError::Full(())) => {}
        }
        let mut waiter = self.state.capacity.waiter();
        poll_fn(|cx| {
            match self.try_reserve() {
                Ok(permit) => {
                    waiter.finish();
                    return Poll::Ready(Ok(permit));
                }
                Err(TrySendError::Disconnected(())) => {
                    waiter.finish();
                    return Poll::Ready(Err(SendError::new(())));
                }
                Err(TrySendError::Full(())) => {}
            }
            waiter.register(cx.waker());
            match self.try_reserve() {
                Ok(permit) => {
                    waiter.finish();
                    Poll::Ready(Ok(permit))
                }
                Err(TrySendError::Disconnected(())) => {
                    waiter.finish();
                    Poll::Ready(Err(SendError::new(())))
                }
                Err(TrySendError::Full(())) => Poll::Pending,
            }
        })
        .await
    }

    /// Reserves capacity for one message without waiting.
    ///
    /// Returns [`TrySendError::Full`] if queued messages and outstanding permits occupy the
    /// buffer, or [`TrySendError::Disconnected`] if the receiver has been dropped.
    pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        self.state.capacity.try_acquire()?;
        Ok(Permit { sender: self })
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
        match self.try_reserve() {
            Ok(permit) => permit
                .send(value)
                .map_err(|error| TrySendError::Disconnected(error.into_inner())),
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
    sender: &'a BoundedSender<T>,
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
    pub fn send(self, value: T) -> Result<(), SendError<T>> {
        // SAFETY: This permit owns one unit of capacity. No user code runs between claiming the
        // position and publishing its value; the consumer returns the capacity after reading it.
        let claim = match unsafe { self.sender.state.buffer.claim() } {
            Ok(claim) => claim,
            Err(()) => return Err(SendError::new(value)),
        };
        // Publication can wake user code that panics. Transfer capacity ownership first so
        // unwinding cannot return a permit for a message that is already in the ring.
        mem::forget(self);
        claim.publish(value);
        Ok(())
    }
}

impl<T> Drop for Permit<'_, T> {
    fn drop(&mut self) {
        self.sender.state.capacity.cancel();
    }
}

/// The receiving endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`] function.
pub struct BoundedReceiver<T> {
    state: Arc<Shared<T>>,
    head: usize,
}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedReceiver<T> {
    fn drop(&mut self) {
        struct DrainOnDrop<'a, T> {
            ring: &'a Ring<T>,
            head: &'a mut usize,
            tail: usize,
        }
        impl<T> Drop for DrainOnDrop<'_, T> {
            fn drop(&mut self) {
                // SAFETY: This guard lives only within the exclusive receiver's drop, after close.
                unsafe { self.ring.drain(self.head, self.tail) };
            }
        }

        let tail = self.state.buffer.close();
        let drain = DrainOnDrop {
            ring: &self.state.buffer,
            head: &mut self.head,
            tail,
        };
        // A registered waker may own a sender; release it to break that ownership cycle.
        let receiver_waker = self.state.buffer.take_receiver_waker();
        // Complete notifications before dropping messages. Either kind of callback may panic;
        // the drain guard still releases buffered values if a wake or waker drop unwinds.
        self.state.capacity.close();
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
        let value = if let Some(value) = ready!(unsafe { self.state.buffer.pop(&mut self.head) }) {
            value
        } else if self.state.senders.load(Ordering::Acquire) == 0 {
            // The final sender can enqueue between the first empty observation and decrementing
            // the sender count, so check the queue again before reporting disconnection.
            // SAFETY: The exclusive receiver borrow still guarantees a single consumer.
            let Some(value) = ready!(unsafe { self.state.buffer.pop(&mut self.head) }) else {
                return Poll::Ready(Err(TryRecvError::Disconnected));
            };
            value
        } else {
            return Poll::Ready(Err(TryRecvError::Empty));
        };
        self.state.capacity.consume(self.head);
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
