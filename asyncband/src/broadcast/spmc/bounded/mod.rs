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

//! A single-producer multi-consumer broadcast channel with a bounded buffer.
//!
//! Each message sent is received by all active receivers. Nothing is ever displaced to make room,
//! so a receive never reports lag; instead the channel retains at most `capacity` messages and
//! makes the producer wait. The sender is not [`Clone`]; [`BoundedSender::send`] and
//! [`BoundedSender::try_send`] take `&mut self` so a second producer cannot exist at compile time.
//!
//! # Capacity
//!
//! Capacity counts the *shared* backlog — the messages retained because the slowest active
//! receiver has not read them yet — not messages per receiver. Adding receivers therefore does not
//! consume capacity; falling behind does.
//!
//! Because the backlog is shared, a single receiver that stops draining stalls the producer,
//! however many other receivers are keeping up. That is what "the slowest subscription exerts
//! backpressure" means, and it is the trade a lossless bounded broadcast makes. Drop a receiver
//! that will not drain, and its backlog is released immediately.
//!
//! If no receivers are active the channel retains nothing, so a send never waits.
//!
//! A successful receive releases its subscription's claim before returning the value; processing
//! that value afterward does not hold capacity. The capacity limit excludes pending sends and
//! values already handed to application code.
//!
//! # Receivers
//!
//! Each receiver has an independent cursor. Use [`BoundedSender::subscribe`] or
//! [`BoundedReceiver::resubscribe`] to create a receiver that starts at the current tail. A new
//! subscription never sees messages published before it existed.
//!
//! [`BoundedSender::send`] holds `&mut self` while it waits, so the sender cannot
//! [`subscribe`](BoundedSender::subscribe) until that send completes. Create the extra
//! subscription from a live receiver with [`BoundedReceiver::resubscribe`] instead.
//!
//! # Cancel safety
//!
//! Publication itself is one indivisible step, so cancelling a send can never leave a gap in the
//! committed order. Cancelling a pending `recv` does not advance the subscription cursor.
//!
//! # Examples
//!
//! Basic usage:
//!
//! ```
//! use asyncband::broadcast::spmc;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let (mut tx, mut rx1) = spmc::bounded(4);
//! let mut rx2 = tx.subscribe();
//!
//! tx.send(10).await;
//! tx.send(20).await;
//!
//! assert_eq!(rx1.recv().await, Ok(10));
//! assert_eq!(rx1.recv().await, Ok(20));
//! assert_eq!(rx2.recv().await, Ok(10));
//! assert_eq!(rx2.recv().await, Ok(20));
//! # }
//! ```
//!
//! The slowest receiver holds the capacity:
//!
//! ```
//! use asyncband::broadcast::spmc;
//! use asyncband::broadcast::spmc::TrySendError;
//!
//! let (mut tx, mut rx1) = spmc::bounded(2);
//! let rx2 = tx.subscribe();
//!
//! tx.try_send(1).unwrap();
//! tx.try_send(2).unwrap();
//! assert_eq!(tx.try_send(3), Err(TrySendError::Full(3)));
//!
//! // `rx1` draining is not enough: `rx2` has read neither message, so both stay retained.
//! assert_eq!(rx1.try_recv(), Ok(1));
//! assert_eq!(tx.retained_message_count(), 2);
//! assert_eq!(tx.try_send(3), Err(TrySendError::Full(3)));
//!
//! // Dropping the lagging receiver releases the backlog only it was holding. `rx1` has still not
//! // read the second message, so that one stays.
//! drop(rx2);
//! assert_eq!(tx.retained_message_count(), 1);
//! tx.try_send(3).unwrap();
//! ```

use std::fmt;
use std::future::Future;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use super::common;
use super::common::Backlog;
use super::common::Inner;
use super::error::RecvError;
use super::error::TryRecvError;
use super::error::TrySendError;
use crate::internal::arena::SlotId;
use crate::internal::mutex::Mutex;
use crate::internal::wake_all;
use crate::internal::wakerset::WakerToken;

#[cfg(test)]
mod tests;

/// Creates a new broadcast channel that retains at most `capacity` messages.
///
/// Every accepted value stays readable by every receiver that was active when it was accepted.
/// Once `capacity` messages are retained, [`BoundedSender::send`] waits and
/// [`BoundedSender::try_send`] reports [`TrySendError::Full`] until the slowest active receiver
/// consumes a message or is dropped.
///
/// # Panics
///
/// Panics if `capacity` is zero.
///
/// # Examples
///
/// ```
/// use asyncband::broadcast::spmc;
///
/// let (mut tx, mut rx) = spmc::bounded(1);
/// tx.try_send(10).unwrap();
/// assert_eq!(rx.try_recv(), Ok(10));
/// ```
#[track_caller]
pub fn bounded<T: Clone>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(
        capacity > 0,
        "broadcast bounded channel requires capacity > 0"
    );

    let (inner, key) = Inner::with_first_subscription(Backlog::fixed(capacity));
    let shared = Arc::new(Shared {
        inner,
        senders: AtomicUsize::new(1),
        capacity,
    });
    let sender = BoundedSender {
        shared: shared.clone(),
    };
    let receiver = BoundedReceiver { shared, key };
    (sender, receiver)
}

struct Shared<T> {
    /// Buffer, receiver cursors, parked receivers, and the single parked producer, all under one
    /// lock.
    inner: Mutex<Inner<T>>,
    /// `1` while the sender is alive, `0` after it is dropped.
    senders: AtomicUsize,
    /// The logical limit on the retained backlog.
    capacity: usize,
}

/// The sending side of a bounded broadcast channel.
///
/// This handle is not [`Clone`]. Dropping it disconnects the channel. Each receiver may drain its
/// own buffered messages before observing disconnection.
pub struct BoundedSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> fmt::Debug for BoundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedSender").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedSender<T> {
    fn drop(&mut self) {
        self.shared.senders.store(0, Ordering::Release);
        common::disconnect(&self.shared.inner);
    }
}

impl<T> BoundedSender<T> {
    /// Broadcasts a value to all active receivers, waiting for capacity if the channel is full.
    ///
    /// The wait ends when the slowest active receiver consumes a retained message or is dropped.
    /// If no receivers are active, the message is dropped immediately and this returns without
    /// waiting.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe in the sense that matters for a lossless log: the value is
    /// either published to every active receiver or not published at all. Publication happens in
    /// one indivisible step, so a cancelled send cannot leave a reserved but unfilled position in
    /// the committed order. A send cancelled before it published drops the value with the future.
    ///
    /// # Panics
    ///
    /// Panics if the internal message version counter overflows. After `u64::MAX` successful sends
    /// on one channel instance, the next send panics.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::spmc;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (mut tx, mut rx) = spmc::bounded(1);
    /// tx.send(10).await;
    /// assert_eq!(rx.recv().await, Ok(10));
    /// # }
    /// ```
    pub async fn send(&mut self, value: T) {
        let value = match self.try_send(value) {
            Ok(()) => return,
            Err(TrySendError::Full(value)) => value,
        };

        struct SendState<'a, T> {
            sender: &'a mut BoundedSender<T>,
            // Boxed once, out of the critical section, and reused by every retry. Dropped after
            // `SendState::drop` has already released the producer slot, so a cancelled send
            // unregisters before running the payload destructor.
            value: Option<Arc<T>>,
        }

        impl<T> Drop for SendState<'_, T> {
            fn drop(&mut self) {
                // Take the slot with the channel unlocked afterward so the replaced waker is
                // dropped outside the lock. The payload in `value` is dropped only after this
                // returns.
                let waker = {
                    let mut inner = self.sender.shared.inner.lock();
                    inner.producer.take()
                };
                drop(waker);
            }
        }

        impl<T> SendState<'_, T> {
            fn poll_send(&mut self, cx: &mut Context<'_>) -> Poll<()> {
                let msg = match self.value.take() {
                    Some(msg) => msg,
                    None => return Poll::Ready(()),
                };

                let mut inner = self.sender.shared.inner.lock();

                if !inner.log.has_receivers() {
                    inner.log.publish_discarded();
                    let retired_producer = inner.producer.take();
                    let wakers = inner.waiters.drain();
                    drop(inner);
                    wake_all(wakers);
                    drop(retired_producer);
                    drop(msg);
                    return Poll::Ready(());
                }

                if inner.log.retained() == self.sender.shared.capacity {
                    // Same critical section as the capacity check: a reclaim that lands between
                    // those two observations cannot skip this waiter.
                    let retired = inner.producer.replace(cx.waker().clone());
                    drop(inner);
                    drop(retired);
                    self.value = Some(msg);
                    return Poll::Pending;
                }

                inner.log.publish_retained(msg);
                let retired_producer = inner.producer.take();
                let wakers = inner.waiters.drain();
                drop(inner);
                // Wake receivers before dropping the retired producer waker: that waker is this
                // send, so it must not run under the lock, but a panic in its Drop must not skip
                // the receiver wake-ups either.
                wake_all(wakers);
                drop(retired_producer);
                Poll::Ready(())
            }
        }

        let mut send = SendState {
            sender: self,
            value: Some(Arc::new(value)),
        };
        poll_fn(|cx| send.poll_send(cx)).await
    }

    /// Attempts to broadcast a value to all active receivers without waiting.
    ///
    /// # Returns
    ///
    /// * `Ok(())`: The value was published, or discarded because no receivers are active.
    /// * `Err(TrySendError::Full(value))`: The channel already retains `capacity` messages. The
    ///   value was not published and is returned unchanged.
    ///
    /// # Panics
    ///
    /// Panics if the internal message version counter overflows.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::spmc;
    /// use asyncband::broadcast::spmc::TrySendError;
    ///
    /// let (mut tx, mut rx) = spmc::bounded(1);
    /// tx.try_send(10).unwrap();
    /// assert_eq!(tx.try_send(20), Err(TrySendError::Full(20)));
    ///
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// tx.try_send(20).unwrap();
    /// ```
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        // `Arc::new` runs inside the critical section, but only after the capacity check, so a
        // rejected send never allocates. Unlike `T::clone` and `T::drop` it cannot run user code
        // that reenters this channel, so it is safe to hold the lock across it.
        self.publish(value, Arc::new).map_err(TrySendError::Full)
    }

    /// The publish step both send paths share.
    ///
    /// `into_msg` is called only once this decides the message will actually be retained, which is
    /// what lets `try_send` defer its allocation past the capacity check.
    ///
    /// Publishing and draining the wait set share one critical section, so a receiver can never
    /// observe an empty buffer and park after this message became visible.
    fn publish<P>(&mut self, payload: P, into_msg: impl FnOnce(P) -> Arc<T>) -> Result<(), P> {
        let mut discarded = None;
        let wakers = {
            let mut inner = self.shared.inner.lock();

            if !inner.log.has_receivers() {
                // Nothing can read this message. The payload leaves the critical section with us
                // and is dropped below, so `T::drop` never runs under the lock.
                inner.log.publish_discarded();
                discarded = Some(payload);
            } else if inner.log.retained() == self.shared.capacity {
                // Nothing was published, so there is no wait set to drain.
                return Err(payload);
            } else {
                inner.log.publish_retained(into_msg(payload));
            }

            inner.waiters.drain()
        };

        wake_all(wakers);
        drop(discarded);
        Ok(())
    }

    /// Returns the number of messages currently retained by the channel.
    ///
    /// This is not the number of messages any single receiver can still read. It is the shared
    /// backlog kept alive by the slowest active receiver, and it is what this channel measures
    /// against its [`capacity`](BoundedSender::capacity).
    ///
    /// The returned value is an instantaneous snapshot. It is suitable for diagnostics and soft
    /// flow-control decisions, but concurrent sends and receives may change it immediately.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::spmc;
    ///
    /// let (mut tx, mut rx) = spmc::bounded(4);
    /// tx.try_send(10).unwrap();
    /// assert_eq!(tx.retained_message_count(), 1);
    ///
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// assert_eq!(tx.retained_message_count(), 0);
    /// ```
    pub fn retained_message_count(&self) -> usize {
        self.shared.inner.lock().log.retained()
    }

    /// Returns the number of messages this channel retains before the producer waits.
    ///
    /// This is the value passed to [`bounded`] and never changes. Pair it with
    /// [`retained_message_count`](BoundedSender::retained_message_count) to compute headroom.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::spmc;
    ///
    /// let (tx, _rx) = spmc::bounded::<i32>(8);
    /// assert_eq!(tx.capacity(), 8);
    /// ```
    pub fn capacity(&self) -> usize {
        self.shared.capacity
    }

    /// Creates a new receiver that starts receiving messages from the current tail of the channel.
    ///
    /// Subscribing never consumes capacity: the new cursor starts at the tail, so it retains
    /// nothing that was not already retained.
    ///
    /// This cannot be called while [`send`](BoundedSender::send) is waiting, because that future
    /// holds `&mut self`. Use [`BoundedReceiver::resubscribe`] from a live receiver instead.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::spmc;
    /// use asyncband::broadcast::spmc::TryRecvError;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (mut tx, _rx) = spmc::bounded(4);
    /// tx.send(10).await;
    ///
    /// let mut rx = tx.subscribe();
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    /// tx.send(20).await;
    /// assert_eq!(rx.recv().await, Ok(20));
    /// # }
    /// ```
    #[must_use = "the receiver is dropped immediately if it is not retained"]
    pub fn subscribe(&self) -> BoundedReceiver<T> {
        let key = self.shared.inner.lock().log.subscribe();
        BoundedReceiver {
            shared: self.shared.clone(),
            key,
        }
    }
}

/// A receiver for a bounded broadcast channel.
///
/// Each receiver sees every message sent to the channel while the receiver is active. A receiver
/// that stops draining holds capacity for the whole channel, so dropping one that will not keep up
/// is how a caller releases the producer.
pub struct BoundedReceiver<T> {
    shared: Arc<Shared<T>>,
    key: SlotId,
}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedReceiver<T> {
    fn drop(&mut self) {
        let (reclaimed, producer) = {
            let mut inner = self.shared.inner.lock();
            let reclaimed = inner.log.remove_receiver(self.key);
            let drained_last = !inner.log.has_receivers();
            let producer = common::take_producer_on_reclaim(&mut inner, &reclaimed, drained_last);
            (reclaimed, producer)
        };

        // Wake before dropping reclaimed payloads: a panicking destructor must not strand the
        // producer on capacity this drop already freed.
        common::wake_producer(producer);
        drop(reclaimed);
    }
}

impl<T: Clone> BoundedReceiver<T> {
    /// Receives the next value for this receiver.
    ///
    /// # Returns
    ///
    /// * `Ok(T)`: The next message.
    /// * `Err(RecvError::Disconnected)`: The sender has been dropped and this receiver has no
    ///   remaining messages.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a `select` statement and some
    /// other branch completes first, it is guaranteed that no messages were received on this
    /// channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::spmc;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (mut tx, mut rx) = spmc::bounded(4);
    /// tx.send(10).await;
    /// assert_eq!(rx.recv().await, Ok(10));
    /// # }
    /// ```
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        Recv {
            receiver: self,
            token: None,
        }
        .await
    }

    /// Attempts to receive the next value for this receiver without blocking.
    ///
    /// # Returns
    ///
    /// * `Ok(T)`: The next message.
    /// * `Err(TryRecvError::Empty)`: No message is currently available.
    /// * `Err(TryRecvError::Disconnected)`: The sender has been dropped and this receiver has no
    ///   remaining messages.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::spmc;
    ///
    /// let (mut tx, mut rx) = spmc::bounded(4);
    /// tx.try_send(10).unwrap();
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// ```
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let ((msg, reclaimed), producer) =
            common::try_receive(&self.shared.inner, &self.shared.senders, self.key)?;

        // Wake before taking the payload: `take_msg` runs `T::clone` and `T::drop`, and if either
        // panics the slot this receive already freed would otherwise never reach the parked
        // producer, stalling it permanently.
        common::wake_producer(producer);
        Ok(common::take_msg(msg, reclaimed))
    }
}

impl<T> BoundedReceiver<T> {
    /// Re-subscribes to the channel, returning a new receiver that starts receiving messages from
    /// the *current* tail of the channel.
    ///
    /// The new receiver skips every value already published, including the latest retained value.
    /// The original receiver is unchanged and continues to retain its own backlog until it
    /// consumes those messages or is dropped.
    ///
    /// This is also how to add a subscription while [`BoundedSender::send`] is waiting: that
    /// future holds `&mut self` on the sender, so [`BoundedSender::subscribe`] cannot be called
    /// until it completes.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::spmc;
    ///
    /// let (mut tx, mut rx) = spmc::bounded(4);
    /// tx.try_send(1).unwrap();
    /// tx.try_send(2).unwrap();
    ///
    /// let mut rx2 = rx.resubscribe();
    /// tx.try_send(3).unwrap();
    ///
    /// assert_eq!(rx2.try_recv(), Ok(3));
    /// ```
    ///
    /// Adding a subscription while the producer is waiting for capacity:
    ///
    /// ```
    /// use std::future::Future;
    /// use std::task::Context;
    /// use std::task::Poll;
    /// use std::task::Waker;
    ///
    /// use asyncband::broadcast::spmc;
    ///
    /// let (mut tx, rx) = spmc::bounded(1);
    /// tx.try_send(0).unwrap();
    ///
    /// let mut send = Box::pin(tx.send(1));
    /// let mut cx = Context::from_waker(Waker::noop());
    /// assert!(matches!(send.as_mut().poll(&mut cx), Poll::Pending));
    ///
    /// // `tx.subscribe()` would not compile here: `send` holds `&mut tx`.
    /// let _late = rx.resubscribe();
    /// assert_eq!(rx.unread_message_count(), 1);
    /// ```
    #[must_use = "the receiver is dropped immediately if it is not retained"]
    pub fn resubscribe(&self) -> Self {
        let key = self.shared.inner.lock().log.subscribe();
        Self {
            shared: self.shared.clone(),
            key,
        }
    }

    /// Returns the number of messages this receiver can still read.
    ///
    /// This count is specific to this receiver, unlike
    /// [`BoundedSender::retained_message_count`], which reports the shared backlog retained by the
    /// slowest active receiver.
    ///
    /// The returned value is an instantaneous snapshot. It is suitable for detecting that this
    /// receiver is falling behind, but concurrent sends may change it immediately.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::spmc;
    ///
    /// let (mut tx, mut rx) = spmc::bounded(4);
    /// assert_eq!(rx.unread_message_count(), 0);
    ///
    /// tx.try_send(10).unwrap();
    /// tx.try_send(20).unwrap();
    /// assert_eq!(rx.unread_message_count(), 2);
    ///
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// assert_eq!(rx.unread_message_count(), 1);
    /// ```
    pub fn unread_message_count(&self) -> usize {
        self.shared.inner.lock().log.unread(self.key)
    }
}

struct Recv<'a, T> {
    receiver: &'a mut BoundedReceiver<T>,
    token: Option<WakerToken>,
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        // Ready paths clear the token, so only a cancelled pending receive takes this lock.
        if self.token.is_none() {
            return;
        }

        common::unregister(
            &self.receiver.shared.inner,
            &self.receiver.shared.senders,
            self.receiver.key,
            &mut self.token,
        );
    }
}

impl<T: Clone> Future for Recv<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self { receiver, token } = self.get_mut();

        let ((msg, reclaimed), producer) = match common::poll_receive(
            &receiver.shared.inner,
            &receiver.shared.senders,
            receiver.key,
            token,
            cx,
        ) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Ready(Ok(outcome)) => outcome,
        };

        // Wake before taking the payload, for the same reason as `try_recv`: a panicking
        // `T::clone` must not strand the producer on a slot this receive already freed.
        common::wake_producer(producer);
        Poll::Ready(Ok(common::take_msg(msg, reclaimed)))
    }
}
