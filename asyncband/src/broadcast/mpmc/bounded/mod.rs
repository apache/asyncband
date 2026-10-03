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

//! A multi-producer multi-consumer broadcast channel with a bounded buffer.
//!
//! This channel supports multiple senders and multiple receivers. Each message sent by any sender
//! is received by all active receivers. Nothing is ever displaced to make room, so a receive never
//! reports lag; instead the channel retains at most `capacity` messages and makes producers wait.
//!
//! # Capacity
//!
//! Capacity counts the *shared* backlog — the messages retained because the slowest active
//! receiver has not read them yet — not messages per receiver. Adding receivers therefore does not
//! consume capacity; falling behind does.
//!
//! Because the backlog is shared, a single receiver that stops draining stalls **every** producer
//! on the channel, however many other receivers are keeping up. That is what "the slowest
//! subscription exerts backpressure" means, and it is the trade a lossless bounded broadcast
//! makes. Drop a receiver that will not drain, and its backlog is released immediately.
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
//! # Fairness
//!
//! Waiting producers are woken as capacity frees, but capacity is not reserved for them: a
//! producer calling [`BoundedSender::try_send`] can take a slot that a woken producer was about to
//! use, and that producer then waits again. Publication itself is one indivisible step, so
//! cancelling a send can never leave a gap in the committed order.
//!
//! # Examples
//!
//! Basic usage:
//!
//! ```
//! use asyncband::broadcast::mpmc;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let (tx, mut rx1) = mpmc::bounded(4);
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
//! use asyncband::broadcast::mpmc;
//! use asyncband::broadcast::mpmc::TrySendError;
//!
//! let (tx, mut rx1) = mpmc::bounded(2);
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
use std::pin::pin;
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
use crate::internal::semaphore::Acquire;
use crate::internal::semaphore::Semaphore;
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
/// use asyncband::broadcast::mpmc;
///
/// let (tx, mut rx) = mpmc::bounded(1);
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
        tx_permits: Semaphore::new(0),
        blocked_senders: AtomicUsize::new(0),
    });
    let sender = BoundedSender {
        shared: shared.clone(),
    };
    let receiver = BoundedReceiver { shared, key };
    (sender, receiver)
}

struct Shared<T> {
    inner: Mutex<Inner<T>>,
    /// Number of active senders.
    senders: AtomicUsize,
    /// The logical limit on the retained backlog.
    capacity: usize,
    /// Producers parked in [`BoundedSender::send`].
    ///
    /// A permit here is a wake-up hint, not a reserved slot: a woken producer rechecks the backlog
    /// and parks again if another producer took the space first. The semaphore starts empty and
    /// only ever grows when a reclaim finds someone waiting, so an idle channel accumulates none.
    tx_permits: Semaphore,
    /// Number of producers in the waiting path of [`BoundedSender::send`].
    ///
    /// This upper bound lets receives skip the semaphore lock when no producer is waiting.
    blocked_senders: AtomicUsize,
}

impl<T> Shared<T> {
    /// Notifies blocked producers after a receive or subscription removal frees capacity.
    ///
    /// Call with the channel unlocked, before payload cloning or destruction can panic.
    fn release_reclaimed(&self, freed: usize) {
        // Cap permits at the number of blocked producers. Surplus permits make later sends retry
        // a full channel instead of parking, especially after dropping a lagging subscription.
        let waiting = self.waiting_senders();
        if freed > 0 && waiting > 0 {
            self.tx_permits.release_if_nonempty(freed.min(waiting));
        }
    }

    /// Wakes every blocked producer when the last subscription leaves.
    ///
    /// Sends now discard payloads without consuming capacity, so every producer can proceed.
    fn release_all(&self) {
        if self.waiting_senders() > 0 {
            self.tx_permits.notify_all();
        }
    }

    /// Returns an upper bound on parked producers without locking the semaphore.
    ///
    /// Producers increment before rechecking capacity under the channel lock; reclaim paths load
    /// after releasing it. A producer is either covered by this count or rechecks capacity after
    /// the reclaim, so skipping or capping notifications cannot strand it.
    fn waiting_senders(&self) -> usize {
        self.blocked_senders.load(Ordering::Acquire)
    }
}

/// The sending side of a bounded broadcast channel.
///
/// The sender can be cloned to create multiple producers. Dropping the final sender disconnects
/// the channel. Each receiver may drain its own buffered messages before observing disconnection.
pub struct BoundedSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        // Relaxed is enough because this count publishes nothing on its own: receivers read it
        // only to decide whether any sender remains, and every message it could hide is published
        // under `inner`, which a receiver holds before it observes the count.
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        Self {
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
        // Only parked receivers need waking. A parked producer borrows a live sender for the
        // duration of its `send`, so the last sender cannot be dropping while one exists.
        if self.shared.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            common::disconnect(&self.shared.inner);
        }
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
    /// use asyncband::broadcast::mpmc;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (tx, mut rx) = mpmc::bounded(1);
    /// tx.send(10).await;
    /// assert_eq!(rx.recv().await, Ok(10));
    /// # }
    /// ```
    pub async fn send(&self, value: T) {
        let value = match self.try_send(value) {
            Ok(()) => return,
            Err(TrySendError::Full(value)) => value,
        };

        struct SendState<'a, T> {
            sender: &'a BoundedSender<T>,
            // Declared before `value` so a cancelled send hands its registration back to the next
            // waiting producer before running the payload's destructor.
            acquire: Acquire<'a>,
            // Boxed once, out of the critical section, and reused by every retry.
            value: Option<Arc<T>>,
        }

        impl<T> Drop for SendState<'_, T> {
            fn drop(&mut self) {
                // Runs before the fields, so the count drops while `acquire` is still queued.
                // That ordering is safe in the direction that matters: the window can only make a
                // receiver skip a wake-up this producer no longer wants, because the future being
                // dropped is exactly the one leaving. Every other waiting producer still holds its
                // own increment, so the count cannot reach zero while one of them needs waking.
                self.sender
                    .shared
                    .blocked_senders
                    .fetch_sub(1, Ordering::Release);
            }
        }

        impl<T> SendState<'_, T> {
            fn poll_send(&mut self, cx: &mut Context<'_>) -> Poll<()> {
                let mut msg = match self.value.take() {
                    Some(msg) => msg,
                    None => return Poll::Ready(()),
                };

                loop {
                    // Enqueue before rechecking. `release_if_nonempty` adds nothing when no
                    // producer is queued, so a reclaim landing between the recheck and the
                    // registration would otherwise drop its wake-up and park this producer for
                    // good. Registering first orders this producer's semaphore acquisition ahead
                    // of the reclaim's, so either the recheck sees the freed slot or the reclaim
                    // sees this waiter.
                    let poll = pin!(&mut self.acquire).poll(cx);

                    msg = match self.sender.try_publish(msg) {
                        Ok(()) => return Poll::Ready(()),
                        Err(msg) => msg,
                    };

                    if poll.is_ready() {
                        self.acquire = self.sender.shared.tx_permits.poll_acquire(1);
                    } else {
                        self.value = Some(msg);
                        return Poll::Pending;
                    }
                }
            }
        }

        // Announce this producer before it can recheck capacity, so a concurrent reclaim either
        // sees it here or is seen by that recheck.
        self.shared.blocked_senders.fetch_add(1, Ordering::Release);
        let acquire = self.shared.tx_permits.poll_acquire(1);
        let mut send = SendState {
            sender: self,
            acquire,
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
    /// use asyncband::broadcast::mpmc;
    /// use asyncband::broadcast::mpmc::TrySendError;
    ///
    /// let (tx, mut rx) = mpmc::bounded(1);
    /// tx.try_send(10).unwrap();
    /// assert_eq!(tx.try_send(20), Err(TrySendError::Full(20)));
    ///
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// tx.try_send(20).unwrap();
    /// ```
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        // `Arc::new` runs inside the critical section, but only after the capacity check, so a
        // rejected send never allocates. Unlike `T::clone` and `T::drop` it cannot run user code
        // that reenters this channel, so it is safe to hold the lock across it. Hoisting it out
        // measured no faster even with eight producers contending — the allocator's thread-local
        // cache already makes it cheap — and it measured slower wherever sends block, because a
        // rejected send would then allocate and free once before `send` boxes the value for real.
        self.publish(value, Arc::new).map_err(TrySendError::Full)
    }

    /// Publishes a message that is already boxed, handing it back if the channel is still full.
    ///
    /// This is the retry step of a waiting `send`, which boxes once with the channel unlocked and
    /// then reuses that `Arc` for every attempt rather than reallocating per retry.
    fn try_publish(&self, msg: Arc<T>) -> Result<(), Arc<T>> {
        self.publish(msg, |msg| msg)
    }

    /// Publishes a message for both send paths.
    ///
    /// Calls `into_msg` only when retaining the payload, so `try_send` can defer its allocation
    /// until capacity is available while `try_publish` passes through its existing `Arc`.
    fn publish<P>(&self, payload: P, into_msg: impl FnOnce(P) -> Arc<T>) -> Result<(), P> {
        let mut discarded = None;
        let mut inner = self.shared.inner.lock();
        if !inner.log.has_receivers() {
            // Drop the discarded payload after unlocking: its destructor may reenter the channel.
            inner.log.publish_discarded();
            discarded = Some(payload);
        } else if inner.log.retained() == self.shared.capacity {
            // Leave waiters registered because no message was published.
            return Err(payload);
        } else {
            inner.log.publish_retained(into_msg(payload));
        }
        let mut wakers = inner.waiters.take_all();
        drop(inner);
        wakers.wake_all();
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
    /// use asyncband::broadcast::mpmc;
    ///
    /// let (tx, mut rx) = mpmc::bounded(4);
    /// tx.try_send(10).unwrap();
    /// assert_eq!(tx.retained_message_count(), 1);
    ///
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// assert_eq!(tx.retained_message_count(), 0);
    /// ```
    pub fn retained_message_count(&self) -> usize {
        self.shared.inner.lock().log.retained()
    }

    /// Returns the number of messages this channel retains before producers wait.
    ///
    /// This is the value passed to [`bounded`] and never changes. Pair it with
    /// [`retained_message_count`](BoundedSender::retained_message_count) to compute headroom.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    ///
    /// let (tx, _rx) = mpmc::bounded::<i32>(8);
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
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    /// use asyncband::broadcast::mpmc::TryRecvError;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (tx, _rx) = mpmc::bounded(4);
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
/// is how a caller releases producers.
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
        let (reclaimed, drained_last) = {
            let mut inner = self.shared.inner.lock();
            let reclaimed = inner.log.remove_receiver(self.key);
            let drained_last = !inner.log.has_receivers();
            (reclaimed, drained_last)
        };

        if drained_last {
            self.shared.release_all();
        } else {
            self.shared.release_reclaimed(reclaimed.len());
        }

        // Payload destructors run last, and unlocked.
        drop(reclaimed);
    }
}

impl<T: Clone> BoundedReceiver<T> {
    /// Receives the next value for this receiver.
    ///
    /// # Returns
    ///
    /// * `Ok(T)`: The next message.
    /// * `Err(RecvError::Disconnected)`: All senders have been dropped and this receiver has no
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
    /// use asyncband::broadcast::mpmc;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (tx, mut rx) = mpmc::bounded(4);
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
    /// * `Err(TryRecvError::Disconnected)`: All senders have been dropped and this receiver has no
    ///   remaining messages.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    ///
    /// let (tx, mut rx) = mpmc::bounded(4);
    /// tx.try_send(10).unwrap();
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// ```
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let (msg, reclaimed) =
            common::try_receive(&self.shared.inner, &self.shared.senders, self.key)?;

        // Release before taking the payload: `take_msg` runs `T::clone` and `T::drop`, and if
        // either panics the slots this receive already freed would otherwise never be handed to a
        // parked producer, stalling it permanently.
        self.shared.release_reclaimed(reclaimed.len());
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
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    ///
    /// let (tx, mut rx) = mpmc::bounded(4);
    /// tx.try_send(1).unwrap();
    /// tx.try_send(2).unwrap();
    ///
    /// let mut rx2 = rx.resubscribe();
    /// tx.try_send(3).unwrap();
    ///
    /// assert_eq!(rx2.try_recv(), Ok(3));
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
    /// use asyncband::broadcast::mpmc;
    ///
    /// let (tx, mut rx) = mpmc::bounded(4);
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

        let (msg, reclaimed) = match common::poll_receive(
            &receiver.shared.inner,
            &receiver.shared.senders,
            receiver.key,
            token,
            cx,
        ) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Ready(Ok(received)) => received,
        };

        // Release before taking the payload, for the same reason as `try_recv`: a panicking
        // `T::clone` must not strand producers on slots this receive already freed.
        receiver.shared.release_reclaimed(reclaimed.len());
        Poll::Ready(Ok(common::take_msg(msg, reclaimed)))
    }
}
