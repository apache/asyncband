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

//! A capacity-limited fan-out channel with multiple senders and receivers.
//!
//! Publishing delivers every accepted value to every receiver subscribed at that moment. Older
//! values are never overwritten to make room, so receiving never reports lag; once the shared
//! backlog reaches `capacity`, producers wait for the slowest active receiver.
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
//! If no receivers are active the channel retains nothing, so sending never waits.
//!
//! Receiving a value releases the subscription's claim before returning that value; processing
//! it afterward does not hold capacity. The capacity limit excludes values held by pending `send`
//! futures and values already handed to application code.
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
//! cancelling a `send` future can never leave a gap in the committed order.
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
//! let (publisher, mut audit) = mpmc::bounded(4);
//! let mut metrics = publisher.subscribe();
//!
//! publisher.send("started").await;
//! publisher.send("finished").await;
//!
//! assert_eq!(*audit.recv().await.unwrap(), "started");
//! assert_eq!(*audit.recv().await.unwrap(), "finished");
//! assert_eq!(*metrics.recv().await.unwrap(), "started");
//! assert_eq!(*metrics.recv().await.unwrap(), "finished");
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
//! assert_eq!(*rx1.try_recv().unwrap(), 1);
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

use crate::broadcast::mpmc::common;
use crate::broadcast::mpmc::common::Backlog;
use crate::broadcast::mpmc::common::Inner;
use crate::broadcast::mpmc::common::Received;
use crate::broadcast::mpmc::error::RecvError;
use crate::broadcast::mpmc::error::TryRecvError;
use crate::broadcast::mpmc::error::TrySendError;
use crate::internal::arena::SlotId;
use crate::internal::mutex::Mutex;
use crate::internal::retry_waiter::RetryWaiter;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::waker_batch::WakerBatch;
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
/// assert_eq!(*rx.try_recv().unwrap(), 10);
/// ```
#[track_caller]
pub fn bounded<T>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(
        capacity > 0,
        "broadcast bounded channel requires capacity > 0"
    );

    let (inner, key) = Inner::with_first_subscription(Backlog::fixed(capacity), WaitList::new());
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
    inner: Mutex<Inner<T, WaitList<RetryWaiter>>>,
    /// Number of active senders.
    senders: AtomicUsize,
    /// The logical limit on the retained backlog.
    capacity: usize,
}

/// The sending side of a bounded broadcast channel.
///
/// Cloning this handle adds another producer. Dropping the final sender disconnects
/// the channel. Each receiver may drain its own buffered messages before observing disconnection.
pub struct BoundedSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        // Relaxed is enough because borrowing `&self` keeps `senders > 0`; terminal disconnection
        // synchronizes through the `AcqRel` decrement chain in `Drop`.
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
        // lifetime of its `send` future, so the last sender cannot be dropping while one exists.
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
    /// # Cancellation
    ///
    /// Dropping a pending future does not publish the value and drops the unsent input. Publication
    /// is indivisible: it cannot leave a reserved but unfilled position in the message order.
    ///
    /// # Panics
    ///
    /// Panics if the internal message version counter overflows. After `u64::MAX` successful
    /// publications on one channel instance, the next attempt to publish a message panics.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (publisher, mut receiver) = mpmc::bounded(1);
    /// publisher.send("ready").await;
    /// assert_eq!(*receiver.recv().await.unwrap(), "ready");
    /// # }
    /// ```
    pub async fn send(&self, value: T) {
        let value = match self.try_send(value) {
            Ok(()) => return,
            Err(TrySendError::Full(value)) => value,
        };

        struct SendState<'a, T> {
            sender: &'a BoundedSender<T>,
            waiter: Option<WaiterId>,
            // Boxed once, out of the critical section, and reused by every retry.
            value: Option<Arc<T>>,
        }

        impl<T> Drop for SendState<'_, T> {
            fn drop(&mut self) {
                let Some(id) = self.waiter.take() else {
                    return;
                };
                let (retired, next_waker) = {
                    let mut inner = self.sender.shared.inner.lock();
                    let retired = inner.send_waiters.remove(id);
                    // Hand an unconsumed notification to the next producer while capacity remains.
                    let next_waker = if matches!(retired, RetryWaiter::Notified)
                        && inner.log.retained() < self.sender.shared.capacity
                    {
                        inner.send_waiters.notify_one()
                    } else {
                        None
                    };
                    (retired, next_waker)
                };
                // Wake the follower before `value` drops so a panicking payload destructor cannot
                // strand a waiting producer on capacity that was already freed.
                if let Some(waker) = next_waker {
                    waker.wake();
                }
                drop(retired);
            }
        }

        impl<T> SendState<'_, T> {
            fn poll_send(&mut self, cx: &mut Context<'_>) -> Poll<()> {
                let mut unretained;
                #[allow(unused_assignments)]
                let mut retired_waiter = None;
                let mut wakers = WakerBatch::new();
                {
                    let mut inner = self.sender.shared.inner.lock();
                    if inner.log.retained() == self.sender.shared.capacity {
                        let retired_waker =
                            inner.send_waiters.register(&mut self.waiter, cx.waker());
                        drop(inner);
                        drop(retired_waker);
                        return Poll::Pending;
                    }
                    let msg = self
                        .value
                        .take()
                        .expect("bounded broadcast send polled after completion");
                    retired_waiter = self.waiter.take().map(|id| inner.send_waiters.remove(id));
                    unretained = Some(msg);
                    inner.log.publish(&mut unretained, |msg| msg);
                    inner.waiters.drain_into(&mut wakers);
                }
                wakers.wake_all();
                drop(retired_waiter);
                drop(unretained);
                Poll::Ready(())
            }
        }

        let mut send = SendState {
            sender: self,
            waiter: None,
            value: Some(Arc::new(value)),
        };
        poll_fn(|cx| send.poll_send(cx)).await
    }

    /// Broadcasts a value to all active receivers without waiting for capacity.
    ///
    /// If no receivers are active, the value is discarded and this returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] containing the unsent value if the channel has no capacity.
    /// The value has not been published.
    ///
    /// # Panics
    ///
    /// Panics if the channel's message version counter would overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    /// use asyncband::broadcast::mpmc::TrySendError;
    ///
    /// let (publisher, mut receiver) = mpmc::bounded(1);
    /// publisher.try_send("first").unwrap();
    /// assert_eq!(
    ///     publisher.try_send("second"),
    ///     Err(TrySendError::Full("second"))
    /// );
    ///
    /// assert_eq!(*receiver.try_recv().unwrap(), "first");
    /// publisher.try_send("second").unwrap();
    /// ```
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        // `Arc::new` runs inside the critical section, but only after the capacity check, so a
        // rejected attempt never allocates. Unlike `T::clone` and `T::drop` it cannot run user code
        // that reenters this channel, so it is safe to hold the lock across it. Hoisting it out
        // measured no faster even with eight producers contending — the allocator's thread-local
        // cache already makes it cheap — and it measured slower when producers wait for capacity,
        // because a rejected attempt would then allocate and free once before the `send` future
        // boxes the value for real.
        let mut unretained;
        let mut inner = self.shared.inner.lock();
        // A channel without receivers has an empty backlog and positive capacity.
        if inner.log.retained() == self.shared.capacity {
            return Err(TrySendError::Full(value));
        }
        unretained = Some(value);
        inner.log.publish(&mut unretained, Arc::new);
        if !inner.waiters.is_empty() {
            let mut wakers = WakerBatch::new();
            inner.waiters.drain_into(&mut wakers);
            drop(inner);
            wakers.wake_all();
            drop(unretained);
            return Ok(());
        }
        drop(inner);
        drop(unretained);
        Ok(())
    }

    /// Returns the number of messages currently retained by the channel.
    ///
    /// This is not the number of messages any single receiver can still read. It is the shared
    /// backlog kept alive by the slowest active receiver, and it is what this channel measures
    /// against its [`capacity`](BoundedSender::capacity).
    ///
    /// The returned value is an instantaneous snapshot. It is suitable for diagnostics and soft
    /// flow-control decisions, but other tasks may change it immediately by sending or receiving.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    ///
    /// let (publisher, mut receiver) = mpmc::bounded(4);
    /// publisher.try_send("pending").unwrap();
    /// assert_eq!(publisher.retained_message_count(), 1);
    ///
    /// assert_eq!(*receiver.try_recv().unwrap(), "pending");
    /// assert_eq!(publisher.retained_message_count(), 0);
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
    /// let (publisher, _receiver) = mpmc::bounded::<i32>(8);
    /// assert_eq!(publisher.capacity(), 8);
    /// ```
    pub fn capacity(&self) -> usize {
        self.shared.capacity
    }

    /// Subscribes a new receiver for values published from this point forward.
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
    /// let (publisher, _initial) = mpmc::bounded(4);
    /// publisher.send("earlier").await;
    ///
    /// let mut late = publisher.subscribe();
    /// assert_eq!(late.try_recv(), Err(TryRecvError::Empty));
    /// publisher.send("later").await;
    /// assert_eq!(*late.recv().await.unwrap(), "later");
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
        let mut wakers = WakerBatch::new();
        let reclaimed = {
            let mut inner = self.shared.inner.lock();
            let reclaimed = inner.log.remove_receiver(self.key);
            if !inner.log.has_receivers() {
                inner.send_waiters.notify_all(&mut wakers);
            } else {
                inner
                    .send_waiters
                    .notify_up_to(reclaimed.len(), &mut wakers);
            }
            reclaimed
        };

        // Wake producers before payload destructors can panic, and run both unlocked.
        wakers.wake_all();
        drop(reclaimed);
    }
}

impl<T> BoundedReceiver<T> {
    /// Waits for this receiver's next value and returns the channel's shared `Arc<T>`.
    ///
    /// Receiving never clones `T`. The `Arc` outlives the message's stay in the backlog: a
    /// caller that keeps it keeps the payload alive but holds no capacity. Use
    /// [`recv_cloned`](BoundedReceiver::recv_cloned) to take an owned value instead.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Disconnected`] once all senders have been dropped and this
    /// receiver has consumed its remaining messages.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending future leaves this receiver's cursor unchanged, so a subsequent receive
    /// observes the same next value.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (publisher, mut receiver) = mpmc::bounded(4);
    /// publisher.send("ready").await;
    /// assert_eq!(*receiver.recv().await.unwrap(), "ready");
    /// # }
    /// ```
    pub async fn recv(&mut self) -> Result<Arc<T>, RecvError> {
        Ok(self.wait_next().await?.msg)
    }

    /// Waits for this receiver's next value and returns it as an owned `T`.
    ///
    /// The payload is moved out of the channel without cloning when this read reclaims the message
    /// and nothing else still refers to it, such as an `Arc` kept from a shared receive or another
    /// receiver's clone still in progress. Otherwise it is cloned with the channel unlocked.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Disconnected`] once all senders have been dropped and this
    /// receiver has consumed its remaining messages.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending future leaves this receiver's cursor unchanged, so a subsequent receive
    /// observes the same next value.
    ///
    /// # Panics
    ///
    /// If `T::clone` panics, this receiver has already advanced past the message. Retrying
    /// receives a later message rather than the one whose clone panicked.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (publisher, mut receiver) = mpmc::bounded(4);
    /// publisher.send(String::from("owned")).await;
    /// assert_eq!(receiver.recv_cloned().await, Ok(String::from("owned")));
    /// # }
    /// ```
    pub async fn recv_cloned(&mut self) -> Result<T, RecvError>
    where
        T: Clone,
    {
        let received = self.wait_next().await?;
        Ok(common::take_msg(received))
    }

    /// Attempts to take this receiver's next value without waiting, as the channel's shared
    /// `Arc<T>`.
    ///
    /// See [`recv`](BoundedReceiver::recv) for what keeping the `Arc` means, and
    /// [`try_recv_cloned`](BoundedReceiver::try_recv_cloned) to take an owned value instead.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] when no message is available but a sender remains.
    /// Returns [`TryRecvError::Disconnected`] once all senders have been dropped and this
    /// receiver has consumed its remaining messages.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    ///
    /// let (publisher, mut receiver) = mpmc::bounded(4);
    /// publisher.try_send("ready").unwrap();
    /// assert_eq!(*receiver.try_recv().unwrap(), "ready");
    /// ```
    pub fn try_recv(&mut self) -> Result<Arc<T>, TryRecvError> {
        Ok(self.receive_released()?.msg)
    }

    /// Attempts to take this receiver's next value without waiting and returns it as an owned
    /// `T`.
    ///
    /// See [`recv_cloned`](BoundedReceiver::recv_cloned) for when the payload is moved out rather
    /// than cloned.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] when no message is available but a sender remains.
    /// Returns [`TryRecvError::Disconnected`] once all senders have been dropped and this
    /// receiver has consumed its remaining messages.
    ///
    /// # Panics
    ///
    /// If `T::clone` panics, this receiver has already advanced past the message. Retrying
    /// receives a later message rather than the one whose clone panicked.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    ///
    /// let (publisher, mut receiver) = mpmc::bounded(4);
    /// publisher.try_send(String::from("owned")).unwrap();
    /// assert_eq!(receiver.try_recv_cloned(), Ok(String::from("owned")));
    /// ```
    pub fn try_recv_cloned(&mut self) -> Result<T, TryRecvError>
    where
        T: Clone,
    {
        let received = self.receive_released()?;
        Ok(common::take_msg(received))
    }

    /// Receives without waiting and hands the capacity this receive released to blocked producers
    /// before the caller can touch the payload.
    ///
    /// `take_msg` may run `T::clone` and `T::drop`, and if either panicked before the freed slot
    /// was released, a waiting producer would be stalled permanently. The `Recv` future orders its
    /// poll the same way.
    fn receive_released(&mut self) -> Result<Received<T>, TryRecvError> {
        let mut inner = self.shared.inner.lock();
        let received = inner.try_receive(&self.shared.senders, self.key)?;
        if received.reclaimed {
            if let Some(waker) = inner.send_waiters.notify_one() {
                drop(inner);
                waker.wake();
                return Ok(received);
            }
        }
        Ok(received)
    }

    /// Creates a new subscription positioned after all values already published to the channel.
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
    /// let (publisher, mut original) = mpmc::bounded(4);
    /// publisher.try_send("first").unwrap();
    /// publisher.try_send("second").unwrap();
    ///
    /// let mut fresh = original.resubscribe();
    /// publisher.try_send("third").unwrap();
    ///
    /// assert_eq!(*fresh.try_recv().unwrap(), "third");
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
    /// receiver is falling behind, but other tasks may publish more messages immediately.
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
    /// assert_eq!(*rx.try_recv().unwrap(), 10);
    /// assert_eq!(rx.unread_message_count(), 1);
    /// ```
    pub fn unread_message_count(&self) -> usize {
        self.shared.inner.lock().log.unread(self.key)
    }

    /// Returns `true` if all senders have been dropped.
    ///
    /// Unread messages buffered for this receiver remain available after disconnection, so
    /// [`recv`](Self::recv) and [`try_recv`](Self::try_recv) report disconnection only once this
    /// receiver has drained its remaining backlog. A `false` result can become outdated immediately
    /// if the last sender is dropped concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    ///
    /// let (tx, mut rx) = mpmc::bounded(2);
    /// tx.try_send(1).unwrap();
    /// assert!(!rx.is_disconnected());
    ///
    /// drop(tx);
    /// assert!(rx.is_disconnected());
    /// assert_eq!(*rx.try_recv().unwrap(), 1);
    /// ```
    pub fn is_disconnected(&self) -> bool {
        self.shared.senders.load(Ordering::Acquire) == 0
    }

    /// Returns `true` if this receiver has no unread messages waiting in the backlog.
    ///
    /// Messages already consumed by this receiver that remain retained for slower receivers do not
    /// count, nor do sends waiting for capacity. The result is an instantaneous snapshot and may
    /// become stale as other tasks publish.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    ///
    /// let (tx, mut fast) = mpmc::bounded(2);
    /// let _slow = tx.subscribe();
    /// assert!(fast.is_empty());
    ///
    /// tx.try_send(10).unwrap();
    /// assert!(!fast.is_empty());
    ///
    /// assert_eq!(*fast.try_recv().unwrap(), 10);
    /// assert!(fast.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.unread_message_count() == 0
    }

    fn wait_next(&mut self) -> Recv<'_, T> {
        Recv {
            receiver: self,
            token: None,
        }
    }
}

struct Recv<'a, T> {
    receiver: &'a mut BoundedReceiver<T>,
    token: Option<WakerToken>,
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        // Ready paths clear the token, so only dropping a pending `Recv` future takes this lock.
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

impl<T> Future for Recv<'_, T> {
    type Output = Result<Received<T>, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self { receiver, token } = self.get_mut();

        // Notify a waiting producer before handing out the payload, for the same reason as
        // `receive_released`: a panicking `T::clone` or `T::drop` in `take_msg` must not strand
        // producers waiting for the reclaimed slot.
        let (poll, retired_waker, sender_waker) = {
            let mut inner = receiver.shared.inner.lock();
            let (poll, retired_waker) =
                inner.poll_receive(&receiver.shared.senders, receiver.key, token, cx);
            let sender_waker = match &poll {
                Poll::Ready(Ok(received)) if received.reclaimed => inner.send_waiters.notify_one(),
                _ => None,
            };
            (poll, retired_waker, sender_waker)
        };
        drop(retired_waker);
        if let Some(waker) = sender_waker {
            waker.wake();
        }
        poll
    }
}
