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

//! A multi-producer multi-consumer broadcast channel with an unbounded buffer.
//!
//! This channel supports multiple senders and multiple receivers. Each message sent by any
//! sender is received by all active receivers. If a receiver falls behind, messages are buffered
//! until the receiver consumes them or is dropped.
//!
//! # Memory usage
//!
//! This channel does not impose a capacity limit. A slow or stalled receiver can cause the
//! buffer to grow without bound, because messages are retained until every active receiver has
//! consumed them or the receiver is dropped. Use
//! [`UnboundedSender::retained_message_count`] to monitor the number of messages currently retained
//! by the channel. Use [`bounded`] instead when producers should wait for the slowest receiver
//! rather than let the backlog grow.
//!
//! The buffer keeps the capacity a steady workload needs, so a channel that repeatedly fills and
//! drains does not reallocate. Capacity grown for a one-off burst is released once a later cycle
//! drains completely without needing it.
//!
//! # Receivers
//!
//! Each receiver has an independent cursor. Use [`UnboundedSender::subscribe`] or
//! [`UnboundedReceiver::resubscribe`] to create a receiver that starts at the current tail.
//!
//! Messages are reclaimed once the slowest receiver moves past them, which scans one slot per
//! receiver. Only the receiver that advances the slowest cursor pays for that scan. The channel
//! keeps a slot for every receiver it hands out, so the cost follows the largest number of
//! receivers that were ever active at once rather than the number active now.
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
//! let (tx, mut rx1) = mpmc::unbounded();
//! let mut rx2 = tx.subscribe();
//!
//! tx.send(10);
//! tx.send(20);
//!
//! assert_eq!(rx1.recv().await, Ok(10));
//! assert_eq!(rx1.recv().await, Ok(20));
//! assert_eq!(rx2.recv().await, Ok(10));
//! assert_eq!(rx2.recv().await, Ok(20));
//! # }
//! ```
//!
//! Slow receivers do not miss messages:
//!
//! ```
//! use asyncband::broadcast::mpmc;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let (tx, mut rx1) = mpmc::unbounded();
//! let mut rx2 = tx.subscribe();
//!
//! tx.send(1);
//! tx.send(2);
//!
//! // One receiver draining the channel does not discard what the other has not read yet.
//! assert_eq!(rx1.recv().await, Ok(1));
//! assert_eq!(rx1.recv().await, Ok(2));
//! assert_eq!(tx.retained_message_count(), 2);
//!
//! assert_eq!(rx2.recv().await, Ok(1));
//! assert_eq!(rx2.recv().await, Ok(2));
//! assert_eq!(tx.retained_message_count(), 0);
//! # }
//! ```
//!
//! [`bounded`]: super::bounded

use std::fmt;
use std::future::Future;
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
use crate::internal::arena::SlotId;
use crate::internal::mutex::Mutex;
use crate::internal::waitset::WakerToken;
use crate::internal::waitset::wake_all;

#[cfg(test)]
mod tests;

/// Creates a new broadcast channel with an unbounded buffer.
///
/// Every accepted value is retained until all active receivers consume it or are dropped.
///
/// # Examples
///
/// ```
/// use asyncband::broadcast::mpmc;
///
/// let (tx, mut rx) = mpmc::unbounded();
/// tx.send(10);
/// assert_eq!(rx.try_recv(), Ok(10));
/// ```
pub fn unbounded<T: Clone>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let (inner, key) = Inner::with_first_subscription(Backlog::elastic());
    let shared = Arc::new(Shared {
        inner,
        senders: AtomicUsize::new(1),
    });
    let sender = UnboundedSender {
        shared: shared.clone(),
    };
    let receiver = UnboundedReceiver { shared, key };
    (sender, receiver)
}

struct Shared<T> {
    /// Buffer, receiver cursors, and parked receivers, all under a single lock.
    inner: Mutex<Inner<T>>,
    /// Number of active senders.
    senders: AtomicUsize,
}

/// The sending side of an unbounded broadcast channel.
///
/// The sender can be cloned to create multiple producers. Dropping the final sender disconnects
/// the channel. Each receiver may drain its own buffered messages before observing disconnection.
pub struct UnboundedSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for UnboundedSender<T> {
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

impl<T> fmt::Debug for UnboundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedSender").finish_non_exhaustive()
    }
}

impl<T> Drop for UnboundedSender<T> {
    fn drop(&mut self) {
        match self.shared.senders.fetch_sub(1, Ordering::AcqRel) {
            1 => common::disconnect(&self.shared.inner),
            _ => {
                // there are still other senders left, do nothing
            }
        }
    }
}

impl<T> UnboundedSender<T> {
    /// Broadcasts a value to all active receivers.
    ///
    /// This operation does not wait for receiver capacity. If receivers fall behind, messages
    /// remain buffered until all active receivers have consumed them or the lagging receivers
    /// are dropped.
    ///
    /// If no receivers are active, the message is dropped immediately.
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
    /// let (tx, mut rx) = mpmc::unbounded();
    /// tx.send(10);
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// ```
    pub fn send(&self, msg: T) {
        let msg = Arc::new(msg);

        // Publishing and draining the wait set share one critical section, so a receiver can never
        // observe an empty buffer and park after this message became visible.
        let (unretained, wakers) = {
            let mut inner = self.shared.inner.lock();
            let unretained = inner.log.publish(msg);
            let wakers = inner.waiters.drain();
            (unretained, wakers)
        };

        // Notify all waiting receivers. An unsent message is dropped here too, once the lock is
        // released.
        wake_all(wakers);
        drop(unretained);
    }

    /// Returns the number of messages currently retained by the channel.
    ///
    /// This is not the number of messages any single receiver can still read. It is the shared
    /// backlog kept alive by the slowest active receiver.
    ///
    /// The returned value is an instantaneous snapshot. It is suitable for diagnostics and soft
    /// flow-control decisions, but concurrent sends and receives may change it immediately.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    ///
    /// let (tx, mut rx) = mpmc::unbounded();
    /// tx.send(10);
    /// assert_eq!(tx.retained_message_count(), 1);
    ///
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// assert_eq!(tx.retained_message_count(), 0);
    /// ```
    pub fn retained_message_count(&self) -> usize {
        self.shared.inner.lock().log.retained()
    }

    /// Creates a new receiver that starts receiving messages from the current tail of the channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    /// use asyncband::broadcast::mpmc::TryRecvError;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (tx, _) = mpmc::unbounded();
    /// tx.send(10);
    ///
    /// let mut rx = tx.subscribe();
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    /// tx.send(20);
    /// assert_eq!(rx.recv().await, Ok(20));
    /// # }
    /// ```
    pub fn subscribe(&self) -> UnboundedReceiver<T> {
        let key = self.shared.inner.lock().log.subscribe();
        UnboundedReceiver {
            shared: self.shared.clone(),
            key,
        }
    }
}

/// A receiver for an unbounded broadcast channel.
///
/// Each receiver sees every message sent to the channel while the receiver is active.
pub struct UnboundedReceiver<T> {
    shared: Arc<Shared<T>>,
    key: SlotId,
}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for UnboundedReceiver<T> {
    fn drop(&mut self) {
        let reclaimed = {
            let mut inner = self.shared.inner.lock();
            inner.log.remove_receiver(self.key)
        };
        drop(reclaimed);
    }
}

impl<T: Clone> UnboundedReceiver<T> {
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
    /// let (tx, mut rx) = mpmc::unbounded();
    /// tx.send(10);
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
    /// let (tx, mut rx) = mpmc::unbounded();
    /// tx.send(10);
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// ```
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let (msg, reclaimed) =
            common::try_receive(&self.shared.inner, &self.shared.senders, self.key)?;
        Ok(common::take_msg(msg, reclaimed))
    }
}

impl<T> UnboundedReceiver<T> {
    /// Re-subscribes to the channel, returning a new receiver that starts receiving messages from
    /// the *current* tail of the channel.
    ///
    /// This is useful if the receiver wants to jump to the latest message, skipping everything in
    /// between. The original receiver is unchanged and continues to retain its own backlog until
    /// it consumes those messages or is dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    ///
    /// let (tx, mut rx) = mpmc::unbounded();
    /// tx.send(1);
    /// tx.send(2);
    ///
    /// let mut rx2 = rx.resubscribe();
    /// tx.send(3);
    ///
    /// assert_eq!(rx2.try_recv(), Ok(3));
    /// ```
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
    /// [`UnboundedSender::retained_message_count`], which reports the shared backlog retained by
    /// the slowest active receiver.
    ///
    /// The returned value is an instantaneous snapshot. It is suitable for detecting that this
    /// receiver is falling behind, but concurrent sends may change it immediately.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc;
    ///
    /// let (tx, mut rx) = mpmc::unbounded();
    /// assert_eq!(rx.unread_message_count(), 0);
    ///
    /// tx.send(10);
    /// tx.send(20);
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
    receiver: &'a mut UnboundedReceiver<T>,
    token: Option<WakerToken>,
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        // Ready paths clear the token, so only a cancelled pending receive takes this lock.
        if self.token.is_none() {
            return;
        }

        common::unregister(&self.receiver.shared.inner, &mut self.token);
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

        Poll::Ready(Ok(common::take_msg(msg, reclaimed)))
    }
}
