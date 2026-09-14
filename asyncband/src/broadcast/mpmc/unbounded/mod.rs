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

//! An unbounded fan-out channel with multiple senders and receivers.
//!
//! A send publishes one value to every receiver that exists at that moment. Receivers advance
//! independently, and a receiver created later starts with the next value rather than replaying
//! earlier values.
//!
//! # Backlog and memory
//!
//! Published values remain in the shared backlog until every receiver that was eligible for them
//! has advanced past them or been dropped. Because sending has no capacity limit, one stalled
//! receiver can make that backlog exhaust available memory.
//! [`UnboundedSender::retained_message_count`] reports its current length. Use [`bounded`] when
//! producers should wait for the slowest receiver instead of growing the backlog.
//!
//! # Receivers
//!
//! [`UnboundedSender::subscribe`] and [`UnboundedReceiver::resubscribe`] add a receiver at the
//! current publication boundary. They do not copy another receiver's unread backlog.
//!
//! # Example
//!
//! ```
//! use asyncband::broadcast::mpmc::TryRecvError;
//! use asyncband::broadcast::mpmc::unbounded;
//!
//! let (publisher, mut early) = unbounded();
//! publisher.send("before subscription");
//!
//! let mut late = publisher.subscribe();
//! publisher.send("after subscription");
//!
//! assert_eq!(early.try_recv(), Ok("before subscription"));
//! assert_eq!(early.try_recv(), Ok("after subscription"));
//! assert_eq!(late.try_recv(), Ok("after subscription"));
//! assert_eq!(late.try_recv(), Err(TryRecvError::Empty));
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
use crate::internal::wake_all;
use crate::internal::wakerset::WakerToken;

#[cfg(test)]
mod tests;

/// Creates an unbounded broadcast channel and its first receiver.
///
/// The returned receiver is subscribed before any value can be published. Additional receivers can
/// be added with [`UnboundedSender::subscribe`].
///
/// # Examples
///
/// ```
/// use asyncband::broadcast::mpmc::unbounded;
///
/// let (publisher, mut receiver) = unbounded();
/// publisher.send("ready");
/// assert_eq!(receiver.try_recv(), Ok("ready"));
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

/// A publishing handle for an unbounded broadcast channel.
///
/// Cloning this handle adds another publisher. Once the final sender is dropped, each receiver can
/// drain the values already published for it and then observes disconnection.
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
    /// Publishes `msg` to every receiver currently subscribed.
    ///
    /// Sending has no backpressure. The channel retains the value until every eligible receiver
    /// consumes it or is dropped.
    ///
    /// When no receivers exist, `msg` is discarded without entering the backlog.
    ///
    /// # Panics
    ///
    /// Panics if the channel has already published `u64::MAX` values.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc::unbounded;
    ///
    /// let (publisher, mut first) = unbounded();
    /// let mut second = publisher.subscribe();
    /// publisher.send("update");
    ///
    /// assert_eq!(first.try_recv(), Ok("update"));
    /// assert_eq!(second.try_recv(), Ok("update"));
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

    /// Returns the number of values in the shared backlog.
    ///
    /// This is not an unread count for any particular receiver. A value remains included until the
    /// last receiver eligible for it advances or is dropped.
    ///
    /// The result is an instantaneous observation and may become stale as other tasks send or
    /// receive.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc::unbounded;
    ///
    /// let (publisher, mut fast) = unbounded();
    /// let mut slow = publisher.subscribe();
    /// publisher.send("update");
    /// assert_eq!(publisher.retained_message_count(), 1);
    ///
    /// assert_eq!(fast.try_recv(), Ok("update"));
    /// assert_eq!(publisher.retained_message_count(), 1);
    /// assert_eq!(slow.try_recv(), Ok("update"));
    /// assert_eq!(publisher.retained_message_count(), 0);
    /// ```
    pub fn retained_message_count(&self) -> usize {
        self.shared.inner.lock().log.retained()
    }

    /// Subscribes a new receiver for values published from this point forward.
    ///
    /// Values already in the backlog are not visible to the new receiver.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc::TryRecvError;
    /// use asyncband::broadcast::mpmc::unbounded;
    ///
    /// let (publisher, _) = unbounded();
    /// publisher.send("earlier");
    ///
    /// let mut receiver = publisher.subscribe();
    /// assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    /// publisher.send("later");
    /// assert_eq!(receiver.try_recv(), Ok("later"));
    /// ```
    #[must_use = "the receiver is dropped immediately if it is not retained"]
    pub fn subscribe(&self) -> UnboundedReceiver<T> {
        let key = self.shared.inner.lock().log.subscribe();
        UnboundedReceiver {
            shared: self.shared.clone(),
            key,
        }
    }
}

/// An independent subscription to an unbounded broadcast channel.
///
/// This receiver observes every value published after its subscription point and retains its own
/// position in the shared backlog.
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
    /// Waits for this receiver's next value.
    ///
    /// Values already published for this receiver are returned before disconnection.
    /// [`RecvError::Disconnected`] is returned only when no sender remains and this receiver's
    /// backlog is empty.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending `recv` leaves this receiver's cursor unchanged. Its next call can still
    /// return the same next value, so `recv` can be raced with other futures in a selection
    /// construct.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc::RecvError;
    /// use asyncband::broadcast::mpmc::unbounded;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (publisher, mut receiver) = unbounded();
    /// publisher.send("final update");
    /// drop(publisher);
    ///
    /// assert_eq!(receiver.recv().await, Ok("final update"));
    /// assert_eq!(receiver.recv().await, Err(RecvError::Disconnected));
    /// # }
    /// ```
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        Recv {
            receiver: self,
            token: None,
        }
        .await
    }

    /// Attempts to take this receiver's next value without waiting.
    ///
    /// [`TryRecvError::Empty`] means this receiver is currently caught up while a sender remains.
    /// [`TryRecvError::Disconnected`] means no sender remains and this receiver has drained its
    /// backlog.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc::TryRecvError;
    /// use asyncband::broadcast::mpmc::unbounded;
    ///
    /// let (publisher, mut receiver) = unbounded();
    /// assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    ///
    /// publisher.send("update");
    /// assert_eq!(receiver.try_recv(), Ok("update"));
    /// drop(publisher);
    /// assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
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
    /// The new receiver skips this receiver's unread backlog. The original receiver remains at its
    /// current position and continues retaining those values until it consumes them or is dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc::TryRecvError;
    /// use asyncband::broadcast::mpmc::unbounded;
    ///
    /// let (publisher, mut original) = unbounded();
    /// publisher.send("pending for original");
    ///
    /// let mut fresh = original.resubscribe();
    /// assert_eq!(fresh.try_recv(), Err(TryRecvError::Empty));
    /// publisher.send("visible to both");
    ///
    /// assert_eq!(original.try_recv(), Ok("pending for original"));
    /// assert_eq!(original.try_recv(), Ok("visible to both"));
    /// assert_eq!(fresh.try_recv(), Ok("visible to both"));
    /// ```
    #[must_use = "the receiver is dropped immediately if it is not retained"]
    pub fn resubscribe(&self) -> Self {
        let key = self.shared.inner.lock().log.subscribe();
        Self {
            shared: self.shared.clone(),
            key,
        }
    }

    /// Returns this receiver's unread value count.
    ///
    /// Unlike [`UnboundedSender::retained_message_count`], this excludes values retained only for
    /// other receivers.
    ///
    /// The result is an instantaneous observation and may become stale as other tasks publish
    /// values.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc::unbounded;
    ///
    /// let (publisher, mut receiver) = unbounded();
    /// publisher.send("first");
    /// publisher.send("second");
    /// assert_eq!(receiver.unread_message_count(), 2);
    ///
    /// assert_eq!(receiver.try_recv(), Ok("first"));
    /// assert_eq!(receiver.unread_message_count(), 1);
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

        Poll::Ready(Ok(common::take_msg(msg, reclaimed)))
    }
}
