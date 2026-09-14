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

//! An unbounded fan-out channel with one sender and many receivers.
//!
//! A send publishes one value to every receiver that exists at that moment. Receivers advance
//! independently, and a receiver created later starts with the next value rather than replaying
//! earlier values. The sender is not [`Clone`]; [`UnboundedSender::send`] takes `&mut self` so a
//! second producer cannot exist at compile time.
//!
//! # Backlog and memory
//!
//! Published values remain in the shared backlog until every receiver that was eligible for them
//! has advanced past them or been dropped. Because sending has no capacity limit, one stalled
//! receiver can make that backlog exhaust available memory.
//! [`UnboundedSender::retained_message_count`] reports its current length. Use [`bounded`] when
//! the producer should wait for the slowest receiver instead of growing the backlog.
//!
//! # Receivers
//!
//! [`UnboundedSender::subscribe`] and [`UnboundedReceiver::resubscribe`] add a receiver at the
//! current publication boundary. They do not copy another receiver's unread backlog.
//!
//! # Example
//!
//! ```
//! use asyncband::broadcast::spmc::TryRecvError;
//! use asyncband::broadcast::spmc::unbounded;
//!
//! let (mut publisher, mut early) = unbounded();
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
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use super::common;
use super::common::Shared;
use super::common::UnboundedBuffer;
use super::error::RecvError;
use super::error::TryRecvError;
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
/// use asyncband::broadcast::spmc::unbounded;
///
/// let (mut publisher, mut receiver) = unbounded();
/// publisher.send("ready");
/// assert_eq!(receiver.try_recv(), Ok("ready"));
/// ```
pub fn unbounded<T: Clone>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let shared = Arc::new(Shared::new(UnboundedBuffer::new()));
    let sender = UnboundedSender {
        shared: shared.clone(),
    };
    let receiver = UnboundedReceiver { shared, cursor: 0 };
    (sender, receiver)
}

/// A publishing handle for an unbounded broadcast channel.
///
/// This handle is not [`Clone`]. Once it is dropped, each receiver can drain the values already
/// published for it and then observes disconnection.
pub struct UnboundedSender<T> {
    shared: Arc<Shared<UnboundedBuffer<T>>>,
}

impl<T> fmt::Debug for UnboundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedSender").finish_non_exhaustive()
    }
}

impl<T> Drop for UnboundedSender<T> {
    fn drop(&mut self) {
        self.shared.senders.store(0, Ordering::Release);
        common::disconnect(&self.shared);
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
    /// use asyncband::broadcast::spmc::unbounded;
    ///
    /// let (mut publisher, mut first) = unbounded();
    /// let mut second = publisher.subscribe();
    /// publisher.send("update");
    ///
    /// assert_eq!(first.try_recv(), Ok("update"));
    /// assert_eq!(second.try_recv(), Ok("update"));
    /// ```
    pub fn send(&mut self, msg: T) {
        self.shared.send_in_progress.store(true, Ordering::Release);
        let n = self.shared.receiver_count.load(Ordering::Acquire);

        let unretained = if n == 0 {
            let state = self.shared.state.lock();
            let tail = self.shared.tail.load(Ordering::Relaxed);
            let next = Shared::<UnboundedBuffer<T>>::next_tail(tail);
            if state.receiver_count == 0 {
                common::commit_discard(&self.shared.head, &self.shared.tail, next);
                drop(state);
                self.shared.send_in_progress.store(false, Ordering::Release);
                Some(msg)
            } else {
                let slot = self.shared.buffer.slot_for_publish(tail);
                unsafe {
                    slot.write(msg, state.receiver_count);
                }
                common::commit_publish(&self.shared.tail, next);
                drop(state);
                self.shared.send_in_progress.store(false, Ordering::Release);
                None
            }
        } else {
            let tail = self.shared.tail.load(Ordering::Relaxed);
            let next = Shared::<UnboundedBuffer<T>>::next_tail(tail);
            let slot = self.shared.buffer.slot_for_publish(tail);
            unsafe {
                slot.write(msg, n);
            }
            common::commit_publish(&self.shared.tail, next);
            self.shared.send_in_progress.store(false, Ordering::Release);
            None
        };

        if self.shared.has_waiters.load(Ordering::Acquire) {
            let wakers = {
                let mut state = self.shared.state.lock();
                let wakers = state.waiters.drain();
                self.shared.has_waiters.store(false, Ordering::Release);
                wakers
            };
            wake_all(wakers);
        }
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
    /// use asyncband::broadcast::spmc::unbounded;
    ///
    /// let (mut publisher, mut fast) = unbounded();
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
        self.shared.retained()
    }

    /// Subscribes a new receiver for values published from this point forward.
    ///
    /// Values already in the backlog are not visible to the new receiver.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::spmc::TryRecvError;
    /// use asyncband::broadcast::spmc::unbounded;
    ///
    /// let (mut publisher, _) = unbounded();
    /// publisher.send("earlier");
    ///
    /// let mut receiver = publisher.subscribe();
    /// assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    /// publisher.send("later");
    /// assert_eq!(receiver.try_recv(), Ok("later"));
    /// ```
    #[must_use = "the receiver is dropped immediately if it is not retained"]
    pub fn subscribe(&self) -> UnboundedReceiver<T> {
        let cursor = self.shared.subscribe();
        UnboundedReceiver {
            shared: self.shared.clone(),
            cursor,
        }
    }
}

/// An independent subscription to an unbounded broadcast channel.
///
/// This receiver observes every value published after its subscription point and retains its own
/// position in the shared backlog.
pub struct UnboundedReceiver<T> {
    shared: Arc<Shared<UnboundedBuffer<T>>>,
    cursor: u64,
}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for UnboundedReceiver<T> {
    fn drop(&mut self) {
        let (reclaimed, _producer) = common::drop_subscription(&self.shared, self.cursor);
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
    /// use asyncband::broadcast::spmc::RecvError;
    /// use asyncband::broadcast::spmc::unbounded;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (mut publisher, mut receiver) = unbounded();
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
    /// use asyncband::broadcast::spmc::TryRecvError;
    /// use asyncband::broadcast::spmc::unbounded;
    ///
    /// let (mut publisher, mut receiver) = unbounded();
    /// assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    ///
    /// publisher.send("update");
    /// assert_eq!(receiver.try_recv(), Ok("update"));
    /// drop(publisher);
    /// assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    /// ```
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        Ok(common::try_receive(&self.shared, &mut self.cursor)?.value)
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
    /// use asyncband::broadcast::spmc::TryRecvError;
    /// use asyncband::broadcast::spmc::unbounded;
    ///
    /// let (mut publisher, mut original) = unbounded();
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
        let cursor = self.shared.subscribe();
        Self {
            shared: self.shared.clone(),
            cursor,
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
    /// use asyncband::broadcast::spmc::unbounded;
    ///
    /// let (mut publisher, mut receiver) = unbounded();
    /// publisher.send("first");
    /// publisher.send("second");
    /// assert_eq!(receiver.unread_message_count(), 2);
    ///
    /// assert_eq!(receiver.try_recv(), Ok("first"));
    /// assert_eq!(receiver.unread_message_count(), 1);
    /// ```
    pub fn unread_message_count(&self) -> usize {
        self.shared.unread(self.cursor)
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

        common::unregister(&self.receiver.shared, self.receiver.cursor, &mut self.token);
    }
}

impl<T: Clone> Future for Recv<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self { receiver, token } = self.get_mut();

        let consumed = match common::poll_receive(&receiver.shared, &mut receiver.cursor, token, cx)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Ready(Ok(consumed)) => consumed,
        };

        Poll::Ready(Ok(consumed.value))
    }
}
