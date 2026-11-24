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
//! Sending publishes one value to every receiver that exists at that moment. Receivers advance
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
//! assert_eq!(*early.try_recv().unwrap(), "before subscription");
//! assert_eq!(*early.try_recv().unwrap(), "after subscription");
//! assert_eq!(*late.try_recv().unwrap(), "after subscription");
//! assert_eq!(late.try_recv(), Err(TryRecvError::Empty));
//! ```
//!
//! [`bounded`]: crate::broadcast::mpmc::bounded

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use crate::broadcast::mpmc::common;
use crate::broadcast::mpmc::error::RecvError;
use crate::broadcast::mpmc::error::TryRecvError;
use crate::broadcast::mpmc::unbounded::queue::Shared;
use crate::broadcast::mpmc::unbounded::queue::Subscription;
use crate::broadcast::mpmc::unbounded::segment::Shard;
use crate::broadcast::mpmc::unbounded::waiters::WaitReg;

mod queue;
mod segment;
mod waiters;

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
/// assert_eq!(*receiver.try_recv().unwrap(), "ready");
/// ```
pub fn unbounded<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let (shared, sub) = Shared::with_first_subscription();
    let shard = NonNull::from(&shared.shard0);
    let sender = UnboundedSender {
        shared: shared.clone(),
        shard,
    };
    let receiver = UnboundedReceiver::from_subscription(shared, sub);
    (sender, receiver)
}

/// A publishing handle for an unbounded broadcast channel.
///
/// Cloning this handle adds another publisher. Once the final sender is dropped, each receiver can
/// drain the values already published for it and then observes disconnection.
pub struct UnboundedSender<T> {
    shared: Arc<Shared<T>>,
    shard: NonNull<Shard>,
}

// SAFETY: `shard` points into `shared` (`Arc<Shared<T>>`), and `Shared<T>` is `Send + Sync` iff
// `T: Send + Sync`.
unsafe impl<T: Send + Sync> Send for UnboundedSender<T> {}
unsafe impl<T: Send + Sync> Sync for UnboundedSender<T> {}

impl<T> Clone for UnboundedSender<T> {
    #[inline]
    fn clone(&self) -> Self {
        let shared = self.shared.clone();
        let shard = shared.clone_sender();
        Self { shared, shard }
    }
}

impl<T> fmt::Debug for UnboundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedSender").finish_non_exhaustive()
    }
}

impl<T> Drop for UnboundedSender<T> {
    #[inline]
    fn drop(&mut self) {
        self.shared.drop_sender();
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
    /// assert_eq!(*first.try_recv().unwrap(), "update");
    /// assert_eq!(*second.try_recv().unwrap(), "update");
    /// ```
    #[inline(always)]
    pub fn send(&self, msg: T) {
        self.shared.send(self.shard, msg);
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
    /// assert_eq!(*fast.try_recv().unwrap(), "update");
    /// assert_eq!(publisher.retained_message_count(), 1);
    /// assert_eq!(*slow.try_recv().unwrap(), "update");
    /// assert_eq!(publisher.retained_message_count(), 0);
    /// ```
    pub fn retained_message_count(&self) -> usize {
        self.shared.retained_message_count()
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
    /// assert_eq!(*receiver.try_recv().unwrap(), "later");
    /// ```
    #[must_use = "the receiver is dropped immediately if it is not retained"]
    pub fn subscribe(&self) -> UnboundedReceiver<T> {
        let sub = self.shared.subscribe();
        UnboundedReceiver::from_subscription(self.shared.clone(), sub)
    }
}

/// An independent subscription to an unbounded broadcast channel.
///
/// This receiver observes every value published after its subscription point and retains its own
/// position in the shared backlog.
pub struct UnboundedReceiver<T> {
    shared: Arc<Shared<T>>,
    sub: Subscription<T>,
}

// SAFETY: `UnboundedReceiver<T>` synchronizes shared access to `Segment<T>` through `Shared<T>`
// and requires `T: Send + Sync` because payloads are shared via `Arc<T>`.
unsafe impl<T: Send + Sync> Send for UnboundedReceiver<T> {}
unsafe impl<T: Send + Sync> Sync for UnboundedReceiver<T> {}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for UnboundedReceiver<T> {
    fn drop(&mut self) {
        self.shared.drop_receiver(self.sub);
    }
}

impl<T> UnboundedReceiver<T> {
    #[inline]
    fn from_subscription(shared: Arc<Shared<T>>, sub: Subscription<T>) -> Self {
        Self { shared, sub }
    }

    /// Waits for this receiver's next value and returns the channel's shared `Arc<T>`.
    ///
    /// Receiving never clones `T`. The `Arc` outlives the value's stay in the backlog, so a
    /// caller that keeps it keeps the payload alive. Use
    /// [`recv_cloned`](UnboundedReceiver::recv_cloned) to take an owned value instead.
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
    /// Polling the returned future after it has returned [`Poll::Ready`] panics.
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
    /// assert_eq!(*receiver.recv().await.unwrap(), "final update");
    /// assert_eq!(receiver.recv().await, Err(RecvError::Disconnected));
    /// # }
    /// ```
    #[inline(always)]
    pub fn recv(&mut self) -> impl Future<Output = Result<Arc<T>, RecvError>> {
        RecvShared {
            inner: Recv {
                receiver: Some(self),
                wait_slot: None,
            },
        }
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
    /// receives a later message rather than the one whose clone panicked. Polling the returned
    /// future after it has returned [`Poll::Ready`] also panics.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc::unbounded;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (publisher, mut receiver) = unbounded();
    /// publisher.send(String::from("owned"));
    /// assert_eq!(receiver.recv_cloned().await, Ok(String::from("owned")));
    /// # }
    /// ```
    #[inline(always)]
    pub fn recv_cloned(&mut self) -> impl Future<Output = Result<T, RecvError>>
    where
        T: Clone,
    {
        RecvCloned {
            inner: Recv {
                receiver: Some(self),
                wait_slot: None,
            },
        }
    }

    /// Attempts to take this receiver's next value without waiting for new messages, as the
    /// channel's shared `Arc<T>`.
    ///
    /// If a concurrent sender has already claimed the next message version and is still writing
    /// its slot, this call waits for that in-flight publication to finish before returning it.
    /// See [`recv`](UnboundedReceiver::recv) for what keeping the `Arc` means, and
    /// [`try_recv_cloned`](UnboundedReceiver::try_recv_cloned) to take an owned value instead.
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
    /// use asyncband::broadcast::mpmc::TryRecvError;
    /// use asyncband::broadcast::mpmc::unbounded;
    ///
    /// let (publisher, mut receiver) = unbounded();
    /// assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    ///
    /// publisher.send("update");
    /// assert_eq!(*receiver.try_recv().unwrap(), "update");
    /// drop(publisher);
    /// assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    /// ```
    #[inline(always)]
    pub fn try_recv(&mut self) -> Result<Arc<T>, TryRecvError> {
        let received = self.shared.try_recv(&mut self.sub)?;
        Ok(received.msg)
    }

    /// Attempts to take this receiver's next value without waiting for new messages and returns it
    /// as an owned `T`.
    ///
    /// If a concurrent sender has already claimed the next message version and is still writing
    /// its slot, this call waits for that in-flight publication to finish. See
    /// [`recv_cloned`](UnboundedReceiver::recv_cloned) for when the payload is moved out rather
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
    /// use asyncband::broadcast::mpmc::unbounded;
    ///
    /// let (publisher, mut receiver) = unbounded();
    /// publisher.send(String::from("owned"));
    /// assert_eq!(receiver.try_recv_cloned(), Ok(String::from("owned")));
    /// ```
    #[inline(always)]
    pub fn try_recv_cloned(&mut self) -> Result<T, TryRecvError>
    where
        T: Clone,
    {
        let received = self.shared.try_recv(&mut self.sub)?;
        Ok(common::take_msg(received))
    }

    /// Creates a new subscription positioned after all values already published to the channel.
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
    /// assert_eq!(*original.try_recv().unwrap(), "pending for original");
    /// assert_eq!(*original.try_recv().unwrap(), "visible to both");
    /// assert_eq!(*fresh.try_recv().unwrap(), "visible to both");
    /// ```
    #[must_use = "the receiver is dropped immediately if it is not retained"]
    pub fn resubscribe(&self) -> Self {
        let sub = self.shared.subscribe();
        Self::from_subscription(self.shared.clone(), sub)
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
    /// assert_eq!(*receiver.try_recv().unwrap(), "first");
    /// assert_eq!(receiver.unread_message_count(), 1);
    /// ```
    #[inline]
    pub fn unread_message_count(&self) -> usize {
        self.shared.unread_from(self.sub.cursor)
    }

    /// Returns `true` if all senders have been dropped.
    ///
    /// Unread values buffered for this receiver remain available after disconnection, so
    /// [`recv`](Self::recv) and [`try_recv`](Self::try_recv) report disconnection only once this
    /// receiver has drained its remaining backlog. A `false` result can become outdated immediately
    /// if the last sender is dropped concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc::unbounded;
    ///
    /// let (publisher, mut receiver) = unbounded();
    /// publisher.send("final");
    /// assert!(!receiver.is_disconnected());
    ///
    /// drop(publisher);
    /// assert!(receiver.is_disconnected());
    /// assert_eq!(*receiver.try_recv().unwrap(), "final");
    /// ```
    #[inline]
    pub fn is_disconnected(&self) -> bool {
        self.shared.is_disconnected()
    }

    /// Returns `true` if no unread value has been claimed for delivery to this receiver.
    ///
    /// Values already consumed by this receiver that remain retained for slower receivers do not
    /// count. The result is an instantaneous observation and may become stale as other tasks
    /// publish values. A `false` result means the next receive on this receiver will observe a
    /// value, though [`recv`](Self::recv) may briefly suspend and [`try_recv`](Self::try_recv) may
    /// wait if the sender that claimed the next version is still writing its slot.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::mpmc::unbounded;
    ///
    /// let (publisher, mut fast) = unbounded();
    /// let _slow = publisher.subscribe();
    /// assert!(fast.is_empty());
    ///
    /// publisher.send("first");
    /// assert!(!fast.is_empty());
    ///
    /// assert_eq!(*fast.try_recv().unwrap(), "first");
    /// assert!(fast.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.unread_message_count() == 0
    }
}

struct Recv<'a, T> {
    receiver: Option<&'a mut UnboundedReceiver<T>>,
    wait_slot: Option<WaitReg>,
}

impl<T> Recv<'_, T> {
    #[inline(always)]
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<common::Received<T>, RecvError>> {
        let Some(receiver) = self.receiver.as_deref_mut() else {
            panic!("unbounded broadcast recv polled after completion");
        };
        let poll = receiver
            .shared
            .poll_recv(&mut receiver.sub, &mut self.wait_slot, cx);
        if poll.is_ready() {
            self.receiver = None;
        }
        poll
    }

    #[cold]
    #[inline(never)]
    fn drop_registered(&mut self, reg: WaitReg) {
        if let Some(receiver) = self.receiver.as_deref() {
            if reg.full_gen(receiver.sub.wait_gen_hi)
                == receiver.shared.wake_gen.load(Ordering::Acquire)
            {
                receiver
                    .shared
                    .unregister_recv(receiver.sub.wait_gen_hi, &mut self.wait_slot);
            }
        }
    }
}

impl<T> Drop for Recv<'_, T> {
    #[inline(always)]
    fn drop(&mut self) {
        let Some(reg) = self.wait_slot else {
            return;
        };
        self.drop_registered(reg);
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct RecvShared<'a, T> {
    inner: Recv<'a, T>,
}

impl<T> Future for RecvShared<'_, T> {
    type Output = Result<Arc<T>, RecvError>;

    #[inline(always)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut()
            .inner
            .poll_next(cx)
            .map(|res| res.map(|received| received.msg))
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct RecvCloned<'a, T> {
    inner: Recv<'a, T>,
}

impl<T: Clone> Future for RecvCloned<'_, T> {
    type Output = Result<T, RecvError>;

    #[inline(always)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut()
            .inner
            .poll_next(cx)
            .map(|res| res.map(common::take_msg))
    }
}
