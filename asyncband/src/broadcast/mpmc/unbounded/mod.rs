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
//! [`UnboundedSender::retained_message_count`] reports its current length.
//!
//! Buffer capacity is reused across steady bursts. Allocation retained for an unusually large burst
//! is released after a later, substantially smaller cycle drains completely.
//!
//! # Receivers
//!
//! [`UnboundedSender::subscribe`] and [`UnboundedReceiver::resubscribe`] add a receiver at the
//! current publication boundary. They do not copy another receiver's unread backlog.
//!
//! Reclaiming the oldest value requires finding the earliest remaining receiver cursor. That work
//! scales with the receiver slots allocated by the channel, including slots retained for reuse
//! after receivers are dropped.
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

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use crate::internal::arena::Arena;
use crate::internal::arena::SlotId;
use crate::internal::mutex::Mutex;
use crate::internal::wake_all;
use crate::internal::wakerset::WakerSet;
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
    let mut receivers = Arena::new();
    let key = receivers.insert(0);
    let shared = Arc::new(Shared {
        inner: Mutex::new(Inner {
            buffer: VecDeque::new(),
            head: 0,
            head_receivers: 1,
            tail: 0,
            receivers,
            peak_len: 0,
            waiters: WakerSet::new(),
        }),
        senders: AtomicUsize::new(1),
    });
    let sender = UnboundedSender {
        shared: shared.clone(),
    };
    let receiver = UnboundedReceiver { shared, key };
    (sender, receiver)
}

/// A receive operation reached the end of its subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    /// No sender remains and this receiver has consumed its entire backlog.
    Disconnected,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("receiving on a disconnected channel")
    }
}

impl std::error::Error for RecvError {}

/// A non-blocking receive did not yield a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TryRecvError {
    /// This receiver is caught up, but a sender can still publish more values.
    Empty,
    /// No sender remains and this receiver has consumed its entire backlog.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            TryRecvError::Empty => "receiving on an empty channel",
            TryRecvError::Disconnected => "receiving on a disconnected channel",
        };
        f.write_str(message)
    }
}

impl std::error::Error for TryRecvError {}

/// Retained capacity below which the shared buffer is never shrunk back.
const MIN_RETAINED_CAPACITY: usize = 64;

struct Inner<T> {
    /// Messages whose versions are in the range `[head, tail)`.
    ///
    /// Each message is held behind an `Arc` so the receive path can move the payload out of the
    /// critical section. Cloning the `Arc` under the lock keeps `T::clone` — and, for reclaimed
    /// messages, `T::drop` — outside it, which matters because both are arbitrary user code that
    /// may call back into this channel.
    buffer: VecDeque<Arc<T>>,
    /// The version of the first message in `buffer`.
    head: u64,
    /// The number of active receivers whose cursor equals `head`.
    head_receivers: usize,
    /// The next message version to assign.
    tail: u64,
    /// Cursor for each active receiver.
    receivers: Arena<u64>,
    /// The largest backlog retained since the buffer was last empty.
    peak_len: usize,
    /// Receivers parked in [`UnboundedReceiver::recv`].
    waiters: WakerSet,
}

/// Messages removed from the shared buffer and waiting to be dropped after it is unlocked.
///
/// Keeping the first message out of the `Vec` avoids a heap allocation on the common path where
/// one receive reclaims exactly one message.
struct Reclaimed<T> {
    first: Option<Arc<T>>,
    rest: Vec<Arc<T>>,
}

impl<T> Reclaimed<T> {
    fn empty() -> Self {
        Self {
            first: None,
            rest: vec![],
        }
    }

    fn first(&self) -> Option<&Arc<T>> {
        self.first.as_ref()
    }

    fn is_empty(&self) -> bool {
        self.first.is_none()
    }

    fn drop_messages(self) {
        let Self { first, rest } = self;
        drop((first, rest));
    }
}

impl<T> Inner<T> {
    fn insert_receiver(&mut self, head: u64) -> SlotId {
        if head == self.head {
            self.head_receivers += 1;
        }

        self.receivers.insert(head)
    }

    fn remove_receiver(&mut self, key: SlotId) -> Reclaimed<T> {
        let head = self.receivers.remove(key);

        if head == self.head {
            self.release_head_receiver()
        } else {
            Reclaimed::empty()
        }
    }

    fn release_head_receiver(&mut self) -> Reclaimed<T> {
        self.head_receivers -= 1;

        if self.head_receivers == 0 {
            self.reclaim_consumed()
        } else {
            Reclaimed::empty()
        }
    }

    fn receive(&mut self, key: SlotId) -> Option<(Arc<T>, Reclaimed<T>)> {
        let head = {
            let cursor = self
                .receivers
                .get_mut(key)
                .expect("active broadcast receiver must be registered");
            if *cursor >= self.tail {
                return None;
            }
            let head = *cursor;
            *cursor += 1;
            head
        };

        debug_assert!(head >= self.head);
        let offset = (head - self.head) as usize;
        let msg = self.buffer[offset].clone();
        let reclaimed = if head == self.head {
            self.release_head_receiver()
        } else {
            Reclaimed::empty()
        };
        // A reclaim triggered by this receive always begins with this receiver's own message: the
        // reclaim path runs only for a cursor sitting at `head`, so the first slot drained is
        // `msg`. `take_msg` relies on this to recognize that it owns the payload.
        debug_assert!(
            reclaimed
                .first()
                .is_none_or(|first| Arc::ptr_eq(first, &msg))
        );
        Some((msg, reclaimed))
    }

    fn reclaim_consumed(&mut self) -> Reclaimed<T> {
        let mut next_head = self.tail;
        let mut head_receivers = 0;

        for head in self.receivers.values() {
            if *head < next_head {
                next_head = *head;
                head_receivers = 1;
            } else if *head == next_head {
                head_receivers += 1;
            }
        }

        debug_assert!(next_head >= self.head);
        let consumed = usize::try_from(next_head - self.head)
            .expect("retained broadcast message count exceeds usize");
        // Move reclaimed messages out so their Drop impls run after `inner` is unlocked. Keep the
        // first one separate so the usual one-message reclaim does not allocate another buffer.
        let first = if consumed == 0 {
            None
        } else {
            self.buffer.pop_front()
        };
        let rest = self.buffer.drain(..consumed.saturating_sub(1)).collect();
        let reclaimed = Reclaimed { first, rest };

        self.head = next_head;
        self.head_receivers = head_receivers;
        self.shrink_buffer();
        reclaimed
    }

    /// Returns the allocation grown for a stalled receiver once that backlog is behind us.
    ///
    /// Without this, a single burst pins its peak allocation for the lifetime of the channel.
    /// The decision is deliberately made only when the buffer drains completely, and against the
    /// peak of the cycle that just ended rather than the current length: a channel that repeatedly
    /// fills and drains keeps a peak as large as its bursts, so it holds its allocation instead of
    /// reallocating on every cycle. Only once a full cycle stays small does the buffer give the
    /// memory back.
    fn shrink_buffer(&mut self) {
        if !self.buffer.is_empty() {
            return;
        }

        let peak = mem::take(&mut self.peak_len);
        let capacity = self.buffer.capacity();
        if capacity > MIN_RETAINED_CAPACITY && peak <= capacity / 4 {
            self.buffer.shrink_to(MIN_RETAINED_CAPACITY.max(peak * 2));
        }
    }
}

struct Shared<T> {
    /// Buffer, receiver cursors, and parked receivers, all under a single lock.
    ///
    /// The wait set lives here rather than beside it so that publishing a message and draining the
    /// waiters happen in one critical section. That is what makes the park path race-free: a
    /// receiver that finds no message and then registers still holds this lock, so a concurrent
    /// `send` cannot slip between the two steps and skip the wake-up.
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
            1 => {
                // Wake every parked receiver so it can observe the channel's disconnected state.
                let wakers = {
                    let mut inner = self.shared.inner.lock();
                    inner.waiters.take_all()
                };
                wake_all(wakers);
            }
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
    /// Panics when publishing would overflow the channel's internal version counter.
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
        let wakers = {
            let mut inner = self.shared.inner.lock();
            inner.tail = inner
                .tail
                .checked_add(1)
                .expect("broadcast channel version counter overflowed");

            if inner.receivers.is_empty() {
                // No receivers means no one will read this message; advance `head` so the
                // invariant that `buffer` covers versions `[head, tail)` still holds without
                // buffering anything. The buffer is already drained when the last receiver was
                // dropped, so there is nothing to clear here.
                debug_assert!(inner.buffer.is_empty());
                debug_assert_eq!(inner.head_receivers, 0);
                inner.head = inner.tail;
            } else {
                inner.buffer.push_back(msg);
                inner.peak_len = inner.peak_len.max(inner.buffer.len());
            }

            inner.waiters.drain()
        };

        // Notify all waiting receivers. An unsent message is dropped here too, once the lock is
        // released.
        wake_all(wakers);
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
        self.shared.inner.lock().buffer.len()
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
        let mut inner = self.shared.inner.lock();
        let head = inner.tail;
        let key = inner.insert_receiver(head);
        let shared = self.shared.clone();
        UnboundedReceiver { shared, key }
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
            inner.remove_receiver(self.key)
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
        let (msg, reclaimed) = self.try_recv_shared()?;
        Ok(take_msg(msg, reclaimed))
    }
}

/// Drops the reclaimed backlog, then yields the received message, both with the channel unlocked.
///
/// A non-empty backlog means this receive drained `msg` from the buffer, so once the backlog is
/// dropped this receive holds the only reference and the payload can be moved out instead of
/// cloned. A channel with a single receiver therefore never clones a payload.
///
/// Ownership is decided from that bookkeeping rather than by probing the reference count. An
/// [`Arc::try_unwrap`] on every receive would fail under fan-out, and its failed compare-exchange
/// writes to a cache line that every receiver draining the message shares.
fn take_msg<T: Clone>(msg: Arc<T>, reclaimed: Reclaimed<T>) -> T {
    let sole_owner = !reclaimed.is_empty();
    reclaimed.drop_messages();

    if !sole_owner {
        return (*msg).clone();
    }

    // Another receiver can still hold an in-flight reference to the same message, so the clone
    // remains the fallback.
    Arc::try_unwrap(msg).unwrap_or_else(|msg| (*msg).clone())
}

impl<T> UnboundedReceiver<T> {
    fn try_recv_shared(&mut self) -> Result<(Arc<T>, Reclaimed<T>), TryRecvError> {
        // Check this receiver's cursor while holding `inner` before observing `senders`. Senders
        // append messages under the same lock before they can be dropped, so an empty result here
        // means this receiver has no unread buffered message.
        let mut inner = self.shared.inner.lock();
        if let Some(received) = inner.receive(self.key) {
            return Ok(received);
        }

        if self.shared.senders.load(Ordering::Acquire) == 0 {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Creates another receiver at the current publication boundary.
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
        let mut inner = self.shared.inner.lock();
        let head = inner.tail;
        let key = inner.insert_receiver(head);
        let shared = self.shared.clone();
        Self { shared, key }
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
        let inner = self.shared.inner.lock();
        let head = *inner
            .receivers
            .get(self.key)
            .expect("active broadcast receiver must be registered");
        usize::try_from(inner.tail - head).expect("unread broadcast message count exceeds usize")
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

        let mut inner = self.receiver.shared.inner.lock();
        let cursor = *inner
            .receivers
            .get(self.receiver.key)
            .expect("active broadcast receiver must be registered");
        if cursor != inner.tail || self.receiver.shared.senders.load(Ordering::Acquire) == 0 {
            // A publisher or the final sender owns this registration or has already detached it
            // under the channel lock.
            self.token = None;
            return;
        }

        let waker = inner.waiters.unregister(&mut self.token);
        drop(inner);
        drop(waker);
    }
}

impl<T: Clone> Future for Recv<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self { receiver, token } = self.get_mut();

        let received = {
            let mut inner = receiver.shared.inner.lock();
            match inner.receive(receiver.key) {
                Some(received) => received,
                None => {
                    if receiver.shared.senders.load(Ordering::Acquire) == 0 {
                        *token = None;
                        return Poll::Ready(Err(RecvError::Disconnected));
                    }

                    let retired_waker = inner.waiters.register(token, cx.waker());
                    drop(inner);
                    drop(retired_waker);
                    return Poll::Pending;
                }
            }
        };

        let (msg, reclaimed) = received;
        *token = None;
        Poll::Ready(Ok(take_msg(msg, reclaimed)))
    }
}
