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
//! by the channel.
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
use crate::internal::waitset::WaitSet;
use crate::internal::waitset::WakerToken;
use crate::internal::wake_all;

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
            waiters: WaitSet::new(),
        }),
        senders: AtomicUsize::new(1),
    });
    let sender = UnboundedSender {
        shared: shared.clone(),
    };
    let receiver = UnboundedReceiver { shared, key };
    (sender, receiver)
}

/// Error returned by [`UnboundedReceiver::recv`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    /// All senders have been dropped, and this receiver has no remaining messages.
    Disconnected,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecvError::Disconnected => write!(f, "receiving on a disconnected channel"),
        }
    }
}

impl std::error::Error for RecvError {}

/// Error returned by [`UnboundedReceiver::try_recv`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TryRecvError {
    /// No message is currently available, but at least one sender remains.
    Empty,
    /// All senders have been dropped, and this receiver has no remaining messages.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "receiving on an empty channel"),
            TryRecvError::Disconnected => write!(f, "receiving on a disconnected channel"),
        }
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
    waiters: WaitSet,
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
            1 => {
                // Wake every parked receiver so it can observe the channel's disconnected state.
                let wakers = {
                    let mut inner = self.shared.inner.lock();
                    inner.waiters.drain()
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
        self.shared.inner.lock().buffer.len()
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
    #[must_use = "the receiver is dropped immediately if it is not retained"]
    pub fn subscribe(&self) -> UnboundedReceiver<T> {
        let mut inner = self.shared.inner.lock();
        let head = inner.tail;
        let key = inner.insert_receiver(head);
        let shared = self.shared.clone();
        UnboundedReceiver { shared, key }
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
            inner.remove_receiver(self.key)
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
    #[must_use = "the receiver is dropped immediately if it is not retained"]
    pub fn resubscribe(&self) -> Self {
        let mut inner = self.shared.inner.lock();
        let head = inner.tail;
        let key = inner.insert_receiver(head);
        let shared = self.shared.clone();
        Self { shared, key }
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

        let waker = {
            let mut inner = self.receiver.shared.inner.lock();
            inner.waiters.unregister(&mut self.token)
        };
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
