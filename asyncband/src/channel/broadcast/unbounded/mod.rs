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
//! consumed them or the receiver is dropped. Use [`Sender::buffer_len`] to monitor the number of
//! messages currently retained by the shared buffer.
//!
//! The buffer keeps the capacity a steady workload needs, so a channel that repeatedly fills and
//! drains does not reallocate. Capacity grown for a one-off burst is released once a later cycle
//! drains completely without needing it.
//!
//! # Receivers
//!
//! Each receiver has an independent cursor. Use [`Sender::subscribe`] to create a receiver that
//! starts at the current tail of the channel, [`Receiver::clone`] to create one that shares this
//! receiver's unread backlog, or [`Receiver::resubscribe`] to skip this receiver's backlog and
//! start a new receiver at the current tail.
//!
//! Messages are reclaimed once the slowest receiver moves past them, which scans one slot per
//! receiver. Only the receive that advances the slowest cursor pays for that scan, and the channel
//! keeps a slot for every receiver it hands out, so the cost follows the largest number of
//! receivers that were ever active at once rather than the number active now.
//!
//! # Examples
//!
//! Basic usage:
//!
//! ```
//! use asyncband::broadcast::unbounded;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let (tx, mut rx1) = unbounded::channel();
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
//! use asyncband::broadcast::unbounded;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let (tx, mut rx1) = unbounded::channel();
//! let mut rx2 = tx.subscribe();
//!
//! tx.send(1);
//! tx.send(2);
//!
//! // One receiver draining the channel does not discard what the other has not read yet.
//! assert_eq!(rx1.recv().await, Ok(1));
//! assert_eq!(rx1.recv().await, Ok(2));
//! assert_eq!(tx.buffer_len(), 2);
//!
//! assert_eq!(rx2.recv().await, Ok(1));
//! assert_eq!(rx2.recv().await, Ok(2));
//! assert_eq!(tx.buffer_len(), 0);
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
use crate::internal::waitset::WaitRegistration;
use crate::internal::waitset::WaitSet;

#[cfg(test)]
mod tests;

/// Creates a new broadcast channel with an unbounded buffer.
///
/// See [module-level documentation](self) for broadcast channel semantics.
///
/// # Examples
///
/// ```
/// use asyncband::broadcast::unbounded;
///
/// let (tx, mut rx) = unbounded::channel();
/// tx.send(10);
/// assert_eq!(rx.try_recv(), Ok(10));
/// ```
pub fn channel<T: Clone>() -> (Sender<T>, Receiver<T>) {
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
    let sender = Sender {
        shared: shared.clone(),
    };
    let receiver = Receiver { shared, key };
    (sender, receiver)
}

/// Error returned by [`Receiver::recv`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    /// The sender has become disconnected, and there will never be any more data received on it.
    Disconnected,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecvError::Disconnected => write!(f, "receiving on a closed channel"),
        }
    }
}

impl std::error::Error for RecvError {}

/// Error returned by [`Receiver::try_recv`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TryRecvError {
    /// This channel is currently empty, but the sender(s) have not yet disconnected, so data may
    /// yet become available.
    Empty,
    /// The sender has become disconnected, and there will never be any more data received on it.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "receiving on an empty channel"),
            TryRecvError::Disconnected => write!(f, "receiving on a closed channel"),
        }
    }
}

impl std::error::Error for TryRecvError {}

/// Retained capacity below which the shared buffer is never shrunk back.
const MIN_RETAINED_CAPACITY: usize = 64;

struct Inner<T> {
    /// Messages whose versions are in the range `[head, tail)`.
    ///
    /// Each message is held behind an `Arc` so a receive can hand the payload out of the critical
    /// section. Cloning the `Arc` under the lock keeps `T::clone` — and, for reclaimed messages,
    /// `T::drop` — outside it, which matters because both are arbitrary user code that may call
    /// back into this channel.
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
    /// Receivers parked in [`Receiver::recv`].
    waiters: WaitSet,
}

impl<T> Inner<T> {
    fn insert_receiver(&mut self, head: u64) -> SlotId {
        if head == self.head {
            self.head_receivers += 1;
        }

        self.receivers.insert(head)
    }

    fn remove_receiver(&mut self, key: SlotId) -> Vec<Arc<T>> {
        let head = self.receivers.remove(key);

        if head == self.head {
            self.release_head_receiver()
        } else {
            Vec::new()
        }
    }

    fn advance_receiver(&mut self, key: SlotId, next_head: u64) -> Vec<Arc<T>> {
        let head = *self
            .receivers
            .get(key)
            .expect("active broadcast receiver must be registered");
        *self
            .receivers
            .get_mut(key)
            .expect("active broadcast receiver must be registered") = next_head;

        if head == self.head {
            self.release_head_receiver()
        } else {
            Vec::new()
        }
    }

    fn release_head_receiver(&mut self) -> Vec<Arc<T>> {
        self.head_receivers -= 1;

        if self.head_receivers == 0 {
            self.reclaim_consumed()
        } else {
            Vec::new()
        }
    }

    fn receive(&mut self, key: SlotId) -> Option<(Arc<T>, Vec<Arc<T>>)> {
        let head = *self
            .receivers
            .get(key)
            .expect("active broadcast receiver must be registered");

        if head < self.tail {
            debug_assert!(head >= self.head);
            let offset = (head - self.head) as usize;
            let msg = self.buffer[offset].clone();
            let reclaimed = self.advance_receiver(key, head + 1);
            // A reclaim triggered by this receive always begins with this receiver's own message:
            // the reclaim path runs only for a cursor sitting at `head`, so the first slot drained
            // is `msg`. `take_msg` relies on this to recognise that it owns the payload.
            debug_assert!(
                reclaimed
                    .first()
                    .is_none_or(|first| Arc::ptr_eq(first, &msg))
            );
            Some((msg, reclaimed))
        } else {
            None
        }
    }

    fn reclaim_consumed(&mut self) -> Vec<Arc<T>> {
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
        // Move reclaimed messages out so their Drop impls run after `inner` is unlocked.
        let reclaimed = self.buffer.drain(..consumed).collect();

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

/// A sender handle to the broadcast channel.
///
/// The sender can be cloned to create multiple producers. When all senders are dropped,
/// the channel is closed.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        // Relaxed is enough because this count publishes nothing on its own: receivers read it
        // only to decide whether the channel is closed, and every message it could hide is
        // published under `inner`, which a receiver holds before it observes the count.
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        match self.shared.senders.fetch_sub(1, Ordering::AcqRel) {
            1 => {
                // If this is the last sender, we need to wake up the receiver so it can
                // observe the disconnected state.
                let wakers = self.shared.inner.lock().waiters.take_wakers();
                for waker in wakers {
                    waker.wake();
                }
            }
            _ => {
                // there are still other senders left, do nothing
            }
        }
    }
}

impl<T> Sender<T> {
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
    /// use asyncband::broadcast::unbounded;
    ///
    /// let (tx, mut rx) = unbounded::channel();
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

            inner.waiters.take_wakers()
        };

        // Notify all waiting receivers. An unsent message is dropped here too, once the lock is
        // released.
        for waker in wakers {
            waker.wake();
        }
    }

    /// Returns the number of messages currently retained by the shared buffer.
    ///
    /// This is not the number of messages any single receiver can still read. It is the shared
    /// backlog kept alive by the slowest active receiver.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::unbounded;
    ///
    /// let (tx, mut rx) = unbounded::channel();
    /// tx.send(10);
    /// assert_eq!(tx.buffer_len(), 1);
    ///
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// assert_eq!(tx.buffer_len(), 0);
    /// ```
    pub fn buffer_len(&self) -> usize {
        self.shared.inner.lock().buffer.len()
    }

    /// Returns the number of active receivers.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::unbounded;
    ///
    /// let (tx, rx) = unbounded::channel::<i32>();
    /// assert_eq!(tx.receiver_count(), 1);
    ///
    /// let rx2 = tx.subscribe();
    /// assert_eq!(tx.receiver_count(), 2);
    ///
    /// drop(rx);
    /// drop(rx2);
    /// assert_eq!(tx.receiver_count(), 0);
    /// ```
    pub fn receiver_count(&self) -> usize {
        self.shared.inner.lock().receivers.len()
    }

    /// Creates a new receiver that starts receiving messages from the current tail of the channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::unbounded;
    /// use asyncband::broadcast::unbounded::TryRecvError;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (tx, _) = unbounded::channel();
    /// tx.send(10);
    ///
    /// let mut rx = tx.subscribe();
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    /// tx.send(20);
    /// assert_eq!(rx.recv().await, Ok(20));
    /// # }
    /// ```
    pub fn subscribe(&self) -> Receiver<T> {
        let mut inner = self.shared.inner.lock();
        let head = inner.tail;
        let key = inner.insert_receiver(head);
        let shared = self.shared.clone();
        Receiver { shared, key }
    }
}

/// A receiver handle to the broadcast channel.
///
/// Each receiver sees every message sent to the channel while the receiver is active.
///
/// Cloning a receiver creates one that shares this receiver's unread backlog, while
/// [`Receiver::resubscribe`] creates one that starts at the current tail instead.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    key: SlotId,
}

impl<T> Clone for Receiver<T> {
    /// Creates a receiver that starts from this receiver's current position.
    ///
    /// The clone reads this receiver's unread backlog and every later message. Use
    /// [`Receiver::resubscribe`] instead to start at the current tail and skip the backlog.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::unbounded;
    ///
    /// let (tx, mut rx) = unbounded::channel();
    /// tx.send(1);
    ///
    /// let mut clone = rx.clone();
    /// assert_eq!(rx.try_recv(), Ok(1));
    /// assert_eq!(clone.try_recv(), Ok(1));
    /// ```
    fn clone(&self) -> Self {
        let key = {
            let mut inner = self.shared.inner.lock();
            let head = *inner
                .receivers
                .get(self.key)
                .expect("active broadcast receiver must be registered");
            inner.insert_receiver(head)
        };
        Self {
            shared: self.shared.clone(),
            key,
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let reclaimed = {
            let mut inner = self.shared.inner.lock();
            inner.remove_receiver(self.key)
        };
        drop(reclaimed);
    }
}

impl<T: Clone> Receiver<T> {
    /// Receives the next value for this receiver.
    ///
    /// # Returns
    ///
    /// * `Ok(T)`: The next message.
    /// * `Err(RecvError::Disconnected)`: All senders have been dropped and no more messages are
    ///   available.
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
    /// use asyncband::broadcast::unbounded;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (tx, mut rx) = unbounded::channel();
    /// tx.send(10);
    /// assert_eq!(rx.recv().await, Ok(10));
    /// # }
    /// ```
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        Recv {
            receiver: self,
            registration: None,
        }
        .await
    }

    /// Attempts to receive the next value for this receiver without blocking.
    ///
    /// # Returns
    ///
    /// * `Ok(T)`: The next message.
    /// * `Err(TryRecvError::Empty)`: No message is currently available.
    /// * `Err(TryRecvError::Disconnected)`: All senders have been dropped and no more messages are
    ///   available.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::unbounded;
    ///
    /// let (tx, mut rx) = unbounded::channel();
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
fn take_msg<T: Clone>(msg: Arc<T>, reclaimed: Vec<Arc<T>>) -> T {
    let sole_owner = !reclaimed.is_empty();
    drop(reclaimed);

    if !sole_owner {
        return (*msg).clone();
    }

    // Another receiver can still hold an in-flight reference to the same message, so the clone
    // remains the fallback.
    match Arc::try_unwrap(msg) {
        Ok(msg) => msg,
        Err(msg) => (*msg).clone(),
    }
}

impl<T> Receiver<T> {
    fn try_recv_shared(&mut self) -> Result<(Arc<T>, Vec<Arc<T>>), TryRecvError> {
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
    /// use asyncband::broadcast::unbounded;
    ///
    /// let (tx, mut rx) = unbounded::channel();
    /// tx.send(1);
    /// tx.send(2);
    ///
    /// let mut rx2 = rx.resubscribe();
    /// tx.send(3);
    ///
    /// assert_eq!(rx2.try_recv(), Ok(3));
    /// ```
    pub fn resubscribe(&self) -> Self {
        let mut inner = self.shared.inner.lock();
        let head = inner.tail;
        let key = inner.insert_receiver(head);
        let shared = self.shared.clone();
        Self { shared, key }
    }

    /// Returns the number of messages this receiver can still read.
    ///
    /// This count is specific to this receiver, unlike [`Sender::buffer_len`], which reports the
    /// shared backlog retained by the slowest active receiver.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::unbounded;
    ///
    /// let (tx, mut rx) = unbounded::channel();
    /// assert_eq!(rx.len(), 0);
    ///
    /// tx.send(10);
    /// tx.send(20);
    /// assert_eq!(rx.len(), 2);
    ///
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// assert_eq!(rx.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        let inner = self.shared.inner.lock();
        let head = *inner
            .receivers
            .get(self.key)
            .expect("active broadcast receiver must be registered");
        usize::try_from(inner.tail - head).expect("unread broadcast message count exceeds usize")
    }

    /// Returns `true` if this receiver has no currently available messages.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::unbounded;
    ///
    /// let (tx, rx) = unbounded::channel();
    /// assert!(rx.is_empty());
    ///
    /// tx.send(10);
    /// assert!(!rx.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

struct Recv<'a, T> {
    receiver: &'a mut Receiver<T>,
    registration: Option<WaitRegistration>,
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        // Ready paths clear the registration, so only a cancelled pending receive takes this lock.
        if self.registration.is_none() {
            return;
        }

        let waker = {
            let mut inner = self.receiver.shared.inner.lock();
            inner.waiters.unregister_waker(&mut self.registration)
        };
        drop(waker);
    }
}

impl<T: Clone> Future for Recv<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self {
            receiver,
            registration,
        } = self.get_mut();

        // One critical section decides between all three outcomes. Senders append messages and
        // drain the wait set under this same lock, so registering here cannot miss a wake-up and
        // cannot observe a closed channel that still has a message for this receiver.
        let received = {
            let mut inner = receiver.shared.inner.lock();

            match inner.receive(receiver.key) {
                Some(received) => received,
                None => {
                    if receiver.shared.senders.load(Ordering::Acquire) == 0 {
                        *registration = None;
                        return Poll::Ready(Err(RecvError::Disconnected));
                    }

                    let waker = inner.waiters.register_waker(registration, cx);
                    drop(inner);
                    drop(waker);
                    return Poll::Pending;
                }
            }
        };

        let (msg, reclaimed) = received;
        *registration = None;
        Poll::Ready(Ok(take_msg(msg, reclaimed)))
    }
}
