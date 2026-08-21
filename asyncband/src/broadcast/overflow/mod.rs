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

//! A multi-producer multi-consumer broadcast channel.
//!
//! This channel supports multiple senders and multiple receivers. Each message sent by any
//! sender is received by all receivers. If a receiver falls behind, it may miss messages,
//! which is reported via [`RecvError::Lagged`].
//!
//! # Examples
//!
//! Basic usage:
//!
//! ```
//! use asyncband::broadcast::overflow;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let (tx, mut rx1) = overflow::channel(16);
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
//! Handling lag:
//!
//! ```
//! use asyncband::broadcast::overflow;
//! use asyncband::broadcast::overflow::RecvError;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let (tx, mut rx) = overflow::channel(2);
//!
//! tx.send(1);
//! tx.send(2);
//! tx.send(3); // overwrites the oldest message (1)
//!
//! assert_eq!(rx.recv().await, Err(RecvError::Lagged(1)));
//! assert_eq!(rx.recv().await, Ok(2));
//! assert_eq!(rx.recv().await, Ok(3));
//! # }
//! ```

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::rwlock::RwLock;
use crate::internal::waitset::WaitRegistration;
use crate::internal::waitset::WaitSet;

#[cfg(test)]
mod tests;

/// Creates a new broadcast channel with the given hint `capacity`. The actual capacity may be
/// greater than the provided `capacity`.
///
/// See [module-level documentation](self) for broadcast channel semantics.
///
/// # Panics
///
/// Panics if `capacity` is 0.
///
/// # Examples
///
/// ```
/// use asyncband::broadcast::overflow;
///
/// let (tx, mut rx) = overflow::channel(16);
/// tx.send(10);
/// assert_eq!(rx.try_recv(), Ok(10));
/// ```
pub fn channel<T: Clone>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "capacity must be greater than 0");

    let capacity = capacity.next_power_of_two();
    let mask = capacity - 1;

    let mut buffer = Vec::with_capacity(capacity);
    for _ in 0..capacity {
        buffer.push(RwLock::new(Slot {
            msg: None,
            version: 0,
        }));
    }

    let shared = Arc::new(Shared {
        buffer: buffer.into_boxed_slice(),
        capacity,
        mask,
        tail: AtomicU64::new(0),
        state: Mutex::new(State {
            waiters: WaitSet::new(),
        }),
        senders: AtomicUsize::new(1),
    });
    let sender = Sender {
        shared: shared.clone(),
    };
    let receiver = Receiver { shared, head: 0 };
    (sender, receiver)
}

/// Error returned by [`Receiver::recv`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    /// The receiver lagged too far behind.
    ///
    /// The count is the number of messages skipped. The receiver's internal cursor has been
    /// advanced to the oldest available message.
    Lagged(u64),
    /// The sender has become disconnected, and there will never be any more data received on it.
    Disconnected,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecvError::Lagged(n) => write!(f, "receiver has been lagged by {n}"),
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
    /// The receiver lagged too far behind.
    ///
    /// The count is the number of messages skipped. The receiver's internal cursor has been
    /// advanced to the oldest available message.
    Lagged(u64),
    /// The sender has become disconnected, and there will never be any more data received on it.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "receiving on an empty channel"),
            TryRecvError::Lagged(n) => write!(f, "receiver has been lagged by {n}"),
            TryRecvError::Disconnected => write!(f, "receiving on a closed channel"),
        }
    }
}

impl std::error::Error for TryRecvError {}

#[derive(Debug)]
struct Slot<T> {
    /// The message. `None` if the slot is empty (initial state only).
    msg: Option<T>,
    /// The absolute version of the message in this slot.
    version: u64,
}

struct Shared<T> {
    buffer: Box<[RwLock<Slot<T>>]>,
    capacity: usize,
    mask: usize,
    /// The next sequence after the contiguous prefix of fully published slots.
    tail: AtomicU64,
    /// Serializes senders and makes publishing atomic with draining or registering waiters.
    state: Mutex<State>,
    /// Number of active senders.
    senders: AtomicUsize,
}

struct State {
    /// Receivers waiting for a new message.
    waiters: WaitSet,
}

fn wake_waiters(wakers: impl IntoIterator<Item = Waker>) {
    for waker in wakers {
        waker.wake();
    }
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
        self.shared.senders.fetch_add(1, Ordering::Release);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        match self.shared.senders.fetch_sub(1, Ordering::AcqRel) {
            1 => {
                // If this is the last sender, we need to wake up the receiver so it can
                // observe the disconnected state.
                let wakers = self.shared.state.lock().waiters.take_wakers();
                wake_waiters(wakers);
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
    /// This operation is non-blocking. If the channel buffer is full, the oldest message
    /// in the buffer is overwritten. Any receiver that was waiting for that overwritten
    /// message will receive a [`RecvError::Lagged`] error on its next call to `recv`.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::overflow;
    ///
    /// let (tx, mut rx) = overflow::channel(16);
    /// tx.send(10);
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// ```
    pub fn send(&self, msg: T) {
        let wakers = {
            let mut state = self.shared.state.lock();
            let tail = self.shared.tail.load(Ordering::Relaxed);
            let idx = (tail as usize) & self.shared.mask;

            let mut slot = self.shared.buffer[idx].write();
            slot.msg = Some(msg);
            slot.version = tail;

            // Publish the completed slot before releasing its write lock. A receiver that sees the
            // new tail either held the old slot lock first or waits until the new value is
            // complete.
            self.shared
                .tail
                .store(tail.wrapping_add(1), Ordering::Release);
            drop(slot);

            state.waiters.take_wakers()
        };

        // Notify all waiting receivers.
        wake_waiters(wakers);
    }

    /// Creates a new receiver that starts receiving messages from the current tail of the channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::overflow;
    /// use asyncband::broadcast::overflow::TryRecvError;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (tx, _) = overflow::channel(16);
    /// tx.send(10);
    ///
    /// let mut rx = tx.subscribe();
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    /// tx.send(20);
    /// assert_eq!(rx.recv().await, Ok(20));
    /// # }
    /// ```
    pub fn subscribe(&self) -> Receiver<T> {
        // Receiver starts at the current tail.
        let head = self.shared.tail.load(Ordering::Acquire);
        let shared = self.shared.clone();
        Receiver { shared, head }
    }
}

/// A receiver handle to the broadcast channel.
///
/// The receiver can be cloned to create multiple consumers. Each receiver sees every
/// message sent to the channel (unless it lags behind).
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    head: u64,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            head: self.head,
        }
    }
}

impl<T: Clone> Receiver<T> {
    /// Receives the next value for this receiver.
    ///
    /// # Returns
    ///
    /// * `Ok(T)`: The next message.
    /// * `Err(RecvError::Lagged(u64))`: The receiver lagged behind. The internal cursor is advanced
    ///   to the oldest available message. The count indicates how many messages were skipped.
    /// * `Err(RecvError::Disconnected)`: All senders have been dropped and no more messages are
    ///   available.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::overflow;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (tx, mut rx) = overflow::channel(16);
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
    /// * `Err(TryRecvError::Lagged(u64))`: The receiver lagged behind. The internal cursor is
    ///   advanced to the oldest available message. The count indicates how many messages were
    ///   skipped.
    /// * `Err(TryRecvError::Disconnected)`: All senders have been dropped and no more messages are
    ///   available.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::overflow;
    ///
    /// let (tx, mut rx) = overflow::channel(16);
    /// tx.send(10);
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// ```
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let shared = &self.shared;
        let cap = shared.capacity as u64;

        loop {
            let tail = shared.tail.load(Ordering::Acquire);
            let head = self.head;

            // diff represents how far behind the head is from the tail.
            let diff = tail.wrapping_sub(head);

            // 1. Check for Lag
            if diff > cap {
                let missed = diff - cap;
                self.head = tail.wrapping_sub(cap);
                return Err(TryRecvError::Lagged(missed));
            }

            // 2. Check if a message is available
            if diff > 0 {
                let idx = (head as usize) & shared.mask;
                let slot = shared.buffer[idx].read();

                if slot.version == head {
                    return if let Some(msg) = &slot.msg {
                        self.head = head.wrapping_add(1);
                        Ok(msg.clone())
                    } else {
                        Err(TryRecvError::Empty)
                    };
                }

                drop(slot);

                // The slot may have been overwritten after the first tail snapshot. Publication
                // happens while holding the slot write lock, so a fresh tail now includes that
                // overwrite and produces an accurate lag count.
                let tail = shared.tail.load(Ordering::Acquire);
                let diff = tail.wrapping_sub(head);
                if diff > cap {
                    let missed = diff - cap;
                    self.head = tail.wrapping_sub(cap);
                    return Err(TryRecvError::Lagged(missed));
                }

                return Err(TryRecvError::Empty);
            }

            // 3. No message available (diff == 0). Check for Closed.
            if shared.senders.load(Ordering::Acquire) == 0 {
                // Observing the final sender drop synchronizes with all preceding sends, but the
                // first tail snapshot predates that acquire. Reload it before declaring the
                // channel drained so a published final message cannot be hidden by closure.
                if shared.tail.load(Ordering::Acquire) != head {
                    continue;
                }
                return Err(TryRecvError::Disconnected);
            }

            return Err(TryRecvError::Empty);
        }
    }
}

impl<T> Receiver<T> {
    /// Re-subscribes to the channel, returning a new receiver that starts receiving messages
    /// from the *current* tail of the channel.
    ///
    /// This is useful if the receiver has lagged too far behind and wants to jump to the latest
    /// message, skipping everything in between.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::broadcast::overflow;
    ///
    /// let (tx, mut rx) = overflow::channel(2);
    /// tx.send(1);
    /// tx.send(2);
    ///
    /// let mut rx2 = rx.resubscribe();
    /// tx.send(3);
    ///
    /// assert_eq!(rx2.try_recv(), Ok(3));
    /// ```
    pub fn resubscribe(&self) -> Self {
        // Resubscribe starts at the current tail.
        let head = self.shared.tail.load(Ordering::Acquire);
        let shared = self.shared.clone();
        Self { shared, head }
    }
}

struct Recv<'a, T> {
    receiver: &'a mut Receiver<T>,
    registration: Option<WaitRegistration>,
}

impl<T: Clone> Future for Recv<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self {
            receiver,
            registration,
        } = self.get_mut();

        loop {
            // Senders publish data or closure before draining the current wake epoch. Once a
            // result is observable, Drop does not need to lock the waiter set again.
            match receiver.try_recv() {
                Ok(val) => {
                    *registration = None;
                    return Poll::Ready(Ok(val));
                }
                Err(TryRecvError::Lagged(n)) => {
                    *registration = None;
                    return Poll::Ready(Err(RecvError::Lagged(n)));
                }
                Err(TryRecvError::Disconnected) => {
                    *registration = None;
                    return Poll::Ready(Err(RecvError::Disconnected));
                }
                Err(TryRecvError::Empty) => {}
            }

            let shared = &receiver.shared;
            let mut state = shared.state.lock();

            // Double check tail to avoid race conditions.
            if shared.tail.load(Ordering::Acquire) != receiver.head {
                // New message arrived while acquiring the lock. Retry.
                drop(state);
                continue;
            }

            // Check for Closed
            // Use Acquire to ensure we see all writes before the sender dropped.
            if shared.senders.load(Ordering::Acquire) == 0 {
                *registration = None;
                return Poll::Ready(Err(RecvError::Disconnected));
            }

            // Register Waker
            let replaced_waker = state.waiters.register_waker(registration, cx);
            drop(state);
            drop(replaced_waker);
            return Poll::Pending;
        }
    }
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        if self.registration.is_some() {
            let removed_waker = {
                let mut state = self.receiver.shared.state.lock();
                state.waiters.unregister_waker(&mut self.registration)
            };
            drop(removed_waker);
        }
    }
}
