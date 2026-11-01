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

use std::fmt;
use std::sync::Arc;

use crate::mpmc::RecvError;
use crate::mpmc::SendError;
use crate::mpmc::TryRecvError;
use crate::mpmc::TrySendError;
use crate::mpmc::queue::Permit;
use crate::mpmc::queue::Shared;

/// Creates a bounded multi-producer, multi-consumer queue.
///
/// Queued values, held permits, and capacity granted to waiting senders occupy at most `capacity`
/// slots. Pending `send` and `reserve` operations receive capacity in wait-queue order. Sending
/// waits for a receiver to free capacity when none is available.
///
/// The `try_*` methods do not wait for capacity or messages, but may briefly block on an internal
/// mutex.
///
/// # Panics
///
/// Panics if `capacity` is zero.
#[track_caller]
pub fn bounded<T>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(capacity > 0, "mpmc bounded queue requires capacity > 0");
    let shared = Arc::new(Shared::bounded(capacity));
    (
        BoundedSender {
            shared: shared.clone(),
        },
        BoundedReceiver { shared },
    )
}

/// Sends values to the associated [`BoundedReceiver`] handles.
///
/// Instances are created by [`bounded`] and can be cloned to add producers.
pub struct BoundedSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        self.shared.clone_sender();
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
        self.shared.drop_sender();
    }
}

impl<T> BoundedSender<T> {
    /// Sends a value, waiting until capacity is available if the queue is full.
    ///
    /// If all receivers have been dropped, the value is returned in [`SendError`].
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending `send` future releases its waiting resources before dropping `value`,
    /// without sending it or retaining capacity. Use [`reserve`](Self::reserve) to wait for
    /// capacity before constructing a value.
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.shared.send(value).await
    }

    /// Reserves capacity for one value before constructing it.
    ///
    /// A successful reservation returns a [`Permit`]. Dropping it releases capacity. A permit
    /// reserves space, not message order, and does not keep receivers alive.
    ///
    /// Returns `SendError(())` if all receivers have been dropped.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending `reserve` future releases its place in the wait queue. If it was already
    /// granted capacity, that capacity passes to the next waiter or becomes available again.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::bounded(1);
    /// let permit = sender.reserve().await.unwrap();
    /// let message = String::from("constructed after capacity became available");
    /// permit.send(message).unwrap();
    /// assert_eq!(
    ///     receiver.recv().await.unwrap(),
    ///     "constructed after capacity became available"
    /// );
    /// # }
    /// ```
    pub async fn reserve(&self) -> Result<Permit<'_, T>, SendError<()>> {
        self.shared.reserve().await
    }

    /// Reserves capacity for one value without waiting.
    ///
    /// Returns [`TrySendError::Full`] when all capacity belongs to queued values, held permits,
    /// or granted waiters, and [`TrySendError::Disconnected`] when all receivers are gone.
    pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        self.shared.try_reserve()
    }

    /// Attempts to send a value without waiting for capacity.
    ///
    /// Returns [`TrySendError::Full`] when no unassigned capacity remains and
    /// [`TrySendError::Disconnected`] when all receivers have been dropped.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.shared.try_send(value)
    }

    /// Returns `true` if every receiver has been dropped.
    ///
    /// Once this returns `true`, every later [`send`](Self::send), [`try_send`](Self::try_send),
    /// [`reserve`](Self::reserve) or [`try_reserve`](Self::try_reserve) fails with a disconnection
    /// error, as does [`Permit::send`] on a permit reserved earlier. A `false` result can be
    /// outdated as soon as it is returned, because the last receiver may be dropped concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::bounded::<u8>(1);
    /// let competing = receiver.clone();
    /// drop(receiver);
    /// assert!(!sender.is_disconnected());
    /// drop(competing);
    /// assert!(sender.is_disconnected());
    /// ```
    pub fn is_disconnected(&self) -> bool {
        !self.shared.has_receivers()
    }
}

/// Receives values from the associated [`BoundedSender`] handles.
///
/// Cloned receivers compete for values, and every accepted value is returned by exactly one
/// receiver while a receiver remains. Dropping the final receiver releases buffered values.
pub struct BoundedReceiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for BoundedReceiver<T> {
    fn clone(&self) -> Self {
        self.shared.clone_receiver();
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedReceiver<T> {
    fn drop(&mut self) {
        self.shared.drop_receiver();
    }
}

impl<T> BoundedReceiver<T> {
    /// Receives the next available value.
    ///
    /// Buffered values remain available after the final sender is dropped. Once they are drained,
    /// this method returns [`RecvError::Disconnected`].
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending `recv` future does not consume a value or prevent other receivers from
    /// receiving it.
    pub async fn recv(&self) -> Result<T, RecvError> {
        self.shared.recv().await
    }

    /// Attempts to receive the next available value without waiting for a message.
    ///
    /// Returns [`TryRecvError::Empty`] while the queue is empty and a sender remains, or
    /// [`TryRecvError::Disconnected`] once the queue is empty and all senders have been dropped.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.shared.try_recv()
    }

    /// Returns `true` if every sender has been dropped.
    ///
    /// Buffered values stay receivable after disconnection, so [`recv`](Self::recv) and
    /// [`try_recv`](Self::try_recv) report disconnection only once the queue has been drained. A
    /// `false` result can be outdated as soon as it is returned, because the last sender may be
    /// dropped concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::bounded(1);
    /// sender.try_send(1).unwrap();
    /// assert!(!receiver.is_disconnected());
    /// drop(sender);
    /// assert!(receiver.is_disconnected());
    /// assert_eq!(receiver.try_recv(), Ok(1));
    /// ```
    pub fn is_disconnected(&self) -> bool {
        !self.shared.has_senders()
    }

    /// Returns `true` if no queued value is waiting to be received.
    ///
    /// Held permits and sends still waiting for capacity are not queued values. The result is an
    /// instantaneous observation and may become stale as other tasks send or receive.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::bounded(1);
    /// assert!(receiver.is_empty());
    /// sender.try_send(1).unwrap();
    /// assert!(!receiver.is_empty());
    /// assert_eq!(receiver.try_recv(), Ok(1));
    /// assert!(receiver.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.shared.is_empty()
    }
}
