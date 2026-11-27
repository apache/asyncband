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

use crate::spmc::RecvError;
use crate::spmc::SendError;
use crate::spmc::TryRecvError;
use crate::spmc::TrySendError;
use crate::spmc::bounded_queue::Shared;

/// Creates a bounded single-producer, multi-consumer queue.
///
/// The queue stores at most `capacity` values. Sending waits for a receiver to free capacity when
/// the queue is full.
///
/// Storage may be preallocated when the queue is created; very large capacities use lazily
/// allocated storage.
///
/// Operations may wait for internal synchronization, including short spin-based critical
/// sections. No critical section spans an await point or a wake callback. Messages are dropped
/// outside critical sections, except that dropping the final receiver drops buffered values in
/// one that no remaining operation waits for. The `try_*` methods do not wait for new messages or
/// newly available capacity, but are not lock-free.
///
/// # Panics
///
/// Panics if `capacity` is zero.
#[track_caller]
pub fn bounded<T>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(capacity > 0, "spmc bounded queue requires capacity > 0");
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
/// Instances are created by [`bounded`] and cannot be cloned. Sending requires exclusive access to
/// this endpoint.
pub struct BoundedSender<T> {
    shared: Arc<Shared<T>>,
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
    /// Sends a value, waiting for capacity when the queue is full.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] containing the unsent value if all receivers have been dropped.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending `send` future removes its waiter and drops `value`. A future that has
    /// returned `Pending` has not sent the value. Cancellation releases the exclusive sender borrow
    /// and allows the next operation to use any available capacity. Use
    /// [`reserve`](Self::reserve) to wait for capacity before constructing a value, or
    /// [`try_send`](Self::try_send) when the caller must retain ownership immediately if capacity
    /// is unavailable.
    pub async fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        // SAFETY: This non-cloneable endpoint is the queue's only producer. The exclusive borrow
        // lasts through completion or cancellation of the send.
        unsafe { self.shared.send(value).await }
    }

    /// Reserves capacity for one value before the caller supplies it.
    ///
    /// The returned [`Permit`] retains the exclusive borrow of this sender, keeping a slot
    /// available until [`Permit::send`] publishes a value or the permit is dropped. Holding a
    /// permit does not keep any receiver alive.
    ///
    /// # Errors
    ///
    /// Returns `SendError(())` if all receivers have been dropped. A successful reservation can
    /// still fail to send if disconnection happens before [`Permit::send`].
    ///
    /// # Cancellation
    ///
    /// Dropping a pending `reserve` future removes its waiter and releases the exclusive sender
    /// borrow so later operations can use any available capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::spmc;
    ///
    /// let (mut sender, receiver) = spmc::bounded(1);
    /// let permit = sender.reserve().await.unwrap();
    /// let message = String::from("constructed after capacity became available");
    /// permit.send(message).unwrap();
    /// assert_eq!(
    ///     receiver.recv().await.unwrap(),
    ///     "constructed after capacity became available"
    /// );
    /// # }
    /// ```
    pub async fn reserve(&mut self) -> Result<Permit<'_, T>, SendError<()>> {
        // SAFETY: This non-cloneable endpoint is the queue's only producer. The exclusive borrow
        // lasts through completion or cancellation of the reservation.
        unsafe { self.shared.reserve().await? };
        Ok(Permit {
            shared: &self.shared,
        })
    }

    /// Reserves capacity for one value without waiting.
    ///
    /// Dropping the returned [`Permit`] releases the exclusive sender borrow without sending. See
    /// [`reserve`](Self::reserve) for reservation semantics.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] if capacity is unavailable, or
    /// [`TrySendError::Disconnected`] if all receivers have been dropped. Errors carry `()`
    /// because no value has been supplied.
    pub fn try_reserve(&mut self) -> Result<Permit<'_, T>, TrySendError<()>> {
        // SAFETY: This non-cloneable endpoint is the queue's only producer.
        unsafe { self.shared.try_reserve()? };
        Ok(Permit {
            shared: &self.shared,
        })
    }

    /// Sends a value without waiting for capacity.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] if capacity is unavailable, or
    /// [`TrySendError::Disconnected`] if all receivers have been dropped. Both errors contain the
    /// unsent value; nothing is enqueued on error.
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        // SAFETY: This non-cloneable endpoint is the queue's only producer.
        unsafe { self.shared.try_send(value) }
    }

    /// Returns `true` if every receiver has been dropped.
    ///
    /// Once this returns `true`, every later [`send`](Self::send), [`try_send`](Self::try_send),
    /// [`reserve`](Self::reserve), or [`try_reserve`](Self::try_reserve) fails with a
    /// disconnection error. A `false` result can be outdated as soon as it is returned, because
    /// the last receiver may be dropped concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::spmc;
    ///
    /// let (sender, receiver) = spmc::bounded::<u8>(1);
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
    /// Waits for and receives the next queued value, freeing one capacity slot.
    ///
    /// Queued values remain receivable after disconnection.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Disconnected`] once the sender has been dropped and the queue is empty.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending future does not remove a value from the queue or prevent another receiver
    /// from receiving it.
    pub async fn recv(&self) -> Result<T, RecvError> {
        self.shared.recv().await
    }

    /// Receives the next queued value without waiting for a message, freeing one capacity slot.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] when the queue is empty but the sender is alive.
    /// Returns [`TryRecvError::Disconnected`] once the sender has been dropped and the queue is
    /// empty.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.shared.try_recv()
    }

    /// Returns `true` if the sender has been dropped.
    ///
    /// Buffered values stay receivable after disconnection, so [`recv`](Self::recv) and
    /// [`try_recv`](Self::try_recv) report disconnection only once the queue has been drained. A
    /// `false` result can be outdated as soon as it is returned, because the sender may be dropped
    /// concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::spmc;
    ///
    /// let (mut sender, receiver) = spmc::bounded(1);
    /// sender.try_send(1).unwrap();
    /// assert!(!receiver.is_disconnected());
    /// drop(sender);
    /// assert!(receiver.is_disconnected());
    /// assert_eq!(receiver.try_recv(), Ok(1));
    /// ```
    pub fn is_disconnected(&self) -> bool {
        !self.shared.has_sender()
    }

    /// Returns `true` if no queued value is waiting to be received.
    ///
    /// Held permits and sends still waiting for capacity are not queued values. The result is an
    /// instantaneous observation and may become stale as other tasks send or receive.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::spmc;
    ///
    /// let (mut sender, receiver) = spmc::bounded(1);
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

/// Capacity reserved for one value on a bounded SPMC queue.
///
/// Created by [`BoundedSender::reserve`] or [`BoundedSender::try_reserve`]. Holding a permit
/// retains the exclusive borrow of [`BoundedSender`], keeping a capacity slot available until
/// [`Permit::send`] publishes a value or the permit is dropped. Holding a permit does not keep any
/// receiver alive or block receivers from consuming already-queued values.
#[must_use = "dropping the permit releases the exclusive sender borrow without sending"]
pub struct Permit<'a, T> {
    shared: &'a Shared<T>,
}

impl<T> fmt::Debug for Permit<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Permit").finish_non_exhaustive()
    }
}

impl<T> Permit<'_, T> {
    /// Publishes a value using this reserved capacity without waiting for space.
    ///
    /// This consumes the permit, including when sending fails.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] containing the unsent value if all receivers have been dropped.
    pub fn send(self, value: T) -> Result<(), SendError<T>> {
        // SAFETY: The permit retains the only producer's exclusive borrow from its reservation.
        unsafe { self.shared.send_reserved(value) }
    }
}
