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

use super::RecvError;
use super::SendError;
use super::TryRecvError;
use super::TrySendError;
use super::queue::Producer;
use super::queue::Shared;
use super::queue::channel;

/// Creates a bounded single-producer, multi-consumer queue.
///
/// The queue stores at most `capacity` values in preallocated storage. Sending waits for a
/// receiver to free capacity when the queue is full.
///
/// Operations briefly acquire an internal mutex. No lock is held across an await point, while
/// waking tasks, or while dropping messages. The `try_*` methods do not wait for capacity or
/// messages, but may wait to acquire a mutex.
///
/// # Panics
///
/// Panics if `capacity` is zero or the preallocated message storage exceeds the allocation size
/// limit.
#[track_caller]
pub fn bounded<T>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(capacity > 0, "spmc bounded queue requires capacity > 0");
    let (producer, shared) = channel(capacity);
    (
        BoundedSender { producer, capacity },
        BoundedReceiver { shared },
    )
}

/// Sends values to the associated [`BoundedReceiver`] handles.
///
/// Instances are created by [`bounded`] and cannot be cloned. Sending requires exclusive access to
/// this endpoint.
pub struct BoundedSender<T> {
    producer: Producer<T>,
    // Only the producer can increase the queue length, so consumers need no capacity counter.
    capacity: usize,
}

impl<T> fmt::Debug for BoundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedSender").finish_non_exhaustive()
    }
}

impl<T> BoundedSender<T> {
    /// Sends a value, waiting until capacity is available if the queue is full.
    ///
    /// If all receivers have been dropped, the value is returned in [`SendError`].
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending `send` future drops `value` without enqueueing it and releases the
    /// exclusive sender borrow. Any available capacity remains usable by the next operation. Use
    /// [`try_send`](Self::try_send) when the caller must retain ownership if capacity is
    /// unavailable.
    pub async fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        self.producer.send(value, self.capacity).await
    }

    /// Attempts to send a value without waiting for capacity.
    ///
    /// Returns [`TrySendError::Full`] when the queue has reached its exact capacity and
    /// [`TrySendError::Disconnected`] when all receivers have been dropped.
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        self.producer.try_send(value, self.capacity)
    }
}

/// Receives values from the associated [`BoundedSender`].
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
    /// Buffered values remain available after the sender is dropped. Once they are drained,
    /// this method returns [`RecvError::Disconnected`].
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending `recv` future does not consume a value. Any unconsumed notification is
    /// passed to another waiting task, so cancellation does not prevent it from receiving.
    pub async fn recv(&self) -> Result<T, RecvError> {
        self.shared.recv().await
    }

    /// Attempts to receive the next available value without waiting for a message.
    ///
    /// Returns [`TryRecvError::Empty`] while the queue is empty and a sender remains, or
    /// [`TryRecvError::Disconnected`] once the queue is empty and the sender has been dropped.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.shared.try_recv()
    }
}
