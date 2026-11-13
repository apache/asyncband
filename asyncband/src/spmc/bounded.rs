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
use crate::spmc::queue::Shared;

/// Creates a bounded single-producer, multi-consumer queue.
///
/// The queue stores at most `capacity` values. Sending waits for a receiver to free capacity when
/// the queue is full.
///
/// Operations briefly acquire an internal mutex. No lock is held across an await point, while
/// waking tasks, or while dropping messages. The `try_*` methods do not wait for capacity or
/// messages, but may wait to acquire a mutex.
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
    /// # Cancel safety
    ///
    /// Dropping a pending `send` future removes its waiter and drops `value`. A future that has
    /// returned `Pending` has not sent the value. Cancellation releases the exclusive sender borrow
    /// and allows the next operation to use any available capacity. Use
    /// [`try_send`](Self::try_send) when the caller must retain ownership if capacity is
    /// unavailable.
    pub async fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        self.shared.send(value).await
    }

    /// Sends a value without waiting for capacity.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] if capacity is unavailable, or
    /// [`TrySendError::Disconnected`] if all receivers have been dropped. Both errors contain the
    /// unsent value; nothing is enqueued on error.
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        self.shared.try_send(value)
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
    /// # Cancel safety
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
}
