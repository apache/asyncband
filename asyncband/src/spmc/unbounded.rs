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

/// Creates an unbounded single-producer, multi-consumer queue.
///
/// Sends are synchronous and values may be buffered until available memory is exhausted.
///
/// Operations briefly acquire an internal mutex. No lock is held across an await point, while
/// waking tasks, or while dropping messages. Sending and trying to receive may wait to acquire
/// a mutex, but never wait for capacity or new messages.
pub fn unbounded<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let shared = Arc::new(Shared::unbounded());
    (
        UnboundedSender {
            shared: shared.clone(),
        },
        UnboundedReceiver { shared },
    )
}

/// Sends values to the associated [`UnboundedReceiver`] handles.
///
/// Instances are created by [`unbounded`] and cannot be cloned. Sending requires exclusive access
/// to this endpoint.
pub struct UnboundedSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> fmt::Debug for UnboundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedSender").finish_non_exhaustive()
    }
}

impl<T> Drop for UnboundedSender<T> {
    fn drop(&mut self) {
        self.shared.drop_sender();
    }
}

impl<T> UnboundedSender<T> {
    /// Enqueues a value without waiting for capacity.
    ///
    /// Success does not guarantee that a receiver will consume the value before being dropped.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] containing the unsent value if all receivers have been dropped.
    pub fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        match self.shared.try_send(value) {
            Ok(()) => Ok(()),
            Err(TrySendError::Disconnected(value)) => Err(SendError::new(value)),
            Err(TrySendError::Full(_)) => unreachable!("unbounded queue cannot be full"),
        }
    }
}

/// Receives values from the associated [`UnboundedSender`] handles.
///
/// Cloned receivers compete for values, and every accepted value is returned by exactly one
/// receiver while a receiver remains. Dropping the final receiver releases buffered values.
pub struct UnboundedReceiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for UnboundedReceiver<T> {
    fn clone(&self) -> Self {
        self.shared.clone_receiver();
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for UnboundedReceiver<T> {
    fn drop(&mut self) {
        self.shared.drop_receiver();
    }
}

impl<T> UnboundedReceiver<T> {
    /// Waits for and receives the next queued value.
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

    /// Receives the next queued value without waiting for a message.
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
