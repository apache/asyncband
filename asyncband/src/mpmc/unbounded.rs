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
use super::queue::Shared;

/// Creates an unbounded multi-producer, multi-consumer queue.
///
/// Sends are synchronous and values may be buffered until available memory is exhausted.
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
/// Instances are created by [`unbounded`] and can be cloned to add producers.
pub struct UnboundedSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        self.shared.clone_sender();
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
        self.shared.drop_sender();
    }
}

impl<T> UnboundedSender<T> {
    /// Sends a value without waiting.
    ///
    /// If all receivers have been dropped, the value is returned in [`SendError`].
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
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
    /// Receives the next available value.
    ///
    /// Buffered values remain available after the final sender is dropped. Once they are drained,
    /// this method returns [`RecvError::Disconnected`]. This method is cancel safe and passes a
    /// selected value notification to another receiver if the pending future is cancelled.
    pub async fn recv(&self) -> Result<T, RecvError> {
        self.shared.recv().await
    }

    /// Attempts to receive the next available value without waiting.
    ///
    /// Returns [`TryRecvError::Empty`] while the queue is empty and a sender remains, or
    /// [`TryRecvError::Disconnected`] once the queue is empty and all senders have been dropped.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.shared.try_recv()
    }
}
