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
use std::sync::atomic::Ordering;

use super::State;
use crate::mpsc::SendError;

/// The sending endpoint of an unbounded mpsc channel.
///
/// Instances are created by the [`unbounded`](crate::mpsc::unbounded) function.
pub struct UnboundedSender<T> {
    shared: Arc<State<T>>,
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.0.fetch_add(1, Ordering::Relaxed);
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
        // Disconnection participates in the same SC order as receiver registration.
        if self.shared.senders.0.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.shared.recv.wake();
        }
    }
}

impl<T> UnboundedSender<T> {
    pub(super) fn new(shared: Arc<State<T>>) -> Self {
        Self { shared }
    }

    /// Enqueues a message without waiting for capacity.
    ///
    /// This operation is synchronous because the channel has no capacity limit. If the receiver has
    /// been dropped, the returned error contains `value`. Success means the message was queued;
    /// it does not guarantee that the receiver will consume it before being dropped.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.shared.queue.push(value).map_err(SendError::new)?;
        self.shared.recv.wake();
        Ok(())
    }
}
