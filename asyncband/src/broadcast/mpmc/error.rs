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

use std::any::type_name;
use std::fmt;

/// Error returned by [`BoundedSender::try_send`].
///
/// A bounded broadcast channel is lossless, so a publication that would exceed the requested
/// capacity is rejected rather than displacing a retained message. The message that could not be
/// sent can be retrieved again with [`TrySendError::into_inner`].
///
/// [`BoundedSender::try_send`]: crate::broadcast::mpmc::BoundedSender::try_send
#[derive(Clone, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The shared backlog is at capacity, so the message cannot be sent without waiting for the
    /// slowest active receiver to release a retained message.
    Full(T),
}

impl<T> TrySendError<T> {
    /// Gets a reference to the message that failed to be sent.
    pub fn as_inner(&self) -> &T {
        match self {
            TrySendError::Full(msg) => msg,
        }
    }

    /// Consumes the error and returns the message that failed to be sent.
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::Full(msg) => msg,
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TrySendError::Full(_) => "sending on a full channel",
        })
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ty = type_name::<T>();
        match self {
            TrySendError::Full(_) => write!(f, "TrySendError<{ty}>::Full(..)"),
        }
    }
}

impl<T> std::error::Error for TrySendError<T> {}

/// Error returned by `recv`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    /// All senders have been dropped, and this receiver has no remaining messages.
    Disconnected,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("receiving on a disconnected channel")
    }
}

impl std::error::Error for RecvError {}

/// Error returned by `try_recv`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TryRecvError {
    /// No message is currently available, but at least one sender remains.
    Empty,
    /// All senders have been dropped, and this receiver has no remaining messages.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TryRecvError::Empty => "receiving on an empty channel",
            TryRecvError::Disconnected => "receiving on a disconnected channel",
        })
    }
}

impl std::error::Error for TryRecvError {}
