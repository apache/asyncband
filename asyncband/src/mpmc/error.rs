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

/// An error returned when trying to send on a disconnected queue.
///
/// The value that could not be sent can be retrieved with [`SendError::into_inner`].
#[derive(Clone, PartialEq, Eq)]
pub struct SendError<T>(T);

impl<T> SendError<T> {
    /// Gets a reference to the value that failed to be sent.
    pub fn as_inner(&self) -> &T {
        &self.0
    }

    /// Consumes the error and returns the value that failed to be sent.
    pub fn into_inner(self) -> T {
        self.0
    }

    pub(super) fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sending on a disconnected queue")
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SendError<{}>(..)", type_name::<T>())
    }
}

impl<T> std::error::Error for SendError<T> {}

/// Error returned by [`BoundedSender::try_send`](crate::mpmc::BoundedSender::try_send).
#[derive(Clone, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The queue is full, so the value cannot be sent without waiting for capacity.
    Full(T),
    /// All receivers have been dropped, so the value can never be received.
    Disconnected(T),
}

impl<T> TrySendError<T> {
    /// Gets a reference to the value that failed to be sent.
    pub fn as_inner(&self) -> &T {
        match self {
            TrySendError::Full(value) | TrySendError::Disconnected(value) => value,
        }
    }

    /// Consumes the error and returns the value that failed to be sent.
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::Full(value) | TrySendError::Disconnected(value) => value,
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TrySendError::Full(_) => "sending on a full queue",
            TrySendError::Disconnected(_) => "sending on a disconnected queue",
        })
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ty = type_name::<T>();
        match self {
            TrySendError::Full(_) => write!(f, "TrySendError<{ty}>::Full(..)"),
            TrySendError::Disconnected(_) => {
                write!(f, "TrySendError<{ty}>::Disconnected(..)")
            }
        }
    }
}

impl<T> std::error::Error for TrySendError<T> {}

/// Error returned by a receive operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    /// All senders have been dropped, and no buffered values remain.
    Disconnected,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("receiving on a disconnected queue")
    }
}

impl std::error::Error for RecvError {}

/// Error returned by a non-blocking receive operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TryRecvError {
    /// No value is currently available, but at least one sender remains.
    Empty,
    /// All senders have been dropped, and no buffered values remain.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TryRecvError::Empty => "receiving on an empty queue",
            TryRecvError::Disconnected => "receiving on a disconnected queue",
        })
    }
}

impl std::error::Error for TryRecvError {}
