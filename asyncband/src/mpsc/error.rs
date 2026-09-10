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

/// A send or capacity reservation failed because the receiver has been dropped.
///
/// Returned by [`UnboundedSender::send`], [`BoundedSender::send`], [`Permit::send`], and
/// [`reserve`].
///
/// A failed send retains the unsent message. A failed reservation carries `()` because no
/// message has been provided yet. Access the value with [`as_inner`](Self::as_inner) or
/// [`into_inner`](Self::into_inner).
///
/// [`UnboundedSender::send`]: crate::mpsc::UnboundedSender::send
/// [`BoundedSender::send`]: crate::mpsc::BoundedSender::send
/// [`reserve`]: crate::mpsc::BoundedSender::reserve
/// [`Permit::send`]: crate::mpsc::Permit::send
#[derive(Clone, PartialEq, Eq)]
pub struct SendError<T>(T);

impl<T> SendError<T> {
    /// Gets a reference to the unsent message, or `()` for a failed reservation.
    pub fn as_inner(&self) -> &T {
        &self.0
    }

    /// Consumes the error and returns the unsent message, or `()` for a failed reservation.
    pub fn into_inner(self) -> T {
        self.0
    }

    /// Creates a new `SendError` with the given message.
    pub(super) fn new(msg: T) -> SendError<T> {
        SendError(msg)
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sending on a disconnected channel")
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SendError<{}>(..)", type_name::<T>())
    }
}

impl<T> std::error::Error for SendError<T> {}

/// An attempt to send or reserve capacity failed.
///
/// Returned by [`try_send`](crate::mpsc::BoundedSender::try_send) and
/// [`try_reserve`](crate::mpsc::BoundedSender::try_reserve). A failed send retains the unsent
/// message; a failed reservation carries `()` because no message has been provided yet.
#[derive(Clone, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// No capacity is available for sending or reserving a message.
    Full(T),
    /// The receiver has been dropped.
    Disconnected(T),
}

impl<T> TrySendError<T> {
    /// Gets a reference to the unsent message, or `()` for a failed reservation.
    pub fn as_inner(&self) -> &T {
        match self {
            TrySendError::Full(msg) | TrySendError::Disconnected(msg) => msg,
        }
    }

    /// Consumes the error and returns the unsent message, or `()` for a failed reservation.
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::Full(msg) | TrySendError::Disconnected(msg) => msg,
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TrySendError::Full(_) => "sending on a full channel",
            TrySendError::Disconnected(_) => "sending on a disconnected channel",
        })
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ty = type_name::<T>();
        match self {
            TrySendError::Full(_) => write!(f, "TrySendError<{ty}>::Full(..)"),
            TrySendError::Disconnected(_) => write!(f, "TrySendError<{ty}>::Disconnected(..)"),
        }
    }
}

impl<T> std::error::Error for TrySendError<T> {}

/// A receive operation cannot produce another value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    /// All senders have been dropped, and no buffered messages remain.
    Disconnected,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("receiving on a disconnected channel")
    }
}

impl std::error::Error for RecvError {}

/// A non-blocking receive did not produce a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TryRecvError {
    /// No message is currently available, but at least one sender remains.
    Empty,
    /// All senders have been dropped, and no buffered messages remain.
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
