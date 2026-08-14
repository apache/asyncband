// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::type_name;
use std::fmt;

/// An error returned when trying to send on a closed channel.
///
/// Returned from [`UnboundedSender::send`] or [`BoundedSender::send`] if the
/// corresponding [`UnboundedReceiver`] or [`BoundedReceiver`] has already been
/// dropped.
///
/// The message that could not be sent can be retrieved again with
/// [`SendError::into_inner`].
///
/// [`UnboundedSender::send`]: crate::mpsc::UnboundedSender::send
/// [`BoundedSender::send`]: crate::mpsc::BoundedSender::send
/// [`UnboundedReceiver`]: crate::mpsc::UnboundedReceiver
/// [`BoundedReceiver`]: crate::mpsc::BoundedReceiver
#[derive(Clone, PartialEq, Eq)]
pub struct SendError<T>(T);

impl<T> SendError<T> {
    /// Get a reference to the message that failed to be sent.
    pub fn as_inner(&self) -> &T {
        &self.0
    }

    /// Consumes the error and returns the message that failed to be sent.
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
        f.write_str("sending on a closed channel")
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SendError<{}>(..)", type_name::<T>())
    }
}

impl<T> std::error::Error for SendError<T> {}

/// Error returned by `try_send`.
#[derive(Clone, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The channel is full, so data may not be sent at this time, but the receiver has not yet
    /// disconnected.
    Full(T),
    /// The receiver has become disconnected, and there will never be any more data sent on it.
    Disconnected(T),
}

impl<T> TrySendError<T> {
    /// Gets a reference to the message that failed to be sent.
    pub fn as_inner(&self) -> &T {
        match self {
            TrySendError::Full(msg) | TrySendError::Disconnected(msg) => msg,
        }
    }

    /// Consumes the error and returns the message that failed to be sent.
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
            TrySendError::Disconnected(_) => "sending on a closed channel",
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

/// Error returned by `recv`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    /// The sender has become disconnected, and there will never be any more data received on it.
    Disconnected,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("receiving on a closed channel")
    }
}

impl std::error::Error for RecvError {}

/// Error returned by `try_recv`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TryRecvError {
    /// This channel is currently empty, but the sender(s) have not yet disconnected, so data may
    /// yet become available.
    Empty,
    /// The sender has become disconnected, and there will never be any more data received on it.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TryRecvError::Empty => "receiving on an empty channel",
            TryRecvError::Disconnected => "receiving on a closed channel",
        })
    }
}

impl std::error::Error for TryRecvError {}
