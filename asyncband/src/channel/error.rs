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

/// Selects which buffered value an explicit lossy send replaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullBehavior {
    /// Replace the oldest buffered value.
    DropOldest,
    /// Replace the newest buffered value.
    DropNewest,
}

/// The result of an explicit lossy send.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum SendOutcome<T> {
    /// The value was sent without replacing another value.
    Sent,
    /// The value was sent and the contained buffered value was replaced.
    Replaced(T),
}

/// An error returned when all receivers have been dropped.
#[derive(Clone, PartialEq, Eq)]
pub struct SendError<T>(T);

impl<T> SendError<T> {
    /// Returns a reference to the value that was not sent.
    pub fn as_inner(&self) -> &T {
        &self.0
    }

    /// Returns the value that was not sent.
    pub fn into_inner(self) -> T {
        self.0
    }

    pub(crate) fn new(value: T) -> Self {
        Self(value)
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

/// An error returned by a non-waiting send.
#[derive(Clone, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The channel is full but still connected.
    Full(T),
    /// All receivers have been dropped.
    Disconnected(T),
}

impl<T> TrySendError<T> {
    /// Returns a reference to the value that was not sent.
    pub fn as_inner(&self) -> &T {
        match self {
            Self::Full(value) | Self::Disconnected(value) => value,
        }
    }

    /// Returns the value that was not sent.
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(value) | Self::Disconnected(value) => value,
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("sending on a full channel"),
            Self::Disconnected(_) => f.write_str("sending on a closed channel"),
        }
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => write!(f, "TrySendError<{}>::Full(..)", type_name::<T>()),
            Self::Disconnected(_) => {
                write!(f, "TrySendError<{}>::Disconnected(..)", type_name::<T>())
            }
        }
    }
}

impl<T> std::error::Error for TrySendError<T> {}

/// An error returned when a channel is closed and drained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// All senders have been dropped and no buffered value remains.
    Disconnected,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("receiving on a closed channel")
    }
}

impl std::error::Error for RecvError {}

/// An error returned by a non-waiting receive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// No value is currently available, but the channel is still connected.
    Empty,
    /// All senders have been dropped and no buffered value remains.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("receiving on an empty channel"),
            Self::Disconnected => f.write_str("receiving on a closed channel"),
        }
    }
}

impl std::error::Error for TryRecvError {}
