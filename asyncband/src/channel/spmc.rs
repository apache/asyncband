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

//! Single-producer, multi-consumer competing queues.
//!
//! The sender is non-cloneable and requires exclusive access. Receivers are cloneable and may
//! receive concurrently; every accepted value is delivered to exactly one receiver.
//!
//! A bounded queue retains at most its requested capacity and waits rather than dropping values.
//! An unbounded queue sends synchronously and grows subject to process memory. Accepted values
//! drain before disconnection is reported, while a send after the last receiver disconnects
//! returns its value.
//!
//! Pending `send` and `recv` operations are cancel safe: dropping their futures does not transfer
//! a value.
//!
//! ```
//! use asyncband::spmc;
//!
//! let (mut tx, first) = spmc::unbounded();
//! let second = first.clone();
//! tx.send(1).unwrap();
//! tx.send(2).unwrap();
//!
//! assert_eq!(first.try_recv(), Ok(1));
//! assert_eq!(second.try_recv(), Ok(2));
//! ```
//!
//! The single-producer contract is enforced by the sender type:
//!
//! ```compile_fail
//! let (tx, _) = asyncband::spmc::unbounded::<()>();
//! let _ = tx.clone();
//! ```
//!
//! ```compile_fail
//! fn assert_sync<T: Sync>() {}
//! assert_sync::<asyncband::spmc::BoundedSender<()>>();
//! ```

use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;

pub use super::error::RecvError;
pub use super::error::SendError;
pub use super::error::TryRecvError;
pub use super::error::TrySendError;

/// Creates a bounded SPMC queue with the given capacity.
///
/// # Panics
///
/// Panics if `capacity` is zero.
#[track_caller]
pub fn bounded<T>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(capacity > 0, "spmc bounded channel requires capacity > 0");
    let (sender, receiver) = super::queue::bounded(capacity);
    (
        BoundedSender {
            inner: sender,
            not_sync: PhantomData,
        },
        BoundedReceiver { inner: receiver },
    )
}

/// Creates an unbounded SPMC queue.
pub fn unbounded<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let (sender, receiver) = super::queue::unbounded();
    (
        UnboundedSender {
            inner: sender,
            not_sync: PhantomData,
        },
        UnboundedReceiver { inner: receiver },
    )
}

/// The sending endpoint of a bounded SPMC queue.
pub struct BoundedSender<T> {
    inner: super::queue::Sender<T>,
    not_sync: PhantomData<Cell<()>>,
}

impl<T> fmt::Debug for BoundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedSender").finish_non_exhaustive()
    }
}

impl<T> BoundedSender<T> {
    /// Sends a value, waiting for capacity when the queue is full.
    pub async fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        self.inner.send(value).await
    }

    /// Attempts to send a value without waiting.
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        self.inner.try_send(value)
    }
}

/// A receiving endpoint of a bounded SPMC queue.
pub struct BoundedReceiver<T> {
    inner: super::queue::Receiver<T>,
}

impl<T> Clone for BoundedReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T> BoundedReceiver<T> {
    /// Receives the next value, waiting while the connected queue is empty.
    pub async fn recv(&self) -> Result<T, RecvError> {
        self.inner.recv().await
    }

    /// Attempts to receive the next value without waiting.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.inner.try_recv()
    }
}

/// The sending endpoint of an unbounded SPMC queue.
pub struct UnboundedSender<T> {
    inner: super::queue::Sender<T>,
    not_sync: PhantomData<Cell<()>>,
}

impl<T> fmt::Debug for UnboundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedSender").finish_non_exhaustive()
    }
}

impl<T> UnboundedSender<T> {
    /// Sends a value without waiting.
    pub fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        self.inner.send_unbounded(value)
    }
}

/// A receiving endpoint of an unbounded SPMC queue.
pub struct UnboundedReceiver<T> {
    inner: super::queue::Receiver<T>,
}

impl<T> Clone for UnboundedReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedReceiver").finish_non_exhaustive()
    }
}

impl<T> UnboundedReceiver<T> {
    /// Receives the next value, waiting while the connected queue is empty.
    pub async fn recv(&self) -> Result<T, RecvError> {
        self.inner.recv().await
    }

    /// Attempts to receive the next value without waiting.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.inner.try_recv()
    }
}
