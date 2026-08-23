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

//! Single-producer, single-consumer queues.
//!
//! Both endpoints are non-cloneable. Sending and receiving require exclusive access, preserving
//! the topology's static single-writer and single-reader guarantees.
//!
//! A bounded queue retains at most its requested capacity and waits rather than dropping values.
//! An unbounded queue sends synchronously and grows subject to process memory. Accepted values
//! drain before disconnection is reported, while a send after receiver disconnection returns its
//! value.
//!
//! Pending `send` and `recv` operations are cancel safe: dropping their futures does not transfer
//! a value.
//!
//! ```
//! use asyncband::spsc;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let (mut tx, mut rx) = spsc::bounded(2);
//! tx.send("event").await.unwrap();
//! assert_eq!(rx.recv().await, Ok("event"));
//! # }
//! ```
//!
//! The endpoints deliberately cannot be cloned or shared by reference across threads:
//!
//! ```compile_fail
//! let (tx, _) = asyncband::spsc::unbounded::<()>();
//! let _ = tx.clone();
//! ```
//!
//! ```compile_fail
//! fn assert_sync<T: Sync>() {}
//! assert_sync::<asyncband::spsc::BoundedSender<()>>();
//! ```

use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;

pub use super::error::RecvError;
pub use super::error::SendError;
pub use super::error::TryRecvError;
pub use super::error::TrySendError;

/// Creates a bounded SPSC queue with the given capacity.
///
/// # Panics
///
/// Panics if `capacity` is zero.
#[track_caller]
pub fn bounded<T>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    let (sender, receiver) = super::mpsc::bounded(capacity);
    (
        BoundedSender {
            inner: sender,
            not_sync: PhantomData,
        },
        BoundedReceiver {
            inner: receiver,
            not_sync: PhantomData,
        },
    )
}

/// Creates an unbounded SPSC queue.
pub fn unbounded<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let (sender, receiver) = super::mpsc::unbounded();
    (
        UnboundedSender {
            inner: sender,
            not_sync: PhantomData,
        },
        UnboundedReceiver {
            inner: receiver,
            not_sync: PhantomData,
        },
    )
}

/// The sending endpoint of a bounded SPSC queue.
pub struct BoundedSender<T> {
    inner: super::mpsc::BoundedSender<T>,
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

/// The receiving endpoint of a bounded SPSC queue.
pub struct BoundedReceiver<T> {
    inner: super::mpsc::BoundedReceiver<T>,
    not_sync: PhantomData<Cell<()>>,
}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T> BoundedReceiver<T> {
    /// Receives the next value, waiting while the connected queue is empty.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        self.inner.recv().await
    }

    /// Attempts to receive the next value without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.inner.try_recv()
    }
}

/// The sending endpoint of an unbounded SPSC queue.
pub struct UnboundedSender<T> {
    inner: super::mpsc::UnboundedSender<T>,
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
        self.inner.send(value)
    }
}

/// The receiving endpoint of an unbounded SPSC queue.
pub struct UnboundedReceiver<T> {
    inner: super::mpsc::UnboundedReceiver<T>,
    not_sync: PhantomData<Cell<()>>,
}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedReceiver").finish_non_exhaustive()
    }
}

impl<T> UnboundedReceiver<T> {
    /// Receives the next value, waiting while the connected queue is empty.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        self.inner.recv().await
    }

    /// Attempts to receive the next value without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.inner.try_recv()
    }
}
