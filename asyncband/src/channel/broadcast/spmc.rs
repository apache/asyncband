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

//! Single-producer lossless broadcast channels.
//!
//! ```
//! use asyncband::broadcast::spmc;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let (mut tx, mut first) = spmc::bounded(2);
//! let mut second = tx.subscribe();
//! tx.send("event").await.unwrap();
//!
//! assert_eq!(first.recv().await, Ok("event"));
//! assert_eq!(second.recv().await, Ok("event"));
//! # }
//! ```
//!
//! The single-producer contract is enforced by the sender type:
//!
//! ```compile_fail
//! let (tx, _) = asyncband::broadcast::spmc::unbounded::<usize>();
//! let _ = tx.clone();
//! ```
//!
//! ```compile_fail
//! fn assert_sync<T: Sync>() {}
//! assert_sync::<asyncband::broadcast::spmc::BoundedSender<usize>>();
//! ```

use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;

pub use crate::channel::error::RecvError;
pub use crate::channel::error::SendError;
pub use crate::channel::error::TryRecvError;
pub use crate::channel::error::TrySendError;

/// Creates a bounded SPMC broadcast channel.
///
/// The slowest active subscription gates the sender once `capacity` values are retained.
///
/// # Panics
///
/// Panics if `capacity` is zero.
#[track_caller]
pub fn bounded<T: Clone>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(
        capacity > 0,
        "bounded broadcast channel requires capacity > 0"
    );
    let (sender, receiver) = super::internal::bounded(capacity);
    (
        BoundedSender {
            inner: sender,
            not_sync: PhantomData,
        },
        BoundedReceiver { inner: receiver },
    )
}

/// Creates an unbounded SPMC broadcast channel.
pub fn unbounded<T: Clone>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let (sender, receiver) = super::internal::unbounded();
    (
        UnboundedSender {
            inner: sender,
            not_sync: PhantomData,
        },
        UnboundedReceiver { inner: receiver },
    )
}

/// The sending endpoint of a bounded SPMC broadcast channel.
pub struct BoundedSender<T> {
    inner: super::internal::Sender<T>,
    not_sync: PhantomData<Cell<()>>,
}

impl<T> fmt::Debug for BoundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedSender").finish_non_exhaustive()
    }
}

impl<T> BoundedSender<T> {
    /// Broadcasts a value, waiting while the retention buffer is full.
    pub async fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        self.inner.send(value).await
    }

    /// Attempts to broadcast a value without waiting.
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        self.inner.try_send(value)
    }

    /// Creates a subscription starting at the current committed tail.
    pub fn subscribe(&self) -> BoundedReceiver<T> {
        BoundedReceiver {
            inner: self.inner.subscribe(),
        }
    }

    /// Returns the number of active subscriptions.
    pub fn receiver_count(&self) -> usize {
        self.inner.receiver_count()
    }

    /// Returns the number of values retained for the slowest subscription.
    pub fn buffer_len(&self) -> usize {
        self.inner.buffer_len()
    }
}

/// A subscription to a bounded SPMC broadcast channel.
pub struct BoundedReceiver<T> {
    inner: super::internal::Receiver<T>,
}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T: Clone> BoundedReceiver<T> {
    /// Receives the next value for this subscription.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        self.inner.recv().await
    }

    /// Attempts to receive the next value without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.inner.try_recv()
    }
}

impl<T> BoundedReceiver<T> {
    /// Returns the number of values currently available to this subscription.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns whether no value is currently available to this subscription.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns whether every sender has been dropped.
    pub fn is_disconnected(&self) -> bool {
        self.inner.is_disconnected()
    }
}

/// The sending endpoint of an unbounded SPMC broadcast channel.
pub struct UnboundedSender<T> {
    inner: super::internal::Sender<T>,
    not_sync: PhantomData<Cell<()>>,
}

impl<T> fmt::Debug for UnboundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedSender").finish_non_exhaustive()
    }
}

impl<T> UnboundedSender<T> {
    /// Broadcasts a value without waiting.
    pub fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        self.inner.send_unbounded(value)
    }

    /// Creates a subscription starting at the current committed tail.
    pub fn subscribe(&self) -> UnboundedReceiver<T> {
        UnboundedReceiver {
            inner: self.inner.subscribe(),
        }
    }

    /// Returns the number of active subscriptions.
    pub fn receiver_count(&self) -> usize {
        self.inner.receiver_count()
    }

    /// Returns the number of values retained for the slowest subscription.
    pub fn buffer_len(&self) -> usize {
        self.inner.buffer_len()
    }
}

/// A subscription to an unbounded SPMC broadcast channel.
pub struct UnboundedReceiver<T> {
    inner: super::internal::Receiver<T>,
}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedReceiver").finish_non_exhaustive()
    }
}

impl<T: Clone> UnboundedReceiver<T> {
    /// Receives the next value for this subscription.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        self.inner.recv().await
    }

    /// Attempts to receive the next value without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.inner.try_recv()
    }
}

impl<T> UnboundedReceiver<T> {
    /// Returns the number of values currently available to this subscription.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns whether no value is currently available to this subscription.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns whether every sender has been dropped.
    pub fn is_disconnected(&self) -> bool {
        self.inner.is_disconnected()
    }
}
