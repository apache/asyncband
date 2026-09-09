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
use std::future::poll_fn;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::Shared;
use super::semaphore::Capacity;
use crate::mpsc::SendError;
use crate::mpsc::TrySendError;

/// The sending endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`](crate::mpsc::bounded) function.
pub struct BoundedSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        BoundedSender {
            shared: self.shared.clone(),
        }
    }
}

impl<T> fmt::Debug for BoundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedSender").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedSender<T> {
    fn drop(&mut self) {
        if self.shared.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.rx_waker.wake();
        }
    }
}

impl<T> BoundedSender<T> {
    pub(super) fn new(shared: Arc<Shared<T>>) -> Self {
        Self { shared }
    }

    /// Sends a message, waiting until the channel has capacity when necessary.
    ///
    /// If the receiver has been dropped, the returned error contains `value`.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending `send` loses its place waiting for capacity and drops `value`; a call
    /// that has returned `Pending` has not sent the message. Use [`Self::try_send`] when the
    /// caller must retain ownership if capacity is unavailable, or [`Self::reserve`] to wait for
    /// capacity before constructing the message.
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        let mut acquire = self.shared.tx_permits.acquire();
        match poll_fn(|cx| acquire.poll(cx)).await {
            Ok(capacity) => Permit {
                shared: &self.shared,
                capacity,
            }
            .send(value),
            Err(_) => Err(SendError::new(value)),
        }
    }

    /// Reserves capacity for one message before constructing it.
    ///
    /// A successful reservation returns a [`Permit`]. Dropping the permit releases capacity;
    /// [`Permit::send`] publishes a value without waiting for space. Reservations do not establish
    /// message order: other producers may send while a permit is held.
    ///
    /// Returns `SendError(())` if the receiver has been dropped. A permit obtained earlier does
    /// not keep the receiver alive; sending with it can still return the unsent value on
    /// disconnect.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending reservation loses its place in the wait queue. If capacity has already
    /// been granted, it is released to the next waiter or made available to a new sender.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (tx, mut rx) = asyncband::mpsc::bounded(1);
    /// let permit = tx.reserve().await.unwrap();
    /// let message = String::from("constructed after capacity became available");
    /// permit.send(message).unwrap();
    /// assert_eq!(
    ///     rx.recv().await.unwrap(),
    ///     "constructed after capacity became available"
    /// );
    /// # }
    /// ```
    pub async fn reserve(&self) -> Result<Permit<'_, T>, SendError<()>> {
        let mut acquire = self.shared.tx_permits.acquire();
        let capacity = poll_fn(|cx| acquire.poll(cx)).await?;
        Ok(Permit {
            shared: &self.shared,
            capacity,
        })
    }

    /// Reserves capacity for one message without waiting.
    ///
    /// Returns [`TrySendError::Full`] if queued messages and outstanding permits occupy the
    /// buffer, or [`TrySendError::Disconnected`] if the receiver has been dropped.
    pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        let capacity = self.shared.tx_permits.try_acquire()?;
        Ok(Permit {
            shared: &self.shared,
            capacity,
        })
    }

    /// Attempts to send a message without waiting for capacity.
    ///
    /// A full buffer returns [`TrySendError::Full`], while a dropped receiver returns
    /// [`TrySendError::Disconnected`]. Both errors return ownership of the unsent value.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpsc::TrySendError;
    /// use asyncband::mpsc::bounded;
    ///
    /// let (tx, mut rx) = bounded(1);
    /// tx.try_send(10).unwrap();
    /// assert_eq!(tx.try_send(20), Err(TrySendError::Full(20)));
    ///
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// tx.try_send(20).unwrap();
    /// drop(rx);
    /// assert_eq!(tx.try_send(30), Err(TrySendError::Disconnected(30)));
    /// ```
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        match self.try_reserve() {
            Ok(permit) => permit
                .send(value)
                .map_err(|error| TrySendError::Disconnected(error.into_inner())),
            Err(TrySendError::Full(())) => Err(TrySendError::Full(value)),
            Err(TrySendError::Disconnected(())) => Err(TrySendError::Disconnected(value)),
        }
    }
}

/// Capacity reserved for one message on a bounded channel.
///
/// Created by [`BoundedSender::reserve`] or [`BoundedSender::try_reserve`]. Holding a permit
/// reduces available capacity but does not prevent other messages from being received. Dropping
/// it without sending releases capacity and notifies a waiting sender.
#[must_use = "dropping the permit releases its reserved capacity"]
pub struct Permit<'a, T> {
    shared: &'a Shared<T>,
    capacity: Capacity<'a>,
}

impl<T> fmt::Debug for Permit<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Permit").finish_non_exhaustive()
    }
}

impl<T> Permit<'_, T> {
    /// Publishes a message using this permit, without waiting for capacity.
    ///
    /// If the receiver has been dropped, the returned error contains the unsent value.
    pub fn send(self, value: T) -> Result<(), SendError<T>> {
        // SAFETY: This permit owns one capacity unit. Claiming a slot and writing it is a
        // synchronous operation with no user callbacks or await points between the two.
        unsafe { self.shared.buffer.push(value) }.map_err(SendError::new)?;
        // The queued message now owns capacity, even if the wake callback panics.
        mem::forget(self.capacity);
        self.shared.rx_waker.wake();
        Ok(())
    }
}
