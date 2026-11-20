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

use crate::mpmc::RecvError;
use crate::mpmc::SendError;
use crate::mpmc::TryRecvError;
use crate::mpmc::TrySendError;
use crate::mpmc::queue;
use crate::mpmc::ring;

/// Chooses storage once at construction without changing logical capacity or permit ownership.
pub enum Shared<T> {
    Ring(Box<ring::Shared<T>>),
    General(queue::Shared<T>),
}

impl<T> Shared<T> {
    pub fn new(capacity: usize) -> Self {
        if ring::Shared::<T>::supports(capacity) {
            return Self::Ring(Box::new(ring::Shared::new(capacity)));
        }
        Self::General(queue::Shared::bounded(capacity))
    }

    pub fn clone_sender(&self) {
        match self {
            Self::Ring(shared) => shared.clone_sender(),
            Self::General(shared) => shared.clone_sender(),
        }
    }

    pub fn drop_sender(&self) {
        match self {
            Self::Ring(shared) => shared.drop_sender(),
            Self::General(shared) => shared.drop_sender(),
        }
    }

    pub fn clone_receiver(&self) {
        match self {
            Self::Ring(shared) => shared.clone_receiver(),
            Self::General(shared) => shared.clone_receiver(),
        }
    }

    pub fn drop_receiver(&self) {
        match self {
            Self::Ring(shared) => shared.drop_receiver(),
            Self::General(shared) => shared.drop_receiver(),
        }
    }

    pub fn is_disconnected(&self) -> bool {
        match self {
            Self::Ring(shared) => shared.is_disconnected(),
            Self::General(shared) => !shared.has_senders() || !shared.has_receivers(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Ring(shared) => shared.is_empty(),
            Self::General(shared) => shared.is_empty(),
        }
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        match self {
            Self::Ring(shared) => shared.try_send(value),
            Self::General(shared) => shared.try_send(value),
        }
    }

    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        match self {
            Self::Ring(shared) => shared.send(value).await,
            Self::General(shared) => shared.send(value).await,
        }
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self {
            Self::Ring(shared) => shared.try_recv(),
            Self::General(shared) => shared.try_recv(),
        }
    }

    pub async fn recv(&self) -> Result<T, RecvError> {
        match self {
            Self::Ring(shared) => shared.recv().await,
            Self::General(shared) => shared.recv().await,
        }
    }

    pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        let inner = match self {
            Self::Ring(shared) => PermitInner::Ring(shared.try_reserve()?),
            Self::General(shared) => PermitInner::General(shared.try_reserve()?),
        };
        Ok(Permit { inner })
    }

    pub async fn reserve(&self) -> Result<Permit<'_, T>, SendError<()>> {
        let inner = match self {
            Self::Ring(shared) => PermitInner::Ring(shared.reserve().await?),
            Self::General(shared) => PermitInner::General(shared.reserve().await?),
        };
        Ok(Permit { inner })
    }
}

enum PermitInner<'a, T> {
    Ring(ring::Permit<'a, T>),
    General(queue::Permit<'a, T>),
}

/// Capacity reserved for one value on a bounded MPMC queue.
///
/// A permit does not keep any receiver alive or claim message order. Dropping it without sending
/// passes its capacity to the next waiting sender or makes it available again.
#[must_use = "dropping the permit releases its reserved capacity"]
pub struct Permit<'a, T> {
    inner: PermitInner<'a, T>,
}

impl<T> fmt::Debug for Permit<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Permit").finish_non_exhaustive()
    }
}

impl<T> Permit<'_, T> {
    /// Publishes a value using this reserved capacity without waiting for space.
    ///
    /// This consumes the permit, including when sending fails.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] containing the unsent value if all receivers have been dropped.
    pub fn send(self, value: T) -> Result<(), SendError<T>> {
        match self.inner {
            PermitInner::Ring(permit) => permit.send(value),
            PermitInner::General(permit) => permit.send(value),
        }
    }
}
