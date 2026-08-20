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

use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;
use std::sync::Arc;

use super::OwnedPrioritySharePermit;
use super::PriorityShare;
use super::PrioritySharePermit;

/// An admission controller that fairly shares a fixed number of permits across keys.
///
/// Each acquisition belongs to a key. When a permit becomes available,
/// [`FairShare`] admits a queued acquisition for the key with the fewest
/// permits currently held. Ties are resolved by queue order.
///
/// See the [module-level documentation](super) for details about the fairness
/// guarantee.
#[derive(Debug)]
pub struct FairShare<K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    admission: Arc<PriorityShare<K, S>>,
}

impl<K> FairShare<K, RandomState>
where
    K: Eq + Hash,
{
    /// Creates a fair-share admission controller with the given number of permits.
    ///
    /// # Panics
    ///
    /// Panics if `permits` is zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::admission::FairShare;
    ///
    /// let admission = FairShare::<String>::new(3);
    /// assert_eq!(admission.available_permits(), 3);
    /// ```
    pub fn new(permits: usize) -> Self {
        Self::with_hasher(permits, RandomState::new())
    }
}

impl<K, S> FairShare<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Creates a fair-share admission controller with the given number of
    /// permits and hash builder.
    ///
    /// # Panics
    ///
    /// Panics if `permits` is zero.
    pub fn with_hasher(permits: usize, hash_builder: S) -> Self {
        assert!(permits > 0, "FairShare requires at least one permit");
        let admission = PriorityShare::with_hasher([permits], hash_builder)
            .pop()
            .expect("one priority was configured");
        Self {
            admission: Arc::new(admission),
        }
    }

    /// Returns the current number of permits available for immediate admission.
    ///
    /// A permit already assigned to a queued acquisition counts as held by its
    /// key, even if that acquisition has not yet been polled again.
    pub fn available_permits(&self) -> usize {
        self.admission.available_permits()
    }

    /// Returns the number of acquisitions currently waiting for a permit.
    ///
    /// An acquisition is no longer counted once it has been assigned a permit,
    /// even if its future has not yet been polled again.
    pub fn num_waiters(&self) -> usize {
        self.admission.num_waiters()
    }

    /// Attempts to acquire one permit for `key` without waiting.
    ///
    /// This method does not bypass queued acquisitions.
    pub fn try_acquire(&self, key: K) -> Option<FairSharePermit<'_, K, S>> {
        self.admission
            .try_acquire(key)
            .map(|permit| FairSharePermit { permit })
    }

    /// Acquires one permit for `key`.
    ///
    /// # Cancel safety
    ///
    /// Cancelling this method loses the acquisition's place in the queue. If
    /// a permit has already been assigned, cancellation releases it for another
    /// queued acquisition.
    pub async fn acquire(&self, key: K) -> FairSharePermit<'_, K, S> {
        FairSharePermit {
            permit: self.admission.acquire(key).await,
        }
    }

    /// Attempts to acquire one owned permit for `key` without waiting.
    ///
    /// The admission controller must be wrapped in an [`Arc`] to call this
    /// method.
    pub fn try_acquire_owned(self: Arc<Self>, key: K) -> Option<OwnedFairSharePermit<K, S>> {
        let permit = self.admission.clone().try_acquire_owned(key)?;
        Some(OwnedFairSharePermit {
            permit,
            _admission: self,
        })
    }

    /// Acquires one owned permit for `key`.
    ///
    /// The admission controller must be wrapped in an [`Arc`] to call this
    /// method.
    ///
    /// # Cancel safety
    ///
    /// This method has the same cancellation behavior as [`Self::acquire`].
    pub async fn acquire_owned(self: Arc<Self>, key: K) -> OwnedFairSharePermit<K, S> {
        let permit = self.admission.clone().acquire_owned(key).await;
        OwnedFairSharePermit {
            permit,
            _admission: self,
        }
    }
}

/// A permit from a [`FairShare`] admission controller.
///
/// This type is created by the [`acquire`] and [`try_acquire`] methods on
/// [`FairShare`]. It represents one admitted operation associated with a key.
/// Dropping it returns the permit and may admit another queued acquisition.
///
/// [`acquire`]: FairShare::acquire
/// [`try_acquire`]: FairShare::try_acquire
#[must_use = "permits are released immediately when dropped"]
#[derive(Debug)]
pub struct FairSharePermit<'a, K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    permit: PrioritySharePermit<'a, K, S>,
}

impl<K, S> FairSharePermit<'_, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Returns the key associated with this permit.
    pub fn key(&self) -> &K {
        self.permit.key()
    }
}

/// An owned permit from a [`FairShare`] admission controller.
///
/// This type is created by the [`acquire_owned`] and [`try_acquire_owned`]
/// methods on [`FairShare`]. Unlike [`FairSharePermit`], it owns an [`Arc`] to
/// the admission controller and has no lifetime parameter. Dropping it returns
/// the permit and may admit another queued acquisition.
///
/// [`acquire_owned`]: FairShare::acquire_owned
/// [`try_acquire_owned`]: FairShare::try_acquire_owned
#[must_use = "permits are released immediately when dropped"]
#[derive(Debug)]
pub struct OwnedFairSharePermit<K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    permit: OwnedPrioritySharePermit<K, S>,
    _admission: Arc<FairShare<K, S>>,
}

impl<K, S> OwnedFairSharePermit<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Returns the key associated with this permit.
    pub fn key(&self) -> &K {
        self.permit.key()
    }
}
