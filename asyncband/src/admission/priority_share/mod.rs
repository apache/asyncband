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

mod scheduler;

use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;
use std::sync::Arc;

use scheduler::Scheduler;

/// A priority-bound handle in a priority-share admission family.
///
/// [`PriorityShare::new`] creates one handle per configured priority, ordered
/// from lowest to highest. Those handles share one capacity and one scheduler.
/// A handle keeps its priority when it is moved or cloned, so acquisition
/// methods do not take a priority argument. Separate constructor calls create
/// independent admission families.
///
/// Configuration entries add capacity from the lowest to the highest priority.
/// The admission limit at priority `p` is the sum of entries up to and
/// including `p`, and every assigned permit counts toward that limit regardless
/// of its priority. For example, `[4, 1]` lets priority 0 enter while fewer than
/// four permits are assigned and priority 1 while fewer than five are assigned.
/// A higher priority can therefore use all shared capacity, including headroom
/// unavailable to lower priorities.
///
/// When contended, the highest eligible priority is admitted first. Within
/// that priority, the owner with the fewest permits currently held across all
/// priorities is admitted first. Ties are resolved by queue order. The same
/// owner may acquire and wait at multiple priorities; all of its permits count
/// toward one fair-share identity.
///
/// Priority only affects admission. An acquisition that has already been
/// assigned a permit is never revoked for later higher-priority work. Sustained
/// higher-priority demand can therefore starve lower priorities, and reserved
/// headroom can remain unused while lower-priority work waits. Priorities with
/// equal admission limits still differ under contention because the higher one
/// is admitted first.
///
/// Available-permit observations are scoped to the handle's priority. Values
/// from different handles overlap and must not be added together.
#[derive(Debug)]
pub struct PriorityShare<K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    scheduler: Arc<Scheduler<K, S>>,
    priority: usize,
}

impl<K, S> Clone for PriorityShare<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn clone(&self) -> Self {
        Self {
            scheduler: self.scheduler.clone(),
            priority: self.priority,
        }
    }
}

impl<K> PriorityShare<K, RandomState>
where
    K: Eq + Hash,
{
    /// Creates a priority-share admission family.
    ///
    /// `capacity_increments` lists the additional shared capacity unlocked at
    /// each priority from lowest to highest. The returned vector contains one
    /// priority-bound handle for every entry in the same order. Individual
    /// entries may be zero as long as the total capacity is nonzero.
    ///
    /// # Panics
    ///
    /// Panics if `capacity_increments` is empty, its total is zero, or its total
    /// overflows `usize`.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::admission::PriorityShare;
    ///
    /// let priorities = PriorityShare::<String>::new([2, 1]);
    /// let low = &priorities[0];
    /// let high = &priorities[1];
    ///
    /// assert_eq!(low.priority(), 0);
    /// assert_eq!(high.priority(), 1);
    /// assert_eq!(low.available_permits(), 2);
    /// assert_eq!(high.available_permits(), 3);
    /// assert_eq!(low.try_acquire("low".to_owned()).unwrap().priority(), 0);
    /// assert_eq!(high.try_acquire("high".to_owned()).unwrap().priority(), 1);
    /// ```
    pub fn new<C>(capacity_increments: C) -> Vec<Self>
    where
        C: AsRef<[usize]>,
    {
        Self::with_hasher(capacity_increments, RandomState::new())
    }
}

impl<K, S> PriorityShare<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Creates a priority-share admission family with the given hash builder.
    ///
    /// `capacity_increments` has the same meaning as in [`Self::new`].
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`Self::new`].
    pub fn with_hasher<C>(capacity_increments: C, hash_builder: S) -> Vec<Self>
    where
        C: AsRef<[usize]>,
    {
        let capacity_increments = capacity_increments.as_ref();
        assert!(
            !capacity_increments.is_empty(),
            "PriorityShare requires at least one priority"
        );

        let mut total_permits = 0usize;
        let mut admission_limits = Vec::with_capacity(capacity_increments.len());
        for increment in capacity_increments {
            total_permits = total_permits
                .checked_add(*increment)
                .expect("PriorityShare capacity overflow");
            admission_limits.push(total_permits);
        }
        assert!(
            total_permits > 0,
            "PriorityShare requires at least one permit"
        );

        let scheduler = Arc::new(Scheduler::new(
            admission_limits.into_boxed_slice(),
            hash_builder,
        ));
        (0..capacity_increments.len())
            .map(|priority| Self {
                scheduler: scheduler.clone(),
                priority,
            })
            .collect()
    }

    /// Returns the priority bound to this handle.
    ///
    /// Priorities are dense zero-based values, and a larger value means a
    /// higher priority.
    pub fn priority(&self) -> usize {
        self.priority
    }

    /// Returns the number of permits currently available to this priority.
    ///
    /// This is the number of additional acquisitions that fit below this
    /// priority's shared admission limit. A permit assigned at any priority can
    /// reduce this value. Values from different handles overlap and must not be
    /// added together. The highest-priority handle reports the total number of
    /// unassigned permits in the admission family.
    ///
    /// A permit assigned to a queued acquisition is no longer available even
    /// if that acquisition has not been polled again. This method returns an
    /// instantaneous observation; use [`Self::try_acquire`] to atomically test
    /// and acquire capacity.
    pub fn available_permits(&self) -> usize {
        self.scheduler.available_permits(self.priority)
    }

    pub(super) fn num_waiters(&self) -> usize {
        self.scheduler.num_waiters()
    }

    /// Attempts to acquire one permit for `owner` at this handle's priority.
    ///
    /// This method may bypass queued work at lower priorities. It does not
    /// bypass queued work that should be admitted first at the same or a higher
    /// priority.
    pub fn try_acquire(&self, owner: K) -> Option<PrioritySharePermit<'_, K, S>> {
        let owner = Arc::new(owner);
        self.scheduler
            .try_acquire(owner.clone(), self.priority)
            .then(|| PrioritySharePermit {
                admission: self,
                owner,
            })
    }

    /// Acquires one permit for `owner` at this handle's priority.
    ///
    /// # Cancel safety
    ///
    /// Cancelling this method loses the acquisition's place in the queue. If a
    /// permit has already been assigned, cancellation releases it for another
    /// queued acquisition.
    pub async fn acquire(&self, owner: K) -> PrioritySharePermit<'_, K, S> {
        let owner = Arc::new(owner);
        self.scheduler.acquire(owner.clone(), self.priority).await;
        PrioritySharePermit {
            admission: self,
            owner,
        }
    }

    /// Attempts to acquire one owned permit for `owner` at this handle's
    /// priority.
    ///
    /// The handle must be wrapped in an [`Arc`] to call this method.
    pub fn try_acquire_owned(self: Arc<Self>, owner: K) -> Option<OwnedPrioritySharePermit<K, S>> {
        let owner = Arc::new(owner);
        if !self.scheduler.try_acquire(owner.clone(), self.priority) {
            return None;
        }
        Some(OwnedPrioritySharePermit {
            admission: self,
            owner,
        })
    }

    /// Acquires one owned permit for `owner` at this handle's priority.
    ///
    /// The handle must be wrapped in an [`Arc`] to call this method.
    ///
    /// # Cancel safety
    ///
    /// This method has the same cancellation behavior as [`Self::acquire`].
    pub async fn acquire_owned(self: Arc<Self>, owner: K) -> OwnedPrioritySharePermit<K, S> {
        let owner = Arc::new(owner);
        self.scheduler.acquire(owner.clone(), self.priority).await;
        OwnedPrioritySharePermit {
            admission: self,
            owner,
        }
    }

    fn release(&self, owner: &K) {
        self.scheduler.release(owner);
    }
}

/// A borrowed permit from a [`PriorityShare`] handle.
///
/// Dropping this permit returns it to the shared admission family and may admit
/// a queued acquisition.
#[must_use = "permits are released immediately when dropped"]
#[derive(Debug)]
pub struct PrioritySharePermit<'a, K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    admission: &'a PriorityShare<K, S>,
    owner: Arc<K>,
}

impl<K, S> PrioritySharePermit<'_, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Returns the owner associated with this permit.
    pub fn key(&self) -> &K {
        &self.owner
    }

    /// Returns the priority associated with this permit.
    pub fn priority(&self) -> usize {
        self.admission.priority
    }
}

impl<K, S> Drop for PrioritySharePermit<'_, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        self.admission.release(&self.owner);
    }
}

/// An owned permit from a [`PriorityShare`] handle.
///
/// Unlike [`PrioritySharePermit`], this type owns an [`Arc`] to its
/// priority-bound handle and has no lifetime parameter. Dropping it returns the
/// permit and may admit a queued acquisition.
#[must_use = "permits are released immediately when dropped"]
#[derive(Debug)]
pub struct OwnedPrioritySharePermit<K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    admission: Arc<PriorityShare<K, S>>,
    owner: Arc<K>,
}

impl<K, S> OwnedPrioritySharePermit<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Returns the owner associated with this permit.
    pub fn key(&self) -> &K {
        &self.owner
    }

    /// Returns the priority associated with this permit.
    pub fn priority(&self) -> usize {
        self.admission.priority
    }
}

impl<K, S> Drop for OwnedPrioritySharePermit<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        self.admission.release(&self.owner);
    }
}
