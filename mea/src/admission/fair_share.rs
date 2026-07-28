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

use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use slab::Slab;

use crate::internal::Mutex;

/// An admission controller that fairly shares a fixed number of permits across keys.
///
/// Each acquisition belongs to a key. When a permit becomes available,
/// [`FairShare`] admits a queued acquisition for the key with the fewest
/// permits currently in flight. Ties are resolved by queue order.
///
/// See the [module-level documentation](super) for details about the fairness
/// guarantee.
#[derive(Debug)]
pub struct FairShare<K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    state: Mutex<State<K, S>>,
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
    /// use mea::admission::FairShare;
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
        Self {
            state: Mutex::new(State::new(permits, hash_builder)),
        }
    }

    /// Returns the current number of permits available for immediate admission.
    ///
    /// A permit already assigned to a queued acquisition counts as in flight,
    /// even if that acquisition has not yet been polled again.
    pub fn available_permits(&self) -> usize {
        self.state.lock().available_permits
    }

    /// Attempts to acquire one permit for `key` without waiting.
    ///
    /// This method does not bypass queued acquisitions.
    pub fn try_acquire(&self, key: K) -> Option<FairSharePermit<'_, K, S>> {
        let key = Arc::new(key);
        let admitted = self.state.lock().try_admit(key.clone());
        admitted.then(|| FairSharePermit {
            admission: self,
            key,
        })
    }

    /// Acquires one permit for `key`.
    ///
    /// # Cancel safety
    ///
    /// Cancelling this method loses the acquisition's place in the queue. If
    /// a permit has already been assigned, cancellation releases it for another
    /// queued acquisition.
    pub async fn acquire(&self, key: K) -> FairSharePermit<'_, K, S> {
        let key = Arc::new(key);
        Acquire::new(self, key.clone()).await;
        FairSharePermit {
            admission: self,
            key,
        }
    }

    /// Attempts to acquire one owned permit for `key` without waiting.
    ///
    /// The admission controller must be wrapped in an [`Arc`] to call this
    /// method.
    pub fn try_acquire_owned(self: Arc<Self>, key: K) -> Option<OwnedFairSharePermit<K, S>> {
        let key = Arc::new(key);
        let admitted = self.state.lock().try_admit(key.clone());
        admitted.then(|| OwnedFairSharePermit {
            admission: self,
            key,
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
        let key = Arc::new(key);
        Acquire::new(&self, key.clone()).await;
        OwnedFairSharePermit {
            admission: self,
            key,
        }
    }

    fn release(&self, key: &K) {
        let mut wakers = Vec::new();
        {
            let mut state = self.state.lock();
            state.release(key);
            state.admit_waiters(&mut wakers);
        }
        wake_all(wakers);
    }
}

#[derive(Debug)]
struct State<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    total_permits: usize,
    available_permits: usize,
    pending: usize,
    next_sequence: u64,
    groups: HashMap<Arc<K>, GroupState, S>,
    waiters: Slab<Waiter>,
}

impl<K, S> State<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(permits: usize, hash_builder: S) -> Self {
        Self {
            total_permits: permits,
            available_permits: permits,
            pending: 0,
            next_sequence: 0,
            groups: HashMap::with_hasher(hash_builder),
            waiters: Slab::new(),
        }
    }

    fn try_admit(&mut self, key: Arc<K>) -> bool {
        if self.available_permits == 0 || self.pending != 0 {
            return false;
        }

        self.available_permits -= 1;
        self.groups.entry(key).or_default().in_flight += 1;
        true
    }

    fn enqueue(&mut self, key: Arc<K>, waker: &Waker) -> usize {
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .expect("FairShare acquisition sequence overflowed u64::MAX");

        let waiter = self.waiters.insert(Waiter {
            sequence,
            waker: Some(waker.clone()),
            admitted: false,
        });
        self.groups.entry(key).or_default().queue.push_back(waiter);
        self.pending += 1;
        waiter
    }

    fn poll_waiter(&mut self, waiter: usize, waker: &Waker) -> Poll<()> {
        let state = self
            .waiters
            .get_mut(waiter)
            .expect("FairShare waiter is missing");

        if state.admitted {
            self.waiters.remove(waiter);
            Poll::Ready(())
        } else {
            if state
                .waker
                .as_ref()
                .is_none_or(|current| !current.will_wake(waker))
            {
                state.waker = Some(waker.clone());
            }
            Poll::Pending
        }
    }

    fn cancel(&mut self, waiter_id: usize, key: &K) {
        let waiter = self.waiters.remove(waiter_id);
        if waiter.admitted {
            self.release(key);
            return;
        }

        let remove_group = {
            let group = self
                .groups
                .get_mut(key)
                .expect("FairShare waiter group is missing");
            let position = group
                .queue
                .iter()
                .position(|candidate| *candidate == waiter_id)
                .expect("FairShare waiter is missing from its group");
            group.queue.remove(position);
            group.in_flight == 0 && group.queue.is_empty()
        };

        self.pending -= 1;
        if remove_group {
            self.groups.remove(key);
        }
    }

    fn admit_waiters(&mut self, wakers: &mut Vec<Waker>) {
        while self.available_permits > 0 && self.pending > 0 {
            let key = self
                .next_group()
                .expect("FairShare has pending acquisitions without a group");
            let waiter = self.groups[&key]
                .queue
                .front()
                .copied()
                .expect("FairShare pending group has no waiters");

            {
                let group = self
                    .groups
                    .get_mut(&key)
                    .expect("FairShare pending group is missing");
                let popped = group.queue.pop_front();
                debug_assert_eq!(popped, Some(waiter));
                group.in_flight += 1;
            }

            self.available_permits -= 1;
            self.pending -= 1;

            let waiter = &mut self.waiters[waiter];
            waiter.admitted = true;
            if let Some(waker) = waiter.waker.take() {
                wakers.push(waker);
            }
        }
    }

    fn next_group(&self) -> Option<Arc<K>> {
        self.groups
            .iter()
            .filter_map(|(key, group)| {
                let waiter = *group.queue.front()?;
                let sequence = self.waiters[waiter].sequence;
                Some((group.in_flight, sequence, key))
            })
            .min_by_key(|(in_flight, sequence, _)| (*in_flight, *sequence))
            .map(|(_, _, key)| key.clone())
    }

    fn release(&mut self, key: &K) {
        let remove_group = {
            let group = self
                .groups
                .get_mut(key)
                .expect("FairShare released a permit for an unknown key");
            debug_assert!(group.in_flight > 0);
            group.in_flight -= 1;
            group.in_flight == 0 && group.queue.is_empty()
        };

        if remove_group {
            self.groups.remove(key);
        }

        self.available_permits += 1;
        debug_assert!(self.available_permits <= self.total_permits);
    }
}

#[derive(Debug, Default)]
struct GroupState {
    in_flight: usize,
    queue: VecDeque<usize>,
}

#[derive(Debug)]
struct Waiter {
    sequence: u64,
    waker: Option<Waker>,
    admitted: bool,
}

#[derive(Debug)]
struct Acquire<'a, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    admission: &'a FairShare<K, S>,
    key: Arc<K>,
    waiter: Option<usize>,
    completed: bool,
}

impl<'a, K, S> Acquire<'a, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(admission: &'a FairShare<K, S>, key: Arc<K>) -> Self {
        Self {
            admission,
            key,
            waiter: None,
            completed: false,
        }
    }
}

impl<K, S> Drop for Acquire<'_, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        let Some(waiter) = self.waiter.take() else {
            return;
        };

        let mut wakers = Vec::new();
        {
            let mut state = self.admission.state.lock();
            state.cancel(waiter, &self.key);
            state.admit_waiters(&mut wakers);
        }
        wake_all(wakers);
    }
}

impl<K, S> Future for Acquire<'_, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.completed {
            return Poll::Ready(());
        }

        if let Some(waiter) = this.waiter {
            if this
                .admission
                .state
                .lock()
                .poll_waiter(waiter, cx.waker())
                .is_ready()
            {
                this.waiter = None;
                this.completed = true;
                return Poll::Ready(());
            }
            return Poll::Pending;
        }

        let mut wakers = Vec::new();
        let ready = {
            let mut state = this.admission.state.lock();
            if state.try_admit(this.key.clone()) {
                this.completed = true;
                return Poll::Ready(());
            }

            let waiter = state.enqueue(this.key.clone(), cx.waker());
            this.waiter = Some(waiter);
            state.admit_waiters(&mut wakers);
            state.poll_waiter(waiter, cx.waker()).is_ready()
        };
        wake_all(wakers);

        if ready {
            this.waiter = None;
            this.completed = true;
            Poll::Ready(())
        } else {
            Poll::Pending
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
    admission: &'a FairShare<K, S>,
    key: Arc<K>,
}

impl<K, S> FairSharePermit<'_, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Returns the key associated with this permit.
    pub fn key(&self) -> &K {
        &self.key
    }
}

impl<K, S> Drop for FairSharePermit<'_, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        self.admission.release(&self.key);
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
/// [`Arc`]: std::sync::Arc
#[must_use = "permits are released immediately when dropped"]
#[derive(Debug)]
pub struct OwnedFairSharePermit<K, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    admission: Arc<FairShare<K, S>>,
    key: Arc<K>,
}

impl<K, S> OwnedFairSharePermit<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Returns the key associated with this permit.
    pub fn key(&self) -> &K {
        &self.key
    }
}

impl<K, S> Drop for OwnedFairSharePermit<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        self.admission.release(&self.key);
    }
}

fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}
