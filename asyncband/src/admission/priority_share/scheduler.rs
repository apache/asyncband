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

use std::cmp::Reverse;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::arena::Arena;
use crate::internal::arena::ArenaKey;
use crate::internal::mutex::Mutex;

#[derive(Debug)]
pub(super) struct Scheduler<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    state: Mutex<State<K, S>>,
}

impl<K, S> Scheduler<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub(super) fn new(admission_limits: Box<[usize]>, hash_builder: S) -> Self {
        Self {
            state: Mutex::new(State::new(admission_limits, hash_builder)),
        }
    }

    pub(super) fn available_permits(&self, priority: usize) -> usize {
        self.state.lock().available_for(priority)
    }

    pub(super) fn num_waiters(&self) -> usize {
        self.state.lock().total_num_waiters
    }

    pub(super) fn try_acquire(&self, owner: Arc<K>, priority: usize) -> bool {
        self.state.lock().try_admit(owner, priority)
    }

    pub(super) fn acquire(&self, owner: Arc<K>, priority: usize) -> Acquire<'_, K, S> {
        Acquire::new(self, owner, priority)
    }

    pub(super) fn release(&self, owner: &K) {
        let mut wakers = Vec::new();
        {
            let mut state = self.state.lock();
            state.release(owner);
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
    // Entry `p` is the shared-capacity admission limit for priority `p`.
    admission_limits: Box<[usize]>,
    total_available_permits: usize,
    total_num_waiters: usize,
    next_sequence: u64,
    owners: HashMap<Arc<K>, OwnerState, S>,
    waiters: Arena<Waiter>,
}

impl<K, S> State<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(admission_limits: Box<[usize]>, hash_builder: S) -> Self {
        let total_permits = *admission_limits
            .last()
            .expect("priority-share requires at least one admission limit");
        debug_assert!(total_permits > 0);
        Self {
            admission_limits,
            total_available_permits: total_permits,
            total_num_waiters: 0,
            next_sequence: 0,
            owners: HashMap::with_hasher(hash_builder),
            waiters: Arena::new(),
        }
    }

    fn try_admit(&mut self, owner: Arc<K>, priority: usize) -> bool {
        if !self.can_admit(priority) {
            return false;
        }

        self.admit(owner);
        true
    }

    fn enqueue(&mut self, owner: Arc<K>, priority: usize, waker: &Waker) -> ArenaKey {
        let sequence = self.next_sequence;
        self.next_sequence += 1;

        let waiter = self.waiters.insert(Waiter {
            sequence,
            waker: Some(waker.clone()),
            admitted: false,
        });
        let owner = self.owners.entry(owner).or_insert_with(OwnerState::new);
        if let Some(queue) = owner
            .queues
            .iter_mut()
            .find(|queue| queue.priority == priority)
        {
            queue.waiters.push_back(waiter);
        } else {
            owner.queues.push(PriorityQueue::new(priority, waiter));
        }
        self.total_num_waiters += 1;
        waiter
    }

    fn poll_waiter(&mut self, waiter: ArenaKey, waker: &Waker) -> Poll<()> {
        let state = self
            .waiters
            .get_mut(waiter)
            .expect("priority-share waiter is missing");

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

    fn cancel(&mut self, waiter_id: ArenaKey, owner: &K, priority: usize) {
        let waiter = self.waiters.remove(waiter_id);
        if waiter.admitted {
            self.release(owner);
            return;
        }

        let remove_owner = {
            let owner = self
                .owners
                .get_mut(owner)
                .expect("priority-share waiter owner is missing");
            let queue = owner
                .queues
                .iter()
                .position(|queue| queue.priority == priority)
                .expect("priority-share waiter queue is missing");
            let waiter = owner.queues[queue]
                .waiters
                .iter()
                .position(|candidate| *candidate == waiter_id)
                .expect("priority-share waiter is missing from its queue");
            owner.queues[queue].waiters.remove(waiter);
            if owner.queues[queue].waiters.is_empty() {
                owner.queues.swap_remove(queue);
            }
            owner.held_permits == 0 && owner.queues.is_empty()
        };

        self.total_num_waiters -= 1;
        if remove_owner {
            self.owners.remove(owner);
        }
    }

    fn admit_waiters(&mut self, wakers: &mut Vec<Waker>) {
        while self.total_available_permits > 0 && self.total_num_waiters > 0 {
            let Some((owner, priority)) = self.next_owner() else {
                return;
            };
            let owner_state = self
                .owners
                .get_mut(&owner)
                .expect("pending priority-share owner is missing");
            let queue = owner_state
                .queues
                .iter()
                .position(|queue| queue.priority == priority)
                .expect("pending priority-share queue is missing");
            let waiter = owner_state.queues[queue]
                .waiters
                .pop_front()
                .expect("pending priority-share queue has no waiters");
            if owner_state.queues[queue].waiters.is_empty() {
                owner_state.queues.swap_remove(queue);
            }
            owner_state.held_permits += 1;
            self.total_available_permits -= 1;
            self.total_num_waiters -= 1;

            let waiter = &mut self.waiters[waiter];
            waiter.admitted = true;
            if let Some(waker) = waiter.waker.take() {
                wakers.push(waker);
            }
        }
    }

    fn next_owner(&self) -> Option<(Arc<K>, usize)> {
        let mut next = None;
        for (owner, state) in &self.owners {
            for queue in &state.queues {
                if !self.can_admit(queue.priority) {
                    continue;
                }
                let waiter = queue
                    .waiters
                    .front()
                    .expect("pending priority-share queue has no waiters");
                let order = (
                    Reverse(queue.priority),
                    state.held_permits,
                    self.waiters[*waiter].sequence,
                );
                if next.as_ref().is_none_or(|(best, _, _)| order < *best) {
                    next = Some((order, owner.clone(), queue.priority));
                }
            }
        }
        next.map(|(_, owner, priority)| (owner, priority))
    }

    fn can_admit(&self, priority: usize) -> bool {
        self.available_for(priority) > 0
    }

    fn available_for(&self, priority: usize) -> usize {
        debug_assert!(priority < self.admission_limits.len());
        let total_permits = self.admission_limits[self.admission_limits.len() - 1];
        let held_permits = total_permits - self.total_available_permits;
        self.admission_limits[priority].saturating_sub(held_permits)
    }

    fn admit(&mut self, owner: Arc<K>) {
        self.owners
            .entry(owner)
            .or_insert_with(OwnerState::new)
            .held_permits += 1;
        self.total_available_permits -= 1;
    }

    fn release(&mut self, owner: &K) {
        let remove_owner = {
            let owner = self
                .owners
                .get_mut(owner)
                .expect("priority-share released a permit for an unknown owner");
            debug_assert!(owner.held_permits > 0);
            owner.held_permits -= 1;
            owner.held_permits == 0 && owner.queues.is_empty()
        };

        if remove_owner {
            self.owners.remove(owner);
        }

        self.total_available_permits += 1;
        debug_assert!(
            self.total_available_permits <= self.admission_limits[self.admission_limits.len() - 1]
        );
    }
}

#[derive(Debug)]
struct OwnerState {
    held_permits: usize,
    queues: Vec<PriorityQueue>,
}

impl OwnerState {
    fn new() -> Self {
        Self {
            held_permits: 0,
            queues: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct PriorityQueue {
    priority: usize,
    waiters: VecDeque<ArenaKey>,
}

impl PriorityQueue {
    fn new(priority: usize, waiter: ArenaKey) -> Self {
        Self {
            priority,
            waiters: VecDeque::from([waiter]),
        }
    }
}

#[derive(Debug)]
struct Waiter {
    sequence: u64,
    waker: Option<Waker>,
    admitted: bool,
}

#[derive(Debug)]
pub(super) struct Acquire<'a, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    scheduler: &'a Scheduler<K, S>,
    owner: Arc<K>,
    priority: usize,
    waiter: Option<ArenaKey>,
    completed: bool,
}

impl<'a, K, S> Acquire<'a, K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(scheduler: &'a Scheduler<K, S>, owner: Arc<K>, priority: usize) -> Self {
        Self {
            scheduler,
            owner,
            priority,
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
            let mut state = self.scheduler.state.lock();
            state.cancel(waiter, &self.owner, self.priority);
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
                .scheduler
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

        let mut state = this.scheduler.state.lock();
        if state.try_admit(this.owner.clone(), this.priority) {
            this.completed = true;
            return Poll::Ready(());
        }

        let waiter = state.enqueue(this.owner.clone(), this.priority, cx.waker());
        this.waiter = Some(waiter);
        Poll::Pending
    }
}

fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}
