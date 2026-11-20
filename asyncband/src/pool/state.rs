// Copyright 2025 FastLabs Developers
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
//
// This file contains code ported from Fastpool 1.1.1.
// The incorporated code has been modified for use in Apache Asyncband.
// Upstream sources:
// https://github.com/fast/fastpool/blob/e4c65f1ed38395abc58d68eda8bd09925c13028e/fastpool/src/bounded.rs
// https://github.com/fast/fastpool/blob/e4c65f1ed38395abc58d68eda8bd09925c13028e/fastpool/src/unbounded.rs

use std::collections::VecDeque;
use std::thread;

use crate::internal::mutex::Mutex;
use crate::pool::ManageObject;
use crate::pool::ObjectStatus;
use crate::pool::QueueStrategy;
use crate::pool::RecycleCancelledStrategy;
use crate::pool::RetainResult;

#[derive(Debug)]
pub struct ObjectState<T> {
    pub o: T,
    pub status: ObjectStatus,
}

impl<T> ObjectState<T> {
    pub fn new(o: T) -> Self {
        Self {
            o,
            status: ObjectStatus::default(),
        }
    }

    pub async fn try_recycle<M: ManageObject<Object = T>>(
        &mut self,
        manager: &M,
    ) -> Result<(), M::Error> {
        manager.is_recyclable(&mut self.o, &self.status).await?;
        self.status.mark_recycled();
        Ok(())
    }
}

#[derive(Debug)]
pub struct PoolState<T> {
    idle: VecDeque<ObjectState<T>>,
    current_size: usize,
}

impl<T> PoolState<T> {
    pub const fn new() -> Self {
        Self {
            idle: VecDeque::new(),
            current_size: 0,
        }
    }

    pub fn current_size(&self) -> usize {
        self.current_size
    }

    pub fn idle_count(&self) -> usize {
        self.idle.len()
    }

    pub fn pop(&mut self, strategy: QueueStrategy) -> Option<ObjectState<T>> {
        match strategy {
            QueueStrategy::Fifo => self.idle.pop_front(),
            QueueStrategy::Lifo => self.idle.pop_back(),
        }
    }

    pub fn add_idle(&mut self, state: ObjectState<T>) {
        self.current_size += 1;
        self.idle.push_back(state);
    }

    pub fn add_active(&mut self) {
        self.current_size += 1;
    }

    pub fn return_idle(&mut self, state: ObjectState<T>) {
        self.idle.push_back(state);
    }

    pub fn detach(&mut self) {
        self.current_size = self
            .current_size
            .checked_sub(1)
            .expect("detached object must belong to the pool");
    }

    /// Retains matching idle objects without losing any object if the predicate panics.
    pub fn retain(&mut self, mut f: impl FnMut(&mut T, ObjectStatus) -> bool) -> RetainResult<T> {
        let len = self.idle.len();
        let mut retained = 0;
        let mut current = 0;

        // Leave the deque untouched until the first object to remove is found. If the predicate
        // panics, every object remains owned by the pool.
        while current < len {
            let state = &mut self.idle[current];
            if !f(&mut state.o, state.status) {
                current += 1;
                break;
            }
            current += 1;
            retained += 1;
        }

        // Compact retained objects in place. A panic may change their order, but no object is
        // removed until every predicate call has completed.
        while current < len {
            let state = &mut self.idle[current];
            if !f(&mut state.o, state.status) {
                current += 1;
                continue;
            }

            self.idle.swap(retained, current);
            current += 1;
            retained += 1;
        }

        let removed = self
            .idle
            .drain(retained..)
            .map(|state| state.o)
            .collect::<Vec<_>>();
        self.current_size -= removed.len();

        RetainResult { retained, removed }
    }
}

pub struct PoolCore<T, M> {
    pub queue_strategy: QueueStrategy,
    pub recycle_cancelled_strategy: RecycleCancelledStrategy,
    pub manager: M,
    pub slots: Mutex<PoolState<T>>,
}

impl<T, M> PoolCore<T, M> {
    pub fn new(
        queue_strategy: QueueStrategy,
        recycle_cancelled_strategy: RecycleCancelledStrategy,
        manager: M,
    ) -> Self {
        Self {
            queue_strategy,
            recycle_cancelled_strategy,
            manager,
            slots: Mutex::new(PoolState::new()),
        }
    }
}

impl<T, M: ManageObject<Object = T>> PoolCore<T, M> {
    #[inline]
    pub fn pop(&self) -> Option<ObjectState<T>> {
        self.slots.lock().pop(self.queue_strategy)
    }

    #[inline]
    pub async fn create_active(&self) -> Result<ObjectState<T>, M::Error> {
        let object = self.manager.create().await?;
        let state = ObjectState::new(object);
        self.slots.lock().add_active();
        Ok(state)
    }

    pub fn retain(&self, f: impl FnMut(&mut T, ObjectStatus) -> bool) -> RetainResult<T> {
        let mut result = self.slots.lock().retain(f);
        for object in &mut result.removed {
            self.manager.on_detached(object);
        }
        result
    }

    #[inline]
    pub fn return_object(&self, mut state: ObjectState<T>) {
        state.status.mark_returned();
        self.restore_idle(state);
    }

    #[inline]
    pub fn restore_idle(&self, state: ObjectState<T>) {
        self.slots.lock().return_idle(state);
    }

    pub fn detach_object(&self, o: &mut T) {
        let mut slots = self.slots.lock();
        slots.detach();
        drop(slots);
        self.manager.on_detached(o);
    }
}

/// Owns an idle object while `ManageObject::is_recyclable` runs during `Pool::get`.
///
/// Passing validation extracts the `ObjectState` via `into_ready()`. Rejection or unwinding from
/// `is_recyclable` detaches the object; external cancellation applies the pool's
/// [`RecycleCancelledStrategy`].
pub struct UnreadyObject<'a, T, M: ManageObject<Object = T>> {
    state: Option<ObjectState<T>>,
    core: &'a PoolCore<T, M>,
}

impl<T, M: ManageObject<Object = T>> Drop for UnreadyObject<'_, T, M> {
    fn drop(&mut self) {
        if let Some(mut state) = self.state.take() {
            if thread::panicking() {
                self.core.detach_object(&mut state.o);
                return;
            }
            match self.core.recycle_cancelled_strategy {
                RecycleCancelledStrategy::Detach => {
                    self.core.detach_object(&mut state.o);
                }
                RecycleCancelledStrategy::ReturnToPool => {
                    self.core.restore_idle(state);
                }
            }
        }
    }
}

impl<'a, T, M: ManageObject<Object = T>> UnreadyObject<'a, T, M> {
    #[inline]
    pub fn new(core: &'a PoolCore<T, M>, state: ObjectState<T>) -> Self {
        Self {
            state: Some(state),
            core,
        }
    }

    #[inline]
    pub async fn recycle(&mut self) -> Result<(), M::Error> {
        // INVARIANT: `state` is `Some` until this object becomes ready, detaches, or is dropped.
        self.state
            .as_mut()
            .unwrap()
            .try_recycle(&self.core.manager)
            .await
    }

    #[inline]
    pub fn into_ready(mut self) -> ObjectState<T> {
        // INVARIANT: `state` is `Some` until this object becomes ready, detaches, or is dropped.
        self.state.take().unwrap()
    }

    pub fn detach(&mut self) {
        if let Some(mut state) = self.state.take() {
            self.core.detach_object(&mut state.o);
        }
    }
}
