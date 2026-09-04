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
// Portions ported from Fastpool 1.1.1.
// Modified by the Apache Software Foundation.
// See the project LICENSE file for the exact upstream revision and source path.

use std::collections::VecDeque;

use crate::pool::ObjectStatus;
use crate::pool::QueueStrategy;
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

        let removed = if current == retained {
            vec![]
        } else {
            self.idle
                .split_off(retained)
                .into_iter()
                .map(|state| state.o)
                .collect::<Vec<_>>()
        };
        self.current_size -= removed.len();

        RetainResult { retained, removed }
    }
}
