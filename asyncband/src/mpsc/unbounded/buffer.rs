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

use std::collections::VecDeque;
use std::mem;

// Bound the inline storage retained by a partial batch. Boxed payloads belong to individual
// messages, not these backing allocations. Empty buffers are reused without retaining peak size.
pub const SEGMENT_BYTES: usize = 32 * 1024;

pub struct Buffer<T> {
    writable: VecDeque<T>,
    sealed: VecDeque<VecDeque<T>>,
    spare: VecDeque<T>,
}

impl<T> Buffer<T> {
    pub fn new() -> Self {
        Self {
            writable: VecDeque::new(),
            sealed: VecDeque::new(),
            spare: VecDeque::new(),
        }
    }

    fn segment_capacity() -> usize {
        if size_of::<T>() == 0 {
            return usize::MAX;
        }
        let limit = (SEGMENT_BYTES / size_of::<T>()).max(1);
        // Power-of-two limits let VecDeque grow naturally without exceeding the segment budget.
        1 << (usize::BITS - 1 - limit.leading_zeros())
    }

    pub fn push(&mut self, value: T) {
        if self.writable.len() == Self::segment_capacity() {
            let next = if self.spare.capacity() == 0 {
                VecDeque::with_capacity(Self::segment_capacity())
            } else {
                mem::take(&mut self.spare)
            };
            let sealed = mem::replace(&mut self.writable, next);
            self.sealed.push_back(sealed);
        }
        self.writable.push_back(value);
    }

    pub fn refill(&mut self, batch: &mut VecDeque<T>) {
        debug_assert!(batch.is_empty());
        if let Some(sealed) = self.sealed.pop_front() {
            // Keep one empty segment for the next producer rollover. Every other consumed
            // segment is released, so retained payload storage does not track peak occupancy.
            self.spare = mem::replace(batch, sealed);
            if self.sealed.is_empty() && self.sealed.capacity() * size_of::<VecDeque<T>>() > 1024 {
                self.sealed = VecDeque::new();
            }
        } else if !self.writable.is_empty() {
            self.spare = VecDeque::new();
            mem::swap(batch, &mut self.writable);
        }
    }
}

pub fn pop_batch<T>(batch: &mut VecDeque<T>) -> T {
    if batch.len() == 1 && batch.capacity().saturating_mul(size_of::<T>()) > SEGMENT_BYTES {
        // Retire the allocation on the last value, outside the inbox lock. Keep this as a tail
        // expression to avoid intermediate storage for large inline values.
        mem::take(batch).pop_front()
    } else {
        batch.pop_front()
    }
    .expect("receiver batch must not be empty")
}

#[cfg(test)]
#[path = "buffer_tests.rs"]
mod tests;
