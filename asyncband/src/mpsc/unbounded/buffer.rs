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
}

impl<T> Buffer<T> {
    pub fn new() -> Self {
        Self {
            writable: VecDeque::new(),
            sealed: VecDeque::new(),
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
            let next = VecDeque::with_capacity(Self::segment_capacity());
            let sealed = mem::replace(&mut self.writable, next);
            self.sealed.push_back(sealed);
        }
        self.writable.push_back(value);
    }

    /// Returns retired allocations so the receiver can release them after unlocking.
    #[must_use = "retired allocations must be dropped after releasing the shared lock"]
    pub fn refill(
        &mut self,
        batch: &mut VecDeque<T>,
    ) -> Option<(VecDeque<T>, VecDeque<VecDeque<T>>)> {
        debug_assert!(batch.is_empty());
        if let Some(sealed) = self.sealed.pop_front() {
            let retired_batch = mem::replace(batch, sealed);
            let retired_sealed = if self.sealed.is_empty()
                && self.sealed.capacity() * size_of::<VecDeque<T>>() > 1024
            {
                mem::take(&mut self.sealed)
            } else {
                VecDeque::new()
            };
            return Some((retired_batch, retired_sealed));
        }
        if !self.writable.is_empty() {
            mem::swap(batch, &mut self.writable);
        }
        None
    }
}

pub fn pop_batch<T>(batch: &mut VecDeque<T>) -> T {
    if batch.len() == 1 && batch.capacity().saturating_mul(size_of::<T>()) > SEGMENT_BYTES {
        // Retire the allocation on the last value, outside the shared lock. Keep this as a tail
        // expression to avoid intermediate storage for large inline values.
        mem::take(batch).pop_front()
    } else {
        batch.pop_front()
    }
    .expect("receiver batch must not be empty")
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::Buffer;
    use super::SEGMENT_BYTES;
    use super::pop_batch;

    fn allocated_bytes<T>(buffer: &Buffer<T>, batch: &VecDeque<T>) -> usize {
        let slots = batch.capacity()
            + buffer.writable.capacity()
            + buffer.sealed.iter().map(VecDeque::capacity).sum::<usize>();
        slots * size_of::<T>()
    }

    fn receive<T>(buffer: &mut Buffer<T>, batch: &mut VecDeque<T>) -> T {
        if batch.is_empty() {
            drop(buffer.refill(batch));
        }
        pop_batch(batch)
    }

    #[test]
    fn a_partial_drain_reclaims_segments_and_preserves_new_sends() {
        let mut buffer = Buffer::new();
        let mut batch = VecDeque::new();
        for value in 0..1024usize {
            buffer.push([value; 128]);
        }
        let peak = allocated_bytes(&buffer, &batch);
        for value in 0..512 {
            assert_eq!(receive(&mut buffer, &mut batch), [value; 128]);
        }
        assert!(allocated_bytes(&buffer, &batch) <= peak * 3 / 4);
        // This value must stay behind both the current batch and the sealed segments.
        buffer.push([1024; 128]);
        for value in 512..=1024 {
            assert_eq!(receive(&mut buffer, &mut batch), [value; 128]);
        }
        assert!(allocated_bytes(&buffer, &batch) <= 2 * SEGMENT_BYTES);
        drop(buffer.refill(&mut batch));
        assert!(batch.is_empty());
    }

    #[test]
    fn oversized_inline_values_release_the_allocation_on_the_last_receive() {
        let mut buffer = Buffer::new();
        let mut batch = VecDeque::new();
        buffer.push([7u8; SEGMENT_BYTES + 1]);
        assert_eq!(receive(&mut buffer, &mut batch), [7u8; SEGMENT_BYTES + 1]);
        assert_eq!(allocated_bytes(&buffer, &batch), 0);
    }
}
