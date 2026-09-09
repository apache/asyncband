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
        buffer.refill(batch);
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
    buffer.refill(&mut batch);
    assert!(batch.is_empty());
}

#[test]
fn small_batches_are_reused_on_refill() {
    let mut buffer = Buffer::new();
    let mut batch = VecDeque::new();
    for value in 0..32usize {
        buffer.push(value);
    }
    assert_eq!(receive(&mut buffer, &mut batch), 0);
    let capacity = batch.capacity();
    for value in 1..32 {
        assert_eq!(receive(&mut buffer, &mut batch), value);
    }
    assert_eq!(batch.capacity(), capacity);
    buffer.push(32);
    assert_eq!(receive(&mut buffer, &mut batch), 32);
    assert_eq!(buffer.writable.capacity(), capacity);
}

#[test]
fn oversized_inline_values_release_the_allocation_on_the_last_receive() {
    let mut buffer = Buffer::new();
    let mut batch = VecDeque::new();
    buffer.push([7u8; SEGMENT_BYTES + 1]);
    assert_eq!(receive(&mut buffer, &mut batch), [7u8; SEGMENT_BYTES + 1]);
    assert_eq!(allocated_bytes(&buffer, &batch), 0);
}
