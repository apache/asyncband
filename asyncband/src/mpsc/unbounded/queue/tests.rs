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

use std::sync::atomic::Ordering;

use super::BLOCK_BYTES;
use super::Block;
use super::Consumer;
use super::Pop;
use super::Queue;
use super::Slot;

fn allocated_slots<T>(queue: &Queue<T>, consumer: &Consumer<T>) -> usize {
    let mut block = if consumer.block.is_null() {
        queue.first.load(Ordering::Relaxed)
    } else {
        consumer.block
    };
    let mut slots = 0;
    while !block.is_null() {
        // SAFETY: these tests inspect the live chain with no concurrent producers or receiver.
        unsafe {
            let block_slots = &(*block).slots;
            slots += block_slots.len();
            block = (*block).next.load(Ordering::Relaxed);
        }
    }
    slots
}

#[test]
fn storage_is_lazy_and_partial_draining_reclaims_blocks_in_fifo_order() {
    let (queue, mut consumer) = Queue::new();
    assert_eq!(allocated_slots(&queue, &consumer), 0);
    for value in 0..1024 {
        queue.push([value; 128]).unwrap();
    }
    let peak = allocated_slots(&queue, &consumer);
    for expected in 0..512 {
        assert!(matches!(consumer.pop(&queue), Pop::Value(value) if value == [expected; 128]));
    }
    assert!(allocated_slots(&queue, &consumer) <= peak * 3 / 4);
    queue.push([1024; 128]).unwrap();
    for expected in 512..=1024 {
        assert!(matches!(consumer.pop(&queue), Pop::Value(value) if value == [expected; 128]));
    }
    assert!(matches!(consumer.pop(&queue), Pop::Empty));
    assert!(allocated_slots(&queue, &consumer) * size_of::<Slot<[usize; 128]>>() <= BLOCK_BYTES);
    queue.close(&mut consumer);
    assert!(queue.first.load(Ordering::Relaxed).is_null());
}

#[test]
fn oversized_values_use_single_slot_blocks() {
    let (queue, mut consumer) = Queue::new();
    assert_eq!(allocated_slots(&queue, &consumer), 0);
    queue.push([7u8; BLOCK_BYTES + 1]).unwrap();
    assert_eq!(Block::<[u8; BLOCK_BYTES + 1]>::CAPACITY, 1);
    assert!(matches!(consumer.pop(&queue), Pop::Value(value) if value == [7; BLOCK_BYTES + 1]));
    assert_eq!(allocated_slots(&queue, &consumer), 1);
    queue.close(&mut consumer);
}

#[test]
fn positions_wrap_at_a_block_boundary() {
    let (queue, mut consumer) = Queue::new();
    queue.push(0).unwrap();
    assert!(matches!(consumer.pop(&queue), Pop::Value(0)));
    // Place an empty queue immediately before the next complete lap would wrap. Low position
    // bits still identify slot one, and crossing the sentinel must preserve the closed bit.
    let start = usize::MAX - 61;
    queue.tail.0.index.store(start, Ordering::Relaxed);
    consumer.index = start;
    for value in 1..100 {
        queue.push(value).unwrap();
    }
    for expected in 1..100 {
        assert!(matches!(consumer.pop(&queue), Pop::Value(value) if value == expected));
    }
    queue.close(&mut consumer);
    assert_eq!(queue.push(100), Err(100));
}
