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

use super::*;

// These tests stay next to the implementation because they inspect private state.

#[tokio::test]
async fn sequence_number_wraparound() {
    let (tx, mut rx) = channel(4);
    let mut rx2 = rx.clone();

    let boundary = u64::MAX - 2;
    tx.shared.tail.store(boundary, Ordering::Release);
    rx.head = boundary;

    tx.send(1);
    assert_eq!(rx.recv().await, Ok(1));

    for value in 2..=8 {
        tx.send(value);
    }

    assert_eq!(rx.recv().await, Err(RecvError::Lagged(3)));
    for value in 5..=8 {
        assert_eq!(rx.recv().await, Ok(value));
    }

    assert_eq!(rx2.recv().await, Err(RecvError::Lagged(1)));
    for value in 5..=8 {
        assert_eq!(rx2.recv().await, Ok(value));
    }
}

#[tokio::test]
async fn sequence_number_wraparound_exactly_overwritten() {
    let (tx, mut rx) = channel(4);
    let mut rx2 = rx.clone();

    let boundary = u64::MAX - 2;
    tx.shared.tail.store(boundary, Ordering::Release);
    rx.head = boundary;

    tx.send(1);
    assert_eq!(rx.recv().await, Ok(1));

    for value in 2..=5 {
        tx.send(value);
    }

    assert_eq!(rx.recv().await, Ok(2));
    // Wrapping the complete u64 space creates an ABA ambiguity. At 10^9 messages per second this
    // takes roughly 584 years, so the implementation accepts it in favor of cheaper arithmetic.
    assert_eq!(rx2.recv().await, Ok(4));
}

#[test]
fn capacity_is_rounded_to_a_power_of_two() {
    let (tx, _) = channel::<()>(3);
    assert_eq!(tx.shared.capacity, 4);
    assert_eq!(tx.shared.mask, 3);

    let (tx, _) = channel::<()>(4);
    assert_eq!(tx.shared.capacity, 4);
    assert_eq!(tx.shared.mask, 3);

    let (tx, _) = channel::<()>(5);
    assert_eq!(tx.shared.capacity, 8);
    assert_eq!(tx.shared.mask, 7);
}
