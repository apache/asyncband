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

use std::pin::pin;

use asyncband::mpmc;
use asyncband::mpmc::SendError;
use asyncband::mpmc::TrySendError;
use tests_integration::WakeCounter;
use tests_integration::expect_ready;
use tests_integration::poll_with;

#[test]
fn bounded_len_and_is_empty_count_only_queued_values() {
    let (sender, receiver) = mpmc::bounded(4);
    let competing = receiver.clone();
    assert_eq!((sender.len(), receiver.len()), (0, 0));
    assert!(sender.is_empty());

    // Two slots belong to held permits and two to queued values.
    let permit = sender.try_reserve().unwrap();
    let owned = sender.clone().try_reserve_owned().unwrap();
    sender.try_send(1).unwrap();
    sender.try_send(2).unwrap();
    let mut granted = pin!(sender.clone().reserve_owned());
    let mut waiting = pin!(sender.send(5));
    let (granted_waker, _) = WakeCounter::new();
    let (waiting_waker, _) = WakeCounter::new();
    assert!(poll_with(granted.as_mut(), &granted_waker).is_pending());
    assert!(poll_with(waiting.as_mut(), &waiting_waker).is_pending());
    assert_eq!((sender.len(), competing.len()), (2, 2));
    assert!(!sender.is_empty());

    // Each receive hands the freed slot to a waiting sender instead of leaving it available.
    assert_eq!(competing.try_recv(), Ok(1));
    assert_eq!(sender.try_send(9), Err(TrySendError::Full(9)));
    assert_eq!(receiver.try_recv(), Ok(2));
    assert_eq!((sender.len(), receiver.len()), (0, 0));
    assert!(sender.is_empty());

    let granted = expect_ready(poll_with(granted.as_mut(), &granted_waker)).unwrap();
    // A granted `send` queues its value as soon as it is polled.
    expect_ready(poll_with(waiting.as_mut(), &waiting_waker)).unwrap();
    assert_eq!((sender.len(), receiver.len()), (1, 1));
    assert!(!sender.is_empty());
    granted.send(3).unwrap();
    owned.send(4).unwrap();
    permit.send(6).unwrap();
    assert_eq!((sender.len(), sender.capacity(), receiver.len()), (4, 4, 4));
    assert_eq!(sender.try_send(7), Err(TrySendError::Full(7)));
    assert_eq!(competing.try_recv(), Ok(5));
    assert_eq!((sender.len(), receiver.len()), (3, 3));
}

#[test]
fn bounded_capacity_is_fixed_on_both_endpoints() {
    let (sender, receiver) = mpmc::bounded(3);
    assert_eq!((sender.capacity(), receiver.capacity()), (3, 3));

    let permit = sender.try_reserve().unwrap();
    sender.try_send(1).unwrap();
    sender.try_send(2).unwrap();
    assert_eq!(sender.len(), 2);
    assert_eq!((sender.capacity(), receiver.capacity()), (3, 3));

    permit.send(3).unwrap();
    assert_eq!(sender.len(), sender.capacity());
    drop(receiver);
    assert_eq!(sender.capacity(), 3);
}

#[test]
fn bounded_endpoint_counts_track_handles_and_owned_reservations() {
    let (sender, receiver) = mpmc::bounded::<u8>(1);
    let sender_counts = || (sender.sender_count(), sender.receiver_count());
    let receiver_counts = || (receiver.sender_count(), receiver.receiver_count());
    assert_eq!((sender_counts(), receiver_counts()), ((1, 1), (1, 1)));

    let clone = sender.clone();
    let competing = receiver.clone();
    assert_eq!((sender_counts(), receiver_counts()), ((2, 2), (2, 2)));
    drop(competing);
    assert_eq!((sender_counts(), receiver_counts()), ((2, 1), (2, 1)));

    // A pending reservation and the permit it becomes each keep one sender alive.
    let held = sender.try_reserve().unwrap();
    let mut pending = Box::pin(sender.clone().reserve_owned());
    let (waker, _) = WakeCounter::new();
    assert!(poll_with(pending.as_mut(), &waker).is_pending());
    assert_eq!((sender_counts(), receiver_counts()), ((3, 1), (3, 1)));
    drop(held);
    assert_eq!((sender_counts(), receiver_counts()), ((3, 1), (3, 1)));
    drop(pending);
    assert_eq!((sender_counts(), receiver_counts()), ((2, 1), (2, 1)));

    let permit = sender.clone().try_reserve_owned().unwrap();
    assert_eq!((sender_counts(), receiver_counts()), ((3, 1), (3, 1)));
    drop(permit.release());
    assert_eq!((sender_counts(), receiver_counts()), ((2, 1), (2, 1)));
    drop(sender.clone().try_reserve_owned().unwrap());
    assert_eq!((sender_counts(), receiver_counts()), ((2, 1), (2, 1)));

    // A failed reservation drops its sender once the future completes.
    let held = sender.try_reserve().unwrap();
    let mut failing = Box::pin(sender.clone().reserve_owned());
    assert!(poll_with(failing.as_mut(), &waker).is_pending());
    drop(receiver);
    let failed = expect_ready(poll_with(failing.as_mut(), &waker));
    assert_eq!(failed.err().map(SendError::into_inner), Some(()));
    assert_eq!(sender_counts(), (2, 0));
    drop(failing);
    assert_eq!(sender_counts(), (2, 0));

    drop((held, clone));
    assert_eq!(sender_counts(), (1, 0));
}

#[test]
fn bounded_same_channel_tracks_the_queue_not_the_endpoint() {
    let (sender, receiver) = mpmc::bounded::<u8>(1);
    let sender_clone = sender.clone();
    let receiver_clone = receiver.clone();
    // A second queue with the same capacity stays a different channel.
    let (other_sender, other_receiver) = mpmc::bounded::<u8>(1);

    assert!(sender.same_channel(&sender_clone));
    assert!(sender_clone.same_channel(&sender));
    assert!(receiver.same_channel(&receiver_clone));
    assert!(!sender.same_channel(&other_sender));
    assert!(!receiver.same_channel(&other_receiver));

    // A sender returned by an owned permit still belongs to its queue.
    let permit = sender.clone().try_reserve_owned().unwrap();
    assert!(permit.release().same_channel(&sender_clone));

    // Endpoint liveness does not change which queue a handle belongs to.
    drop((receiver_clone, other_receiver));
    assert!(sender.same_channel(&sender_clone));
    assert!(!sender.same_channel(&other_sender));
    drop((sender, sender_clone, other_sender));
    assert!(receiver.is_disconnected());
}

#[test]
fn unbounded_len_and_is_empty_count_queued_values_on_both_endpoints() {
    let (sender, receiver) = mpmc::unbounded();
    let competing = receiver.clone();
    assert_eq!((sender.len(), receiver.len()), (0, 0));
    assert!(sender.is_empty());

    sender.send(1).unwrap();
    sender.send(2).unwrap();
    assert_eq!((sender.len(), competing.len()), (2, 2));
    assert!(!sender.is_empty());
    assert!(!receiver.is_empty());

    assert_eq!(competing.try_recv(), Ok(1));
    assert_eq!((sender.len(), receiver.len()), (1, 1));
    assert!(!sender.is_empty());

    // Queued values stay visible to the receiver after disconnection.
    drop(sender);
    assert_eq!(competing.len(), 1);
    assert!(!competing.is_empty());
    assert_eq!(competing.try_recv(), Ok(2));
    assert_eq!(receiver.len(), 0);
    assert!(receiver.is_empty());
}

#[test]
fn unbounded_endpoint_counts_track_handles() {
    let (sender, receiver) = mpmc::unbounded::<u8>();
    let sender_counts = || (sender.sender_count(), sender.receiver_count());
    let receiver_counts = || (receiver.sender_count(), receiver.receiver_count());
    assert_eq!((sender_counts(), receiver_counts()), ((1, 1), (1, 1)));

    let sender_clone = sender.clone();
    let receiver_clone = receiver.clone();
    assert_eq!((sender_counts(), receiver_counts()), ((2, 2), (2, 2)));

    drop(sender_clone);
    assert_eq!((sender_counts(), receiver_counts()), ((1, 2), (1, 2)));
    drop(receiver_clone);
    assert_eq!((sender_counts(), receiver_counts()), ((1, 1), (1, 1)));

    drop(sender);
    assert_eq!(receiver_counts(), (0, 1));
    assert!(receiver.is_disconnected());
}

#[test]
fn unbounded_same_channel_tracks_the_queue_not_the_endpoint() {
    let (sender, receiver) = mpmc::unbounded::<u8>();
    let sender_clone = sender.clone();
    let receiver_clone = receiver.clone();
    let (other_sender, other_receiver) = mpmc::unbounded::<u8>();

    assert!(sender.same_channel(&sender_clone));
    assert!(receiver.same_channel(&receiver_clone));
    assert!(!sender.same_channel(&other_sender));
    assert!(!receiver.same_channel(&other_receiver));

    // Endpoint liveness does not change which queue a handle belongs to.
    drop((receiver_clone, other_receiver));
    assert!(sender.same_channel(&sender_clone));
    assert!(!sender.same_channel(&other_sender));
    drop((sender, sender_clone, other_sender));
    assert!(receiver.is_disconnected());
}
