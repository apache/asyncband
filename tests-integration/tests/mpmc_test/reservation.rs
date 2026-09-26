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

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use asyncband::mpmc;
use asyncband::mpmc::TryRecvError;
use asyncband::mpmc::TrySendError;
use tests_integration::WakeCounter;
use tests_integration::expect_ready;
use tests_integration::poll_once;
use tests_integration::poll_with;

#[test]
fn permits_and_messages_share_exact_capacity_without_reserving_message_order() {
    let (sender, receiver) = mpmc::bounded(3);
    let competing = receiver.clone();
    let permit: mpmc::Permit<'_, _> = sender.try_reserve().unwrap();
    sender.try_send(1).unwrap();
    expect_ready(poll_once(Box::pin(sender.send(2)).as_mut())).unwrap();
    assert_eq!(sender.try_send(3), Err(TrySendError::Full(3)));
    assert!(matches!(sender.try_reserve(), Err(TrySendError::Full(()))));

    assert_eq!(receiver.try_recv(), Ok(1));
    let second_permit = sender.try_reserve().unwrap();
    assert_eq!(sender.try_send(4), Err(TrySendError::Full(4)));
    permit.send(3).unwrap();
    assert_eq!(competing.try_recv(), Ok(2));
    second_permit.send(4).unwrap();
    assert_eq!(receiver.try_recv(), Ok(3));
    assert_eq!(competing.try_recv(), Ok(4));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn mixed_sends_and_reservations_receive_grants_in_wait_queue_order() {
    let (sender, receiver) = mpmc::bounded(1);
    let competing = receiver.clone();
    sender.try_send(0).unwrap();
    let mut first = Box::pin(sender.reserve());
    let mut second = Box::pin(sender.send(2));
    let mut third = Box::pin(sender.reserve());
    let (first_waker, first_wakes) = WakeCounter::new();
    let (second_waker, second_wakes) = WakeCounter::new();
    let (third_waker, third_wakes) = WakeCounter::new();
    assert!(poll_with(first.as_mut(), &first_waker).is_pending());
    assert!(poll_with(second.as_mut(), &second_waker).is_pending());
    assert!(poll_with(third.as_mut(), &third_waker).is_pending());

    assert_eq!(receiver.try_recv(), Ok(0));
    assert_eq!((first_wakes.count(), second_wakes.count(), third_wakes.count()), (1, 0, 0));
    assert_eq!(sender.try_send(9), Err(TrySendError::Full(9)));
    assert!(matches!(sender.try_reserve(), Err(TrySendError::Full(()))));
    let first_permit = expect_ready(poll_with(first.as_mut(), &first_waker)).unwrap();
    first_permit.send(1).unwrap();
    assert_eq!(competing.try_recv(), Ok(1));
    assert_eq!((second_wakes.count(), third_wakes.count()), (1, 0));
    expect_ready(poll_with(second.as_mut(), &second_waker)).unwrap();
    assert_eq!(receiver.try_recv(), Ok(2));
    assert_eq!(third_wakes.count(), 1);
    let third_permit = expect_ready(poll_with(third.as_mut(), &third_waker)).unwrap();
    third_permit.send(3).unwrap();
    assert_eq!(competing.try_recv(), Ok(3));
}

#[test]
fn dropping_an_unused_permit_transfers_its_one_slot_to_a_waiter() {
    let (sender, receiver) = mpmc::bounded(1);
    let held = sender.try_reserve().unwrap();
    let mut waiting = Box::pin(sender.reserve());
    let (waker, wakes) = WakeCounter::new();
    assert!(poll_with(waiting.as_mut(), &waker).is_pending());
    drop(held);
    assert_eq!(wakes.count(), 1);
    assert!(matches!(sender.try_reserve(), Err(TrySendError::Full(()))));
    assert_eq!(sender.try_send(9), Err(TrySendError::Full(9)));
    let mut barging = Box::pin(sender.send(10));
    assert!(poll_once(barging.as_mut()).is_pending());
    drop(barging);
    let granted = expect_ready(poll_with(waiting.as_mut(), &waker)).unwrap();
    drop(granted);
    let available = sender.try_reserve().unwrap();
    assert!(matches!(sender.try_reserve(), Err(TrySendError::Full(()))));
    available.send(7).unwrap();
    assert_eq!(receiver.try_recv(), Ok(7));
}

#[test]
fn cancelling_a_reservation_before_its_grant_skips_it() {
    let (sender, _receiver) = mpmc::bounded::<usize>(1);
    let held = sender.try_reserve().unwrap();
    let mut cancelled = Box::pin(sender.reserve());
    let mut waiting = Box::pin(sender.reserve());
    let (waker, wakes) = WakeCounter::new();
    assert!(poll_once(cancelled.as_mut()).is_pending());
    assert!(poll_with(waiting.as_mut(), &waker).is_pending());
    drop(cancelled);
    assert_eq!(wakes.count(), 0);
    drop(held);
    assert_eq!(wakes.count(), 1);
    let granted = expect_ready(poll_with(waiting.as_mut(), &waker)).unwrap();
    assert!(matches!(sender.try_reserve(), Err(TrySendError::Full(()))));
    drop(granted);
    drop(sender.try_reserve().unwrap());
}

#[test]
fn cancelling_a_granted_reservation_hands_capacity_to_the_next_sender() {
    let (sender, receiver) = mpmc::bounded(1);
    let held = sender.try_reserve().unwrap();
    let mut cancelled = Box::pin(sender.reserve());
    let mut waiting = Box::pin(sender.send(7));
    let (first_waker, first_wakes) = WakeCounter::new();
    let (second_waker, second_wakes) = WakeCounter::new();
    assert!(poll_with(cancelled.as_mut(), &first_waker).is_pending());
    assert!(poll_with(waiting.as_mut(), &second_waker).is_pending());
    drop(held);
    assert_eq!((first_wakes.count(), second_wakes.count()), (1, 0));
    assert_eq!(sender.try_send(9), Err(TrySendError::Full(9)));
    drop(cancelled);
    assert_eq!(second_wakes.count(), 1);
    assert!(matches!(sender.try_reserve(), Err(TrySendError::Full(()))));
    expect_ready(poll_with(waiting.as_mut(), &second_waker)).unwrap();
    assert_eq!(receiver.try_recv(), Ok(7));
}

#[test]
fn cancelling_a_granted_send_wakes_the_next_waiter_before_dropping_its_value() {
    struct ObserveDrop {
        wakes: Arc<WakeCounter>,
        seen: Arc<AtomicUsize>,
    }

    impl Drop for ObserveDrop {
        fn drop(&mut self) {
            self.seen.store(self.wakes.count(), Ordering::Relaxed);
        }
    }

    let (sender, receiver) = mpmc::bounded(1);
    let held = sender.try_reserve().unwrap();
    let (waiting_waker, waiting_wakes) = WakeCounter::new();
    let wakes_at_drop = Arc::new(AtomicUsize::new(usize::MAX));
    let mut cancelled = Box::pin(sender.send(Some(ObserveDrop {
        wakes: waiting_wakes.clone(),
        seen: wakes_at_drop.clone(),
    })));
    let mut waiting = Box::pin(sender.reserve());
    let (cancelled_waker, cancelled_wakes) = WakeCounter::new();
    assert!(poll_with(cancelled.as_mut(), &cancelled_waker).is_pending());
    assert!(poll_with(waiting.as_mut(), &waiting_waker).is_pending());
    drop(held);
    assert_eq!((cancelled_wakes.count(), waiting_wakes.count()), (1, 0));
    drop(cancelled);
    assert_eq!(wakes_at_drop.load(Ordering::Relaxed), 1);
    let permit = expect_ready(poll_with(waiting.as_mut(), &waiting_waker)).unwrap();
    permit.send(None).unwrap();
    assert!(receiver.try_recv().unwrap().is_none());
}

#[test]
fn cancelling_an_ungranted_send_drops_its_value_without_taking_capacity() {
    struct CountDrop(Arc<AtomicUsize>);

    impl Drop for CountDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let (sender, receiver) = mpmc::bounded(1);
    let held = sender.try_reserve().unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut cancelled = Box::pin(sender.send(Some(CountDrop(drops.clone()))));
    let mut waiting = Box::pin(sender.reserve());
    let (waker, wakes) = WakeCounter::new();
    assert!(poll_once(cancelled.as_mut()).is_pending());
    assert!(poll_with(waiting.as_mut(), &waker).is_pending());
    drop(cancelled);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(wakes.count(), 0);
    drop(held);
    assert_eq!(wakes.count(), 1);
    expect_ready(poll_with(waiting.as_mut(), &waker))
        .unwrap()
        .send(None)
        .unwrap();
    assert!(receiver.try_recv().unwrap().is_none());
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn last_receiver_drop_fails_pending_and_granted_reservations() {
    let (sender, receiver) = mpmc::bounded::<usize>(1);
    let competing = receiver.clone();
    sender.try_send(0).unwrap();
    let mut granted = Box::pin(sender.reserve());
    let mut pending = Box::pin(sender.reserve());
    let (first_waker, first_wakes) = WakeCounter::new();
    let (second_waker, second_wakes) = WakeCounter::new();
    assert!(poll_with(granted.as_mut(), &first_waker).is_pending());
    assert!(poll_with(pending.as_mut(), &second_waker).is_pending());
    drop(receiver);
    assert_eq!((first_wakes.count(), second_wakes.count()), (0, 0));
    assert_eq!(competing.try_recv(), Ok(0));
    assert_eq!((first_wakes.count(), second_wakes.count()), (1, 0));
    drop(competing);
    assert!(first_wakes.count() >= 1);
    assert!(second_wakes.count() >= 1);
    assert!(expect_ready(poll_with(granted.as_mut(), &first_waker)).is_err());
    assert!(expect_ready(poll_with(pending.as_mut(), &second_waker)).is_err());
    assert!(matches!(sender.try_reserve(), Err(TrySendError::Disconnected(()))));
}

#[test]
fn permit_does_not_keep_receivers_alive_and_returns_the_unsent_value() {
    let (sender, receiver) = mpmc::bounded(2);
    let competing = receiver.clone();
    let permit = sender.try_reserve().unwrap();
    drop(receiver);
    sender.try_send(String::from("still connected")).unwrap();
    drop(competing);
    assert_eq!(
        permit.send(String::from("unsent")).unwrap_err().into_inner(),
        "unsent"
    );
    assert!(matches!(sender.try_reserve(), Err(TrySendError::Disconnected(()))));
    let error = match expect_ready(poll_once(Box::pin(sender.reserve()).as_mut())) {
        Ok(_) => panic!("reservation succeeded after the last receiver dropped"),
        Err(error) => error,
    };
    assert_eq!(error.into_inner(), ());
}
