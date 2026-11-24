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
use asyncband::mpmc::OwnedPermit;
use asyncband::mpmc::RecvError;
use asyncband::mpmc::SendError;
use asyncband::mpmc::TryRecvError;
use asyncband::mpmc::TrySendError;
use tests_integration::WakeCounter;
use tests_integration::expect_ready;
use tests_integration::poll_once;
use tests_integration::poll_with;

/// Drains queued values, then expects the next receive to report disconnection.
fn assert_reports_disconnection(receiver: &mpmc::BoundedReceiver<usize>) {
    while receiver.try_recv().is_ok() {}
    assert_eq!(
        expect_ready(poll_once(pin!(receiver.recv()))),
        Err(RecvError::Disconnected)
    );
}

#[derive(Debug)]
enum Settlement {
    Release,
    Drop,
}

#[test]
fn reserve_owned_holds_one_slot_and_send_returns_the_sender() {
    let (sender, receiver) = mpmc::bounded(1);
    let permit: OwnedPermit<usize> =
        expect_ready(poll_once(pin!(sender.clone().reserve_owned()))).unwrap();
    assert!(format!("{permit:?}").contains("OwnedPermit"));
    assert_eq!(sender.try_send(0), Err(TrySendError::Full(0)));
    assert!(matches!(sender.try_reserve(), Err(TrySendError::Full(()))));
    assert!(receiver.is_empty());

    let returned = permit.send(1).unwrap();
    assert!(returned.same_channel(&sender));
    assert_eq!(receiver.try_recv(), Ok(1));
}

#[test]
fn reserve_owned_waits_for_capacity_without_disconnecting_receivers() {
    let (sender, receiver) = mpmc::bounded(1);
    sender.try_send(0).unwrap();
    let mut waiting = pin!(sender.reserve_owned());
    let (waker, wakes) = WakeCounter::new();
    assert!(poll_with(waiting.as_mut(), &waker).is_pending());
    assert!(!receiver.is_disconnected());

    assert_eq!(receiver.try_recv(), Ok(0));
    assert_eq!(wakes.count(), 1);
    let permit = expect_ready(poll_with(waiting.as_mut(), &waker)).unwrap();
    assert!(!receiver.is_disconnected());
    assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));

    let sender = permit.send(1).unwrap();
    assert_eq!(receiver.try_recv(), Ok(1));
    assert!(!receiver.is_disconnected());
    drop(sender);
    assert_reports_disconnection(&receiver);
}

#[test]
fn a_pending_owned_reservation_wakes_the_waker_registered_last() {
    let (sender, receiver) = mpmc::bounded(1);
    let held = sender.try_reserve().unwrap();
    let mut waiting = pin!(sender.clone().reserve_owned());
    let (first_waker, first_wakes) = WakeCounter::new();
    let (second_waker, second_wakes) = WakeCounter::new();
    assert!(poll_with(waiting.as_mut(), &first_waker).is_pending());
    assert!(poll_with(waiting.as_mut(), &second_waker).is_pending());

    drop(held);
    assert_eq!((first_wakes.count(), second_wakes.count()), (0, 1));
    expect_ready(poll_with(waiting.as_mut(), &second_waker))
        .unwrap()
        .send(1)
        .unwrap();
    assert_eq!(receiver.try_recv(), Ok(1));
}

#[test]
fn try_reserve_owned_returns_the_sender_when_full_or_disconnected() {
    let (sender, receiver) = mpmc::bounded(1);
    sender.try_send(1).unwrap();
    let sender = match sender.try_reserve_owned() {
        Ok(_) => panic!("capacity should be unavailable"),
        Err(TrySendError::Full(sender)) => sender,
        Err(TrySendError::Disconnected(_)) => panic!("a receiver remains"),
    };
    assert_eq!(receiver.sender_count(), 1);
    assert_eq!(receiver.try_recv(), Ok(1));

    let permit = sender.try_reserve_owned().unwrap();
    assert_eq!(receiver.sender_count(), 1);
    let sender = permit.send(2).unwrap();
    assert_eq!(sender.try_send(3), Err(TrySendError::Full(3)));
    assert_eq!(receiver.try_recv(), Ok(2));

    let (sender, receiver) = mpmc::bounded::<usize>(1);
    drop(receiver);
    let sender = match sender.try_reserve_owned() {
        Err(TrySendError::Disconnected(sender)) => sender,
        Ok(_) => panic!("reservation succeeded after the last receiver dropped"),
        Err(TrySendError::Full(_)) => panic!("capacity should be available"),
    };
    assert!(sender.is_disconnected());
    assert_eq!(sender.sender_count(), 1);
}

#[test]
fn released_or_dropped_owned_permits_pass_their_slot_to_the_next_waiter() {
    for settlement in [Settlement::Release, Settlement::Drop] {
        let (sender, receiver) = mpmc::bounded(1);
        let permit = sender.clone().try_reserve_owned().unwrap();
        let mut waiting = pin!(sender.send(1));
        let (waker, wakes) = WakeCounter::new();
        assert!(poll_with(waiting.as_mut(), &waker).is_pending());

        let returned = match settlement {
            Settlement::Release => Some(permit.release()),
            Settlement::Drop => {
                drop(permit);
                None
            }
        };
        assert_eq!(wakes.count(), 1, "{settlement:?}");
        // The freed slot is reserved for the woken waiter instead of being queued or reassigned.
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
        expect_ready(poll_once(waiting.as_mut())).unwrap();
        assert_eq!(receiver.try_recv(), Ok(1));
        assert_eq!(receiver.sender_count(), 1 + usize::from(returned.is_some()));
    }
}

#[test]
fn held_owned_permits_delay_disconnection_until_they_settle() {
    fn settles_as_last_sender(delivers: bool, settle: impl FnOnce(OwnedPermit<usize>)) {
        let (sender, receiver) = mpmc::bounded(1);
        let mut recv = pin!(receiver.recv());
        let (waker, wakes) = WakeCounter::new();
        assert!(poll_with(recv.as_mut(), &waker).is_pending());

        let permit = sender.try_reserve_owned().unwrap();
        assert!(!receiver.is_disconnected());
        assert_eq!((receiver.sender_count(), wakes.count()), (1, 0));
        settle(permit);
        assert_eq!((receiver.sender_count(), wakes.count()), (0, 1));
        let expected = if delivers {
            Ok(1)
        } else {
            Err(RecvError::Disconnected)
        };
        assert_eq!(expect_ready(poll_with(recv.as_mut(), &waker)), expected);
        assert_reports_disconnection(&receiver);
    }

    settles_as_last_sender(false, drop);
    settles_as_last_sender(false, |permit| drop(permit.release()));
    settles_as_last_sender(true, |permit| drop(permit.send(1).unwrap()));
}

#[test]
fn pending_owned_reservations_delay_disconnection_until_cancelled() {
    for grant in [false, true] {
        let (sender, receiver) = mpmc::bounded(1);
        sender.try_send(0).unwrap();
        let mut reserve = Box::pin(sender.reserve_owned());
        let (waker, wakes) = WakeCounter::new();
        assert!(poll_with(reserve.as_mut(), &waker).is_pending());

        let mut recv = pin!(receiver.recv());
        let (recv_waker, recv_wakes) = WakeCounter::new();
        if grant {
            assert_eq!(receiver.try_recv(), Ok(0));
            assert_eq!(wakes.count(), 1);
            assert!(poll_with(recv.as_mut(), &recv_waker).is_pending());
        }
        assert!(!receiver.is_disconnected());
        assert_eq!(receiver.sender_count(), 1);
        assert_eq!(recv_wakes.count(), 0);

        drop(reserve);
        assert!(receiver.is_disconnected());
        assert_eq!(receiver.sender_count(), 0);
        if grant {
            assert_eq!(recv_wakes.count(), 1);
            assert_eq!(
                expect_ready(poll_with(recv.as_mut(), &recv_waker)),
                Err(RecvError::Disconnected)
            );
        }
        assert_reports_disconnection(&receiver);
    }
}

#[test]
fn dropping_an_unpolled_owned_reservation_as_last_sender_wakes_waiting_receiver() {
    let (sender, receiver) = mpmc::bounded::<usize>(1);
    let extra = sender.clone().reserve_owned();
    assert_eq!(receiver.sender_count(), 2);
    // An unpolled reservation owns its sender without taking capacity before its first poll.
    let permit = sender.try_reserve().unwrap();
    drop(extra);
    assert_eq!(receiver.sender_count(), 1);
    drop(permit);

    let mut recv = pin!(receiver.recv());
    let (recv_waker, recv_wakes) = WakeCounter::new();
    assert!(poll_with(recv.as_mut(), &recv_waker).is_pending());

    let unpolled = sender.reserve_owned();
    assert!(!receiver.is_disconnected());
    assert_eq!(receiver.sender_count(), 1);
    assert_eq!(recv_wakes.count(), 0);

    drop(unpolled);
    assert!(receiver.is_disconnected());
    assert_eq!(receiver.sender_count(), 0);
    assert_eq!(recv_wakes.count(), 1);
    assert_eq!(
        expect_ready(poll_with(recv.as_mut(), &recv_waker)),
        Err(RecvError::Disconnected)
    );
}

#[test]
fn owned_permit_send_after_disconnection_returns_the_value_and_drops_the_sender() {
    let (sender, receiver) = mpmc::bounded::<String>(1);
    let permit = sender.clone().try_reserve_owned().unwrap();
    let competing = receiver.clone();
    drop(receiver);
    drop(competing);
    assert!(sender.is_disconnected());

    let error = permit.send(String::from("unsent")).unwrap_err();
    assert_eq!(error.into_inner(), "unsent");
    assert_eq!((sender.sender_count(), sender.receiver_count()), (1, 0));
}

#[test]
fn last_receiver_drop_fails_pending_and_granted_owned_reservations() {
    let (sender, receiver) = mpmc::bounded::<usize>(1);
    let probe = sender.clone();
    let competing = receiver.clone();
    sender.try_send(0).unwrap();
    let mut granted = Box::pin(sender.clone().reserve_owned());
    let mut pending = Box::pin(sender.reserve_owned());
    let (first_waker, first_wakes) = WakeCounter::new();
    let (second_waker, second_wakes) = WakeCounter::new();
    assert!(poll_with(granted.as_mut(), &first_waker).is_pending());
    assert!(poll_with(pending.as_mut(), &second_waker).is_pending());
    assert_eq!(probe.sender_count(), 3);

    drop(receiver);
    assert_eq!((first_wakes.count(), second_wakes.count()), (0, 0));
    assert_eq!(competing.try_recv(), Ok(0));
    assert_eq!((first_wakes.count(), second_wakes.count()), (1, 0));
    drop(competing);
    assert!(first_wakes.count() >= 1);
    assert!(second_wakes.count() >= 1);

    // Completing with an error drops the consumed sender, so only the probe remains.
    for failed in [
        expect_ready(poll_with(granted.as_mut(), &first_waker)),
        expect_ready(poll_with(pending.as_mut(), &second_waker)),
    ] {
        assert_eq!(failed.err().map(SendError::into_inner), Some(()));
    }
    assert_eq!(probe.sender_count(), 1);
    drop((granted, pending));
    assert_eq!(probe.sender_count(), 1);

    // Once disconnected, a reservation fails and drops its sender on the first poll.
    let failed = expect_ready(poll_once(pin!(probe.clone().reserve_owned())));
    assert_eq!(failed.err().map(SendError::into_inner), Some(()));
    assert_eq!(probe.sender_count(), 1);
}

#[test]
fn cancelling_an_owned_reservation_before_its_grant_skips_it() {
    let (sender, receiver) = mpmc::bounded::<usize>(1);
    let held = sender.try_reserve().unwrap();
    let mut cancelled = Box::pin(sender.clone().reserve_owned());
    let mut waiting = Box::pin(sender.reserve());
    let (waker, wakes) = WakeCounter::new();
    assert!(poll_once(cancelled.as_mut()).is_pending());
    assert!(poll_with(waiting.as_mut(), &waker).is_pending());

    drop(cancelled);
    assert_eq!(receiver.sender_count(), 1);
    assert_eq!(wakes.count(), 0);
    drop(held);
    assert_eq!(wakes.count(), 1);
    expect_ready(poll_with(waiting.as_mut(), &waker))
        .unwrap()
        .send(1)
        .unwrap();
    assert_eq!(receiver.try_recv(), Ok(1));
    drop(waiting);
    drop(sender);
    assert!(receiver.is_disconnected());
}

#[test]
fn cancelling_a_granted_owned_reservation_hands_capacity_to_the_next_sender() {
    let (sender, receiver) = mpmc::bounded(1);
    let held = sender.try_reserve().unwrap();
    let mut cancelled = Box::pin(sender.clone().reserve_owned());
    let mut waiting = pin!(sender.send(7));
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
    assert_eq!(receiver.sender_count(), 1);
}

#[test]
fn cancelling_a_granted_owned_reservation_restores_available_capacity() {
    let (sender, receiver) = mpmc::bounded(1);
    let held = sender.try_reserve().unwrap();
    let mut cancelled = Box::pin(sender.clone().reserve_owned());
    let (waker, wakes) = WakeCounter::new();
    assert!(poll_with(cancelled.as_mut(), &waker).is_pending());

    drop(held);
    assert_eq!(wakes.count(), 1);
    assert_eq!(sender.try_send(9), Err(TrySendError::Full(9)));
    drop(cancelled);
    assert_eq!(receiver.sender_count(), 1);

    let available = sender.try_reserve().unwrap();
    assert!(matches!(sender.try_reserve(), Err(TrySendError::Full(()))));
    available.send(1).unwrap();
    assert_eq!(receiver.try_recv(), Ok(1));
}

#[test]
fn owned_reservations_share_wait_queue_order_with_sends_and_reservations() {
    let (sender, receiver) = mpmc::bounded(1);
    sender.try_send(0).unwrap();
    let mut reservation = pin!(sender.reserve());
    let mut owned = pin!(sender.clone().reserve_owned());
    let mut first_send = pin!(sender.send(2));
    let mut second_owned = pin!(sender.clone().reserve_owned());
    let mut second_send = pin!(sender.send(4));
    let (reservation_waker, reservation_wakes) = WakeCounter::new();
    let (owned_waker, owned_wakes) = WakeCounter::new();
    let (first_waker, first_wakes) = WakeCounter::new();
    let (second_owned_waker, second_owned_wakes) = WakeCounter::new();
    let (second_waker, second_wakes) = WakeCounter::new();
    assert!(poll_with(reservation.as_mut(), &reservation_waker).is_pending());
    assert!(poll_with(owned.as_mut(), &owned_waker).is_pending());
    assert!(poll_with(first_send.as_mut(), &first_waker).is_pending());
    assert!(poll_with(second_owned.as_mut(), &second_owned_waker).is_pending());
    assert!(poll_with(second_send.as_mut(), &second_waker).is_pending());
    assert_eq!(receiver.sender_count(), 3);

    // Each receive frees one slot, which is granted to exactly the next waiter in line and is
    // reserved for it instead of staying available to other sends.
    let wakes = || {
        (
            reservation_wakes.count(),
            owned_wakes.count(),
            first_wakes.count(),
            second_owned_wakes.count(),
            second_wakes.count(),
        )
    };

    assert_eq!(receiver.try_recv(), Ok(0));
    assert_eq!(wakes(), (1, 0, 0, 0, 0));
    assert_eq!(sender.try_send(9), Err(TrySendError::Full(9)));

    expect_ready(poll_with(reservation.as_mut(), &reservation_waker))
        .unwrap()
        .send(1)
        .unwrap();
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(wakes(), (1, 1, 0, 0, 0));
    assert_eq!(sender.try_send(9), Err(TrySendError::Full(9)));

    let permit = expect_ready(poll_with(owned.as_mut(), &owned_waker)).unwrap();
    permit.send(3).unwrap();
    assert_eq!(receiver.try_recv(), Ok(3));
    assert_eq!(wakes(), (1, 1, 1, 0, 0));
    assert_eq!(sender.try_send(9), Err(TrySendError::Full(9)));

    expect_ready(poll_with(first_send.as_mut(), &first_waker)).unwrap();
    assert_eq!(receiver.try_recv(), Ok(2));
    assert_eq!(wakes(), (1, 1, 1, 1, 0));
    assert_eq!(sender.try_send(9), Err(TrySendError::Full(9)));

    let permit = expect_ready(poll_with(second_owned.as_mut(), &second_owned_waker)).unwrap();
    permit.send(5).unwrap();
    assert_eq!(receiver.try_recv(), Ok(5));
    assert_eq!(wakes(), (1, 1, 1, 1, 1));

    expect_ready(poll_with(second_send.as_mut(), &second_waker)).unwrap();
    assert_eq!(receiver.try_recv(), Ok(4));
    assert_eq!(receiver.sender_count(), 1);
    assert!(receiver.is_empty());
}
