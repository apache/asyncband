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
use asyncband::mpmc::TrySendError;
use tests_integration::WakeCounter;
use tests_integration::assert_completes_without_deadlock;
use tests_integration::expect_ready;
use tests_integration::poll_once;
use tests_integration::poll_with;
use tests_integration::waker_on_drop;
use tests_integration::waker_on_wake;

#[test]
fn replacing_a_send_waker_allows_its_destructor_to_receive() {
    assert_completes_without_deadlock(|| {
        let (sender, receiver) = mpmc::bounded(1);
        sender.try_send(0).unwrap();
        let reentrant = receiver.clone();
        let first = waker_on_drop(move || assert_eq!(reentrant.try_recv(), Ok(0)));
        let (second, second_wakes) = WakeCounter::new();
        let mut send = pin!(sender.send(1));

        assert!(poll_with(send.as_mut(), &first).is_pending());
        drop(first);
        // Replacing the stored waker frees capacity and wakes the newly registered task.
        assert!(poll_with(send.as_mut(), &second).is_pending());
        assert_eq!(second_wakes.count(), 1);
        expect_ready(poll_with(send.as_mut(), &second)).unwrap();
        assert_eq!(receiver.try_recv(), Ok(1));
    });
}

#[test]
fn replacing_a_receive_waker_allows_its_destructor_to_send() {
    assert_completes_without_deadlock(|| {
        let (sender, receiver) = mpmc::unbounded();
        let reentrant = sender.clone();
        let first = waker_on_drop(move || reentrant.send(1).unwrap());
        let (second, second_wakes) = WakeCounter::new();
        let mut recv = pin!(receiver.recv());

        assert!(poll_with(recv.as_mut(), &first).is_pending());
        drop(first);
        // Replacing the stored waker releases its last reference, whose destructor sends.
        assert!(poll_with(recv.as_mut(), &second).is_pending());
        assert_eq!(second_wakes.count(), 1);
        assert_eq!(expect_ready(poll_with(recv.as_mut(), &second)), Ok(1));
        drop(sender);
    });
}

#[test]
fn replacing_an_owned_reservation_waker_allows_its_destructor_to_receive() {
    assert_completes_without_deadlock(|| {
        let (sender, receiver) = mpmc::bounded(1);
        sender.try_send(0).unwrap();
        let reentrant = receiver.clone();
        let first = waker_on_drop(move || assert_eq!(reentrant.try_recv(), Ok(0)));
        let (second, second_wakes) = WakeCounter::new();
        let mut reserve = pin!(sender.reserve_owned());

        assert!(poll_with(reserve.as_mut(), &first).is_pending());
        drop(first);
        // Replacing the stored waker frees capacity, which is granted back to this reservation.
        assert!(poll_with(reserve.as_mut(), &second).is_pending());
        assert_eq!(second_wakes.count(), 1);
        expect_ready(poll_with(reserve.as_mut(), &second))
            .unwrap()
            .send(1)
            .unwrap();
        assert_eq!(receiver.try_recv(), Ok(1));
    });
}

#[test]
fn owned_capacity_reaches_the_next_waiter_outside_the_queue_lock() {
    // Dropping a permit, releasing one, and cancelling a granted reservation all hand the
    // reserved slot to the next waiting sender, whose wake callback reenters the queue.
    fn hands_over_without_deadlock(relinquish: impl FnOnce(OwnedPermit<usize>) + Send + 'static) {
        assert_completes_without_deadlock(|| {
            let (sender, receiver) = mpmc::bounded(1);
            let permit = sender.clone().try_reserve_owned().unwrap();
            let probe = sender.clone();
            let mut waiting = pin!(sender.send(1));
            let next = waker_on_wake(move || {
                assert_eq!(probe.receiver_count(), 1);
                // The freed slot is reserved for the woken waiter, not available to other sends.
                assert_eq!(probe.try_send(9), Err(TrySendError::Full(9)));
            });
            assert!(poll_with(waiting.as_mut(), &next).is_pending());

            relinquish(permit);
            expect_ready(poll_once(waiting.as_mut())).unwrap();
            assert_eq!(receiver.try_recv(), Ok(1));
        });
    }

    hands_over_without_deadlock(drop);
    hands_over_without_deadlock(|permit| drop(permit.release()));

    assert_completes_without_deadlock(|| {
        let (sender, receiver) = mpmc::bounded(1);
        let held = sender.try_reserve().unwrap();
        let probe = sender.clone();
        let mut cancelled = Box::pin(sender.clone().reserve_owned());
        let mut waiting = pin!(sender.send(1));
        let next = waker_on_wake(move || {
            assert_eq!(probe.receiver_count(), 1);
            assert_eq!(probe.try_send(9), Err(TrySendError::Full(9)));
        });
        assert!(poll_once(cancelled.as_mut()).is_pending());
        assert!(poll_with(waiting.as_mut(), &next).is_pending());

        drop(held);
        drop(cancelled);
        expect_ready(poll_once(waiting.as_mut())).unwrap();
        assert_eq!(receiver.try_recv(), Ok(1));
    });
}

#[test]
fn dropping_the_last_owned_sender_notifies_receivers_outside_the_queue_lock() {
    assert_completes_without_deadlock(|| {
        let (sender, receiver) = mpmc::bounded::<usize>(1);
        let probe = receiver.clone();
        let notified = waker_on_wake(move || {
            assert_eq!(probe.sender_count(), 0);
            assert!(probe.is_disconnected());
        });
        let mut recv = pin!(receiver.recv());
        assert!(poll_with(recv.as_mut(), &notified).is_pending());

        drop(sender.try_reserve_owned().unwrap());
        assert_eq!(
            expect_ready(poll_once(recv.as_mut())),
            Err(RecvError::Disconnected)
        );
    });
}

#[test]
fn buffered_value_destructors_reenter_the_queue_while_owned_permits_fail() {
    struct ReenterOnDrop {
        probe: mpmc::BoundedSender<ReenterOnDrop>,
    }

    impl Drop for ReenterOnDrop {
        fn drop(&mut self) {
            assert_eq!(self.probe.receiver_count(), 0);
            assert!(self.probe.is_disconnected());
        }
    }

    assert_completes_without_deadlock(|| {
        let (sender, receiver) = mpmc::bounded(2);
        let permit = sender.clone().try_reserve_owned().unwrap();
        sender
            .try_send(ReenterOnDrop {
                probe: sender.clone(),
            })
            .unwrap();

        // Dropping the last receiver discards queued values outside the queue lock, so their
        // destructors may query the queue that is discarding them.
        drop(receiver);
        let unsent = permit
            .send(ReenterOnDrop {
                probe: sender.clone(),
            })
            .unwrap_err()
            .into_inner();
        // The unsent value reaches the caller intact and drops only when the caller releases it.
        drop(unsent);
        assert_eq!(sender.sender_count(), 1);
    });
}
