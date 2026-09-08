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

use asyncband::mpsc;
use tests_integration::poll_once;

use super::support::WakeCounter;
use super::support::expect_ready;
use super::support::poll_with;

#[test]
fn bounded_wakes_blocked_senders_one_at_a_time() {
    let (tx, mut rx) = mpsc::bounded(1);
    tx.try_send(0).unwrap();

    let first_tx = tx.clone();
    let second_tx = tx.clone();
    let mut first = Box::pin(first_tx.send(1));
    let mut second = Box::pin(second_tx.send(2));

    let (first_waker, first_wakes) = WakeCounter::new();
    let (second_waker, second_wakes) = WakeCounter::new();
    assert!(poll_with(first.as_mut(), &first_waker).is_pending());
    assert!(poll_with(second.as_mut(), &second_waker).is_pending());

    assert_eq!(rx.try_recv(), Ok(0));
    assert_eq!(first_wakes.count(), 1);
    assert_eq!(second_wakes.count(), 0);
    assert_eq!(expect_ready(poll_once(first.as_mut())), Ok(()));
    assert!(poll_with(second.as_mut(), &second_waker).is_pending());

    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(second_wakes.count(), 1);
    assert_eq!(expect_ready(poll_once(second.as_mut())), Ok(()));
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn cancelling_a_sender_preserves_capacity_and_notifies_the_next_waiter() {
    for cancel_after_notification in [false, true] {
        let (tx, mut rx) = mpsc::bounded(1);
        tx.try_send(0).unwrap();
        let mut first = Box::pin(tx.send(1));
        let mut second = Box::pin(tx.send(2));
        let (first_waker, first_wakes) = WakeCounter::new();
        let (second_waker, second_wakes) = WakeCounter::new();
        assert!(poll_with(first.as_mut(), &first_waker).is_pending());
        assert!(poll_with(second.as_mut(), &second_waker).is_pending());

        if cancel_after_notification {
            assert_eq!(rx.try_recv(), Ok(0));
            assert_eq!(first_wakes.count(), 1);
            assert_eq!(second_wakes.count(), 0);
        }
        drop(first);
        if !cancel_after_notification {
            assert_eq!(first_wakes.count(), 0);
            assert_eq!(second_wakes.count(), 0);
            assert_eq!(rx.try_recv(), Ok(0));
        }
        assert_eq!(second_wakes.count(), 1);
        assert_eq!(
            expect_ready(poll_with(second.as_mut(), &second_waker)),
            Ok(())
        );
        assert_eq!(rx.try_recv(), Ok(2));
    }
}

#[test]
fn bounded_receiver_drop_returns_values_to_all_blocked_senders() {
    let (tx, rx) = mpsc::bounded(1);
    tx.try_send(0).unwrap();

    let first_tx = tx.clone();
    let second_tx = tx.clone();
    let mut first = Box::pin(first_tx.send(1));
    let mut second = Box::pin(second_tx.send(2));

    let (first_waker, first_wakes) = WakeCounter::new();
    let (second_waker, second_wakes) = WakeCounter::new();
    assert!(poll_with(first.as_mut(), &first_waker).is_pending());
    assert!(poll_with(second.as_mut(), &second_waker).is_pending());

    drop(rx);
    assert_eq!(first_wakes.count(), 1);
    assert_eq!(second_wakes.count(), 1);

    let first_error = expect_ready(poll_once(first.as_mut())).unwrap_err();
    let second_error = expect_ready(poll_once(second.as_mut())).unwrap_err();
    assert_eq!(first_error.into_inner(), 1);
    assert_eq!(second_error.into_inner(), 2);
}
