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
use asyncband::mpmc::RecvError;
use asyncband::mpmc::TryRecvError;
use tests_integration::WakeCounter;
use tests_integration::expect_ready;
use tests_integration::poll_with;

use super::Receiver;

#[derive(Debug)]
enum NotifiedReceiver {
    Receive,
    Cancel,
    TakeValueThenCancel,
}

fn receiver_notifications<S, R: Receiver<usize>>(
    channel: impl Fn() -> (S, R),
    send: impl Fn(&S, usize),
) {
    use NotifiedReceiver::*;

    for action in [Receive, Cancel, TakeValueThenCancel] {
        let (sender, receiver) = channel();
        let competing = receiver.clone();
        let mut first = Box::pin(receiver.recv());
        let mut second = Box::pin(competing.recv());
        let (first_waker, first_wakes) = WakeCounter::new();
        let (second_waker, second_wakes) = WakeCounter::new();

        assert!(poll_with(first.as_mut(), &first_waker).is_pending());
        assert!(poll_with(second.as_mut(), &second_waker).is_pending());
        send(&sender, 1);
        assert_eq!(first_wakes.count(), 1);
        assert_eq!(second_wakes.count(), 0);

        let expected = match action {
            Receive => {
                assert_eq!(expect_ready(poll_with(first.as_mut(), &first_waker)), Ok(1));
                assert!(poll_with(second.as_mut(), &second_waker).is_pending());
                assert_eq!(second_wakes.count(), 0);
                send(&sender, 2);
                2
            }
            Cancel => {
                drop(first);
                1
            }
            TakeValueThenCancel => {
                assert_eq!(receiver.try_recv(), Ok(1));
                drop(first);
                assert_eq!(second_wakes.count(), 0);
                // Cancellation must leave the next send able to notify this waiter.
                send(&sender, 2);
                2
            }
        };
        assert_eq!(second_wakes.count(), 1, "{action:?}");
        assert_eq!(
            expect_ready(poll_with(second.as_mut(), &second_waker)),
            Ok(expected),
            "{action:?}"
        );
    }
}

#[test]
fn bounded_receiver_notification_is_consumed_or_handed_off() {
    receiver_notifications(
        || mpmc::bounded(2),
        |sender, value| sender.try_send(value).unwrap(),
    );
}

#[test]
fn unbounded_receiver_notification_is_consumed_or_handed_off() {
    receiver_notifications(mpmc::unbounded, |sender, value| sender.send(value).unwrap());
}

#[test]
fn notified_receiver_that_loses_the_value_queues_behind_waiting_receivers() {
    let (sender, receiver) = mpmc::unbounded();
    let second_receiver = receiver.clone();
    let barging = receiver.clone();
    let mut first = Box::pin(receiver.recv());
    let mut second = Box::pin(second_receiver.recv());
    let (first_waker, first_wakes) = WakeCounter::new();
    let (second_waker, second_wakes) = WakeCounter::new();

    assert!(poll_with(first.as_mut(), &first_waker).is_pending());
    assert!(poll_with(second.as_mut(), &second_waker).is_pending());
    sender.send(1).unwrap();
    assert_eq!(first_wakes.count(), 1);
    assert_eq!(barging.try_recv(), Ok(1));
    assert!(poll_with(first.as_mut(), &first_waker).is_pending());

    sender.send(2).unwrap();
    assert_eq!(first_wakes.count(), 1);
    assert_eq!(second_wakes.count(), 1);
    assert_eq!(
        expect_ready(poll_with(second.as_mut(), &second_waker)),
        Ok(2)
    );
}

#[test]
fn notified_sender_that_loses_capacity_queues_behind_waiting_senders() {
    let (sender, receiver) = mpmc::bounded(1);
    sender.try_send(0).unwrap();
    let competing = sender.clone();
    let mut first = Box::pin(sender.send(1));
    let mut second = Box::pin(competing.send(2));
    let (first_waker, first_wakes) = WakeCounter::new();
    let (second_waker, second_wakes) = WakeCounter::new();

    assert!(poll_with(first.as_mut(), &first_waker).is_pending());
    assert!(poll_with(second.as_mut(), &second_waker).is_pending());
    assert_eq!(receiver.try_recv(), Ok(0));
    assert_eq!(first_wakes.count(), 1);
    sender.try_send(3).unwrap();
    assert!(poll_with(first.as_mut(), &first_waker).is_pending());

    assert_eq!(receiver.try_recv(), Ok(3));
    assert_eq!(first_wakes.count(), 1);
    assert_eq!(second_wakes.count(), 1);
    expect_ready(poll_with(second.as_mut(), &second_waker)).unwrap();
    assert_eq!(receiver.try_recv(), Ok(2));
    assert_eq!(first_wakes.count(), 2);
    expect_ready(poll_with(first.as_mut(), &first_waker)).unwrap();
    assert_eq!(receiver.try_recv(), Ok(1));
}

#[test]
fn cancelling_a_sender_after_capacity_is_taken_does_not_wake_next() {
    let (sender, receiver) = mpmc::bounded(1);
    sender.try_send(0).unwrap();
    let competing = sender.clone();
    let mut cancelled = Box::pin(sender.send(1));
    let mut waiting = Box::pin(competing.send(2));
    let (cancelled_waker, cancelled_wakes) = WakeCounter::new();
    let (waiting_waker, waiting_wakes) = WakeCounter::new();

    assert!(poll_with(cancelled.as_mut(), &cancelled_waker).is_pending());
    assert!(poll_with(waiting.as_mut(), &waiting_waker).is_pending());
    assert_eq!(receiver.try_recv(), Ok(0));
    assert_eq!(cancelled_wakes.count(), 1);
    assert_eq!(waiting_wakes.count(), 0);
    sender.try_send(3).unwrap();
    drop(cancelled);
    assert_eq!(waiting_wakes.count(), 0);

    assert_eq!(receiver.try_recv(), Ok(3));
    assert_eq!(waiting_wakes.count(), 1);
    expect_ready(poll_with(waiting.as_mut(), &waiting_waker)).unwrap();
    assert_eq!(receiver.try_recv(), Ok(2));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn bounded_cancelled_sender_notifies_next_sender_before_dropping_value() {
    // A message destructor may depend on another blocked sender making progress.
    struct WakesSeenOnDrop {
        wakes: Arc<WakeCounter>,
        seen: Arc<AtomicUsize>,
    }

    impl Drop for WakesSeenOnDrop {
        fn drop(&mut self) {
            self.seen.store(self.wakes.count(), Ordering::Relaxed);
        }
    }

    let (sender, receiver) = mpmc::bounded(1);
    sender.try_send((0, None)).unwrap();
    let first_sender = sender.clone();
    let second_sender = sender.clone();
    let (cancelled_waker, cancelled_wakes) = WakeCounter::new();
    let (waiting_waker, waiting_wakes) = WakeCounter::new();
    let wakes_during_drop = Arc::new(AtomicUsize::new(usize::MAX));
    let observer = WakesSeenOnDrop {
        wakes: waiting_wakes.clone(),
        seen: wakes_during_drop.clone(),
    };
    let mut cancelled = Box::pin(first_sender.send((1, Some(observer))));
    let mut waiting = Box::pin(second_sender.send((2, None)));

    assert!(poll_with(cancelled.as_mut(), &cancelled_waker).is_pending());
    assert!(poll_with(waiting.as_mut(), &waiting_waker).is_pending());
    assert_eq!(receiver.try_recv().unwrap().0, 0);
    assert_eq!(cancelled_wakes.count(), 1);
    assert_eq!(waiting_wakes.count(), 0);
    drop(cancelled);

    assert_eq!(wakes_during_drop.load(Ordering::Relaxed), 1);
    assert_eq!(waiting_wakes.count(), 1);
    expect_ready(poll_with(waiting.as_mut(), &waiting_waker)).unwrap();
    assert_eq!(receiver.try_recv().unwrap().0, 2);
    assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
}

#[test]
fn last_sender_wakes_every_pending_receiver() {
    let (sender, receiver) = mpmc::unbounded::<usize>();
    let competing = receiver.clone();
    let mut first = Box::pin(receiver.recv());
    let mut second = Box::pin(competing.recv());
    let mut cancelled = Box::pin(receiver.recv());
    let (first_waker, first_wakes) = WakeCounter::new();
    let (second_waker, second_wakes) = WakeCounter::new();
    let (cancelled_waker, cancelled_wakes) = WakeCounter::new();
    assert!(poll_with(first.as_mut(), &first_waker).is_pending());
    assert!(poll_with(second.as_mut(), &second_waker).is_pending());
    assert!(poll_with(cancelled.as_mut(), &cancelled_waker).is_pending());

    // Disconnection must handle both notified and still-linked registrations.
    sender.send(1).unwrap();

    drop(sender);
    assert_eq!(first_wakes.count(), 1);
    assert_eq!(second_wakes.count(), 1);
    assert_eq!(cancelled_wakes.count(), 1);
    drop(cancelled);
    assert_eq!(expect_ready(poll_with(first.as_mut(), &first_waker)), Ok(1));
    assert_eq!(
        expect_ready(poll_with(second.as_mut(), &second_waker)),
        Err(RecvError::Disconnected)
    );
}

#[test]
fn last_receiver_wakes_every_pending_sender() {
    let (sender, receiver) = mpmc::bounded(1);
    sender.try_send(0).unwrap();
    let competing = sender.clone();
    let mut first = Box::pin(sender.send(1));
    let mut second = Box::pin(competing.send(2));
    let mut cancelled = Box::pin(sender.send(3));
    let (first_waker, first_wakes) = WakeCounter::new();
    let (second_waker, second_wakes) = WakeCounter::new();
    let (cancelled_waker, cancelled_wakes) = WakeCounter::new();
    assert!(poll_with(first.as_mut(), &first_waker).is_pending());
    assert!(poll_with(second.as_mut(), &second_waker).is_pending());
    assert!(poll_with(cancelled.as_mut(), &cancelled_waker).is_pending());
    assert_eq!(receiver.try_recv(), Ok(0));

    drop(receiver);
    assert_eq!(first_wakes.count(), 1);
    assert_eq!(second_wakes.count(), 1);
    assert_eq!(cancelled_wakes.count(), 1);
    drop(cancelled);
    assert_eq!(
        expect_ready(poll_with(first.as_mut(), &first_waker))
            .unwrap_err()
            .into_inner(),
        1
    );
    assert_eq!(
        expect_ready(poll_with(second.as_mut(), &second_waker))
            .unwrap_err()
            .into_inner(),
        2
    );
}
