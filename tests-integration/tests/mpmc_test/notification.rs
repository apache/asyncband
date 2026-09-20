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

fn send_wakes_only_the_first_receiver(receiver: impl Receiver<usize>, send: impl FnOnce()) {
    let competing = receiver.clone();
    let mut first = Box::pin(receiver.recv());
    let mut second = Box::pin(competing.recv());
    let (first_waker, first_wakes) = WakeCounter::new();
    let (second_waker, second_wakes) = WakeCounter::new();

    assert!(poll_with(first.as_mut(), &first_waker).is_pending());
    assert!(poll_with(second.as_mut(), &second_waker).is_pending());
    send();

    assert_eq!(first_wakes.count(), 1);
    assert_eq!(second_wakes.count(), 0);
    assert_eq!(expect_ready(poll_with(first.as_mut(), &first_waker)), Ok(1));
    assert!(poll_with(second.as_mut(), &second_waker).is_pending());
    assert_eq!(second_wakes.count(), 0);
}

#[test]
fn bounded_send_wakes_only_the_first_receiver() {
    let (sender, receiver) = mpmc::bounded(2);
    send_wakes_only_the_first_receiver(receiver, || sender.try_send(1).unwrap());
}

#[test]
fn unbounded_send_wakes_only_the_first_receiver() {
    let (sender, receiver) = mpmc::unbounded();
    send_wakes_only_the_first_receiver(receiver, || sender.send(1).unwrap());
}

fn cancelled_notified_receiver_wakes_next_receiver(
    receiver: impl Receiver<usize>,
    send: impl FnOnce(),
) {
    let competing = receiver.clone();
    let mut cancelled = Box::pin(receiver.recv());
    let mut waiting = Box::pin(competing.recv());
    let (cancelled_waker, cancelled_wakes) = WakeCounter::new();
    let (waiting_waker, waiting_wakes) = WakeCounter::new();

    assert!(poll_with(cancelled.as_mut(), &cancelled_waker).is_pending());
    assert!(poll_with(waiting.as_mut(), &waiting_waker).is_pending());
    send();
    assert_eq!(cancelled_wakes.count(), 1);
    assert_eq!(waiting_wakes.count(), 0);
    drop(cancelled);

    assert_eq!(waiting_wakes.count(), 1);
    assert_eq!(
        expect_ready(poll_with(waiting.as_mut(), &waiting_waker)),
        Ok(1)
    );
}

#[test]
fn bounded_cancelled_notified_receiver_wakes_next_receiver() {
    let (sender, receiver) = mpmc::bounded(1);
    cancelled_notified_receiver_wakes_next_receiver(receiver, || sender.try_send(1).unwrap());
}

#[test]
fn unbounded_cancelled_notified_receiver_wakes_next_receiver() {
    let (sender, receiver) = mpmc::unbounded();
    cancelled_notified_receiver_wakes_next_receiver(receiver, || sender.send(1).unwrap());
}

fn cancelling_a_receiver_after_its_value_is_taken_does_not_wake_next(
    receiver: impl Receiver<usize>,
    send: impl Fn(usize),
    take_value: impl FnOnce() -> Result<usize, TryRecvError>,
) {
    let competing = receiver.clone();
    let mut cancelled = Box::pin(receiver.recv());
    let mut waiting = Box::pin(competing.recv());
    let (cancelled_waker, cancelled_wakes) = WakeCounter::new();
    let (waiting_waker, waiting_wakes) = WakeCounter::new();

    assert!(poll_with(cancelled.as_mut(), &cancelled_waker).is_pending());
    assert!(poll_with(waiting.as_mut(), &waiting_waker).is_pending());
    send(1);
    assert_eq!(cancelled_wakes.count(), 1);
    assert_eq!(waiting_wakes.count(), 0);
    assert_eq!(take_value(), Ok(1));
    drop(cancelled);
    assert_eq!(waiting_wakes.count(), 0);

    // The next value must still notify the waiter whose predecessor was cancelled.
    send(2);
    assert_eq!(waiting_wakes.count(), 1);
    assert_eq!(
        expect_ready(poll_with(waiting.as_mut(), &waiting_waker)),
        Ok(2)
    );
}

#[test]
fn bounded_cancelling_a_receiver_after_its_value_is_taken_does_not_wake_next() {
    let (sender, receiver) = mpmc::bounded(1);
    cancelling_a_receiver_after_its_value_is_taken_does_not_wake_next(
        receiver.clone(),
        |value| sender.try_send(value).unwrap(),
        || receiver.try_recv(),
    );
}

#[test]
fn unbounded_cancelling_a_receiver_after_its_value_is_taken_does_not_wake_next() {
    let (sender, receiver) = mpmc::unbounded();
    cancelling_a_receiver_after_its_value_is_taken_does_not_wake_next(
        receiver.clone(),
        |value| sender.send(value).unwrap(),
        || receiver.try_recv(),
    );
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
    assert_eq!(
        expect_ready(poll_with(first.as_mut(), &first_waker)),
        Ok(1)
    );
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
