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
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;

use asyncband::spmc;
use asyncband::spmc::RecvError;
use asyncband::spmc::TryRecvError;
use asyncband::spmc::TrySendError;
use tests_integration::WakeCounter;
use tests_integration::expect_ready;
use tests_integration::poll_once;
use tests_integration::poll_with;

use super::DropSpy;

#[test]
fn bounded_one_send_wakes_one_of_eight_receivers_and_cancellation_hands_off() {
    let (mut sender, receiver) = spmc::bounded::<usize>(2);
    let receivers: Vec<_> = (0..8).map(|_| receiver.clone()).collect();
    let mut pending: Vec<_> = receivers
        .iter()
        .map(|receiver| {
            let (waker, wakes) = WakeCounter::new();
            let mut receive = Box::pin(receiver.recv());
            assert!(poll_with(receive.as_mut(), &waker).is_pending());
            (receive, wakes)
        })
        .collect();

    sender.try_send(7).unwrap();
    // Each cancellation passes the notification to exactly one remaining receiver.
    for _ in 0..7 {
        let (cancelled, wakes) = pending.remove(0);
        assert_eq!(wakes.count(), 1);
        assert!(pending.iter().all(|(_, wakes)| wakes.count() == 0));
        drop(cancelled);
    }
    let (mut last, wakes) = pending.pop().unwrap();
    assert_eq!(wakes.count(), 1);
    assert_eq!(poll_once(last.as_mut()), Poll::Ready(Ok(7)));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn bounded_cancelling_before_notification_removes_the_waiter() {
    let (mut sender, receiver) = spmc::bounded::<usize>(2);
    let competing = receiver.clone();
    let (cancelled_waker, cancelled_wakes) = WakeCounter::new();
    let (waiting_waker, waiting_wakes) = WakeCounter::new();
    let mut cancelled = Box::pin(receiver.recv());
    let mut waiting = Box::pin(competing.recv());
    assert!(poll_with(cancelled.as_mut(), &cancelled_waker).is_pending());
    assert!(poll_with(waiting.as_mut(), &waiting_waker).is_pending());

    drop(cancelled);
    sender.try_send(5).unwrap();
    assert_eq!(cancelled_wakes.count(), 0);
    assert_eq!(waiting_wakes.count(), 1);
    assert_eq!(poll_once(waiting.as_mut()), Poll::Ready(Ok(5)));
}

#[test]
fn bounded_sender_disconnection_wakes_all_receivers() {
    let (sender, receiver) = spmc::bounded::<usize>(2);
    let receivers: Vec<_> = (0..8).map(|_| receiver.clone()).collect();
    let pending: Vec<_> = receivers
        .iter()
        .map(|receiver| {
            let (waker, wakes) = WakeCounter::new();
            let mut receive = Box::pin(receiver.recv());
            assert!(poll_with(receive.as_mut(), &waker).is_pending());
            (receive, wakes)
        })
        .collect();

    drop(sender);
    for (mut receive, wakes) in pending {
        assert_eq!(wakes.count(), 1);
        assert_eq!(
            poll_once(receive.as_mut()),
            Poll::Ready(Err(RecvError::Disconnected))
        );
    }
}

#[test]
fn unbounded_one_send_wakes_one_of_eight_receivers_and_cancellation_hands_off() {
    let (mut sender, receiver) = spmc::unbounded::<usize>();
    let receivers: Vec<_> = (0..8).map(|_| receiver.clone()).collect();
    let mut pending: Vec<_> = receivers
        .iter()
        .map(|receiver| {
            let (waker, wakes) = WakeCounter::new();
            let mut receive = Box::pin(receiver.recv());
            assert!(poll_with(receive.as_mut(), &waker).is_pending());
            (receive, wakes)
        })
        .collect();

    sender.send(7).unwrap();
    // Each cancellation passes the notification to exactly one remaining receiver.
    for _ in 0..7 {
        let (cancelled, wakes) = pending.remove(0);
        assert_eq!(wakes.count(), 1);
        assert!(pending.iter().all(|(_, wakes)| wakes.count() == 0));
        drop(cancelled);
    }
    let (mut last, wakes) = pending.pop().unwrap();
    assert_eq!(wakes.count(), 1);
    assert_eq!(poll_once(last.as_mut()), Poll::Ready(Ok(7)));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn unbounded_cancelling_before_notification_removes_the_waiter() {
    let (mut sender, receiver) = spmc::unbounded::<usize>();
    let competing = receiver.clone();
    let (cancelled_waker, cancelled_wakes) = WakeCounter::new();
    let (waiting_waker, waiting_wakes) = WakeCounter::new();
    let mut cancelled = Box::pin(receiver.recv());
    let mut waiting = Box::pin(competing.recv());
    assert!(poll_with(cancelled.as_mut(), &cancelled_waker).is_pending());
    assert!(poll_with(waiting.as_mut(), &waiting_waker).is_pending());

    drop(cancelled);
    sender.send(5).unwrap();
    assert_eq!(cancelled_wakes.count(), 0);
    assert_eq!(waiting_wakes.count(), 1);
    assert_eq!(poll_once(waiting.as_mut()), Poll::Ready(Ok(5)));
}

#[test]
fn unbounded_sender_disconnection_wakes_all_receivers() {
    let (sender, receiver) = spmc::unbounded::<usize>();
    let receivers: Vec<_> = (0..8).map(|_| receiver.clone()).collect();
    let pending: Vec<_> = receivers
        .iter()
        .map(|receiver| {
            let (waker, wakes) = WakeCounter::new();
            let mut receive = Box::pin(receiver.recv());
            assert!(poll_with(receive.as_mut(), &waker).is_pending());
            (receive, wakes)
        })
        .collect();

    drop(sender);
    for (mut receive, wakes) in pending {
        assert_eq!(wakes.count(), 1);
        assert_eq!(
            poll_once(receive.as_mut()),
            Poll::Ready(Err(RecvError::Disconnected))
        );
    }
}

#[test]
fn bounded_capacity_and_pending_send_progress() {
    for capacity in [1, 2, 3, 8] {
        let (mut sender, receiver) = spmc::bounded(capacity);
        for value in 0..capacity {
            sender.try_send(value).unwrap();
        }
        assert_eq!(sender.try_send(capacity), Err(TrySendError::Full(capacity)));
        let mut waiting = Box::pin(sender.send(capacity));
        let (waker, wakes) = WakeCounter::new();
        assert!(poll_with(waiting.as_mut(), &waker).is_pending());

        assert_eq!(receiver.try_recv(), Ok(0));
        assert_eq!(wakes.count(), 1);
        assert_eq!(poll_once(waiting.as_mut()), Poll::Ready(Ok(())));
        drop(waiting);
        drop(sender);
        for value in 1..=capacity {
            assert_eq!(receiver.try_recv(), Ok(value));
        }
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }
}

#[test]
fn cancelling_an_unnotified_send_preserves_the_buffered_value() {
    let (mut sender, receiver) = spmc::bounded(1);
    let drops = Arc::new(AtomicUsize::new(0));
    sender.try_send(DropSpy(drops.clone())).unwrap();
    let mut cancelled = Box::pin(sender.send(DropSpy(drops.clone())));
    assert!(poll_once(cancelled.as_mut()).is_pending());

    drop(cancelled);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(receiver.try_recv().unwrap());
    assert_eq!(drops.load(Ordering::SeqCst), 2);

    expect_ready(poll_once(pin!(sender.send(DropSpy(drops.clone()))))).unwrap();
    drop(receiver);
    drop(sender);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
}

#[test]
fn cancelling_a_notified_send_leaves_capacity_for_the_next_send() {
    let (mut sender, receiver) = spmc::bounded(1);
    let drops = Arc::new(AtomicUsize::new(0));
    sender.try_send(DropSpy(drops.clone())).unwrap();
    let mut cancelled = Box::pin(sender.send(DropSpy(drops.clone())));
    let (waker, wakes) = WakeCounter::new();
    assert!(poll_with(cancelled.as_mut(), &waker).is_pending());

    drop(receiver.try_recv().unwrap());
    assert_eq!(wakes.count(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(cancelled);
    assert_eq!(drops.load(Ordering::SeqCst), 2);

    expect_ready(poll_once(pin!(sender.send(DropSpy(drops.clone()))))).unwrap();
    drop(receiver);
    drop(sender);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
}

#[test]
fn last_receiver_wakes_pending_sender_and_returns_its_value() {
    let (mut sender, receiver) = spmc::bounded(1);
    let competing = receiver.clone();
    sender.try_send(0).unwrap();
    let (waker, wakes) = WakeCounter::new();
    let mut pending = Box::pin(sender.send(1));
    assert!(poll_with(pending.as_mut(), &waker).is_pending());

    drop(receiver);
    assert_eq!(wakes.count(), 0);
    drop(competing);
    assert_eq!(wakes.count(), 1);
    let error = expect_ready(poll_once(pending.as_mut())).unwrap_err();
    assert_eq!(error.into_inner(), 1);
}
