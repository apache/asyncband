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
use std::task::Poll;

use asyncband::mpsc;
use asyncband::mpsc::RecvError;
use asyncband::mpsc::TryRecvError;
use asyncband::mpsc::TrySendError;
use tests_integration::WakeCounter;
use tests_integration::expect_ready;
use tests_integration::poll_once;
use tests_integration::poll_with;

// Public channel contracts. The other suites cover backpressure, callbacks, and concurrency.
mod backpressure;
mod callbacks;
mod concurrency;
mod reservation;

#[test]
fn unbounded_try_recv_preserves_order_and_reports_state() {
    let (tx, mut rx) = mpsc::unbounded();

    for i in 0..4 {
        tx.send(i).unwrap();
    }

    assert_eq!(rx.try_recv(), Ok(0));
    tx.send(4).unwrap();
    for i in 1..5 {
        assert_eq!(rx.try_recv(), Ok(i));
    }
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    drop(tx);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn bounded_try_send_respects_capacity_and_order() {
    for capacity in [1, 3, 4, 16] {
        let (tx, mut rx) = mpsc::bounded(capacity);

        for i in 0..capacity {
            tx.try_send(i).unwrap();
        }

        assert_eq!(tx.try_send(capacity), Err(TrySendError::Full(capacity)));

        for i in 0..capacity {
            assert_eq!(rx.try_recv(), Ok(i));
        }

        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }
}

#[test]
fn buffered_messages_are_drained_before_disconnection() {
    let (unbounded_tx, mut unbounded_rx) = mpsc::unbounded();
    unbounded_tx.send(1).unwrap();
    unbounded_tx.send(2).unwrap();
    drop(unbounded_tx);
    assert_eq!(unbounded_rx.try_recv(), Ok(1));
    assert_eq!(unbounded_rx.try_recv(), Ok(2));
    assert_eq!(unbounded_rx.try_recv(), Err(TryRecvError::Disconnected));

    let (bounded_tx, mut bounded_rx) = mpsc::bounded(2);
    bounded_tx.try_send(3).unwrap();
    bounded_tx.try_send(4).unwrap();
    drop(bounded_tx);
    assert_eq!(bounded_rx.try_recv(), Ok(3));
    assert_eq!(bounded_rx.try_recv(), Ok(4));
    assert_eq!(bounded_rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn senders_report_disconnection_once_the_receiver_drops() {
    let (tx, rx) = mpsc::bounded(1);
    let other = tx.clone();
    let permit = tx.try_reserve().unwrap();
    assert!(!tx.is_disconnected());
    assert!(!other.is_disconnected());
    drop(rx);
    assert!(tx.is_disconnected());
    assert!(other.is_disconnected());
    assert_eq!(permit.send(1).unwrap_err().into_inner(), 1);
    assert_eq!(tx.try_send(2), Err(TrySendError::Disconnected(2)));

    let (tx, rx) = mpsc::unbounded();
    let other = tx.clone();
    assert!(!tx.is_disconnected());
    drop(rx);
    assert!(tx.is_disconnected());
    assert!(other.is_disconnected());
    assert_eq!(tx.send(3).unwrap_err().into_inner(), 3);
}

#[test]
fn receivers_report_disconnection_once_the_last_sender_drops() {
    let (tx, mut rx) = mpsc::bounded(1);
    let other = tx.clone();
    tx.try_send(1).unwrap();
    drop(tx);
    assert!(!rx.is_disconnected());
    drop(other);
    assert!(rx.is_disconnected());
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));

    let (tx, mut rx) = mpsc::unbounded();
    let other = tx.clone();
    for value in 1..4 {
        tx.send(value).unwrap();
    }
    // The first receive moves every queued message into the receiver's local batch.
    assert_eq!(rx.try_recv(), Ok(1));
    drop(tx);
    assert!(!rx.is_disconnected());
    drop(other);
    assert!(rx.is_disconnected());
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Ok(3));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn bounded_is_empty_ignores_reservations_and_blocked_sends() {
    let (tx, mut rx) = mpsc::bounded(1);
    assert!(rx.is_empty());

    let permit = tx.try_reserve().unwrap();
    assert!(rx.is_empty());
    permit.send(1).unwrap();
    assert!(!rx.is_empty());

    {
        let mut blocked = pin!(tx.send(2));
        assert!(poll_once(blocked.as_mut()).is_pending());
        assert_eq!(rx.try_recv(), Ok(1));
        assert!(rx.is_empty());
        assert_eq!(expect_ready(poll_once(blocked.as_mut())), Ok(()));
        assert!(!rx.is_empty());
    }
    drop(tx);
    assert!(!rx.is_empty());
    assert_eq!(rx.try_recv(), Ok(2));
    assert!(rx.is_empty());
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn unbounded_is_empty_sees_batched_and_queued_messages() {
    let (tx, mut rx) = mpsc::unbounded();
    assert!(rx.is_empty());

    for value in 0..3 {
        tx.send(value).unwrap();
    }
    // The first receive moves the remaining queued messages into the receiver's local batch.
    assert_eq!(rx.try_recv(), Ok(0));
    assert!(!rx.is_empty());
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
    assert!(rx.is_empty());
    tx.send(3).unwrap();
    assert!(!rx.is_empty());

    drop(tx);
    assert!(!rx.is_empty());
    assert_eq!(rx.try_recv(), Ok(3));
    assert!(rx.is_empty());
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn cancelled_receive_does_not_consume_a_later_message() {
    let (unbounded_tx, mut unbounded_rx) = mpsc::unbounded();
    {
        let mut receive = pin!(unbounded_rx.recv());
        assert!(poll_once(receive.as_mut()).is_pending());
    }
    unbounded_tx.send(1).unwrap();
    assert_eq!(unbounded_rx.try_recv(), Ok(1));

    let (bounded_tx, mut bounded_rx) = mpsc::bounded(1);
    {
        let mut receive = pin!(bounded_rx.recv());
        assert!(poll_once(receive.as_mut()).is_pending());
    }
    bounded_tx.try_send(2).unwrap();
    assert_eq!(bounded_rx.try_recv(), Ok(2));
}

#[test]
fn disconnected_sends_return_the_unsent_value() {
    let (tx, rx) = mpsc::bounded(1);
    tx.try_send(String::from("queued")).unwrap();
    drop(rx);
    assert_eq!(
        tx.try_send(String::from("try")),
        Err(TrySendError::Disconnected(String::from("try")))
    );
    let error = expect_ready(poll_once(pin!(tx.send(String::from("async"))))).unwrap_err();
    assert_eq!(error.into_inner(), "async");

    let (tx, rx) = mpsc::unbounded::<String>();
    drop(rx);
    assert_eq!(
        tx.send(String::from("unbounded")).unwrap_err().into_inner(),
        "unbounded"
    );
}

#[test]
fn receives_wake_for_messages_and_the_last_sender_drop() {
    let (waker, counter) = WakeCounter::new();
    let (tx, mut rx) = mpsc::bounded(1);
    let other = tx.clone();
    let mut receive = Box::pin(rx.recv());
    assert!(poll_with(receive.as_mut(), &waker).is_pending());
    tx.try_send(7).unwrap();
    assert_eq!(counter.count(), 1);
    assert_eq!(poll_with(receive.as_mut(), &waker), Poll::Ready(Ok(7)));
    drop(receive);

    let mut receive = pin!(rx.recv());
    assert!(poll_with(receive.as_mut(), &waker).is_pending());
    drop(tx);
    assert_eq!(counter.count(), 1);
    drop(other);
    assert_eq!(counter.count(), 2);
    assert_eq!(
        poll_with(receive.as_mut(), &waker),
        Poll::Ready(Err(RecvError::Disconnected))
    );

    let (waker, counter) = WakeCounter::new();
    let (tx, mut rx) = mpsc::unbounded();
    let other = tx.clone();
    let mut receive = Box::pin(rx.recv());
    assert!(poll_with(receive.as_mut(), &waker).is_pending());
    tx.send(7).unwrap();
    assert_eq!(counter.count(), 1);
    assert_eq!(poll_with(receive.as_mut(), &waker), Poll::Ready(Ok(7)));
    drop(receive);

    let mut receive = pin!(rx.recv());
    assert!(poll_with(receive.as_mut(), &waker).is_pending());
    drop(tx);
    assert_eq!(counter.count(), 1);
    drop(other);
    assert_eq!(counter.count(), 2);
    assert_eq!(
        poll_with(receive.as_mut(), &waker),
        Poll::Ready(Err(RecvError::Disconnected))
    );
}

#[test]
#[should_panic(expected = "must be nonzero")]
fn bounded_rejects_zero_capacity() {
    let _ = mpsc::bounded::<usize>(0);
}

#[test]
fn bounded_supports_full_usize_capacity_for_zero_sized_messages() {
    let (tx, mut rx) = mpsc::bounded::<()>(usize::MAX);
    // Returning capacity at this boundary must not overflow the counter.
    drop(tx.try_reserve().unwrap());
    tx.try_reserve().unwrap().send(()).unwrap();
    assert_eq!(rx.try_recv(), Ok(()));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}
