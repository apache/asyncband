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
use tests_integration::WakeCounter;
use tests_integration::expect_ready;
use tests_integration::poll_once;
use tests_integration::poll_with;

#[test]
fn bounded_wakes_blocked_senders_one_at_a_time() {
    let (tx, mut rx) = mpsc::bounded(1);
    tx.try_send(0).unwrap();

    let first_tx = tx.clone();
    let second_tx = tx.clone();
    let mut first = pin!(first_tx.send(1));
    let mut second = pin!(second_tx.send(2));

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
        let mut second = pin!(tx.send(2));
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
        // Even after capacity is granted, the `send` future must be polled to publish its value.
        if cancel_after_notification {
            assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        }
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
    let mut first = pin!(first_tx.send(1));
    let mut second = pin!(second_tx.send(2));

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

#[test]
fn large_pending_send_preserves_ownership_on_cancel_and_disconnect() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    #[derive(Debug)]
    struct Payload {
        bytes: [u8; 1024],
        drops: Arc<AtomicUsize>,
    }

    impl Drop for Payload {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    for capacity in [1, 3] {
        for cancel_after_grant in [false, true] {
            let drops = Arc::new(AtomicUsize::new(0));
            let payload = |byte| Payload {
                bytes: [byte; 1024],
                drops: drops.clone(),
            };
            let (tx, mut rx) = mpsc::bounded(capacity);
            for _ in 0..capacity {
                assert!(matches!(
                    poll_once(pin!(tx.send(payload(1)))),
                    Poll::Ready(Ok(()))
                ));
            }
            let mut pending = Box::pin(tx.send(payload(2)));
            assert!(poll_once(pending.as_mut()).is_pending());
            if cancel_after_grant {
                assert_eq!(rx.try_recv().unwrap().bytes, [1; 1024]);
            }
            drop(pending);
            assert_eq!(
                drops.load(Ordering::Relaxed),
                1 + usize::from(cancel_after_grant)
            );
            if !cancel_after_grant {
                assert_eq!(rx.try_recv().unwrap().bytes, [1; 1024]);
            }
            // Cancelling either a queued or granted send returns exactly one slot.
            tx.try_send(payload(3)).unwrap();
            assert!(tx.try_reserve().is_err());
            for _ in 1..capacity {
                assert_eq!(rx.try_recv().unwrap().bytes, [1; 1024]);
            }
            assert_eq!(rx.try_recv().unwrap().bytes, [3; 1024]);
            assert_eq!(drops.load(Ordering::Relaxed), capacity + 2);

            tx.try_send(payload(4)).unwrap();
            let mut pending = pin!(tx.send(payload(5)));
            // Fill any remaining slots so this send must wait.
            for _ in 1..capacity {
                tx.try_send(payload(4)).unwrap();
            }
            assert!(poll_once(pending.as_mut()).is_pending());
            drop(rx);
            let error = match poll_once(pending.as_mut()) {
                Poll::Ready(Err(error)) => error,
                other => panic!("expected disconnected send, got {other:?}"),
            };
            assert_eq!(error.into_inner().bytes, [5; 1024]);
            assert_eq!(drops.load(Ordering::Relaxed), capacity * 2 + 3);

            let (tx, mut rx) = mpsc::bounded(1);
            tx.try_send(payload(6)).unwrap();
            let mut pending = Box::pin(tx.send(payload(7)));
            assert!(poll_once(pending.as_mut()).is_pending());
            assert_eq!(rx.try_recv().unwrap().bytes, [6; 1024]);
            assert!(matches!(poll_once(pending.as_mut()), Poll::Ready(Ok(()))));
            drop(pending);
            drop(tx);
            let received = expect_ready(poll_once(pin!(rx.recv()))).unwrap();
            assert_eq!(received.bytes, [7; 1024]);
            drop(received);
            assert_eq!(drops.load(Ordering::Relaxed), capacity * 2 + 5);
        }
    }
}
