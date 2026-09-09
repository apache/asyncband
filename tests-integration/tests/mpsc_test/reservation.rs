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

use std::cell::Cell;
use std::mem;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::task::Wake;
use std::task::Waker;

use asyncband::mpsc;
use asyncband::mpsc::TryRecvError;
use asyncband::mpsc::TrySendError;
use tests_integration::poll_once;

use super::support::WakeCounter;
use super::support::expect_ready;
use super::support::poll_with;

#[test]
fn held_permits_consume_capacity_without_claiming_message_order() {
    for capacity in [1, 3, 64] {
        let (tx, mut rx) = mpsc::bounded(capacity);
        let permit = tx.try_reserve().unwrap();
        // The held permit stays usable while other messages repeatedly reuse the buffer.
        for lap in 0..8 {
            for offset in 1..capacity {
                tx.try_send(lap * capacity + offset).unwrap();
            }
            assert!(matches!(tx.try_reserve(), Err(TrySendError::Full(()))));
            assert_eq!(tx.try_send(0), Err(TrySendError::Full(0)));
            for offset in 1..capacity {
                assert_eq!(rx.try_recv(), Ok(lap * capacity + offset));
            }
            assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        }
        permit.send(0).unwrap();
        assert_eq!(rx.try_recv(), Ok(0));
        // Repeated reservation and cancellation must restore the exact original capacity.
        for _ in 0..3 {
            let permits: Vec<_> = (0..capacity).map(|_| tx.try_reserve().unwrap()).collect();
            assert!(matches!(tx.try_reserve(), Err(TrySendError::Full(()))));
            drop(permits);
        }
    }
}

#[test]
fn zero_sized_messages_preserve_capacity_across_reservation_and_close() {
    for capacity in [1, 3, 64] {
        let (tx, mut rx) = mpsc::bounded::<()>(capacity);
        let permit = tx.try_reserve().unwrap();
        for _ in 1..capacity {
            tx.try_send(()).unwrap();
        }
        assert_eq!(tx.try_send(()), Err(TrySendError::Full(())));
        for _ in 1..capacity {
            assert_eq!(rx.try_recv(), Ok(()));
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        drop(permit);
        tx.try_reserve().unwrap().send(()).unwrap();
        assert_eq!(rx.try_recv(), Ok(()));
        // Closing restores buffered capacity before outstanding permits are dropped.
        let held = tx.try_reserve().unwrap();
        for _ in 1..capacity {
            tx.try_send(()).unwrap();
        }
        drop(rx);
        drop(held);
        assert!(matches!(
            tx.try_reserve(),
            Err(TrySendError::Disconnected(()))
        ));
    }
}

#[test]
fn zero_sized_messages_are_dropped_once_when_received_or_discarded() {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    static DROPS: AtomicUsize = AtomicUsize::new(0);
    #[repr(align(128))]
    struct Message;
    impl Drop for Message {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }

    let (tx, mut rx) = mpsc::bounded(3);
    for _ in 0..3 {
        assert!(tx.try_send(Message).is_ok());
    }
    drop(rx.try_recv().unwrap());
    assert_eq!(DROPS.load(Ordering::Relaxed), 1);
    drop(rx);
    assert_eq!(DROPS.load(Ordering::Relaxed), 3);
    drop(tx.try_send(Message).err().unwrap().into_inner());
    assert_eq!(DROPS.load(Ordering::Relaxed), 4);
}

#[test]
fn released_capacity_is_granted_to_the_oldest_waiter() {
    let (tx, mut rx) = mpsc::bounded(1);
    let held = tx.try_reserve().unwrap();
    let mut waiting = Box::pin(tx.reserve());
    let (waker, wakes) = WakeCounter::new();
    assert!(poll_with(waiting.as_mut(), &waker).is_pending());
    drop(held);
    assert_eq!(wakes.count(), 1);
    // The waiting future owns the released slot even before the executor polls it again.
    assert!(matches!(tx.try_reserve(), Err(TrySendError::Full(()))));
    assert_eq!(tx.try_send(9), Err(TrySendError::Full(9)));
    let permit = expect_ready(poll_with(waiting.as_mut(), &waker)).unwrap();
    assert!(matches!(tx.try_reserve(), Err(TrySendError::Full(()))));
    permit.send(7).unwrap();
    assert_eq!(rx.try_recv(), Ok(7));
}

#[test]
fn cancelling_a_granted_reservation_passes_capacity_to_a_waiting_send() {
    let (tx, mut rx) = mpsc::bounded(1);
    let held = tx.try_reserve().unwrap();
    let mut reservation = Box::pin(tx.reserve());
    let mut send = Box::pin(tx.send(7));
    let (waker, wakes) = WakeCounter::new();
    assert!(poll_once(reservation.as_mut()).is_pending());
    assert!(poll_with(send.as_mut(), &waker).is_pending());
    drop(held);
    assert_eq!(wakes.count(), 0);
    drop(reservation);
    assert_eq!(wakes.count(), 1);
    assert_eq!(tx.try_send(9), Err(TrySendError::Full(9)));
    assert_eq!(expect_ready(poll_once(send.as_mut())), Ok(()));
    assert_eq!(rx.try_recv(), Ok(7));
    let held = tx.try_reserve().unwrap();
    assert!(matches!(tx.try_reserve(), Err(TrySendError::Full(()))));
    drop(held);
}

#[test]
fn closing_after_a_grant_returns_the_unsent_message() {
    let (tx, mut rx) = mpsc::bounded(1);
    tx.try_send(String::from("queued")).unwrap();
    let mut send = Box::pin(tx.send(String::from("unsent")));
    let mut reservation = Box::pin(tx.reserve());
    assert!(poll_once(send.as_mut()).is_pending());
    assert!(poll_once(reservation.as_mut()).is_pending());
    assert_eq!(rx.try_recv().unwrap(), "queued");
    drop(rx);
    let error = expect_ready(poll_once(send.as_mut())).unwrap_err();
    assert_eq!(error.into_inner(), "unsent");
    assert!(expect_ready(poll_once(reservation.as_mut())).is_err());
}

#[test]
fn receiver_drop_does_not_wait_for_held_or_forgotten_permits() {
    let (tx, mut rx) = mpsc::bounded(3);
    let held = tx.try_reserve().unwrap();
    mem::forget(tx.try_reserve().unwrap());
    tx.try_send(String::from("ready")).unwrap();
    assert_eq!(rx.try_recv().unwrap(), "ready");
    tx.try_send(String::from("discarded on close")).unwrap();
    let mut waiting = Box::pin(tx.reserve());
    let (waker, wakes) = WakeCounter::new();
    assert!(poll_with(waiting.as_mut(), &waker).is_pending());
    drop(rx);
    assert_eq!(wakes.count(), 1);
    assert!(expect_ready(poll_with(waiting.as_mut(), &waker)).is_err());
    assert!(matches!(
        tx.try_reserve(),
        Err(TrySendError::Disconnected(()))
    ));
    assert_eq!(
        held.send(String::from("unsent")).unwrap_err().into_inner(),
        "unsent"
    );
}

#[test]
fn a_permit_can_publish_send_only_payloads_from_another_thread() {
    let (tx, mut rx) = mpsc::bounded(1);
    let permit = tx.try_reserve().unwrap();
    std::thread::scope(|scope| {
        scope
            .spawn(move || permit.send(Cell::new(42)).unwrap())
            .join()
            .unwrap();
    });
    assert_eq!(rx.try_recv().unwrap().get(), 42);
}

#[test]
fn a_panicking_publication_wake_cannot_return_capacity_twice() {
    struct PanicOnWake;
    impl Wake for PanicOnWake {
        fn wake(self: Arc<Self>) {
            panic!("publication wake");
        }
    }
    let (tx, mut rx) = mpsc::bounded(1);
    let permit = tx.try_reserve().unwrap();
    let waker = Waker::from(Arc::new(PanicOnWake));
    let mut receive = Box::pin(rx.recv());
    assert!(poll_with(receive.as_mut(), &waker).is_pending());
    assert!(catch_unwind(AssertUnwindSafe(|| permit.send(1))).is_err());
    assert_eq!(tx.try_send(2), Err(TrySendError::Full(2)));
    assert_eq!(expect_ready(poll_once(receive.as_mut())), Ok(1));
    drop(receive);
    tx.try_send(2).unwrap();
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn concurrent_cancellation_preserves_capacity_and_message_order() {
    const PRODUCERS: usize = 3;
    const MESSAGES: usize = if cfg!(miri) { 8 } else { 256 };
    let (tx, mut rx) = mpsc::bounded(3);
    let mut out_of_order = 0;
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for producer in 0..PRODUCERS {
            let tx = tx.clone();
            workers.push(scope.spawn(move || {
                for sequence in 0..MESSAGES {
                    for cancel in [true, false] {
                        let permit = loop {
                            match tx.try_reserve() {
                                Ok(permit) => break permit,
                                Err(TrySendError::Full(())) => std::thread::yield_now(),
                                Err(TrySendError::Disconnected(())) => panic!("receiver is alive"),
                            }
                        };
                        if cancel {
                            drop(permit);
                        } else {
                            permit.send((producer, sequence)).unwrap();
                        }
                    }
                }
            }));
        }
        let mut next = [0; PRODUCERS];
        let mut count = 0;
        while count < PRODUCERS * MESSAGES {
            match rx.try_recv() {
                Ok((producer, sequence)) => {
                    out_of_order += usize::from(next[producer] != sequence);
                    next[producer] += 1;
                    count += 1;
                }
                Err(TryRecvError::Empty) => std::thread::yield_now(),
                Err(TryRecvError::Disconnected) => panic!("senders are alive"),
            }
        }
        for worker in workers {
            worker.join().unwrap();
        }
    });
    // Drain and join before asserting so a regression cannot strand producers on a full channel.
    assert_eq!(out_of_order, 0);
    let permits: Vec<_> = (0..3).map(|_| tx.try_reserve().unwrap()).collect();
    assert!(matches!(tx.try_reserve(), Err(TrySendError::Full(()))));
    drop(permits);
}
