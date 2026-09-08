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
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
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
        for value in 1..capacity {
            tx.try_send(value).unwrap();
        }
        assert!(matches!(tx.try_reserve(), Err(TrySendError::Full(()))));
        assert_eq!(tx.try_send(0), Err(TrySendError::Full(0)));
        for value in 1..capacity {
            assert_eq!(rx.try_recv(), Ok(value));
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
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
fn dropping_a_permit_wakes_a_pending_reservation() {
    let (tx, mut rx) = mpsc::bounded(1);
    let held = tx.try_reserve().unwrap();
    let mut waiting = Box::pin(tx.reserve());
    let (waker, wakes) = WakeCounter::new();
    assert!(poll_with(waiting.as_mut(), &waker).is_pending());
    drop(held);
    assert_eq!(wakes.count(), 1);
    let permit = expect_ready(poll_with(waiting.as_mut(), &waker)).unwrap();
    assert!(matches!(tx.try_reserve(), Err(TrySendError::Full(()))));
    permit.send(7).unwrap();
    assert_eq!(rx.try_recv(), Ok(7));
}

#[test]
fn receiver_drop_does_not_wait_for_held_or_forgotten_permits() {
    let (tx, mut rx) = mpsc::bounded(3);
    let held = tx.try_reserve().unwrap();
    std::mem::forget(tx.try_reserve().unwrap());
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
fn an_old_permit_observes_consumption_before_reusing_a_slot() {
    let (tx, mut rx) = mpsc::bounded(2);
    let old = tx.try_reserve().unwrap();
    let recycled = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let recycled = &recycled;
        let producer = scope.spawn(move || {
            // Coordinate the schedule without supplying the happens-before edge that the
            // channel itself must provide between the previous read and this slot's reuse.
            while !recycled.load(Ordering::Relaxed) {
                std::thread::yield_now();
            }
            old.send(String::from("reused")).unwrap();
        });
        for value in ["first", "second"] {
            tx.try_send(String::from(value)).unwrap();
            assert_eq!(rx.try_recv().unwrap(), value);
        }
        recycled.store(true, Ordering::Relaxed);
        producer.join().unwrap();
    });
    assert_eq!(rx.try_recv().unwrap(), "reused");
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
    // Drain and join before asserting so a regression cannot strand producers on a full ring.
    assert_eq!(out_of_order, 0);
    let permits: Vec<_> = (0..3).map(|_| tx.try_reserve().unwrap()).collect();
    assert!(matches!(tx.try_reserve(), Err(TrySendError::Full(()))));
    drop(permits);
}
