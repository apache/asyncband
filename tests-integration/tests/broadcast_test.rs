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

use std::future::Future;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Wake;
use std::task::Waker;
use std::thread;

use asyncband::broadcast::overflow::*;

struct TrackWake(AtomicUsize);

impl Wake for TrackWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct PanicOnDrop {
    value: u64,
    panic: bool,
    panicked: Arc<AtomicBool>,
}

impl Clone for PanicOnDrop {
    fn clone(&self) -> Self {
        Self {
            value: self.value,
            panic: false,
            panicked: self.panicked.clone(),
        }
    }
}

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        if self.panic && !self.panicked.swap(true, Ordering::Relaxed) {
            panic!("panic while replacing a broadcast slot");
        }
    }
}

#[tokio::test]
async fn test_broadcast_basic() {
    let (tx, mut rx1) = channel(10);
    let mut rx2 = rx1.clone();

    tx.send(10);
    tx.send(20);

    assert_eq!(rx1.recv().await, Ok(10));
    assert_eq!(rx1.recv().await, Ok(20));
    assert_eq!(rx2.recv().await, Ok(10));
    assert_eq!(rx2.recv().await, Ok(20));
}

#[tokio::test]
async fn test_broadcast_lagged() {
    let (tx, mut rx) = channel(2);

    tx.send(1);
    tx.send(2);
    tx.send(3);

    // Overwrites 1. Rx lagged by 1 (missed msg '1').
    // Rx should return Lagged(1) and catch up to 2 (oldest valid).
    assert_eq!(rx.recv().await, Err(RecvError::Lagged(1)));
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx.recv().await, Ok(3));
}

#[tokio::test]
async fn test_broadcast_lagged_multi() {
    let (tx, mut rx) = channel(2);

    tx.send(1);
    tx.send(2);
    tx.send(3);
    tx.send(4);

    // Overwrites 1 and 2. Missed 2 messages.
    assert_eq!(rx.recv().await, Err(RecvError::Lagged(2)));
    assert_eq!(rx.recv().await, Ok(3));
    assert_eq!(rx.recv().await, Ok(4));
}

#[tokio::test]
async fn test_broadcast_closed() {
    let (tx, mut rx) = channel::<()>(10);
    drop(tx);
    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[tokio::test]
async fn test_wait_mechanism() {
    let (tx, mut rx) = channel(10);

    let handle = tokio::spawn(async move { rx.recv().await });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    tx.send(42);

    assert_eq!(handle.await.unwrap(), Ok(42));
}

#[test]
fn cancelled_recv_releases_its_waker() {
    let (tx, mut rx) = channel::<()>(1);
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let baseline = Arc::strong_count(&tracker);
    let mut context = Context::from_waker(&waker);
    let mut recv = Box::pin(rx.recv());

    assert!(recv.as_mut().poll(&mut context).is_pending());
    assert_eq!(Arc::strong_count(&tracker), baseline + 1);

    drop(recv);
    assert_eq!(Arc::strong_count(&tracker), baseline);

    tx.send(());
    assert_eq!(tracker.0.load(Ordering::Relaxed), 0);
    assert_eq!(rx.try_recv(), Ok(()));
}

#[tokio::test]
async fn test_subscribe() {
    let (tx, _rx) = channel(10);
    let mut rx = tx.subscribe();

    tx.send(100);
    assert_eq!(rx.recv().await, Ok(100));
}

#[tokio::test]
async fn test_resubscribe() {
    let (tx, mut rx) = channel(2);

    tx.send(1);
    tx.send(2);

    let mut rx2 = rx.resubscribe();

    // rx sees 1, 2
    // rx2 sees nothing yet (starts at tail=2)

    tx.send(3);

    assert_eq!(rx.recv().await, Err(RecvError::Lagged(1)));
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx2.recv().await, Ok(3));
}

#[tokio::test]
async fn test_try_recv() {
    let (tx, mut rx) = channel(16);

    // Empty
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    // Success
    tx.send(10);
    assert_eq!(rx.try_recv(), Ok(10));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    // Closed
    drop(tx);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[tokio::test]
async fn test_try_recv_lagged() {
    let (tx, mut rx) = channel(2);
    tx.send(1);
    tx.send(2);
    tx.send(3);

    assert_eq!(rx.try_recv(), Err(TryRecvError::Lagged(1)));
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Ok(3));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn panicking_send_does_not_publish_an_unwritten_slot() {
    let panicked = Arc::new(AtomicBool::new(false));
    let (tx, mut rx) = channel(1);
    tx.send(PanicOnDrop {
        value: 1,
        panic: true,
        panicked: panicked.clone(),
    });

    let received = rx.try_recv().unwrap();
    assert_eq!(received.value, 1);
    drop(received);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tx.send(PanicOnDrop {
            value: 2,
            panic: false,
            panicked: panicked.clone(),
        });
    }));
    assert!(result.is_err());
    assert!(panicked.load(Ordering::Relaxed));

    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    tx.send(PanicOnDrop {
        value: 3,
        panic: false,
        panicked: panicked.clone(),
    });
    assert_eq!(rx.try_recv().unwrap().value, 3);

    drop(tx);
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
}

#[test]
fn concurrent_overwrite_preserves_sequence_and_lag_count() {
    const MESSAGE_COUNT: u64 = 200_000;

    let (tx, mut rx) = channel(2);
    let producer = thread::spawn(move || {
        for value in 0..MESSAGE_COUNT {
            tx.send(value);
        }
    });

    let mut next = 0_u64;
    loop {
        match rx.try_recv() {
            Ok(value) => {
                assert_eq!(value, next);
                next = next.wrapping_add(1);
            }
            Err(TryRecvError::Lagged(missed)) => {
                assert!(missed > 0);
                next = next.wrapping_add(missed);
            }
            Err(TryRecvError::Empty) => thread::yield_now(),
            Err(TryRecvError::Disconnected) => break,
        }
    }

    producer.join().unwrap();
    assert_eq!(next, MESSAGE_COUNT);
}

#[test]
fn concurrent_receivers_observe_the_same_sequence() {
    const MESSAGE_COUNT: usize = 4096;
    const RECEIVER_COUNT: usize = 8;

    let (tx, receiver) = channel(MESSAGE_COUNT);
    let mut receivers = Vec::with_capacity(RECEIVER_COUNT);
    receivers.push(receiver);
    for _ in 1..RECEIVER_COUNT {
        receivers.push(receivers[0].clone());
    }

    let ready = Arc::new(Barrier::new(RECEIVER_COUNT + 1));
    let workers = receivers
        .into_iter()
        .map(|mut receiver| {
            let ready = ready.clone();
            thread::spawn(move || {
                ready.wait();
                let mut received = Vec::with_capacity(MESSAGE_COUNT);
                loop {
                    match receiver.try_recv() {
                        Ok(value) => received.push(value),
                        Err(TryRecvError::Empty) => thread::yield_now(),
                        Err(TryRecvError::Disconnected) => return received,
                        Err(TryRecvError::Lagged(missed)) => {
                            panic!("receiver unexpectedly lagged by {missed}")
                        }
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    ready.wait();
    for value in 0..MESSAGE_COUNT {
        tx.send(value);
    }
    drop(tx);

    let expected = (0..MESSAGE_COUNT).collect::<Vec<_>>();
    for worker in workers {
        assert_eq!(worker.join().unwrap(), expected);
    }
}

#[tokio::test]
async fn test_multi_senders_concurrent() {
    let (tx, mut rx) = channel(100);
    let tx1 = tx.clone();
    let tx2 = tx.clone();

    tokio::spawn(async move {
        for i in 0..10 {
            tx1.send(i);
        }
    });

    tokio::spawn(async move {
        for i in 10..20 {
            tx2.send(i);
        }
    });

    // Main tx can also send
    for i in 20..30 {
        tx.send(i);
    }
    drop(tx);

    let mut received = Vec::new();
    while let Ok(n) = rx.recv().await {
        received.push(n);
    }
    received.sort();

    let expected = (0..30).collect::<Vec<_>>();
    assert_eq!(received, expected);
}
