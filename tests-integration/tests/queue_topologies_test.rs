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
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use asyncband::mpmc;
use asyncband::spmc;
use asyncband::spsc;
use tokio_test::assert_pending;
use tokio_test::assert_ready_eq;
use tokio_test::task::spawn;

mod support;
use support::TrackWake;
use support::callback_waker;
use support::poll_with_waker;

#[tokio::test]
async fn spsc_preserves_fifo_and_disconnection() {
    let (mut tx, mut rx) = spsc::bounded(2);
    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();
    drop(tx);

    assert_eq!(rx.recv().await, Ok(1));
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx.recv().await, Err(spsc::RecvError::Disconnected));
}

#[test]
fn bounded_queue_capacity_must_be_positive() {
    assert!(std::panic::catch_unwind(|| spsc::bounded::<()>(0)).is_err());
    assert!(std::panic::catch_unwind(|| spmc::bounded::<()>(0)).is_err());
    assert!(std::panic::catch_unwind(|| mpmc::bounded::<()>(0)).is_err());
}

#[tokio::test]
async fn cancelling_a_bounded_send_does_not_enqueue_its_value() {
    let (mut tx, rx) = spmc::bounded(1);
    tx.send(1).await.unwrap();

    let mut cancelled = spawn(tx.send(2));
    assert_pending!(cancelled.poll());
    drop(cancelled);

    assert_eq!(rx.recv().await, Ok(1));
    assert_eq!(rx.try_recv(), Err(spmc::TryRecvError::Empty));
    tx.try_send(3).unwrap();
    assert_eq!(rx.recv().await, Ok(3));
}

#[test]
fn bounded_send_wakes_after_receive_frees_capacity() {
    let (tx, rx) = mpmc::bounded(1);
    tx.try_send(1).unwrap();

    let mut send = spawn(tx.send(2));
    assert_pending!(send.poll());
    assert_eq!(rx.try_recv(), Ok(1));
    assert_ready_eq!(send.poll(), Ok(()));
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn freeing_capacity_wakes_every_sender_for_cancellation_transfer() {
    let (tx, rx) = mpmc::bounded(1);
    tx.try_send(0).unwrap();
    let mut first = spawn(tx.send(1));
    let mut second = spawn(tx.send(2));
    assert_pending!(first.poll());
    assert_pending!(second.poll());

    assert_eq!(rx.try_recv(), Ok(0));
    assert!(first.is_woken());
    assert!(second.is_woken());
    drop(first);
    assert_ready_eq!(second.poll(), Ok(()));
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn sending_wakes_every_receiver_for_cancellation_transfer() {
    let (tx, rx) = mpmc::unbounded();
    let other = rx.clone();
    let mut first = spawn(rx.recv());
    let mut second = spawn(other.recv());
    assert_pending!(first.poll());
    assert_pending!(second.poll());

    tx.send(1).unwrap();
    assert!(first.is_woken());
    assert!(second.is_woken());
    drop(first);
    assert_ready_eq!(second.poll(), Ok(1));
}

#[test]
fn cancelled_queue_operations_release_their_wakers() {
    let (tx, rx) = mpmc::bounded::<usize>(1);
    tx.try_send(0).unwrap();
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let baseline = Arc::strong_count(&tracker);
    let mut send = Box::pin(tx.send(1));
    assert!(poll_with_waker(send.as_mut(), &waker).is_pending());
    assert_eq!(Arc::strong_count(&tracker), baseline + 1);
    drop(send);
    assert_eq!(Arc::strong_count(&tracker), baseline);
    assert_eq!(rx.try_recv(), Ok(0));
    assert_eq!(tracker.0.load(Ordering::Relaxed), 0);

    let mut recv = Box::pin(rx.recv());
    assert!(poll_with_waker(recv.as_mut(), &waker).is_pending());
    assert_eq!(Arc::strong_count(&tracker), baseline + 1);
    drop(recv);
    assert_eq!(Arc::strong_count(&tracker), baseline);
    tx.try_send(2).unwrap();
    assert_eq!(tracker.0.load(Ordering::Relaxed), 0);
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn queue_wakers_run_after_unlocking_the_queue() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (tx, rx) = mpmc::unbounded();
        let callback_sender = tx.clone();
        let waker = callback_waker(move || {
            callback_sender.send(2).unwrap();
        });
        let mut recv = Box::pin(rx.recv());

        assert!(poll_with_waker(recv.as_mut(), &waker).is_pending());
        tx.send(1).unwrap();
        assert_eq!(poll_with_waker(recv.as_mut(), &waker), Poll::Ready(Ok(1)));
        assert_eq!(rx.try_recv(), Ok(2));
        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("waker callback deadlocked against the queue lock");
}

#[test]
fn dropping_the_last_receiver_wakes_a_backpressured_sender() {
    let (tx, rx) = mpmc::bounded(1);
    tx.try_send(1).unwrap();
    let mut send = spawn(tx.send(2));
    assert_pending!(send.poll());

    drop(rx);
    assert!(send.is_woken());
    let Poll::Ready(Err(error)) = send.poll() else {
        panic!("send should fail after the last receiver drops");
    };
    assert_eq!(error.into_inner(), 2);
}

#[test]
fn dropping_the_last_sender_wakes_a_parked_receiver() {
    let (tx, rx) = mpmc::unbounded::<()>();
    let other = tx.clone();
    let mut recv = spawn(rx.recv());
    assert_pending!(recv.poll());

    drop(tx);
    assert!(!recv.is_woken());
    drop(other);
    assert!(recv.is_woken());
    assert_ready_eq!(recv.poll(), Err(mpmc::RecvError::Disconnected));
}

#[test]
fn spmc_receivers_compete_for_each_value() {
    let (mut tx, rx1) = spmc::unbounded();
    let rx2 = rx1.clone();
    tx.send(1).unwrap();
    tx.send(2).unwrap();

    assert_eq!(rx1.try_recv(), Ok(1));
    assert_eq!(rx2.try_recv(), Ok(2));
    assert_eq!(rx1.try_recv(), Err(spmc::TryRecvError::Empty));
    assert_eq!(rx2.try_recv(), Err(spmc::TryRecvError::Empty));
}

#[test]
fn dropping_last_receiver_returns_and_drops_queued_values() {
    let (mut tx, rx) = spmc::unbounded();
    tx.send(String::from("queued")).unwrap();
    drop(rx);

    let error = tx.send(String::from("unsent")).unwrap_err();
    assert_eq!(error.into_inner(), "unsent");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mpmc_delivers_every_value_exactly_once_under_contention() {
    const PRODUCERS: usize = 4;
    const CONSUMERS: usize = 4;
    const VALUES_PER_PRODUCER: usize = 2_000;

    let (tx, rx) = mpmc::unbounded();
    let mut consumers = Vec::new();
    for _ in 0..CONSUMERS {
        let receiver = rx.clone();
        consumers.push(tokio::spawn(async move {
            let mut values = Vec::new();
            while let Ok(value) = receiver.recv().await {
                values.push(value);
            }
            values
        }));
    }
    drop(rx);

    let mut producers = Vec::new();
    for producer in 0..PRODUCERS {
        let sender = tx.clone();
        producers.push(tokio::spawn(async move {
            let start = producer * VALUES_PER_PRODUCER;
            for value in start..start + VALUES_PER_PRODUCER {
                sender.send(value).unwrap();
            }
        }));
    }
    drop(tx);

    for producer in producers {
        producer.await.unwrap();
    }

    let mut received = Vec::new();
    for consumer in consumers {
        received.extend(consumer.await.unwrap());
    }
    received.sort_unstable();

    assert_eq!(
        received,
        (0..PRODUCERS * VALUES_PER_PRODUCER).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_mpmc_makes_progress_under_contention() {
    const PRODUCERS: usize = 4;
    const CONSUMERS: usize = 4;
    const VALUES_PER_PRODUCER: usize = 500;

    let (tx, rx) = mpmc::bounded(7);
    let consumers = (0..CONSUMERS)
        .map(|_| {
            let receiver = rx.clone();
            tokio::spawn(async move {
                let mut values = Vec::new();
                while let Ok(value) = receiver.recv().await {
                    values.push(value);
                }
                values
            })
        })
        .collect::<Vec<_>>();
    drop(rx);

    let producers = (0..PRODUCERS)
        .map(|producer| {
            let sender = tx.clone();
            tokio::spawn(async move {
                let start = producer * VALUES_PER_PRODUCER;
                for value in start..start + VALUES_PER_PRODUCER {
                    sender.send(value).await.unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    drop(tx);

    for producer in producers {
        producer.await.unwrap();
    }
    let mut received = Vec::new();
    for consumer in consumers {
        received.extend(consumer.await.unwrap());
    }
    received.sort_unstable();
    assert_eq!(
        received,
        (0..PRODUCERS * VALUES_PER_PRODUCER).collect::<Vec<_>>()
    );
}

struct ReentrantDrop(Option<Box<dyn FnOnce() + Send>>);

impl Drop for ReentrantDrop {
    fn drop(&mut self) {
        if let Some(callback) = self.0.take() {
            callback();
        }
    }
}

#[test]
fn queued_payloads_are_dropped_after_unlocking_the_queue() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (tx, rx) = mpmc::unbounded();
        let callback_sender = tx.clone();
        tx.send(ReentrantDrop(Some(Box::new(move || {
            assert!(callback_sender.send(ReentrantDrop(None)).is_err());
        }))))
        .unwrap();
        drop(rx);
        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("payload destructor deadlocked against the queue lock");
}
