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
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use asyncband::mpmc;
use asyncband::mpmc::RecvError;
use asyncband::mpmc::TryRecvError;
use asyncband::mpmc::TrySendError;
use tests_integration::poll_once;

struct WakeCounter(AtomicUsize);

impl WakeCounter {
    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn expect_ready<T>(poll: Poll<T>) -> T {
    match poll {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("future should be ready"),
    }
}

fn poll_with_waker<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

#[test]
fn bounded_enforces_exact_capacity_and_fifo_order() {
    let (sender, receiver) = mpmc::bounded(2);
    let competing = receiver.clone();

    sender.try_send(0).unwrap();
    sender.try_send(1).unwrap();
    assert_eq!(sender.try_send(2), Err(TrySendError::Full(2)));

    assert_eq!(receiver.try_recv(), Ok(0));
    assert_eq!(competing.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
}

#[test]
#[should_panic(expected = "mpmc bounded queue requires capacity > 0")]
fn bounded_rejects_zero_capacity() {
    let _ = mpmc::bounded::<()>(0);
}

#[test]
fn receiver_and_sender_clone_counts_control_disconnection() {
    let (sender, receiver) = mpmc::unbounded();
    let sender_clone = sender.clone();
    let receiver_clone = receiver.clone();

    drop(receiver);
    sender.send(1).unwrap();
    assert_eq!(receiver_clone.try_recv(), Ok(1));

    drop(sender);
    assert_eq!(receiver_clone.try_recv(), Err(TryRecvError::Empty));
    drop(sender_clone);
    assert_eq!(receiver_clone.try_recv(), Err(TryRecvError::Disconnected));

    drop(receiver_clone);
}

#[test]
fn last_receiver_returns_each_unsent_value_once() {
    let (bounded_sender, bounded_receiver) = mpmc::bounded(1);
    let bounded_receiver_clone = bounded_receiver.clone();
    drop(bounded_receiver);
    drop(bounded_receiver_clone);
    assert_eq!(
        bounded_sender.try_send(1),
        Err(TrySendError::Disconnected(1))
    );
    assert_eq!(
        bounded_sender.try_send(2),
        Err(TrySendError::Disconnected(2))
    );

    let (unbounded_sender, unbounded_receiver) = mpmc::unbounded();
    drop(unbounded_receiver);
    assert_eq!(unbounded_sender.send(3).unwrap_err().into_inner(), 3);
}

#[tokio::test]
async fn buffered_values_drain_before_disconnection() {
    let (sender, receiver) = mpmc::bounded(3);
    sender.send(0).await.unwrap();
    sender.send(1).await.unwrap();
    sender.send(2).await.unwrap();
    drop(sender);

    assert_eq!(receiver.recv().await, Ok(0));
    assert_eq!(receiver.recv().await, Ok(1));
    assert_eq!(receiver.recv().await, Ok(2));
    assert_eq!(receiver.recv().await, Err(RecvError::Disconnected));
}

#[tokio::test]
async fn unbounded_preserves_fifo_order_and_drains_before_disconnection() {
    let (sender, receiver) = mpmc::unbounded();
    sender.send(0).unwrap();
    sender.send(1).unwrap();
    sender.send(2).unwrap();
    drop(sender);

    assert_eq!(receiver.recv().await, Ok(0));
    assert_eq!(receiver.recv().await, Ok(1));
    assert_eq!(receiver.recv().await, Ok(2));
    assert_eq!(receiver.recv().await, Err(RecvError::Disconnected));
}

#[test]
fn bounded_send_wakes_only_the_first_receiver() {
    let (sender, receiver) = mpmc::bounded(2);
    let competing = receiver.clone();
    let mut first = Box::pin(receiver.recv());
    let mut second = Box::pin(competing.recv());
    let first_wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let second_wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let first_waker = Waker::from(first_wakes.clone());
    let second_waker = Waker::from(second_wakes.clone());

    assert!(poll_with_waker(first.as_mut(), &first_waker).is_pending());
    assert!(poll_with_waker(second.as_mut(), &second_waker).is_pending());
    sender.try_send(1).unwrap();

    assert_eq!(first_wakes.count(), 1);
    assert_eq!(second_wakes.count(), 0);
    assert_eq!(
        expect_ready(poll_with_waker(first.as_mut(), &first_waker)),
        Ok(1)
    );
    assert!(poll_with_waker(second.as_mut(), &second_waker).is_pending());
    assert_eq!(second_wakes.count(), 0);
}

#[test]
fn unbounded_send_wakes_only_the_first_receiver() {
    let (sender, receiver) = mpmc::unbounded();
    let competing = receiver.clone();
    let mut first = Box::pin(receiver.recv());
    let mut second = Box::pin(competing.recv());
    let first_wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let second_wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let first_waker = Waker::from(first_wakes.clone());
    let second_waker = Waker::from(second_wakes.clone());

    assert!(poll_with_waker(first.as_mut(), &first_waker).is_pending());
    assert!(poll_with_waker(second.as_mut(), &second_waker).is_pending());
    sender.send(1).unwrap();

    assert_eq!(first_wakes.count(), 1);
    assert_eq!(second_wakes.count(), 0);
    assert_eq!(
        expect_ready(poll_with_waker(first.as_mut(), &first_waker)),
        Ok(1)
    );
    assert!(poll_with_waker(second.as_mut(), &second_waker).is_pending());
    assert_eq!(second_wakes.count(), 0);
}

#[test]
fn cancelled_notified_receiver_passes_value_to_next_receiver() {
    let (sender, receiver) = mpmc::unbounded();
    let competing = receiver.clone();
    let mut cancelled = Box::pin(receiver.recv());
    let mut waiting = Box::pin(competing.recv());
    let cancelled_wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waiting_wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let cancelled_waker = Waker::from(cancelled_wakes.clone());
    let waiting_waker = Waker::from(waiting_wakes.clone());

    assert!(poll_with_waker(cancelled.as_mut(), &cancelled_waker).is_pending());
    assert!(poll_with_waker(waiting.as_mut(), &waiting_waker).is_pending());
    sender.send(1).unwrap();
    assert_eq!(cancelled_wakes.count(), 1);
    assert_eq!(waiting_wakes.count(), 0);
    drop(cancelled);

    assert_eq!(waiting_wakes.count(), 1);
    assert_eq!(
        expect_ready(poll_with_waker(waiting.as_mut(), &waiting_waker)),
        Ok(1)
    );
}

#[test]
fn bounded_cancelled_notified_receiver_passes_value_to_next_receiver() {
    let (sender, receiver) = mpmc::bounded(1);
    let competing = receiver.clone();
    let mut cancelled = Box::pin(receiver.recv());
    let mut waiting = Box::pin(competing.recv());
    let cancelled_wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waiting_wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let cancelled_waker = Waker::from(cancelled_wakes.clone());
    let waiting_waker = Waker::from(waiting_wakes.clone());

    assert!(poll_with_waker(cancelled.as_mut(), &cancelled_waker).is_pending());
    assert!(poll_with_waker(waiting.as_mut(), &waiting_waker).is_pending());
    sender.try_send(1).unwrap();
    assert_eq!(cancelled_wakes.count(), 1);
    assert_eq!(waiting_wakes.count(), 0);
    drop(cancelled);

    assert_eq!(waiting_wakes.count(), 1);
    assert_eq!(
        expect_ready(poll_with_waker(waiting.as_mut(), &waiting_waker)),
        Ok(1)
    );
}

#[test]
fn bounded_cancelled_notified_sender_passes_capacity_to_next_sender() {
    let (sender, receiver) = mpmc::bounded(1);
    sender.try_send(0).unwrap();
    let first_sender = sender.clone();
    let second_sender = sender.clone();
    let mut cancelled = Box::pin(first_sender.send(1));
    let mut waiting = Box::pin(second_sender.send(2));
    let cancelled_wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waiting_wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let cancelled_waker = Waker::from(cancelled_wakes.clone());
    let waiting_waker = Waker::from(waiting_wakes.clone());

    assert!(poll_with_waker(cancelled.as_mut(), &cancelled_waker).is_pending());
    assert!(poll_with_waker(waiting.as_mut(), &waiting_waker).is_pending());
    assert_eq!(receiver.try_recv(), Ok(0));
    assert_eq!(cancelled_wakes.count(), 1);
    assert_eq!(waiting_wakes.count(), 0);
    drop(cancelled);

    assert_eq!(waiting_wakes.count(), 1);
    assert_eq!(
        expect_ready(poll_with_waker(waiting.as_mut(), &waiting_waker)),
        Ok(())
    );
    assert_eq!(receiver.try_recv(), Ok(2));
}

#[test]
fn last_endpoint_wakes_all_opposite_waiters() {
    let (sender, receiver) = mpmc::bounded(1);
    sender.try_send(0).unwrap();
    let sender_clone = sender.clone();
    let mut first_send = Box::pin(sender.send(1));
    let mut second_send = Box::pin(sender_clone.send(2));
    assert!(poll_once(first_send.as_mut()).is_pending());
    assert!(poll_once(second_send.as_mut()).is_pending());
    drop(receiver);
    assert_eq!(
        expect_ready(poll_once(first_send.as_mut()))
            .unwrap_err()
            .into_inner(),
        1
    );
    assert_eq!(
        expect_ready(poll_once(second_send.as_mut()))
            .unwrap_err()
            .into_inner(),
        2
    );

    let (sender, receiver) = mpmc::unbounded::<usize>();
    let competing = receiver.clone();
    let mut first_recv = Box::pin(receiver.recv());
    let mut second_recv = Box::pin(competing.recv());
    assert!(poll_once(first_recv.as_mut()).is_pending());
    assert!(poll_once(second_recv.as_mut()).is_pending());
    drop(sender);
    assert_eq!(
        expect_ready(poll_once(first_recv.as_mut())),
        Err(RecvError::Disconnected)
    );
    assert_eq!(
        expect_ready(poll_once(second_recv.as_mut())),
        Err(RecvError::Disconnected)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_values_are_delivered_exactly_once_under_contention() {
    const PRODUCERS: usize = 8;
    const CONSUMERS: usize = 8;
    const VALUES_PER_PRODUCER: usize = 512;
    const TOTAL: usize = PRODUCERS * VALUES_PER_PRODUCER;

    let (sender, receiver) = mpmc::bounded(32);
    let consumers = (0..CONSUMERS)
        .map(|_| {
            let receiver = receiver.clone();
            tokio::spawn(async move {
                let mut values = Vec::new();
                while let Ok(value) = receiver.recv().await {
                    values.push(value);
                }
                values
            })
        })
        .collect::<Vec<_>>();
    drop(receiver);

    let producers = (0..PRODUCERS)
        .map(|producer| {
            let sender = sender.clone();
            tokio::spawn(async move {
                let first = producer * VALUES_PER_PRODUCER;
                for value in first..first + VALUES_PER_PRODUCER {
                    sender.send(value).await.unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    drop(sender);

    for producer in producers {
        producer.await.unwrap();
    }
    let mut received = Vec::with_capacity(TOTAL);
    for consumer in consumers {
        received.extend(
            tokio::time::timeout(Duration::from_secs(10), consumer)
                .await
                .expect("bounded consumers must make progress")
                .unwrap(),
        );
    }
    received.sort_unstable();
    assert_eq!(received, (0..TOTAL).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unbounded_values_are_delivered_exactly_once_under_contention() {
    const PRODUCERS: usize = 8;
    const CONSUMERS: usize = 8;
    const VALUES_PER_PRODUCER: usize = 512;
    const TOTAL: usize = PRODUCERS * VALUES_PER_PRODUCER;

    let (sender, receiver) = mpmc::unbounded();
    let consumers = (0..CONSUMERS)
        .map(|_| {
            let receiver = receiver.clone();
            tokio::spawn(async move {
                let mut values = Vec::new();
                while let Ok(value) = receiver.recv().await {
                    values.push(value);
                }
                values
            })
        })
        .collect::<Vec<_>>();
    drop(receiver);

    let producers = (0..PRODUCERS)
        .map(|producer| {
            let sender = sender.clone();
            tokio::spawn(async move {
                let first = producer * VALUES_PER_PRODUCER;
                for value in first..first + VALUES_PER_PRODUCER {
                    sender.send(value).unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    drop(sender);

    for producer in producers {
        producer.await.unwrap();
    }
    let mut received = Vec::with_capacity(TOTAL);
    for consumer in consumers {
        received.extend(
            tokio::time::timeout(Duration::from_secs(10), consumer)
                .await
                .expect("unbounded consumers must make progress")
                .unwrap(),
        );
    }
    received.sort_unstable();
    assert_eq!(received, (0..TOTAL).collect::<Vec<_>>());
}
