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

use asyncband::broadcast::mpmc;
use asyncband::broadcast::spmc;
use tokio_test::assert_pending;
use tokio_test::assert_ready_eq;
use tokio_test::task::spawn;

mod support;
use support::TrackWake;
use support::callback_waker;
use support::poll_with_waker;

#[test]
fn bounded_capacity_is_strict_and_gated_by_the_slowest_subscription() {
    let (tx, mut first) = mpmc::bounded(3);
    let mut second = tx.subscribe();

    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    tx.try_send(3).unwrap();
    assert_eq!(tx.try_send(4), Err(mpmc::TrySendError::Full(4)));

    assert_eq!(first.try_recv(), Ok(1));
    assert_eq!(tx.try_send(4), Err(mpmc::TrySendError::Full(4)));
    assert_eq!(second.try_recv(), Ok(1));
    tx.try_send(4).unwrap();
}

#[test]
fn bounded_send_wakes_only_after_retention_is_reclaimed() {
    let (tx, mut first) = mpmc::bounded(1);
    let mut second = tx.subscribe();
    tx.try_send(1).unwrap();

    let mut send = spawn(tx.send(2));
    assert_pending!(send.poll());
    assert_eq!(first.try_recv(), Ok(1));
    assert_pending!(send.poll());
    assert_eq!(second.try_recv(), Ok(1));
    assert_ready_eq!(send.poll(), Ok(()));

    assert_eq!(first.try_recv(), Ok(2));
    assert_eq!(second.try_recv(), Ok(2));
}

#[test]
fn dropping_the_slowest_subscription_releases_bounded_capacity() {
    let (tx, mut fast) = mpmc::bounded(1);
    let slow = tx.subscribe();
    tx.try_send(1).unwrap();
    assert_eq!(fast.try_recv(), Ok(1));

    let mut send = spawn(tx.send(2));
    assert_pending!(send.poll());
    drop(slow);
    assert_ready_eq!(send.poll(), Ok(()));
    assert_eq!(fast.try_recv(), Ok(2));
}

#[test]
fn cancelling_a_bounded_send_does_not_publish() {
    let (tx, mut rx) = mpmc::bounded(1);
    tx.try_send(1).unwrap();
    let mut cancelled = spawn(tx.send(2));
    assert_pending!(cancelled.poll());
    drop(cancelled);

    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Err(mpmc::TryRecvError::Empty));
    tx.try_send(3).unwrap();
    assert_eq!(rx.try_recv(), Ok(3));
}

#[test]
fn unbounded_retains_until_every_subscription_advances() {
    let (tx, mut first) = mpmc::unbounded();
    let mut second = tx.subscribe();
    tx.send(1).unwrap();
    tx.send(2).unwrap();

    assert_eq!(first.try_recv(), Ok(1));
    assert_eq!(first.try_recv(), Ok(2));
    assert_eq!(tx.buffer_len(), 2);
    assert_eq!(second.try_recv(), Ok(1));
    assert_eq!(tx.buffer_len(), 1);
    assert_eq!(second.try_recv(), Ok(2));
    assert_eq!(tx.buffer_len(), 0);
}

#[test]
fn subscription_counts_and_backlogs_track_each_receiver() {
    let (tx, mut first) = mpmc::unbounded();
    assert_eq!(tx.receiver_count(), 1);
    assert!(first.is_empty());

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    let mut second = tx.subscribe();
    tx.send(3).unwrap();
    assert_eq!(tx.receiver_count(), 2);
    assert_eq!(first.len(), 3);
    assert_eq!(second.len(), 1);

    assert_eq!(second.try_recv(), Ok(3));
    assert!(second.is_empty());
    drop(second);
    assert_eq!(tx.receiver_count(), 1);
    assert_eq!(first.try_recv(), Ok(1));
    assert_eq!(first.len(), 2);
}

#[test]
fn a_sender_can_create_a_new_subscription_after_all_previous_ones_drop() {
    let (tx, rx) = mpmc::unbounded();
    drop(rx);
    assert_eq!(tx.send(1).unwrap_err().into_inner(), 1);
    assert_eq!(tx.buffer_len(), 0);

    let mut replacement = tx.subscribe();
    tx.send(2).unwrap();
    assert_eq!(replacement.try_recv(), Ok(2));
}

#[test]
fn new_subscriptions_start_at_the_committed_tail() {
    let (tx, mut first) = mpmc::unbounded();
    tx.send(1).unwrap();
    let mut second = tx.subscribe();
    tx.send(2).unwrap();

    assert_eq!(first.try_recv(), Ok(1));
    assert_eq!(first.try_recv(), Ok(2));
    assert_eq!(second.try_recv(), Ok(2));
    assert_eq!(second.try_recv(), Err(mpmc::TryRecvError::Empty));
}

#[tokio::test]
async fn accepted_values_drain_before_disconnection() {
    let (mut tx, mut rx) = spmc::unbounded();
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    drop(tx);

    assert_eq!(rx.recv().await, Ok(1));
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx.recv().await, Err(spmc::RecvError::Disconnected));
}

#[test]
fn sends_return_the_value_after_the_last_subscription_drops() {
    let (tx, rx) = mpmc::bounded(1);
    drop(rx);
    assert_eq!(tx.try_send(1), Err(mpmc::TrySendError::Disconnected(1)));

    let (tx, rx) = mpmc::unbounded();
    drop(rx);
    assert_eq!(tx.send(2).unwrap_err().into_inner(), 2);
}

#[test]
fn bounded_capacity_must_be_positive() {
    assert!(std::panic::catch_unwind(|| mpmc::bounded::<()>(0)).is_err());
    assert!(std::panic::catch_unwind(|| spmc::bounded::<()>(0)).is_err());
}

#[test]
fn concurrent_publishers_expose_one_order_to_every_subscription() {
    const PRODUCERS: usize = 4;
    const VALUES_PER_PRODUCER: usize = 1_000;

    let (tx, mut first) = mpmc::unbounded();
    let mut second = tx.subscribe();
    std::thread::scope(|scope| {
        for producer in 0..PRODUCERS {
            let sender = tx.clone();
            scope.spawn(move || {
                let start = producer * VALUES_PER_PRODUCER;
                for value in start..start + VALUES_PER_PRODUCER {
                    sender.send(value).unwrap();
                }
            });
        }
    });

    let expected = PRODUCERS * VALUES_PER_PRODUCER;
    let first = (0..expected)
        .map(|_| first.try_recv().unwrap())
        .collect::<Vec<_>>();
    let second = (0..expected)
        .map(|_| second.try_recv().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(first, second);
}

#[test]
fn concurrent_publishers_deliver_every_value_to_every_subscription() {
    const PRODUCERS: usize = 4;
    const SUBSCRIPTIONS: usize = 4;
    const VALUES_PER_PRODUCER: usize = 500;

    let (tx, first) = mpmc::unbounded();
    let mut receivers = vec![first];
    for _ in 1..SUBSCRIPTIONS {
        receivers.push(tx.subscribe());
    }

    let drains = receivers
        .into_iter()
        .map(|mut receiver| {
            std::thread::spawn(move || {
                let mut values = Vec::new();
                while let Ok(value) = pollster::block_on(receiver.recv()) {
                    values.push(value);
                }
                values
            })
        })
        .collect::<Vec<_>>();

    std::thread::scope(|scope| {
        for producer in 0..PRODUCERS {
            let sender = tx.clone();
            scope.spawn(move || {
                let start = producer * VALUES_PER_PRODUCER;
                for value in start..start + VALUES_PER_PRODUCER {
                    sender.send(value).unwrap();
                }
            });
        }
    });
    drop(tx);

    let expected = (0..PRODUCERS * VALUES_PER_PRODUCER).collect::<Vec<_>>();
    for drain in drains {
        let mut values = drain.join().unwrap();
        values.sort_unstable();
        assert_eq!(values, expected);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_concurrent_publishers_make_progress_in_one_shared_order() {
    const PRODUCERS: usize = 4;
    const VALUES_PER_PRODUCER: usize = 500;
    let expected = PRODUCERS * VALUES_PER_PRODUCER;

    let (tx, mut first) = mpmc::bounded(7);
    let mut second = tx.subscribe();
    let first_drain = tokio::spawn(async move {
        let mut values = Vec::with_capacity(expected);
        for _ in 0..expected {
            values.push(first.recv().await.unwrap());
        }
        values
    });
    let second_drain = tokio::spawn(async move {
        let mut values = Vec::with_capacity(expected);
        for _ in 0..expected {
            values.push(second.recv().await.unwrap());
        }
        values
    });

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

    let first = first_drain.await.unwrap();
    let second = second_drain.await.unwrap();
    assert_eq!(first, second);
    let mut values = first;
    values.sort_unstable();
    assert_eq!(values, (0..expected).collect::<Vec<_>>());
}

#[derive(Debug)]
struct CloneProbe {
    value: usize,
    clones: Arc<AtomicUsize>,
}

impl Clone for CloneProbe {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, Ordering::Relaxed);
        Self {
            value: self.value,
            clones: self.clones.clone(),
        }
    }
}

#[test]
fn single_subscription_receive_moves_the_payload_without_cloning() {
    let clones = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpmc::unbounded();
    tx.send(CloneProbe {
        value: 7,
        clones: clones.clone(),
    })
    .unwrap();

    assert_eq!(rx.try_recv().unwrap().value, 7);
    assert_eq!(clones.load(Ordering::Relaxed), 0);
}

struct ReentrantDrop(Option<Box<dyn FnOnce() + Send + Sync>>);

impl Clone for ReentrantDrop {
    fn clone(&self) -> Self {
        Self(None)
    }
}

impl Drop for ReentrantDrop {
    fn drop(&mut self) {
        if let Some(callback) = self.0.take() {
            callback();
        }
    }
}

#[test]
fn reclaimed_payloads_are_dropped_after_unlocking_the_channel() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (tx, mut fast) = mpmc::unbounded();
        let slow = tx.subscribe();
        let callback_sender = tx.clone();
        tx.send(ReentrantDrop(Some(Box::new(move || {
            let _ = callback_sender.receiver_count();
        }))))
        .unwrap();

        drop(fast.try_recv().unwrap());
        drop(slow);
        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("payload destructor deadlocked against the broadcast lock");
}

#[derive(Debug)]
struct PanicOnClone {
    value: usize,
    panic: bool,
}

impl Clone for PanicOnClone {
    fn clone(&self) -> Self {
        assert!(!self.panic, "panic while cloning a broadcast value");
        Self {
            value: self.value,
            panic: self.panic,
        }
    }
}

#[test]
fn panicking_payload_clone_leaves_the_channel_consistent() {
    let (tx, mut first) = mpmc::unbounded();
    let mut second = tx.subscribe();
    tx.send(PanicOnClone {
        value: 1,
        panic: true,
    })
    .unwrap();
    tx.send(PanicOnClone {
        value: 2,
        panic: false,
    })
    .unwrap();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| first.try_recv()));
    assert!(result.is_err());
    assert_eq!(first.try_recv().unwrap().value, 2);
    assert_eq!(second.try_recv().unwrap().value, 1);
    assert_eq!(second.try_recv().unwrap().value, 2);
    assert_eq!(tx.buffer_len(), 0);
}

#[test]
fn cancelled_broadcast_operations_release_their_wakers() {
    let (tx, mut rx) = mpmc::bounded::<usize>(1);
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
fn broadcast_wakers_run_after_unlocking_the_channel() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (tx, mut rx) = mpmc::unbounded();
        let callback_sender = tx.clone();
        let waker = callback_waker(move || {
            callback_sender.send(2).unwrap();
        });
        let mut recv = Box::pin(rx.recv());

        assert!(poll_with_waker(recv.as_mut(), &waker).is_pending());
        tx.send(1).unwrap();
        assert_eq!(poll_with_waker(recv.as_mut(), &waker), Poll::Ready(Ok(1)));
        drop(recv);
        assert_eq!(rx.try_recv(), Ok(2));
        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("waker callback deadlocked against the broadcast lock");
}

#[test]
fn parked_subscription_wakes_when_the_last_sender_drops() {
    let (tx, mut rx) = mpmc::unbounded::<()>();
    let other = tx.clone();
    let mut recv = spawn(rx.recv());
    assert_pending!(recv.poll());

    drop(tx);
    assert!(!recv.is_woken());
    drop(other);
    assert!(recv.is_woken());
    assert_ready_eq!(recv.poll(), Err(mpmc::RecvError::Disconnected));
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, upper: u64) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value % upper
    }
}

#[test]
fn randomized_subscription_operations_match_a_reference_model() {
    for seed in 1..32u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let (tx, rx) = mpmc::unbounded::<u64>();
        let mut tail = 0u64;
        let mut model = vec![(rx, 0u64)];

        for _ in 0..512 {
            match rng.below(100) {
                0..=44 => {
                    if model.is_empty() {
                        assert_eq!(tx.send(tail).unwrap_err().into_inner(), tail);
                    } else {
                        tx.send(tail).unwrap();
                        tail += 1;
                    }
                }
                45..=79 if !model.is_empty() => {
                    let index = rng.below(model.len() as u64) as usize;
                    let (receiver, cursor) = &mut model[index];
                    if *cursor < tail {
                        assert_eq!(receiver.try_recv(), Ok(*cursor), "seed {seed}");
                        *cursor += 1;
                    } else {
                        assert_eq!(
                            receiver.try_recv(),
                            Err(mpmc::TryRecvError::Empty),
                            "seed {seed}"
                        );
                    }
                }
                80..=89 => model.push((tx.subscribe(), tail)),
                _ if !model.is_empty() => {
                    let index = rng.below(model.len() as u64) as usize;
                    model.swap_remove(index);
                }
                _ => {}
            }

            assert_eq!(tx.receiver_count(), model.len(), "seed {seed}");
            let retained = model
                .iter()
                .map(|(_, cursor)| *cursor)
                .min()
                .map_or(0, |slowest| tail - slowest);
            assert_eq!(tx.buffer_len(), retained as usize, "seed {seed}");
            for (receiver, cursor) in &model {
                assert_eq!(receiver.len(), (tail - cursor) as usize, "seed {seed}");
                assert_eq!(receiver.is_empty(), *cursor == tail, "seed {seed}");
            }
        }
    }
}
