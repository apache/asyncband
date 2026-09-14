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

// The following MPMC cases are omitted on purpose: a non-cloneable `&mut self` sender makes them
// unrepresentable, not untested.
//
// * `concurrent_senders_deliver_every_message_to_every_receiver` — there is no second producer;
//   committed order is program order, covered by `publish_order_is_program_order`.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread;

use asyncband::broadcast::spmc::*;
use tests_integration::WakeCounter;
use tests_integration::assert_completes_without_deadlock;

/// A payload whose destructor re-enters the channel it was sent through.
///
/// The sender is not `Clone`, so the probe is a shared receiver handle.
struct Reentrant {
    value: u64,
    probe: Option<Arc<Mutex<UnboundedReceiver<Reentrant>>>>,
}

impl Clone for Reentrant {
    fn clone(&self) -> Self {
        Self {
            value: self.value,
            probe: self.probe.clone(),
        }
    }
}

impl Drop for Reentrant {
    fn drop(&mut self) {
        if let Some(probe) = &self.probe {
            // `try_lock`: draining the probe itself may drop a payload while this mutex is held.
            // Deadlocks if the channel still holds its lock while dropping reclaimed messages.
            if let Ok(probe) = probe.try_lock() {
                // `resubscribe` takes the waiter mutex; `unread_message_count` does not.
                let _ = probe.resubscribe();
            }
        }
    }
}

/// A payload that panics while a shared receive clones it.
#[derive(Debug)]
struct PanicOnClone {
    value: u64,
    panic: bool,
}

impl Clone for PanicOnClone {
    fn clone(&self) -> Self {
        if self.panic {
            panic!("panic while cloning a broadcast message");
        }
        Self {
            value: self.value,
            panic: self.panic,
        }
    }
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x % n
    }
}

#[test]
fn try_recv_reports_empty_then_value_then_disconnected() {
    let (mut tx, mut rx) = unbounded();

    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    tx.send(10);
    assert_eq!(rx.try_recv(), Ok(10));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    drop(tx);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[tokio::test]
async fn slow_receiver_keeps_every_message() {
    let (mut tx, mut rx1) = unbounded();
    let mut rx2 = tx.subscribe();

    for i in 0..1024 {
        tx.send(i);
    }

    // The fast receiver draining fully must not reclaim anything the slow one still needs.
    for i in 0..1024 {
        assert_eq!(rx1.recv().await, Ok(i));
    }
    assert_eq!(tx.retained_message_count(), 1024);

    for i in 0..1024 {
        assert_eq!(rx2.recv().await, Ok(i));
    }
    assert_eq!(tx.retained_message_count(), 0);
}

#[tokio::test]
async fn retained_message_count_tracks_the_slowest_receiver() {
    let (mut tx, mut rx1) = unbounded();
    let mut rx2 = tx.subscribe();

    tx.send(1);
    tx.send(2);
    assert_eq!(tx.retained_message_count(), 2);

    // Reclaiming waits for the slowest receiver, message by message.
    assert_eq!(rx1.recv().await, Ok(1));
    assert_eq!(tx.retained_message_count(), 2);
    assert_eq!(rx2.recv().await, Ok(1));
    assert_eq!(tx.retained_message_count(), 1);

    assert_eq!(rx1.recv().await, Ok(2));
    assert_eq!(tx.retained_message_count(), 1);
    assert_eq!(rx2.recv().await, Ok(2));
    assert_eq!(tx.retained_message_count(), 0);
}

#[tokio::test]
async fn dropping_a_lagging_receiver_releases_its_backlog() {
    let (mut tx, mut rx1) = unbounded();
    let rx2 = tx.subscribe();

    for i in 0..128 {
        tx.send(i);
    }
    for i in 0..128 {
        assert_eq!(rx1.recv().await, Ok(i));
    }
    assert_eq!(tx.retained_message_count(), 128);

    drop(rx2);
    assert_eq!(tx.retained_message_count(), 0);
}

#[tokio::test]
async fn resubscribe_keeps_the_original_receivers_backlog() {
    let (mut tx, mut rx) = unbounded();

    tx.send(1);
    tx.send(2);

    let mut rx2 = rx.resubscribe();
    assert_eq!(tx.retained_message_count(), 2);

    tx.send(3);

    assert_eq!(rx2.recv().await, Ok(3));
    assert_eq!(tx.retained_message_count(), 3);

    assert_eq!(rx.recv().await, Ok(1));
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx.recv().await, Ok(3));
    assert_eq!(tx.retained_message_count(), 0);
}

#[tokio::test]
async fn send_without_receivers_does_not_buffer() {
    let (mut tx, rx) = unbounded();
    drop(rx);

    tx.send(1);
    tx.send(2);
    assert_eq!(tx.retained_message_count(), 0);

    let mut rx = tx.subscribe();
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    tx.send(3);
    assert_eq!(rx.recv().await, Ok(3));
}

#[test]
fn unread_message_count_tracks_each_receiver() {
    let (mut tx, mut rx1) = unbounded();
    assert_eq!(rx1.unread_message_count(), 0);

    tx.send(1);
    tx.send(2);
    assert_eq!(rx1.unread_message_count(), 2);

    let mut rx2 = tx.subscribe();
    assert_eq!(rx2.unread_message_count(), 0);

    tx.send(3);
    assert_eq!(rx1.unread_message_count(), 3);
    assert_eq!(rx2.unread_message_count(), 1);

    assert_eq!(rx2.try_recv(), Ok(3));
    assert_eq!(rx2.unread_message_count(), 0);
    drop(rx2);

    assert_eq!(rx1.try_recv(), Ok(1));
    assert_eq!(rx1.unread_message_count(), 2);
}

#[test]
fn sole_receiver_takes_messages_without_cloning() {
    static CLONES: AtomicUsize = AtomicUsize::new(0);

    struct CountClone(u32);

    impl Clone for CountClone {
        fn clone(&self) -> Self {
            CLONES.fetch_add(1, Ordering::Relaxed);
            Self(self.0)
        }
    }

    let (mut tx, mut rx) = unbounded();
    for i in 0..8 {
        tx.send(CountClone(i));
        assert_eq!(rx.try_recv().unwrap().0, i);
    }
    assert_eq!(CLONES.load(Ordering::Relaxed), 0);

    // A second receiver means the payload is shared, so it has to be cloned again.
    let mut second = tx.subscribe();
    tx.send(CountClone(8));
    assert_eq!(rx.try_recv().unwrap().0, 8);
    assert_eq!(second.try_recv().unwrap().0, 8);
    assert_eq!(CLONES.load(Ordering::Relaxed), 1);
}

#[test]
fn panicking_clone_leaves_the_channel_consistent() {
    let (mut tx, mut rx1) = unbounded();
    let mut rx2 = tx.subscribe();

    tx.send(PanicOnClone {
        value: 1,
        panic: true,
    });
    tx.send(PanicOnClone {
        value: 2,
        panic: false,
    });

    // Two receivers share the payload, so this receive has to clone it.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rx1.try_recv().map(|msg| msg.value)
    }));
    assert!(result.is_err());

    // The failed receive still consumed the message for `rx1`, and left the channel usable for
    // both receivers.
    assert_eq!(rx1.try_recv().unwrap().value, 2);
    assert_eq!(rx2.try_recv().unwrap().value, 1);
    assert_eq!(rx2.try_recv().unwrap().value, 2);
    assert_eq!(tx.retained_message_count(), 0);
    assert_eq!(rx1.try_recv().unwrap_err(), TryRecvError::Empty);
}

#[test]
fn message_destructors_run_outside_the_channel_lock() {
    assert_completes_without_deadlock(|| {
        let (mut tx, mut rx1) = unbounded();
        let rx2 = tx.subscribe();
        let probe = Arc::new(Mutex::new(tx.subscribe()));

        for value in 0..4 {
            tx.send(Reentrant {
                value,
                probe: Some(probe.clone()),
            });
            // Keep the probe at the tail so it does not retain the messages under test.
            let _ = probe.lock().unwrap().try_recv();
        }

        // Reclaim through a receive, and then through a receiver drop.
        assert_eq!(rx1.try_recv().unwrap().value, 0);
        drop(rx2);
        assert_eq!(rx1.try_recv().unwrap().value, 1);
        drop(rx1);
        drop(probe);
    });
}

#[test]
fn send_wakes_a_parked_receiver_exactly_once() {
    let (mut tx, mut rx) = unbounded();
    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker.clone());
    let mut context = Context::from_waker(&waker);
    let mut recv = Box::pin(rx.recv());

    assert!(recv.as_mut().poll(&mut context).is_pending());

    tx.send(42);

    assert_eq!(tracker.count(), 1);
    assert_eq!(recv.as_mut().poll(&mut context), Poll::Ready(Ok(42)));
}

#[test]
fn cancelled_recv_releases_its_waker() {
    let (mut tx, mut rx) = unbounded::<()>();
    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker.clone());
    let baseline = Arc::strong_count(&tracker);
    let mut context = Context::from_waker(&waker);
    let mut recv = Box::pin(rx.recv());

    assert!(recv.as_mut().poll(&mut context).is_pending());
    assert_eq!(Arc::strong_count(&tracker), baseline + 1);

    drop(recv);
    assert_eq!(Arc::strong_count(&tracker), baseline);

    tx.send(());
    assert_eq!(tracker.count(), 0);
    assert_eq!(rx.try_recv(), Ok(()));
}

#[test]
fn dropping_a_woken_recv_keeps_another_receivers_waiter() {
    let (mut tx, mut rx1) = unbounded::<i32>();
    let mut rx2 = tx.subscribe();
    let first = Arc::new(WakeCounter::default());
    let waker = Waker::from(first.clone());
    let mut context = Context::from_waker(&waker);
    let mut recv1 = Box::pin(rx1.recv());

    assert!(recv1.as_mut().poll(&mut context).is_pending());

    tx.send(1);
    assert_eq!(first.count(), 1);
    assert_eq!(rx2.try_recv(), Ok(1));

    let second = Arc::new(WakeCounter::default());
    let waker = Waker::from(second.clone());
    let mut context = Context::from_waker(&waker);
    let mut recv2 = Box::pin(rx2.recv());
    assert!(recv2.as_mut().poll(&mut context).is_pending());

    // `recv1` was already woken, so dropping it must not release the slot `recv2` now owns.
    drop(recv1);
    tx.send(2);

    assert_eq!(second.count(), 1);
}

#[test]
fn parked_recv_wakes_when_the_sender_drops() {
    let (tx, mut rx) = unbounded::<()>();
    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker.clone());
    let mut context = Context::from_waker(&waker);
    let mut recv = Box::pin(rx.recv());

    assert!(recv.as_mut().poll(&mut context).is_pending());

    drop(tx);
    assert_eq!(tracker.count(), 1);

    drop(recv);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn parked_recv_prefers_buffered_messages_over_disconnection() {
    let (mut tx, mut rx) = unbounded();
    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker);
    let mut context = Context::from_waker(&waker);
    let mut recv = Box::pin(rx.recv());

    assert!(recv.as_mut().poll(&mut context).is_pending());

    tx.send(7);
    drop(tx);

    assert_eq!(recv.as_mut().poll(&mut context), Poll::Ready(Ok(7)));
    drop(recv);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[tokio::test]
async fn recv_drains_buffered_messages_before_reporting_disconnection() {
    let (mut tx, mut rx) = unbounded();

    tx.send(1);
    tx.send(2);
    drop(tx);

    assert_eq!(rx.recv().await, Ok(1));
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[tokio::test]
async fn recv_reports_disconnection_without_any_message() {
    let (tx, mut rx) = unbounded::<()>();
    drop(tx);
    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[test]
fn concurrent_receivers_drain_a_published_batch() {
    const RECEIVERS: usize = 8;
    const MESSAGES: usize = 256;

    let (mut tx, rx) = unbounded();
    let mut receivers = vec![rx];
    for _ in 1..RECEIVERS {
        receivers.push(tx.subscribe());
    }
    for value in 0..MESSAGES as u64 {
        tx.send(value);
    }

    let expected = (MESSAGES as u64 - 1) * MESSAGES as u64 / 2;
    let handles: Vec<_> = receivers
        .into_iter()
        .map(|mut rx| {
            thread::spawn(move || {
                let mut sum = 0u64;
                for _ in 0..MESSAGES {
                    sum += rx.try_recv().unwrap();
                }
                assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
                sum
            })
        })
        .collect();

    for handle in handles {
        assert_eq!(handle.join().unwrap(), expected);
    }
}

#[test]
fn publish_order_is_program_order() {
    let (mut tx, mut rx1) = unbounded();
    let mut rx2 = tx.subscribe();

    for value in 0..64 {
        tx.send(value);
    }

    for value in 0..64 {
        assert_eq!(rx1.try_recv(), Ok(value));
        assert_eq!(rx2.try_recv(), Ok(value));
    }
}

#[test]
fn randomized_operations_track_the_reference_model() {
    for seed in 1..32u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let (mut tx, rx) = unbounded::<u64>();
        let mut tail = 0u64;
        let mut model = vec![(rx, 0u64)];

        for _ in 0..512 {
            match rng.below(100) {
                0..=44 => {
                    tx.send(tail);
                    tail += 1;
                }
                45..=79 if !model.is_empty() => {
                    let index = rng.below(model.len() as u64) as usize;
                    let (receiver, cursor) = &mut model[index];
                    if *cursor < tail {
                        assert_eq!(receiver.try_recv(), Ok(*cursor), "seed {seed}");
                        *cursor += 1;
                    } else {
                        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty), "seed {seed}");
                    }
                }
                80..=89 => model.push((tx.subscribe(), tail)),
                _ if !model.is_empty() => {
                    let index = rng.below(model.len() as u64) as usize;
                    model.swap_remove(index);
                }
                _ => {}
            }

            let retained = model
                .iter()
                .map(|(_, cursor)| *cursor)
                .min()
                .map_or(0, |slowest| tail - slowest);
            assert_eq!(
                tx.retained_message_count(),
                retained as usize,
                "seed {seed}"
            );
            for (receiver, cursor) in &model {
                assert_eq!(
                    receiver.unread_message_count(),
                    (tail - cursor) as usize,
                    "seed {seed}"
                );
            }
        }
    }
}
