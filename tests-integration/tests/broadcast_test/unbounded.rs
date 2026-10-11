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
use std::task::Context;
use std::task::Waker;
use std::thread;

use asyncband::blocking::FutureExt;
use asyncband::broadcast::mpmc::*;
use tests_integration::WakeCounter;
use tests_integration::assert_completes_without_deadlock;
use tests_integration::expect_ready;
use tests_integration::poll_with;

/// A payload whose destructor re-enters the channel it was sent through.
struct Reentrant {
    value: u64,
    channel: Option<UnboundedSender<Reentrant>>,
}

impl Clone for Reentrant {
    fn clone(&self) -> Self {
        Self {
            value: self.value,
            channel: self.channel.clone(),
        }
    }
}

impl Drop for Reentrant {
    fn drop(&mut self) {
        if let Some(channel) = &self.channel {
            // Deadlocks if the channel still holds its lock while dropping reclaimed messages.
            let _ = channel.retained_message_count();
        }
    }
}

/// A payload that panics when a receiver clones it.
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
fn test_try_recv() {
    let (tx, mut rx) = unbounded();

    // Empty
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    // Success
    tx.send(10);
    assert_eq!(rx.try_recv().as_deref(), Ok(&10));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    // Disconnected
    drop(tx);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[tokio::test]
async fn test_slow_receiver_keeps_every_message() {
    let (tx, mut rx1) = unbounded();
    let mut rx2 = tx.subscribe();

    for i in 0..1024 {
        tx.send(i);
    }

    // The fast receiver draining fully must not reclaim anything the slow one still needs.
    for i in 0..1024 {
        assert_eq!(rx1.recv().await.as_deref(), Ok(&i));
    }
    assert_eq!(tx.retained_message_count(), 1024);

    for i in 0..1024 {
        assert_eq!(rx2.recv().await.as_deref(), Ok(&i));
    }
    assert_eq!(tx.retained_message_count(), 0);
}

#[tokio::test]
async fn retained_message_count_tracks_the_slowest_receiver() {
    let (tx, mut rx1) = unbounded();
    let mut rx2 = tx.subscribe();

    tx.send(1);
    tx.send(2);
    assert_eq!(tx.retained_message_count(), 2);

    // Reclaiming waits for the slowest receiver, message by message.
    assert_eq!(rx1.recv().await.as_deref(), Ok(&1));
    assert_eq!(tx.retained_message_count(), 2);
    assert_eq!(rx2.recv().await.as_deref(), Ok(&1));
    assert_eq!(tx.retained_message_count(), 1);

    assert_eq!(rx1.recv().await.as_deref(), Ok(&2));
    assert_eq!(tx.retained_message_count(), 1);
    assert_eq!(rx2.recv().await.as_deref(), Ok(&2));
    assert_eq!(tx.retained_message_count(), 0);
}

#[tokio::test]
async fn test_dropping_a_lagging_receiver_releases_its_backlog() {
    let (tx, mut rx1) = unbounded();
    let rx2 = tx.subscribe();

    for i in 0..128 {
        tx.send(i);
    }
    for i in 0..128 {
        assert_eq!(rx1.recv().await.as_deref(), Ok(&i));
    }
    assert_eq!(tx.retained_message_count(), 128);

    drop(rx2);
    assert_eq!(tx.retained_message_count(), 0);
}

#[tokio::test]
async fn resubscribe_keeps_the_original_receivers_backlog() {
    let (tx, mut rx) = unbounded();

    tx.send(1);
    tx.send(2);

    let mut rx2 = rx.resubscribe();
    assert_eq!(tx.retained_message_count(), 2);

    tx.send(3);

    assert_eq!(rx2.recv().await.as_deref(), Ok(&3));
    assert_eq!(tx.retained_message_count(), 3);

    assert_eq!(rx.recv().await.as_deref(), Ok(&1));
    assert_eq!(rx.recv().await.as_deref(), Ok(&2));
    assert_eq!(rx.recv().await.as_deref(), Ok(&3));
    assert_eq!(tx.retained_message_count(), 0);
}

#[tokio::test]
async fn send_without_receivers_does_not_buffer() {
    let (tx, rx) = unbounded();
    drop(rx);

    tx.send(1);
    tx.send(2);
    assert_eq!(tx.retained_message_count(), 0);

    let mut rx = tx.subscribe();
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    tx.send(3);
    assert_eq!(rx.recv().await.as_deref(), Ok(&3));
}

#[test]
fn unread_message_count_tracks_each_receiver() {
    let (tx, mut rx1) = unbounded();
    assert_eq!(rx1.unread_message_count(), 0);

    tx.send(1);
    tx.send(2);
    assert_eq!(rx1.unread_message_count(), 2);

    let mut rx2 = tx.subscribe();
    assert_eq!(rx2.unread_message_count(), 0);

    tx.send(3);
    assert_eq!(rx1.unread_message_count(), 3);
    assert_eq!(rx2.unread_message_count(), 1);

    assert_eq!(rx2.try_recv().as_deref(), Ok(&3));
    assert_eq!(rx2.unread_message_count(), 0);
    drop(rx2);

    assert_eq!(rx1.try_recv().as_deref(), Ok(&1));
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

    let (tx, mut rx) = unbounded();
    for i in 0..8 {
        tx.send(CountClone(i));
        assert_eq!(rx.try_recv_cloned().unwrap().0, i);
    }
    assert_eq!(CLONES.load(Ordering::Relaxed), 0);

    // A second receiver means the payload is shared, so it has to be cloned again.
    let mut second = tx.subscribe();
    tx.send(CountClone(8));
    assert_eq!(rx.try_recv_cloned().unwrap().0, 8);
    assert_eq!(second.try_recv_cloned().unwrap().0, 8);
    assert_eq!(CLONES.load(Ordering::Relaxed), 1);

    // An `Arc` that outlives the last read keeps the payload shared, so even the read that
    // reclaims the slot has to clone.
    tx.send(CountClone(9));
    let shared = second.try_recv().unwrap();
    assert_eq!(rx.try_recv_cloned().unwrap().0, 9);
    assert_eq!(CLONES.load(Ordering::Relaxed), 2);
    assert_eq!(shared.0, 9);
}

#[tokio::test]
async fn shares_one_arc_across_receivers() {
    // Not `Clone`: fanning out never needs to copy the payload.
    struct Payload(u64);

    let (tx, mut rx1) = unbounded();
    let mut rx2 = tx.subscribe();
    let mut rx3 = tx.subscribe();
    tx.send(Payload(7));

    let first = rx1.recv().await.unwrap();
    let second = rx2.try_recv().unwrap();
    let third = rx3.recv().await.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert!(Arc::ptr_eq(&first, &third));
    assert_eq!(first.0, 7);
    assert_eq!(tx.retained_message_count(), 0);
}

#[tokio::test]
async fn shared_and_cloned_receives_advance_the_same_cursor() {
    let (tx, mut rx) = unbounded();
    for value in 1..=4 {
        tx.send(value);
    }

    assert_eq!(rx.recv().await.as_deref(), Ok(&1));
    assert_eq!(rx.recv_cloned().await, Ok(2));
    assert_eq!(rx.try_recv().as_deref(), Ok(&3));
    assert_eq!(rx.try_recv_cloned(), Ok(4));
    assert_eq!(rx.unread_message_count(), 0);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn panicking_clone_leaves_the_channel_consistent() {
    let (tx, mut rx1) = unbounded();
    let mut rx2 = tx.subscribe();

    tx.send(PanicOnClone {
        value: 1,
        panic: true,
    });
    tx.send(PanicOnClone {
        value: 2,
        panic: false,
    });

    // Two receivers share the payload, so `try_recv_cloned` has to clone it.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rx1.try_recv_cloned().map(|msg| msg.value)
    }));
    assert!(result.is_err());

    // The panicking `try_recv_cloned` call still consumed the message for `rx1`, and left the
    // channel usable for both receivers.
    assert_eq!(rx1.try_recv().unwrap().value, 2);
    assert_eq!(rx2.try_recv().unwrap().value, 1);
    assert_eq!(rx2.try_recv().unwrap().value, 2);
    assert_eq!(tx.retained_message_count(), 0);
    assert_eq!(rx1.try_recv().unwrap_err(), TryRecvError::Empty);
}

#[test]
fn message_destructors_run_outside_the_channel_lock() {
    assert_completes_without_deadlock(|| {
        let (tx, mut rx1) = unbounded();
        let rx2 = tx.subscribe();

        for value in 0..4 {
            tx.send(Reentrant {
                value,
                channel: Some(tx.clone()),
            });
        }

        // Exercise reclamation while receiving messages and dropping receivers.
        assert_eq!(rx1.try_recv().unwrap().value, 0);
        drop(rx2);
        assert_eq!(rx1.try_recv().unwrap().value, 1);
        drop(rx1);

        // With no receiver left, `send` drops the message itself; that must be unlocked too.
        tx.send(Reentrant {
            value: 4,
            channel: Some(tx.clone()),
        });
        drop(tx);
    });
}

#[test]
fn send_wakes_a_parked_receiver_exactly_once() {
    let (tx, mut rx) = unbounded();
    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker.clone());
    let mut context = Context::from_waker(&waker);
    let mut recv = Box::pin(rx.recv());

    assert!(recv.as_mut().poll(&mut context).is_pending());

    tx.send(42);

    assert_eq!(tracker.count(), 1);
    assert_eq!(
        expect_ready(recv.as_mut().poll(&mut context)).as_deref(),
        Ok(&42)
    );
}

#[test]
fn cancelled_recv_releases_its_waker() {
    let (tx, mut rx) = unbounded::<()>();
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
    assert_eq!(rx.try_recv().as_deref(), Ok(&()));
}

#[test]
fn cancelled_recv_cloned_leaves_the_cursor_unchanged() {
    let (tx, mut rx) = unbounded();
    let (waker, counter) = WakeCounter::new();

    let mut recv = Box::pin(rx.recv_cloned());
    assert!(poll_with(recv.as_mut(), &waker).is_pending());
    tx.send(1);
    assert_eq!(counter.count(), 1);

    // The woken future is dropped before it is polled again, so it never advances the cursor.
    drop(recv);
    assert_eq!(rx.unread_message_count(), 1);
    assert_eq!(rx.try_recv_cloned(), Ok(1));
}

#[test]
fn dropping_a_woken_recv_keeps_another_receivers_waiter() {
    let (tx, mut rx1) = unbounded::<i32>();
    let mut rx2 = tx.subscribe();
    let first = Arc::new(WakeCounter::default());
    let waker = Waker::from(first.clone());
    let mut context = Context::from_waker(&waker);
    let mut recv1 = Box::pin(rx1.recv());

    assert!(recv1.as_mut().poll(&mut context).is_pending());

    tx.send(1);
    assert_eq!(first.count(), 1);
    assert_eq!(rx2.try_recv().as_deref(), Ok(&1));

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
fn parked_recv_wakes_when_the_last_sender_drops() {
    let (tx, mut rx) = unbounded::<()>();
    let extra = tx.clone();
    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker.clone());
    let mut context = Context::from_waker(&waker);
    let mut recv = Box::pin(rx.recv());

    assert!(recv.as_mut().poll(&mut context).is_pending());

    drop(tx);
    assert_eq!(tracker.count(), 0);

    drop(extra);
    assert_eq!(tracker.count(), 1);

    drop(recv);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn parked_recv_prefers_buffered_messages_over_disconnection() {
    let (tx, mut rx) = unbounded();
    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker);
    let mut context = Context::from_waker(&waker);
    let mut recv = Box::pin(rx.recv());

    assert!(recv.as_mut().poll(&mut context).is_pending());

    tx.send(7);
    drop(tx);

    assert_eq!(
        expect_ready(recv.as_mut().poll(&mut context)).as_deref(),
        Ok(&7)
    );
    drop(recv);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[tokio::test]
async fn recv_drains_buffered_messages_before_reporting_disconnection() {
    let (tx, mut rx) = unbounded();

    tx.send(1);
    tx.send(2);
    drop(tx);

    assert_eq!(rx.recv().await.as_deref(), Ok(&1));
    assert_eq!(rx.recv().await.as_deref(), Ok(&2));
    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[tokio::test]
async fn recv_reports_disconnection_without_any_message() {
    let (tx, mut rx) = unbounded::<()>();
    drop(tx);
    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[test]
fn concurrent_senders_deliver_every_message_to_every_receiver() {
    const SENDERS: u64 = 4;
    const PER_SENDER: u64 = 512;

    let (tx, rx) = unbounded();
    let mut receivers = Vec::with_capacity(4);
    receivers.push(rx);
    receivers.extend((1..4).map(|_| tx.subscribe()));

    let senders = (0..SENDERS)
        .map(|worker| {
            let tx = tx.clone();
            thread::spawn(move || {
                for value in 0..PER_SENDER {
                    tx.send(worker * PER_SENDER + value);
                }
            })
        })
        .collect::<Vec<_>>();

    let drains = receivers
        .into_iter()
        .map(|mut receiver| {
            thread::spawn(move || {
                let mut seen = vec![];
                while let Ok(value) = FutureExt::block_on(receiver.recv()) {
                    seen.push(*value);
                }
                seen
            })
        })
        .collect::<Vec<_>>();

    for sender in senders {
        sender.join().unwrap();
    }
    drop(tx);

    let expected = (0..SENDERS * PER_SENDER).collect::<Vec<_>>();
    for drain in drains {
        let mut seen = drain.join().unwrap();
        seen.sort_unstable();
        assert_eq!(seen, expected);
    }
}

#[test]
fn randomized_operations_track_the_reference_model() {
    for seed in 1..32u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let (tx, rx) = unbounded::<u64>();
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
                        assert_eq!(receiver.try_recv().as_deref(), Ok(&*cursor), "seed {seed}");
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
