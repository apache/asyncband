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

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::pin;
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
    let result = catch_unwind(AssertUnwindSafe(|| {
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
    let mut recv = pin!(rx.recv());

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
    let mut recv2 = pin!(rx2.recv());
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
    assert_eq!(
        expect_ready(recv.as_mut().poll(&mut context)),
        Err(RecvError::Disconnected)
    );
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
    const PER_SENDER: u64 = if cfg!(miri) { 16 } else { 600 };

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
    const SEEDS: u64 = if cfg!(miri) { 4 } else { 32 };
    const STEPS: usize = if cfg!(miri) { 64 } else { 512 };
    for seed in 1..SEEDS {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let (tx, rx) = unbounded::<u64>();
        let mut tail = 0u64;
        let mut model = vec![(rx, 0u64)];

        for _ in 0..STEPS {
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
                assert_eq!(receiver.is_empty(), *cursor == tail, "seed {seed}");
            }
        }
    }
}

#[test]
fn unbounded_receiver_is_disconnected_tracks_final_sender_drop_with_backlog() {
    let (tx1, mut rx) = unbounded();
    let tx2 = tx1.clone();

    assert!(!rx.is_disconnected());
    assert!(rx.is_empty());

    tx1.send(42);
    drop(tx1);
    assert!(!rx.is_disconnected());
    assert!(!rx.is_empty());

    drop(tx2);
    assert!(rx.is_disconnected());
    assert!(!rx.is_empty());
    assert_eq!(*rx.try_recv().unwrap(), 42);
    assert!(rx.is_empty());
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn recv_and_recv_cloned_panic_when_polled_after_completion() {
    let (waker, _) = WakeCounter::new();

    let (tx, mut rx) = unbounded();
    tx.send(7usize);
    {
        let mut fut = pin!(rx.recv());
        assert_eq!(
            expect_ready(poll_with(fut.as_mut(), &waker)).as_deref(),
            Ok(&7)
        );
        let repoll = catch_unwind(AssertUnwindSafe(|| poll_with(fut.as_mut(), &waker)));
        assert!(repoll.is_err());
    }

    tx.send(8usize);
    {
        let mut fut = pin!(rx.recv_cloned());
        assert_eq!(expect_ready(poll_with(fut.as_mut(), &waker)), Ok(8));
        let repoll = catch_unwind(AssertUnwindSafe(|| poll_with(fut.as_mut(), &waker)));
        assert!(repoll.is_err());
    }

    drop(tx);
    let mut fut = pin!(rx.recv());
    assert_eq!(
        expect_ready(poll_with(fut.as_mut(), &waker)),
        Err(RecvError::Disconnected)
    );
    let repoll = catch_unwind(AssertUnwindSafe(|| poll_with(fut.as_mut(), &waker)));
    assert!(repoll.is_err());
}

#[test]
fn overflow_waiters_wake_cancel_and_reuse_slots_cleanly() {
    let (tx, mut rx0) = unbounded::<usize>();
    let mut rx1 = tx.subscribe();
    let mut rx2 = tx.subscribe();
    let mut rx3 = tx.subscribe();

    let (w0, c0) = WakeCounter::new();
    let (w1, c1) = WakeCounter::new();
    let (w1_replacement, c1_replacement) = WakeCounter::new();
    let (w2, c2) = WakeCounter::new();
    let (w3, c3) = WakeCounter::new();

    let mut f0 = pin!(rx0.recv());
    let mut f1 = pin!(rx1.recv());
    let mut f2 = Box::pin(rx2.recv());
    assert!(poll_with(f0.as_mut(), &w0).is_pending());
    assert!(poll_with(f1.as_mut(), &w1).is_pending());
    assert!(poll_with(f2.as_mut(), &w2).is_pending());

    // Replace an overflow waiter's waker and cancel another overflow waiter so its slot reuses.
    assert!(poll_with(f1.as_mut(), &w1_replacement).is_pending());
    drop(f2);

    let mut f3 = pin!(rx3.recv());
    assert!(poll_with(f3.as_mut(), &w3).is_pending());

    tx.send(99);
    assert_eq!(c0.count(), 1);
    assert_eq!(c1.count(), 0);
    assert_eq!(c1_replacement.count(), 1);
    assert_eq!(c2.count(), 0);
    assert_eq!(c3.count(), 1);

    assert_eq!(
        expect_ready(poll_with(f0.as_mut(), &w0)).as_deref(),
        Ok(&99)
    );
    assert_eq!(
        expect_ready(poll_with(f1.as_mut(), &w1_replacement)).as_deref(),
        Ok(&99)
    );
    assert_eq!(
        expect_ready(poll_with(f3.as_mut(), &w3)).as_deref(),
        Ok(&99)
    );
    assert_eq!(rx2.try_recv().as_deref(), Ok(&99));
    assert_eq!(tx.retained_message_count(), 0);
}

#[test]
fn concurrent_subscribe_drop_and_publish_across_segments() {
    const PRODUCERS: u64 = 3;
    const MSGS_PER_PRODUCER: u64 = if cfg!(miri) { 12 } else { 320 };
    const SUBSCRIBER_THREADS: usize = if cfg!(miri) { 2 } else { 6 };

    let (tx, initial_rx) = unbounded::<u64>();
    drop(initial_rx);

    let producers = (0..PRODUCERS)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                for seq in 0..MSGS_PER_PRODUCER {
                    tx.send((p << 32) | seq);
                }
            })
        })
        .collect::<Vec<_>>();

    let subscribers = (0..SUBSCRIBER_THREADS)
        .map(|_| {
            let tx = tx.clone();
            thread::spawn(move || {
                for round in 0..4 {
                    let mut rx = tx.subscribe();
                    let mut last_per_producer = [None::<u64>; PRODUCERS as usize];
                    for _ in 0..(if cfg!(miri) { 4 } else { 96 }) {
                        if let Ok(msg) = rx.try_recv() {
                            let p = (*msg >> 32) as usize;
                            let seq = *msg & 0xFFFF_FFFF;
                            if let Some(prev) = last_per_producer[p] {
                                assert!(seq > prev);
                            }
                            last_per_producer[p] = Some(seq);
                        } else {
                            thread::yield_now();
                        }
                    }
                    if round % 2 == 0 {
                        let mut resub = rx.resubscribe();
                        let _ = resub.try_recv();
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    for handle in producers {
        handle.join().unwrap();
    }
    for handle in subscribers {
        handle.join().unwrap();
    }
    assert_eq!(tx.retained_message_count(), 0);
}

#[test]
fn close_vs_try_recv_linearizability() {
    let rounds = if cfg!(miri) { 8 } else { 256 };
    for round in 0..rounds {
        let (tx1, rx1) = unbounded::<usize>();
        let rx2 = tx1.subscribe();
        let tx2 = tx1.clone();

        let p1 = thread::spawn(move || {
            for i in 0..4 {
                tx1.send(round * 8 + i);
            }
            drop(tx1);
        });
        let p2 = thread::spawn(move || {
            for i in 4..8 {
                tx2.send(round * 8 + i);
            }
            drop(tx2);
        });

        let drain = |mut rx: UnboundedReceiver<usize>| {
            thread::spawn(move || {
                let mut got = Vec::with_capacity(8);
                loop {
                    match rx.try_recv() {
                        Ok(val) => got.push(*val),
                        Err(TryRecvError::Empty) => thread::yield_now(),
                        Err(TryRecvError::Disconnected) => {
                            assert!(rx.is_disconnected());
                            assert!(rx.is_empty());
                            break;
                        }
                    }
                }
                got.sort_unstable();
                got
            })
        };

        let d1 = drain(rx1);
        let d2 = drain(rx2);
        p1.join().unwrap();
        p2.join().unwrap();
        let expected: Vec<usize> = (round * 8..round * 8 + 8).collect();
        assert_eq!(d1.join().unwrap(), expected);
        assert_eq!(d2.join().unwrap(), expected);
    }
}

#[test]
fn completed_recv_futures_can_move_by_value_after_payload_is_dropped() {
    use std::hint::black_box;
    use std::pin::Pin;

    let (waker, _) = WakeCounter::new();
    let (tx, mut rx) = unbounded();

    tx.send(Box::new(42_u64));
    let mut shared_recv = rx.recv();
    let arc_box = expect_ready(poll_with(Pin::new(&mut shared_recv), &waker)).unwrap();
    assert_eq!(**arc_box, 42);
    drop(arc_box);
    drop(black_box(shared_recv));

    tx.send(Box::new(99_u64));
    let mut cloned_recv = rx.recv_cloned();
    let owned_box = expect_ready(poll_with(Pin::new(&mut cloned_recv), &waker)).unwrap();
    assert_eq!(*owned_box, 99);
    drop(owned_box);
    drop(black_box(cloned_recv));
}

#[test]
fn concurrent_drop_last_receiver_and_resubscribe_preserves_retained_count() {
    use std::sync::Barrier;

    let rounds = if cfg!(miri) { 8 } else { 256 };
    for _ in 0..rounds {
        let (tx, rx1) = unbounded::<usize>();
        for i in 0..12 {
            tx.send(i);
        }
        assert_eq!(tx.retained_message_count(), 12);

        let start = Barrier::new(2);
        let start_ref = &start;
        let tx_sub = tx.clone();
        let mut rx2 = None;

        thread::scope(|scope| {
            let dropper = scope.spawn(|| {
                start_ref.wait();
                drop(rx1);
            });
            start.wait();
            let sub = tx_sub.subscribe();
            for i in 100..105 {
                tx_sub.send(i);
            }
            rx2 = Some(sub);
            dropper.join().unwrap();
        });

        let mut rx2 = rx2.unwrap();
        assert_eq!(tx.retained_message_count(), 5);
        for i in 100..105 {
            assert_eq!(rx2.try_recv().as_deref(), Ok(&i));
        }
        assert_eq!(tx.retained_message_count(), 0);
    }
}

#[test]
fn concurrent_lagging_and_caught_up_drops_with_resubscribe_never_corrupt_inline_segment() {
    use std::sync::Barrier;

    let rounds = if cfg!(miri) { 8 } else { 256 };
    for _ in 0..rounds {
        let (tx, rx_lag) = unbounded::<Box<usize>>();
        let mut rx_fast = tx.subscribe();

        // Cross `INLINE_CAP` (`8`) onto a heap segment while `rx_lag` stays parked on slot 0 of
        // `inline_seg0` and `rx_fast` drains to the heap segment.
        for i in 0..14 {
            tx.send(Box::new(i));
            assert_eq!(**rx_fast.try_recv().unwrap(), i);
        }

        let start = Barrier::new(3);
        let start_ref = &start;
        let tx_resub = tx.clone();
        let mut rx_fresh = None;

        thread::scope(|scope| {
            let lag_drop = scope.spawn(|| {
                start_ref.wait();
                drop(rx_lag);
            });
            let fast_drop = scope.spawn(|| {
                start_ref.wait();
                drop(rx_fast);
            });
            start.wait();
            let sub = tx_resub.subscribe();
            for i in 1000..1010 {
                tx_resub.send(Box::new(i));
            }
            rx_fresh = Some(sub);
            lag_drop.join().unwrap();
            fast_drop.join().unwrap();
        });

        let mut rx_fresh = rx_fresh.unwrap();
        for i in 1000..1010 {
            assert_eq!(**rx_fresh.try_recv().unwrap(), i);
        }
        assert_eq!(rx_fresh.try_recv(), Err(TryRecvError::Empty));
        assert_eq!(tx.retained_message_count(), 0);
    }
}
