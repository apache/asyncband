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
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use asyncband::broadcast::unbounded::*;

struct TrackWake(AtomicUsize);

impl Wake for TrackWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// A payload whose destructor re-enters the channel it was sent through.
struct Reentrant {
    value: u64,
    channel: Option<Sender<Reentrant>>,
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
            let _ = channel.buffer_len();
            let _ = channel.receiver_count();
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

#[tokio::test]
async fn test_broadcast_basic() {
    let (tx, mut rx1) = channel();
    let mut rx2 = tx.subscribe();

    tx.send(10);
    tx.send(20);

    assert_eq!(rx1.recv().await, Ok(10));
    assert_eq!(rx1.recv().await, Ok(20));
    assert_eq!(rx2.recv().await, Ok(10));
    assert_eq!(rx2.recv().await, Ok(20));
}

#[tokio::test]
async fn test_subscribe() {
    let (tx, _rx) = channel();
    let mut rx = tx.subscribe();

    tx.send(100);
    assert_eq!(rx.recv().await, Ok(100));
}

#[tokio::test]
async fn test_resubscribe() {
    let (tx, mut rx) = channel();

    tx.send(1);
    tx.send(2);

    let mut rx2 = rx.resubscribe();

    // rx sees 1, 2
    // rx2 sees nothing yet (starts at tail=2)

    tx.send(3);

    assert_eq!(rx.recv().await, Ok(1));
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx2.recv().await, Ok(3));
}

#[test]
fn test_try_recv() {
    let (tx, mut rx) = channel();

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
async fn test_slow_receiver_keeps_every_message() {
    let (tx, mut rx1) = channel();
    let mut rx2 = tx.subscribe();

    for i in 0..1024 {
        tx.send(i);
    }

    // The fast receiver draining fully must not reclaim anything the slow one still needs.
    for i in 0..1024 {
        assert_eq!(rx1.recv().await, Ok(i));
    }
    assert_eq!(tx.buffer_len(), 1024);

    for i in 0..1024 {
        assert_eq!(rx2.recv().await, Ok(i));
    }
    assert_eq!(tx.buffer_len(), 0);
}

#[tokio::test]
async fn buffer_len_tracks_the_slowest_receiver() {
    let (tx, mut rx1) = channel();
    let mut rx2 = tx.subscribe();

    tx.send(1);
    tx.send(2);
    assert_eq!(tx.buffer_len(), 2);

    // Reclaiming waits for the slowest receiver, message by message.
    assert_eq!(rx1.recv().await, Ok(1));
    assert_eq!(tx.buffer_len(), 2);
    assert_eq!(rx2.recv().await, Ok(1));
    assert_eq!(tx.buffer_len(), 1);

    assert_eq!(rx1.recv().await, Ok(2));
    assert_eq!(tx.buffer_len(), 1);
    assert_eq!(rx2.recv().await, Ok(2));
    assert_eq!(tx.buffer_len(), 0);
}

#[tokio::test]
async fn test_dropping_a_lagging_receiver_releases_its_backlog() {
    let (tx, mut rx1) = channel();
    let rx2 = tx.subscribe();

    for i in 0..128 {
        tx.send(i);
    }
    for i in 0..128 {
        assert_eq!(rx1.recv().await, Ok(i));
    }
    assert_eq!(tx.buffer_len(), 128);

    drop(rx2);
    assert_eq!(tx.buffer_len(), 0);
}

#[tokio::test]
async fn resubscribe_keeps_the_original_receivers_backlog() {
    let (tx, mut rx) = channel();

    tx.send(1);
    tx.send(2);

    let mut rx2 = rx.resubscribe();
    assert_eq!(tx.buffer_len(), 2);

    tx.send(3);

    assert_eq!(rx2.recv().await, Ok(3));
    assert_eq!(tx.buffer_len(), 3);

    assert_eq!(rx.recv().await, Ok(1));
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx.recv().await, Ok(3));
    assert_eq!(tx.buffer_len(), 0);
}

#[tokio::test]
async fn send_without_receivers_does_not_buffer() {
    let (tx, rx) = channel();
    drop(rx);

    tx.send(1);
    tx.send(2);
    assert_eq!(tx.buffer_len(), 0);

    let mut rx = tx.subscribe();
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    tx.send(3);
    assert_eq!(rx.recv().await, Ok(3));
}

#[test]
fn receiver_count_and_len_track_each_receiver() {
    let (tx, mut rx1) = channel();
    assert_eq!(tx.receiver_count(), 1);
    assert_eq!(rx1.len(), 0);
    assert!(rx1.is_empty());

    tx.send(1);
    tx.send(2);
    assert_eq!(rx1.len(), 2);
    assert!(!rx1.is_empty());

    let mut rx2 = tx.subscribe();
    assert_eq!(tx.receiver_count(), 2);
    assert_eq!(rx2.len(), 0);
    assert!(rx2.is_empty());

    tx.send(3);
    assert_eq!(rx1.len(), 3);
    assert_eq!(rx2.len(), 1);

    assert_eq!(rx2.try_recv(), Ok(3));
    assert_eq!(rx2.len(), 0);
    drop(rx2);
    assert_eq!(tx.receiver_count(), 1);

    assert_eq!(rx1.try_recv(), Ok(1));
    assert_eq!(rx1.len(), 2);
}

#[tokio::test]
async fn clone_shares_the_current_position() {
    let (tx, mut rx) = channel();

    tx.send(1);
    tx.send(2);
    assert_eq!(rx.recv().await, Ok(1));

    // The clone inherits the unread backlog, unlike `resubscribe`.
    let mut clone = rx.clone();
    let mut fresh = rx.resubscribe();
    assert_eq!(tx.receiver_count(), 3);

    tx.send(3);

    assert_eq!(clone.recv().await, Ok(2));
    assert_eq!(clone.recv().await, Ok(3));
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx.recv().await, Ok(3));
    assert_eq!(fresh.recv().await, Ok(3));
    assert_eq!(tx.buffer_len(), 0);
}

#[tokio::test]
async fn clone_at_head_keeps_the_backlog_alive() {
    let (tx, mut rx) = channel();

    tx.send(1);
    let clone = rx.clone();

    assert_eq!(rx.recv().await, Ok(1));
    // The clone still sits at `head`, so the message must not be reclaimed yet.
    assert_eq!(tx.buffer_len(), 1);

    drop(clone);
    assert_eq!(tx.buffer_len(), 0);
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

    let (tx, mut rx) = channel();
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
    let (tx, mut rx1) = channel();
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
    assert_eq!(tx.buffer_len(), 0);
    assert_eq!(rx1.try_recv().unwrap_err(), TryRecvError::Empty);
}

#[test]
fn message_destructors_run_outside_the_channel_lock() {
    let finished = Arc::new(AtomicUsize::new(0));
    let flag = finished.clone();

    let worker = thread::spawn(move || {
        let (tx, mut rx1) = channel();
        let rx2 = tx.subscribe();

        for value in 0..4 {
            tx.send(Reentrant {
                value,
                channel: Some(tx.clone()),
            });
        }

        // Reclaim through a receive, and then through a receiver drop.
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

        flag.store(1, Ordering::SeqCst);
    });

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && finished.load(Ordering::SeqCst) == 0 {
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        finished.load(Ordering::SeqCst),
        1,
        "a message destructor deadlocked against the channel lock"
    );
    worker.join().unwrap();
}

#[tokio::test]
async fn test_wait_mechanism() {
    let (tx, mut rx) = channel();

    let handle = tokio::spawn(async move { rx.recv().await });

    tokio::time::sleep(Duration::from_millis(100)).await;
    tx.send(42);

    assert_eq!(handle.await.unwrap(), Ok(42));
}

#[test]
fn send_wakes_a_parked_receiver_exactly_once() {
    let (tx, mut rx) = channel();
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let mut context = Context::from_waker(&waker);
    let mut recv = Box::pin(rx.recv());

    assert!(recv.as_mut().poll(&mut context).is_pending());

    tx.send(42);

    assert_eq!(tracker.0.load(Ordering::Relaxed), 1);
    assert_eq!(recv.as_mut().poll(&mut context), Poll::Ready(Ok(42)));
}

#[test]
fn cancelled_recv_releases_its_waker() {
    let (tx, mut rx) = channel::<()>();
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

#[test]
fn dropping_a_woken_recv_keeps_another_receivers_waiter() {
    let (tx, mut rx1) = channel::<i32>();
    let mut rx2 = tx.subscribe();
    let first = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(first.clone());
    let mut context = Context::from_waker(&waker);
    let mut recv1 = Box::pin(rx1.recv());

    assert!(recv1.as_mut().poll(&mut context).is_pending());

    tx.send(1);
    assert_eq!(first.0.load(Ordering::Relaxed), 1);
    assert_eq!(rx2.try_recv(), Ok(1));

    let second = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(second.clone());
    let mut context = Context::from_waker(&waker);
    let mut recv2 = Box::pin(rx2.recv());
    assert!(recv2.as_mut().poll(&mut context).is_pending());

    // `recv1` was already woken, so dropping it must not release the slot `recv2` now owns.
    drop(recv1);
    tx.send(2);

    assert_eq!(second.0.load(Ordering::Relaxed), 1);
}

#[test]
fn parked_recv_wakes_when_the_last_sender_drops() {
    let (tx, mut rx) = channel::<()>();
    let extra = tx.clone();
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let mut context = Context::from_waker(&waker);
    let mut recv = Box::pin(rx.recv());

    assert!(recv.as_mut().poll(&mut context).is_pending());

    drop(tx);
    assert_eq!(tracker.0.load(Ordering::Relaxed), 0);

    drop(extra);
    assert_eq!(tracker.0.load(Ordering::Relaxed), 1);

    drop(recv);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn parked_recv_prefers_buffered_messages_over_disconnect() {
    let (tx, mut rx) = channel();
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
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
async fn recv_drains_buffered_messages_before_reporting_disconnect() {
    let (tx, mut rx) = channel();

    tx.send(1);
    tx.send(2);
    drop(tx);

    assert_eq!(rx.recv().await, Ok(1));
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[tokio::test]
async fn recv_reports_disconnect_without_any_message() {
    let (tx, mut rx) = channel::<()>();
    drop(tx);
    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[test]
fn concurrent_senders_deliver_every_message_to_every_receiver() {
    const SENDERS: u64 = 4;
    const PER_SENDER: u64 = 512;

    let (tx, rx) = channel();
    let receivers = (0..4)
        .map(|index| {
            if index == 0 {
                rx.clone()
            } else {
                tx.subscribe()
            }
        })
        .collect::<Vec<_>>();
    drop(rx);

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
                let mut seen = Vec::new();
                while let Ok(value) = pollster::block_on(receiver.recv()) {
                    seen.push(value);
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
        let (tx, rx) = channel::<u64>();
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
