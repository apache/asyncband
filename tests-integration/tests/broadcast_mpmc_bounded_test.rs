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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;

use asyncband::broadcast::mpmc::*;
use tests_integration::poll_once;

struct TrackWake(AtomicUsize);

impl TrackWake {
    fn new() -> Arc<Self> {
        Arc::new(Self(AtomicUsize::new(0)))
    }

    fn count(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl Wake for TrackWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// A payload whose destructor re-enters the channel it was sent through.
struct Reentrant {
    value: u64,
    channel: Option<BoundedSender<Reentrant>>,
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

/// A payload that panics while the channel drops a message it reclaimed.
///
/// Clones disarm themselves, so only the copy the channel retains is dangerous. That lets a test
/// drain a receiver normally and still blow up inside the reclaim.
struct PanicOnDrop {
    armed: bool,
}

impl Clone for PanicOnDrop {
    fn clone(&self) -> Self {
        Self { armed: false }
    }
}

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        if self.armed {
            panic!("panic while dropping a broadcast message");
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Fanout and subscription
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn bounded_delivers_every_message_to_every_receiver() {
    let (tx, mut rx1) = bounded(4);
    let mut rx2 = tx.subscribe();

    tx.send(10).await;
    tx.send(20).await;

    assert_eq!(rx1.recv().await, Ok(10));
    assert_eq!(rx1.recv().await, Ok(20));
    assert_eq!(rx2.recv().await, Ok(10));
    assert_eq!(rx2.recv().await, Ok(20));
}

#[test]
fn bounded_slow_receiver_keeps_every_message_under_backpressure() {
    let (tx, mut fast) = bounded(2);
    let mut slow = tx.subscribe();

    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();

    // The fast subscription draining does not release anything, because the slow one has read
    // nothing — being bounded must not turn into dropping what the slow subscription still owes.
    assert_eq!(fast.try_recv(), Ok(1));
    assert_eq!(fast.try_recv(), Ok(2));
    assert_eq!(tx.try_send(3), Err(TrySendError::Full(3)));

    // One read by the slow subscription frees exactly one slot.
    assert_eq!(slow.try_recv(), Ok(1));
    tx.try_send(3).unwrap();

    // Every value accepted while both were active reaches both, in order.
    assert_eq!(slow.try_recv(), Ok(2));
    assert_eq!(slow.try_recv(), Ok(3));
    assert_eq!(fast.try_recv(), Ok(3));
    assert_eq!(tx.retained_message_count(), 0);
}

#[test]
fn bounded_subscribe_starts_at_the_committed_tail() {
    let (tx, _rx) = bounded(4);
    tx.try_send(1).unwrap();

    let mut late = tx.subscribe();
    assert_eq!(late.try_recv(), Err(TryRecvError::Empty));

    tx.try_send(2).unwrap();
    assert_eq!(late.try_recv(), Ok(2));
}

#[test]
fn bounded_resubscribe_keeps_the_original_receivers_backlog() {
    let (tx, mut rx) = bounded(4);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();

    let mut rx2 = rx.resubscribe();
    tx.try_send(3).unwrap();

    assert_eq!(rx2.try_recv(), Ok(3));
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Ok(3));
}

#[test]
fn bounded_unread_message_count_tracks_each_receiver() {
    let (tx, mut rx1) = bounded(4);
    let rx2 = tx.subscribe();

    assert_eq!(rx1.unread_message_count(), 0);

    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert_eq!(rx1.unread_message_count(), 2);
    assert_eq!(rx2.unread_message_count(), 2);

    assert_eq!(rx1.try_recv(), Ok(1));
    assert_eq!(rx1.unread_message_count(), 1);
    assert_eq!(rx2.unread_message_count(), 2);
}

// ---------------------------------------------------------------------------------------------
// Strict capacity
// ---------------------------------------------------------------------------------------------

#[test]
fn try_send_rejects_at_capacity_and_returns_the_value() {
    let (tx, mut rx) = bounded(2);

    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert_eq!(tx.try_send(3), Err(TrySendError::Full(3)));

    // The rejected value is handed back untouched, and nothing was published.
    assert_eq!(tx.retained_message_count(), 2);
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn capacity_counts_the_shared_backlog_not_receivers() {
    let (tx, _rx) = bounded(2);
    let _extra = (0..8).map(|_| tx.subscribe()).collect::<Vec<_>>();

    // Eight more subscriptions do not consume capacity; only unread messages do.
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert_eq!(tx.try_send(3), Err(TrySendError::Full(3)));
    assert_eq!(tx.capacity(), 2);
    assert_eq!(tx.retained_message_count(), 2);
}

#[test]
fn retained_message_count_tracks_the_slowest_receiver() {
    let (tx, mut rx1) = bounded(4);
    let mut rx2 = tx.subscribe();

    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert_eq!(tx.retained_message_count(), 2);

    // Draining one receiver does not release what the other has not read.
    assert_eq!(rx1.try_recv(), Ok(1));
    assert_eq!(rx1.try_recv(), Ok(2));
    assert_eq!(tx.retained_message_count(), 2);

    assert_eq!(rx2.try_recv(), Ok(1));
    assert_eq!(tx.retained_message_count(), 1);
    assert_eq!(rx2.try_recv(), Ok(2));
    assert_eq!(tx.retained_message_count(), 0);
}

// ---------------------------------------------------------------------------------------------
// Backpressure and capacity release
// ---------------------------------------------------------------------------------------------

#[test]
fn send_waits_while_the_slowest_subscription_holds_capacity() {
    let (tx, mut rx1) = bounded(1);
    let mut rx2 = tx.subscribe();
    tx.try_send(1).unwrap();

    let mut send = Box::pin(tx.send(2));
    assert!(poll_once(send.as_mut()).is_pending());

    // The fast receiver draining is not enough while the slow one still retains the message.
    assert_eq!(rx1.try_recv(), Ok(1));
    assert!(poll_once(send.as_mut()).is_pending());

    assert_eq!(rx2.try_recv(), Ok(1));
    assert!(poll_once(send.as_mut()).is_ready());
    assert_eq!(rx1.try_recv(), Ok(2));
}

#[test]
fn receive_that_vacates_the_head_wakes_a_blocked_sender() {
    let (tx, mut rx) = bounded(1);
    tx.try_send(0).unwrap();

    let tracker = TrackWake::new();
    let waker = Waker::from(tracker.clone());
    let mut send = Box::pin(tx.send(1));
    assert!(
        send.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(tracker.count(), 0);

    assert_eq!(rx.try_recv(), Ok(0));
    assert_eq!(tracker.count(), 1);
    assert!(poll_once(send.as_mut()).is_ready());
}

#[test]
fn parked_recv_that_reclaims_wakes_a_blocked_sender() {
    let (tx, mut rx) = bounded(1);
    tx.try_send(0).unwrap();

    let tracker = TrackWake::new();
    let waker = Waker::from(tracker.clone());
    let mut send = Box::pin(tx.send(1));
    assert!(
        send.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    // Reclaim through the `recv` future rather than `try_recv`: it is a separate call site, and a
    // release wired into only one of them would strand this producer.
    let mut recv = Box::pin(rx.recv());
    assert_eq!(poll_once(recv.as_mut()), std::task::Poll::Ready(Ok(0)));
    drop(recv);

    assert_eq!(tracker.count(), 1);
    assert!(poll_once(send.as_mut()).is_ready());
}

#[test]
fn wakes_blocked_senders_as_capacity_frees() {
    let (tx, mut rx) = bounded(1);
    tx.try_send(0).unwrap();

    let mut first = Box::pin(tx.send(1));
    let mut second = Box::pin(tx.send(2));
    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());

    // One freed slot admits exactly one producer.
    assert_eq!(rx.try_recv(), Ok(0));
    assert!(poll_once(first.as_mut()).is_ready());
    assert!(poll_once(second.as_mut()).is_pending());

    assert_eq!(rx.try_recv(), Ok(1));
    assert!(poll_once(second.as_mut()).is_ready());
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn dropping_a_lagging_receiver_wakes_blocked_senders() {
    let (tx, mut rx1) = bounded(1);
    let rx2 = tx.subscribe();
    tx.try_send(0).unwrap();

    let mut send = Box::pin(tx.send(1));
    assert_eq!(rx1.try_recv(), Ok(0));
    assert!(poll_once(send.as_mut()).is_pending());

    // `rx2` is the one holding the backlog; dropping it releases the slot.
    drop(rx2);
    assert_eq!(tx.retained_message_count(), 0);
    assert!(poll_once(send.as_mut()).is_ready());
}

#[test]
fn dropping_the_last_receiver_wakes_every_blocked_sender() {
    const BLOCKED: usize = 3;

    let (tx, rx) = bounded(2);
    tx.try_send(0).unwrap();
    tx.try_send(1).unwrap();

    // More blocked producers than the drop will reclaim slots. Once no receiver remains every
    // send succeeds unconditionally, so waking only `reclaimed` of them would strand the rest.
    let trackers = (0..BLOCKED).map(|_| TrackWake::new()).collect::<Vec<_>>();
    let mut sends = (0..BLOCKED)
        .map(|value| Box::pin(tx.send(10 + value as i32)))
        .collect::<Vec<_>>();

    for (send, tracker) in sends.iter_mut().zip(&trackers) {
        let waker = Waker::from(tracker.clone());
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
    }

    drop(rx);

    for (index, tracker) in trackers.iter().enumerate() {
        assert!(
            tracker.count() > 0,
            "blocked sender {index} was never woken after the last receiver was dropped"
        );
    }
    for send in &mut sends {
        assert!(poll_once(send.as_mut()).is_ready());
    }
}

#[test]
fn sends_never_block_once_all_receivers_are_gone() {
    let (tx, rx) = bounded(1);
    tx.try_send(0).unwrap();
    drop(rx);

    assert_eq!(tx.retained_message_count(), 0);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();

    let mut send = Box::pin(tx.send(3));
    assert!(poll_once(send.as_mut()).is_ready());
    assert_eq!(tx.retained_message_count(), 0);
}

#[test]
fn subscribing_while_producers_are_blocked_does_not_release_capacity() {
    let (tx, _rx) = bounded(1);
    tx.try_send(0).unwrap();

    let mut send = Box::pin(tx.send(1));
    assert!(poll_once(send.as_mut()).is_pending());

    // A new cursor starts at the tail, so it cannot lower the retained backlog.
    let _late = tx.subscribe();
    assert_eq!(tx.retained_message_count(), 1);
    assert!(poll_once(send.as_mut()).is_pending());
}

// ---------------------------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------------------------

#[test]
fn cancelled_send_publishes_nothing() {
    let (tx, mut rx) = bounded(1);
    tx.try_send(0).unwrap();

    let mut send = Box::pin(tx.send(1));
    assert!(poll_once(send.as_mut()).is_pending());
    drop(send);

    // The cancelled value never entered the committed order, so the next receive sees only what
    // was already published, and the one after it is a fresh send.
    assert_eq!(rx.try_recv(), Ok(0));
    tx.try_send(2).unwrap();
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn cancelled_notified_sender_passes_capacity_to_the_next_sender() {
    let (tx, mut rx) = bounded(1);
    tx.try_send(0).unwrap();

    let mut first = Box::pin(tx.send(1));
    let mut second = Box::pin(tx.send(2));
    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());

    assert_eq!(rx.try_recv(), Ok(0));
    drop(first);

    assert!(poll_once(second.as_mut()).is_ready());
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn cancelled_recv_releases_its_waker() {
    let (tx, mut rx) = bounded(4);

    let tracker = TrackWake::new();
    let waker = Waker::from(tracker.clone());
    let baseline = Arc::strong_count(&tracker);

    let mut recv = Box::pin(rx.recv());
    assert!(
        recv.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(Arc::strong_count(&tracker), baseline + 1);

    drop(recv);
    assert_eq!(Arc::strong_count(&tracker), baseline);

    tx.try_send(1).unwrap();
    assert_eq!(tracker.count(), 0);
}

// ---------------------------------------------------------------------------------------------
// Disconnection
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn bounded_recv_drains_buffered_messages_before_reporting_disconnection() {
    let (tx, mut rx) = bounded(4);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    drop(tx);

    assert_eq!(rx.recv().await, Ok(1));
    assert_eq!(rx.recv().await, Ok(2));
    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[tokio::test]
async fn bounded_recv_reports_disconnection_without_any_message() {
    let (tx, mut rx) = bounded::<i32>(4);
    drop(tx);
    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[test]
fn bounded_parked_recv_wakes_when_the_last_sender_drops() {
    let (tx, mut rx) = bounded::<i32>(4);
    let second_tx = tx.clone();

    let tracker = TrackWake::new();
    let waker = Waker::from(tracker.clone());
    let mut recv = Box::pin(rx.recv());
    assert!(
        recv.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    drop(tx);
    assert_eq!(tracker.count(), 0);

    drop(second_tx);
    assert_eq!(tracker.count(), 1);
    assert_eq!(
        poll_once(recv.as_mut()),
        std::task::Poll::Ready(Err(RecvError::Disconnected))
    );
}

// ---------------------------------------------------------------------------------------------
// Panic safety
// ---------------------------------------------------------------------------------------------

#[test]
fn bounded_panicking_clone_leaves_the_channel_consistent() {
    let (tx, mut rx1) = bounded(4);
    let mut rx2 = tx.subscribe();

    tx.try_send(PanicOnClone {
        value: 1,
        panic: true,
    })
    .unwrap();
    tx.try_send(PanicOnClone {
        value: 2,
        panic: false,
    })
    .unwrap();

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
fn panicking_payload_destructor_still_releases_capacity() {
    let (tx, mut rx1) = bounded(3);
    let rx2 = tx.subscribe();

    // Only the first retained message is armed: the reclaim drops the whole prefix, and a second
    // panic while the first one unwinds would abort the process instead of failing the test.
    for index in 0..3 {
        tx.try_send(PanicOnDrop { armed: index == 0 }).unwrap();
    }
    // `rx1` reads clones, which are disarmed; the armed originals stay retained for `rx2`.
    for _ in 0..3 {
        rx1.try_recv().unwrap();
    }

    let mut send = Box::pin(tx.send(PanicOnDrop { armed: false }));
    assert!(poll_once(send.as_mut()).is_pending());

    // Dropping `rx2` reclaims all three retained messages and their destructors panic. The
    // capacity they released must already have reached the parked producer by then.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(rx2)));
    assert!(result.is_err());

    assert!(
        poll_once(send.as_mut()).is_ready(),
        "a panicking payload destructor must not strand a producer on capacity it already freed"
    );
}

#[test]
fn bounded_message_destructors_run_outside_the_channel_lock() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();

    let worker = thread::spawn(move || {
        let (tx, mut rx1) = bounded(8);
        let rx2 = tx.subscribe();

        for value in 0..4 {
            tx.try_send(Reentrant {
                value,
                channel: Some(tx.clone()),
            })
            .unwrap();
        }

        // Draining both receivers reclaims the prefix, whose destructors re-enter the channel.
        for _ in 0..4 {
            rx1.try_recv().unwrap();
        }
        drop(rx2);
        drop(rx1);

        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("reclaimed message destructors must not run while the channel is locked");
    worker.join().unwrap();
}

// ---------------------------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------------------------

#[test]
fn dropping_the_last_receiver_never_strands_a_racing_producer() {
    const ROUNDS: u64 = 150;
    const PRODUCERS: u64 = 4;

    // The deterministic tests above drop the receiver at a fixed point. This races the drop
    // against producers entering the waiting path, which is the window where the channel decides
    // whether anybody needs waking. A missed wake-up here parks a producer forever, so the failure
    // mode is a hang rather than a wrong value — hence the timeout instead of an assertion.
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();

    let worker = thread::spawn(move || {
        for round in 0..ROUNDS {
            let (tx, rx) = bounded(1);
            tx.try_send(0).unwrap();

            let producers = (0..PRODUCERS)
                .map(|producer| {
                    let tx = tx.clone();
                    thread::spawn(move || pollster::block_on(tx.send(round * 10 + producer + 1)))
                })
                .collect::<Vec<_>>();

            drop(rx);

            for producer in producers {
                producer.join().unwrap();
            }
        }
        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("a producer was left waiting after the last receiver went away");
    worker.join().unwrap();
}

#[test]
fn bounded_concurrent_producers_commit_one_order_seen_by_every_receiver() {
    const PRODUCERS: u64 = 4;
    const PER_PRODUCER: u64 = 128;
    const RECEIVERS: usize = 4;
    const TOTAL: u64 = PRODUCERS * PER_PRODUCER;

    // Several producers publishing concurrently must still commit one contiguous order, and every
    // subscription must observe that same order — not merely the same set.
    //
    // Capacity is far below the batch, so the producers really do block on the slowest receiver.
    // This still terminates: the run could only wedge if every producer and every receiver waited
    // at once, but a producer waits only while at least one message is retained, and a retained
    // message is by definition unread by the slowest receiver — so that receiver is runnable.
    let (tx, rx) = bounded(8);
    let mut receivers = vec![rx];
    receivers.extend((1..RECEIVERS).map(|_| tx.subscribe()));

    let drains = receivers
        .into_iter()
        .map(|mut receiver| {
            thread::spawn(move || {
                let mut seen = Vec::with_capacity(TOTAL as usize);
                for _ in 0..TOTAL {
                    seen.push(pollster::block_on(receiver.recv()).expect("sender dropped early"));
                }
                seen
            })
        })
        .collect::<Vec<_>>();

    let producers = (0..PRODUCERS)
        .map(|worker| {
            let tx = tx.clone();
            thread::spawn(move || {
                for value in 0..PER_PRODUCER {
                    pollster::block_on(tx.send(worker * PER_PRODUCER + value));
                }
            })
        })
        .collect::<Vec<_>>();

    for producer in producers {
        producer.join().unwrap();
    }
    drop(tx);

    let orders = drains
        .into_iter()
        .map(|drain| drain.join().unwrap())
        .collect::<Vec<_>>();

    // Every subscription saw the identical sequence.
    for (index, order) in orders.iter().enumerate().skip(1) {
        assert_eq!(
            order, &orders[0],
            "subscription {index} observed a different committed order"
        );
    }

    // And that sequence is every published value exactly once — no gap, no duplicate.
    let mut sorted = orders[0].clone();
    sorted.sort_unstable();
    assert_eq!(sorted, (0..TOTAL).collect::<Vec<_>>());
}
