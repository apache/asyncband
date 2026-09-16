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
// * `bounded_concurrent_producers_commit_one_order_seen_by_every_receiver` — no concurrent
//   producers; committed order is program order, covered by `publish_order_is_program_order`.
// * `wakes_blocked_senders_as_capacity_frees` — only one producer can wait; the singular case is
//   `receive_that_vacates_the_head_wakes_the_blocked_sender` and
//   `parked_recv_that_reclaims_wakes_the_blocked_sender`.
// * `dropping_the_last_receiver_wakes_every_blocked_sender` — singular case is
//   `dropping_the_last_receiver_wakes_the_blocked_sender`.
// * `cancelled_notified_sender_passes_capacity_to_the_next_sender` — there is no next producer.
// * `a_large_reclaim_leaves_no_permit_slack` — SPMC has no semaphore and no permit slack.
// * `dropping_the_last_receiver_never_strands_a_racing_producer` — the multi-producer race is gone;
//   the remaining drop-vs-wait race is `dropping_the_last_receiver_wakes_the_blocked_sender`
//   together with `sends_never_block_once_all_receivers_are_gone`.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Wake;
use std::task::Waker;
use std::thread;

use asyncband::blocking::FutureExt;
use asyncband::broadcast::spmc::*;
use tests_integration::WakeCounter;
use tests_integration::assert_completes_without_deadlock;
use tests_integration::poll_once;
use tests_integration::waker_on_wake;

/// A payload whose destructor re-enters the channel it was sent through.
///
/// The sender is not `Clone`, so the probe is a shared receiver handle.
struct Reentrant {
    value: u64,
    probe: Option<Arc<Mutex<BoundedReceiver<Reentrant>>>>,
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

/// A payload whose clone lets another subscription consume the same slot, then panics.
///
/// That makes the panicking receive the slot's last reader, so it releases the slot from the drop
/// guard inside the receive step — a path that unwinds before the caller can wake the producer.
struct RaceOnClone {
    armed: bool,
    probe: Option<Arc<Mutex<BoundedReceiver<RaceOnClone>>>>,
}

impl Clone for RaceOnClone {
    fn clone(&self) -> Self {
        if self.armed {
            if let Some(probe) = &self.probe
                && let Ok(mut probe) = probe.try_lock()
            {
                // Takes this slot's remaining count from 2 to 1 while we are still cloning.
                let _ = probe.try_recv();
            }
            panic!("panic while cloning a broadcast message");
        }
        Self {
            armed: self.armed,
            probe: self.probe.clone(),
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

#[test]
fn delivers_every_message_to_every_receiver() {
    let (mut tx, mut rx1) = bounded(4);
    let mut rx2 = tx.subscribe();

    tx.try_send(10).unwrap();
    tx.try_send(20).unwrap();

    assert_eq!(rx1.try_recv(), Ok(10));
    assert_eq!(rx1.try_recv(), Ok(20));
    assert_eq!(rx2.try_recv(), Ok(10));
    assert_eq!(rx2.try_recv(), Ok(20));
}

#[test]
fn send_and_recv_deliver_every_accepted_value() {
    let (mut tx, mut rx1) = bounded(4);
    let mut rx2 = tx.subscribe();

    FutureExt::block_on(tx.send(10));
    FutureExt::block_on(tx.send(20));

    assert_eq!(FutureExt::block_on(rx1.recv()), Ok(10));
    assert_eq!(FutureExt::block_on(rx1.recv()), Ok(20));
    assert_eq!(FutureExt::block_on(rx2.recv()), Ok(10));
    assert_eq!(FutureExt::block_on(rx2.recv()), Ok(20));
}

#[test]
fn slow_receiver_keeps_every_message_under_backpressure() {
    let (mut tx, mut fast) = bounded(2);
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
fn subscribe_starts_at_the_committed_tail() {
    let (mut tx, _rx) = bounded(4);
    tx.try_send(1).unwrap();

    let mut late = tx.subscribe();
    assert_eq!(late.try_recv(), Err(TryRecvError::Empty));

    tx.try_send(2).unwrap();
    assert_eq!(late.try_recv(), Ok(2));
}

#[test]
fn resubscribe_keeps_the_original_receivers_backlog() {
    let (mut tx, mut rx) = bounded(4);
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
fn unread_message_count_tracks_each_receiver() {
    let (mut tx, mut rx1) = bounded(4);
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

#[test]
fn concurrent_receivers_keep_up_with_the_producer() {
    const RECEIVERS: usize = 8;
    const MESSAGES: usize = 256;

    let (mut tx, rx) = bounded(64);
    let mut receivers = vec![rx];
    for _ in 1..RECEIVERS {
        receivers.push(tx.subscribe());
    }

    let expected = (MESSAGES as u64 - 1) * MESSAGES as u64 / 2;
    let handles: Vec<_> = receivers
        .into_iter()
        .map(|mut rx| {
            thread::spawn(move || {
                let mut sum = 0u64;
                for _ in 0..MESSAGES {
                    sum += FutureExt::block_on(rx.recv()).unwrap();
                }
                sum
            })
        })
        .collect();

    for value in 0..MESSAGES as u64 {
        FutureExt::block_on(tx.send(value));
    }

    for handle in handles {
        assert_eq!(handle.join().unwrap(), expected);
    }
}

#[test]
fn concurrent_blocking_wait_at_capacity_one() {
    let (mut tx, rx) = bounded(1);
    let mut rx2 = tx.subscribe();
    let handle = thread::spawn(move || {
        let mut sum = 0u64;
        for _ in 0..64 {
            sum += FutureExt::block_on(rx2.recv()).unwrap();
        }
        sum
    });
    let handle1 = thread::spawn(move || {
        let mut sum = 0u64;
        let mut rx = rx;
        for _ in 0..64 {
            sum += FutureExt::block_on(rx.recv()).unwrap();
        }
        sum
    });
    for value in 0..64u64 {
        FutureExt::block_on(tx.send(value));
    }
    assert_eq!(handle.join().unwrap(), 64 * 63 / 2);
    assert_eq!(handle1.join().unwrap(), 64 * 63 / 2);
}

#[test]
fn publish_order_is_program_order() {
    let (mut tx, mut rx1) = bounded(8);
    let mut rx2 = tx.subscribe();

    for value in 0..8 {
        tx.try_send(value).unwrap();
    }

    for value in 0..8 {
        assert_eq!(rx1.try_recv(), Ok(value));
        assert_eq!(rx2.try_recv(), Ok(value));
    }
}

// ---------------------------------------------------------------------------------------------
// Strict capacity
// ---------------------------------------------------------------------------------------------

#[test]
fn try_send_rejects_at_capacity_and_returns_the_value() {
    let (mut tx, mut rx) = bounded(2);

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
    let (mut tx, _rx) = bounded(2);
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
    let (mut tx, mut rx1) = bounded(4);
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
    let (mut tx, mut rx1) = bounded(1);
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
    assert_eq!(rx2.try_recv(), Ok(2));
}

#[test]
fn receive_that_vacates_the_head_wakes_the_blocked_sender() {
    let (mut tx, mut rx) = bounded(1);
    tx.try_send(0).unwrap();

    let tracker = Arc::new(WakeCounter::default());
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
fn parked_recv_that_reclaims_wakes_the_blocked_sender() {
    let (mut tx, mut rx) = bounded(1);
    tx.try_send(0).unwrap();

    let tracker = Arc::new(WakeCounter::default());
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
fn dropping_a_lagging_receiver_wakes_the_blocked_sender() {
    let (mut tx, mut rx1) = bounded(1);
    let rx2 = tx.subscribe();
    tx.try_send(0).unwrap();

    let mut send = Box::pin(tx.send(1));
    assert_eq!(rx1.try_recv(), Ok(0));
    assert!(poll_once(send.as_mut()).is_pending());

    // `rx2` is the one holding the backlog; dropping it releases the slot.
    drop(rx2);
    assert_eq!(rx1.unread_message_count(), 0);
    assert!(poll_once(send.as_mut()).is_ready());
}

#[test]
fn dropping_the_last_receiver_wakes_the_blocked_sender() {
    let (mut tx, rx) = bounded(2);
    tx.try_send(0).unwrap();
    tx.try_send(1).unwrap();

    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker.clone());
    let mut send = Box::pin(tx.send(10));
    assert!(
        send.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    drop(rx);

    assert!(
        tracker.count() > 0,
        "blocked sender was never woken after the last receiver was dropped"
    );
    assert!(poll_once(send.as_mut()).is_ready());
}

#[test]
fn sends_never_block_once_all_receivers_are_gone() {
    let (mut tx, rx) = bounded(1);
    tx.try_send(0).unwrap();
    drop(rx);

    assert_eq!(tx.retained_message_count(), 0);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();

    let mut send = Box::pin(tx.send(3));
    assert!(poll_once(send.as_mut()).is_ready());
    drop(send);
    assert_eq!(tx.retained_message_count(), 0);
}

#[test]
fn subscribing_while_the_producer_is_blocked_does_not_release_capacity() {
    let (mut tx, rx) = bounded(1);
    tx.try_send(0).unwrap();

    let mut send = Box::pin(tx.send(1));
    assert!(poll_once(send.as_mut()).is_pending());

    // A new cursor starts at the tail, so it cannot lower the retained backlog. The sender is
    // exclusively borrowed by `send`, so the extra subscription comes from `resubscribe`.
    let _late = rx.resubscribe();
    assert_eq!(rx.unread_message_count(), 1);
    assert!(poll_once(send.as_mut()).is_pending());
}

// ---------------------------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------------------------

#[test]
fn cancelled_send_publishes_nothing() {
    let (mut tx, mut rx) = bounded(1);
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
fn cancelled_recv_releases_its_waker() {
    let (mut tx, mut rx) = bounded(4);

    let tracker = Arc::new(WakeCounter::default());
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

#[test]
fn dropping_a_woken_recv_keeps_another_receivers_waiter() {
    let (mut tx, mut rx1) = bounded::<i32>(2);
    let mut rx2 = tx.subscribe();
    let first = Arc::new(WakeCounter::default());
    let waker = Waker::from(first.clone());
    let mut context = Context::from_waker(&waker);
    let mut recv1 = Box::pin(rx1.recv());

    assert!(recv1.as_mut().poll(&mut context).is_pending());

    tx.try_send(1).unwrap();
    assert_eq!(first.count(), 1);
    assert_eq!(rx2.try_recv(), Ok(1));

    let second = Arc::new(WakeCounter::default());
    let waker = Waker::from(second.clone());
    let mut context = Context::from_waker(&waker);
    let mut recv2 = Box::pin(rx2.recv());
    assert!(recv2.as_mut().poll(&mut context).is_pending());

    // `recv1` was already woken, so dropping it must not release the slot `recv2` now owns.
    drop(recv1);
    tx.try_send(2).unwrap();

    assert_eq!(second.count(), 1);
}

// ---------------------------------------------------------------------------------------------
// Disconnection
// ---------------------------------------------------------------------------------------------

#[test]
fn recv_drains_buffered_messages_before_reporting_disconnection() {
    let (mut tx, mut rx) = bounded(4);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    drop(tx);

    assert_eq!(FutureExt::block_on(rx.recv()), Ok(1));
    assert_eq!(FutureExt::block_on(rx.recv()), Ok(2));
    assert_eq!(FutureExt::block_on(rx.recv()), Err(RecvError::Disconnected));
}

#[test]
fn recv_reports_disconnection_without_any_message() {
    let (tx, mut rx) = bounded::<i32>(4);
    drop(tx);
    assert_eq!(FutureExt::block_on(rx.recv()), Err(RecvError::Disconnected));
}

#[test]
fn parked_recv_wakes_when_the_sender_drops() {
    let (tx, mut rx) = bounded::<i32>(4);

    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker.clone());
    let mut recv = Box::pin(rx.recv());
    assert!(
        recv.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    drop(tx);
    assert_eq!(tracker.count(), 1);
    assert_eq!(
        poll_once(recv.as_mut()),
        std::task::Poll::Ready(Err(RecvError::Disconnected))
    );
}

#[test]
fn parked_native_thread_recv_wakes_when_the_sender_drops() {
    assert_completes_without_deadlock(|| {
        let (tx, mut rx) = bounded::<i32>(4);
        let parked = thread::spawn(move || FutureExt::block_on(rx.recv()));
        // The worker parks on the empty channel. Dropping the sender must finish that receive
        // with Disconnected; a missed wake hangs inside assert_completes_without_deadlock.
        thread::sleep(std::time::Duration::from_millis(50));
        drop(tx);
        assert_eq!(parked.join().unwrap(), Err(RecvError::Disconnected));
    });
}

// ---------------------------------------------------------------------------------------------
// Panic safety
// ---------------------------------------------------------------------------------------------

#[test]
fn panicking_wake_does_not_strand_the_producer_after_a_large_reclaim() {
    let (mut tx, mut fast) = bounded(8);
    let slow = tx.subscribe();
    for value in 0..8 {
        tx.try_send(value).unwrap();
        fast.try_recv().unwrap();
    }

    let tracker = Arc::new(WakeCounter::default());
    let waker = {
        let tracker = tracker.clone();
        waker_on_wake(move || {
            tracker.wake();
            panic!("producer wake panics");
        })
    };
    let mut send = Box::pin(tx.send(8));
    assert!(
        send.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(slow)));
    assert!(result.is_err());
    assert_eq!(tracker.count(), 1);
    assert!(
        poll_once(send.as_mut()).is_ready(),
        "a panicking producer wake must not strand the send"
    );
    assert_eq!(fast.try_recv(), Ok(8));
}

#[test]
fn panicking_receiver_wake_still_wakes_the_rest() {
    let (mut tx, mut rx1) = bounded(4);
    let mut rx2 = tx.subscribe();
    let mut rx3 = tx.subscribe();

    let trackers = [
        Arc::new(WakeCounter::default()),
        Arc::new(WakeCounter::default()),
        Arc::new(WakeCounter::default()),
    ];
    let wakers = trackers
        .iter()
        .enumerate()
        .map(|(index, tracker)| {
            let tracker = tracker.clone();
            waker_on_wake(move || {
                tracker.wake();
                assert_ne!(index, 0, "first receiver wake panics");
            })
        })
        .collect::<Vec<_>>();

    let mut recvs = [
        Box::pin(rx1.recv()),
        Box::pin(rx2.recv()),
        Box::pin(rx3.recv()),
    ];
    for (recv, waker) in recvs.iter_mut().zip(&wakers) {
        assert!(
            recv.as_mut()
                .poll(&mut Context::from_waker(waker))
                .is_pending()
        );
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tx.try_send(1).unwrap();
    }));
    assert!(result.is_err());
    for tracker in &trackers {
        assert_eq!(tracker.count(), 1);
    }
}

#[test]
fn panicking_clone_leaves_the_channel_consistent() {
    let (mut tx, mut rx1) = bounded(4);
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
    let (mut tx, mut rx1) = bounded(3);
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
fn panicking_clone_that_becomes_the_last_reader_wakes_the_producer() {
    let (mut tx, mut rx1) = bounded(1);
    let rx2 = Arc::new(Mutex::new(tx.subscribe()));

    tx.try_send(RaceOnClone {
        armed: true,
        probe: Some(rx2.clone()),
    })
    .unwrap();

    let (waker, counter) = WakeCounter::new();
    let mut context = Context::from_waker(&waker);
    let mut send = Box::pin(tx.send(RaceOnClone {
        armed: false,
        probe: None,
    }));
    assert!(send.as_mut().poll(&mut context).is_pending());
    assert_eq!(counter.count(), 0);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = rx1.try_recv();
    }));
    assert!(result.is_err(), "the clone was expected to panic");

    assert!(
        counter.count() > 0,
        "a panicking clone must not strand a producer on capacity it already freed"
    );
    drop(send);
    assert_eq!(tx.retained_message_count(), 0);
}

#[test]
fn message_destructors_run_outside_the_channel_lock() {
    assert_completes_without_deadlock(|| {
        let (mut tx, mut rx1) = bounded(8);
        let rx2 = tx.subscribe();
        let probe = Arc::new(Mutex::new(tx.subscribe()));

        for value in 0..4 {
            tx.try_send(Reentrant {
                value,
                probe: Some(probe.clone()),
            })
            .unwrap();
            let _ = probe.lock().unwrap().try_recv();
        }

        // Reclaim through a receive, and then through receiver drops.
        assert_eq!(rx1.try_recv().unwrap().value, 0);
        drop(rx2);
        assert_eq!(rx1.try_recv().unwrap().value, 1);
        drop(rx1);
        drop(probe);

        // With no receiver, both send paths discard the payload immediately.
        tx.try_send(Reentrant {
            value: 4,
            probe: None,
        })
        .unwrap();
        FutureExt::block_on(tx.send(Reentrant {
            value: 5,
            probe: None,
        }));
    });
}

#[test]
fn cancelling_a_blocked_send_drops_its_payload_outside_the_channel_lock() {
    assert_completes_without_deadlock(|| {
        let (mut tx, mut rx) = bounded(1);
        tx.try_send(Reentrant {
            value: 0,
            probe: None,
        })
        .unwrap();
        // Subscribe at the tail so the probe does not retain the buffered message.
        let probe = Arc::new(Mutex::new(rx.resubscribe()));

        let mut send = Box::pin(tx.send(Reentrant {
            value: 1,
            probe: Some(probe.clone()),
        }));
        assert!(poll_once(send.as_mut()).is_pending());
        drop(send);

        assert_eq!(rx.try_recv().unwrap().value, 0);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
        tx.try_send(Reentrant {
            value: 2,
            probe: None,
        })
        .unwrap();
        assert_eq!(rx.try_recv().unwrap().value, 2);
        drop(probe);
    });
}
