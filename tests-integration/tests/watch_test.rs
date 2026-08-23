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

use asyncband::watch;
use tokio_test::assert_pending;
use tokio_test::assert_ready;
use tokio_test::task::spawn;

mod support;
use support::TrackWake;
use support::callback_waker;
use support::poll_with_waker;

#[test]
fn watch_coalesces_updates_to_the_latest_value() {
    let (tx, mut rx) = watch::channel(0);
    assert_eq!(*rx.borrow(), 0);
    assert_eq!(rx.has_changed(), Ok(false));

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(rx.has_changed(), Ok(true));

    let mut changed = spawn(rx.changed());
    let value = assert_ready!(changed.poll()).unwrap();
    assert_eq!(*value, 2);
    drop(changed);
    assert_eq!(rx.has_changed(), Ok(false));
}

#[test]
fn borrow_does_not_mark_a_version_observed() {
    let (tx, mut rx) = watch::channel(0);
    tx.send(1).unwrap();

    assert_eq!(*rx.borrow(), 1);
    assert_eq!(rx.has_changed(), Ok(true));
    assert_eq!(*rx.borrow_and_update(), 1);
    assert_eq!(rx.has_changed(), Ok(false));
}

#[test]
fn subscriptions_start_with_the_current_value_observed() {
    let (tx, _rx) = watch::channel(0);
    tx.send(1).unwrap();
    let mut subscribed = tx.subscribe();
    assert_eq!(*subscribed.borrow(), 1);
    assert_eq!(subscribed.has_changed(), Ok(false));

    tx.send(2).unwrap();
    assert_eq!(*pollster::block_on(subscribed.changed()).unwrap(), 2);
}

#[test]
fn cloned_receiver_preserves_its_observed_version() {
    let (tx, mut first) = watch::channel(0);
    tx.send(1).unwrap();
    let mut second = first.clone();

    assert_eq!(*pollster::block_on(first.changed()).unwrap(), 1);
    assert_eq!(*pollster::block_on(second.changed()).unwrap(), 1);
}

#[test]
fn changed_drains_the_last_update_before_disconnection() {
    let (tx, mut rx) = watch::channel(0);
    tx.send(1).unwrap();
    drop(tx);

    assert_eq!(*pollster::block_on(rx.changed()).unwrap(), 1);
    assert_eq!(
        pollster::block_on(rx.changed()),
        Err(watch::RecvError::Disconnected)
    );
}

#[test]
fn cancelling_changed_does_not_consume_the_next_update() {
    let (tx, mut rx) = watch::channel(0);
    let mut cancelled = spawn(rx.changed());
    assert_pending!(cancelled.poll());
    drop(cancelled);

    tx.send(1).unwrap();
    assert_eq!(*pollster::block_on(rx.changed()).unwrap(), 1);
}

#[test]
fn send_returns_the_value_when_no_receivers_remain() {
    let (tx, rx) = watch::channel(String::from("initial"));
    drop(rx);

    assert_eq!(
        tx.send(String::from("unsent")).unwrap_err().into_inner(),
        "unsent"
    );
}

#[test]
fn sender_tracks_receivers_and_can_subscribe_after_disconnection() {
    let (tx, rx) = watch::channel(0);
    assert_eq!(tx.receiver_count(), 1);
    drop(rx);
    assert_eq!(tx.receiver_count(), 0);
    assert_eq!(tx.send(1).unwrap_err().into_inner(), 1);

    let replacement = tx.subscribe();
    assert_eq!(tx.receiver_count(), 1);
    assert_eq!(*replacement.borrow(), 0);
    tx.send(2).unwrap();
    assert_eq!(*replacement.borrow(), 2);
}

#[test]
fn changed_wakes_on_update_and_on_final_sender_drop() {
    let (tx, mut rx) = watch::channel(0);
    let other = tx.clone();
    let mut changed = spawn(rx.changed());
    assert_pending!(changed.poll());
    tx.send(1).unwrap();
    assert!(changed.is_woken());
    assert_eq!(*assert_ready!(changed.poll()).unwrap(), 1);
    drop(changed);

    let mut changed = spawn(rx.changed());
    assert_pending!(changed.poll());
    drop(tx);
    assert!(!changed.is_woken());
    drop(other);
    assert!(changed.is_woken());
    assert_eq!(
        assert_ready!(changed.poll()),
        Err(watch::RecvError::Disconnected)
    );
}

#[test]
fn cancelled_changed_releases_its_waker() {
    let (tx, mut rx) = watch::channel(0);
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let baseline = Arc::strong_count(&tracker);
    let mut changed = Box::pin(rx.changed());

    assert!(poll_with_waker(changed.as_mut(), &waker).is_pending());
    assert_eq!(Arc::strong_count(&tracker), baseline + 1);
    drop(changed);
    assert_eq!(Arc::strong_count(&tracker), baseline);

    tx.send(1).unwrap();
    assert_eq!(tracker.0.load(Ordering::Relaxed), 0);
    assert_eq!(*pollster::block_on(rx.changed()).unwrap(), 1);
}

#[test]
fn watch_wakers_run_after_unlocking_the_channel() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (tx, mut rx) = watch::channel(0);
        let callback_sender = tx.clone();
        let waker = callback_waker(move || {
            callback_sender.send(2).unwrap();
        });
        let mut changed = Box::pin(rx.changed());

        assert!(poll_with_waker(changed.as_mut(), &waker).is_pending());
        tx.send(1).unwrap();
        let Poll::Ready(Ok(value)) = poll_with_waker(changed.as_mut(), &waker) else {
            panic!("watch update was not ready");
        };
        assert_eq!(*value, 2);
        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("waker callback deadlocked against the watch lock");
}

struct ReentrantDrop(Option<Box<dyn FnOnce() + Send + Sync>>);

impl Drop for ReentrantDrop {
    fn drop(&mut self) {
        if let Some(callback) = self.0.take() {
            callback();
        }
    }
}

#[test]
fn replaced_values_are_dropped_after_unlocking_the_channel() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (tx, _rx) = watch::channel(ReentrantDrop(None));
        let callback_sender = tx.clone();
        tx.send(ReentrantDrop(Some(Box::new(move || {
            let _ = callback_sender.receiver_count();
        }))))
        .unwrap();
        tx.send(ReentrantDrop(None)).unwrap();
        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("value destructor deadlocked against the watch lock");
}
