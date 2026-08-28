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
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;

use asyncband::watch;

struct TrackWake(AtomicUsize);

impl Wake for TrackWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct WakeCallback(Mutex<Option<Box<dyn FnOnce() + Send>>>);

impl Wake for WakeCallback {
    fn wake(self: Arc<Self>) {
        let callback = self.0.lock().unwrap().take();
        if let Some(callback) = callback {
            callback();
        }
    }
}

struct DropCallbackWake(Mutex<Option<Box<dyn FnOnce() + Send>>>);

// This test needs a custom waker whose final `Arc` drop is observable.
#[allow(clippy::manual_noop_waker)]
impl Wake for DropCallbackWake {
    fn wake(self: Arc<Self>) {}
}

impl Drop for DropCallbackWake {
    fn drop(&mut self) {
        if let Some(callback) = self.0.get_mut().unwrap().take() {
            callback();
        }
    }
}

struct ReentrantDrop(Option<watch::Sender<ReentrantDrop>>);

impl Drop for ReentrantDrop {
    fn drop(&mut self) {
        if let Some(sender) = &self.0 {
            let _ = sender.receiver_count();
        }
    }
}

fn poll_with<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

#[test]
fn initial_value_is_observed_and_updates_coalesce() {
    let (tx, mut rx) = watch::channel(0);

    assert_eq!(*rx.borrow(), 0);
    assert_eq!(rx.has_changed(), Ok(false));

    tx.send(1).unwrap();
    tx.send(2).unwrap();

    assert_eq!(rx.has_changed(), Ok(true));
    assert_eq!(*pollster::block_on(rx.changed()).unwrap(), 2);
    assert_eq!(rx.has_changed(), Ok(false));
}

#[test]
fn equal_values_still_create_a_new_version() {
    let (tx, mut rx) = watch::channel(1);

    tx.send(1).unwrap();

    assert_eq!(rx.has_changed(), Ok(true));
    assert_eq!(*pollster::block_on(rx.changed()).unwrap(), 1);
}

#[test]
fn borrow_does_not_consume_but_borrow_and_update_does() {
    let (tx, mut rx) = watch::channel(0);
    tx.send(1).unwrap();

    assert_eq!(*rx.borrow(), 1);
    assert_eq!(rx.has_changed(), Ok(true));
    assert_eq!(*rx.borrow_and_update(), 1);
    assert_eq!(rx.has_changed(), Ok(false));
}

#[test]
fn cloned_receivers_inherit_then_advance_independently() {
    let (tx, mut first) = watch::channel(0);
    tx.send(1).unwrap();
    let mut second = first.clone();

    assert_eq!(*pollster::block_on(first.changed()).unwrap(), 1);
    assert_eq!(first.has_changed(), Ok(false));
    assert_eq!(second.has_changed(), Ok(true));
    assert_eq!(*pollster::block_on(second.changed()).unwrap(), 1);

    tx.send(2).unwrap();
    assert_eq!(*first.borrow_and_update(), 2);
    assert_eq!(second.has_changed(), Ok(true));
}

#[test]
fn subscriptions_start_at_the_current_version() {
    let (tx, _rx) = watch::channel(0);
    tx.send(1).unwrap();
    let mut subscribed = tx.subscribe();

    assert_eq!(*subscribed.borrow(), 1);
    assert_eq!(subscribed.has_changed(), Ok(false));

    tx.send(2).unwrap();
    assert_eq!(*pollster::block_on(subscribed.changed()).unwrap(), 2);
}

#[test]
fn final_unseen_value_is_reported_before_disconnection() {
    let (tx, mut first) = watch::channel(0);
    let mut second = first.clone();
    tx.send(1).unwrap();
    drop(tx);

    assert!(first.is_disconnected());
    assert_eq!(first.has_changed(), Ok(true));
    assert_eq!(*first.borrow(), 1);
    assert_eq!(first.has_changed(), Ok(true));
    assert_eq!(*pollster::block_on(first.changed()).unwrap(), 1);
    assert_eq!(first.has_changed(), Err(watch::RecvError::Disconnected));

    assert_eq!(*pollster::block_on(second.changed()).unwrap(), 1);
    assert_eq!(
        pollster::block_on(second.changed()),
        Err(watch::RecvError::Disconnected)
    );
}

#[test]
fn sending_without_receivers_returns_the_value_and_preserves_current() {
    let (tx, rx) = watch::channel(String::from("initial"));
    assert_eq!(tx.receiver_count(), 1);
    drop(rx);
    assert_eq!(tx.receiver_count(), 0);

    let error = tx.send(String::from("unsent")).unwrap_err();
    assert_eq!(error.as_inner(), "unsent");
    assert_eq!(error.into_inner(), "unsent");

    let mut replacement = tx.subscribe();
    assert_eq!(tx.receiver_count(), 1);
    assert_eq!(&*replacement.borrow(), "initial");
    assert_eq!(replacement.has_changed(), Ok(false));

    tx.send(String::from("accepted")).unwrap();
    assert_eq!(&*replacement.borrow_and_update(), "accepted");
}

#[test]
fn cancelling_changed_releases_its_waker_without_consuming() {
    let (tx, mut rx) = watch::channel(0);
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let baseline = Arc::strong_count(&tracker);
    let mut changed = Box::pin(rx.changed());

    assert!(poll_with(changed.as_mut(), &waker).is_pending());
    assert_eq!(Arc::strong_count(&tracker), baseline + 1);
    drop(changed);
    assert_eq!(Arc::strong_count(&tracker), baseline);

    tx.send(1).unwrap();
    assert_eq!(tracker.0.load(Ordering::Relaxed), 0);
    assert_eq!(*pollster::block_on(rx.changed()).unwrap(), 1);
}

#[test]
fn cancelling_after_wake_still_leaves_the_update_unseen() {
    let (tx, mut rx) = watch::channel(0);
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let mut changed = Box::pin(rx.changed());

    assert!(poll_with(changed.as_mut(), &waker).is_pending());
    tx.send(1).unwrap();
    assert_eq!(tracker.0.load(Ordering::Relaxed), 1);
    drop(changed);

    assert_eq!(*pollster::block_on(rx.changed()).unwrap(), 1);
}

#[test]
fn one_update_wakes_every_waiting_receiver_once() {
    let (tx, mut first) = watch::channel(0);
    let mut second = first.clone();
    let first_tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let second_tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let first_waker = Waker::from(first_tracker.clone());
    let second_waker = Waker::from(second_tracker.clone());
    let mut first_changed = Box::pin(first.changed());
    let mut second_changed = Box::pin(second.changed());

    assert!(poll_with(first_changed.as_mut(), &first_waker).is_pending());
    assert!(poll_with(second_changed.as_mut(), &second_waker).is_pending());

    tx.send(1).unwrap();
    assert_eq!(first_tracker.0.load(Ordering::Relaxed), 1);
    assert_eq!(second_tracker.0.load(Ordering::Relaxed), 1);
    assert_eq!(
        poll_with(first_changed.as_mut(), &first_waker),
        Poll::Ready(Ok(Arc::new(1)))
    );
    assert_eq!(
        poll_with(second_changed.as_mut(), &second_waker),
        Poll::Ready(Ok(Arc::new(1)))
    );
}

#[test]
fn only_the_last_sender_drop_wakes_a_waiter() {
    let (tx, mut rx) = watch::channel(());
    let other = tx.clone();
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let mut changed = Box::pin(rx.changed());

    assert!(poll_with(changed.as_mut(), &waker).is_pending());
    drop(tx);
    assert_eq!(tracker.0.load(Ordering::Relaxed), 0);
    drop(other);
    assert_eq!(tracker.0.load(Ordering::Relaxed), 1);
    assert_eq!(
        poll_with(changed.as_mut(), &waker),
        Poll::Ready(Err(watch::RecvError::Disconnected))
    );
}

#[test]
fn dropping_a_stale_changed_future_keeps_a_new_waiter_registered() {
    let (tx, mut first) = watch::channel(0);
    let mut second = first.clone();
    let first_tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let first_waker = Waker::from(first_tracker.clone());
    let mut first_changed = Box::pin(first.changed());

    assert!(poll_with(first_changed.as_mut(), &first_waker).is_pending());
    tx.send(1).unwrap();
    assert_eq!(first_tracker.0.load(Ordering::Relaxed), 1);

    assert_eq!(*second.borrow_and_update(), 1);
    let second_tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let second_waker = Waker::from(second_tracker.clone());
    let mut second_changed = Box::pin(second.changed());
    assert!(poll_with(second_changed.as_mut(), &second_waker).is_pending());

    drop(first_changed);
    tx.send(2).unwrap();

    assert_eq!(second_tracker.0.load(Ordering::Relaxed), 1);
    assert_eq!(
        poll_with(second_changed.as_mut(), &second_waker),
        Poll::Ready(Ok(Arc::new(2)))
    );
}

#[test]
fn replaced_values_are_dropped_outside_the_channel_lock() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let (tx, _rx) = watch::channel(ReentrantDrop(None));
        tx.send(ReentrantDrop(Some(tx.clone()))).unwrap();
        tx.send(ReentrantDrop(None)).unwrap();
        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("value destructor deadlocked against the watch lock");
    worker.join().unwrap();
}

#[test]
fn wake_callbacks_run_outside_the_channel_lock() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let (tx, mut rx) = watch::channel(0);
        let callback_sender = tx.clone();
        let waker = Waker::from(Arc::new(WakeCallback(Mutex::new(Some(Box::new(
            move || {
                let _ = callback_sender.receiver_count();
            },
        ))))));
        let mut changed = Box::pin(rx.changed());

        assert!(poll_with(changed.as_mut(), &waker).is_pending());
        tx.send(1).unwrap();
        assert_eq!(
            poll_with(changed.as_mut(), &waker),
            Poll::Ready(Ok(Arc::new(1)))
        );
        drop(changed);

        let observer = rx.clone();
        let waker = Waker::from(Arc::new(WakeCallback(Mutex::new(Some(Box::new(
            move || {
                assert!(observer.is_disconnected());
            },
        ))))));
        let mut changed = Box::pin(rx.changed());
        assert!(poll_with(changed.as_mut(), &waker).is_pending());
        drop(tx);
        assert_eq!(
            poll_with(changed.as_mut(), &waker),
            Poll::Ready(Err(watch::RecvError::Disconnected))
        );
        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("wake callback deadlocked against the watch lock");
    worker.join().unwrap();
}

#[test]
fn replaced_wakers_are_dropped_outside_the_channel_lock() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let (tx, mut rx) = watch::channel(0);
        let callback_sender = tx.clone();
        let old_waker = Waker::from(Arc::new(DropCallbackWake(Mutex::new(Some(Box::new(
            move || {
                let _ = callback_sender.receiver_count();
            },
        ))))));
        let mut changed = Box::pin(rx.changed());
        assert!(poll_with(changed.as_mut(), &old_waker).is_pending());
        drop(old_waker);

        let replacement = Waker::from(Arc::new(TrackWake(AtomicUsize::new(0))));
        assert!(poll_with(changed.as_mut(), &replacement).is_pending());
        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("replaced waker destructor deadlocked against the watch lock");
    worker.join().unwrap();
}
