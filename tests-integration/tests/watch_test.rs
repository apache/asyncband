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
use std::sync::atomic::AtomicBool;
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

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("wake failed");
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

#[derive(Clone)]
struct ReentrantDrop(Option<watch::Sender<ReentrantDrop>>);

impl Drop for ReentrantDrop {
    fn drop(&mut self) {
        if let Some(sender) = &self.0 {
            drop(sender.subscribe());
        }
    }
}

struct PanicOnceClone {
    value: usize,
    panic_next: Arc<AtomicBool>,
}

impl Clone for PanicOnceClone {
    fn clone(&self) -> Self {
        assert!(
            !self.panic_next.swap(false, Ordering::Relaxed),
            "clone failed"
        );
        Self {
            value: self.value,
            panic_next: self.panic_next.clone(),
        }
    }
}

fn poll_with<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

#[test]
fn initial_value_is_observed_and_updates_coalesce() {
    let (tx, mut rx) = watch::channel(0);

    assert_eq!(rx.get(), 0);
    assert_eq!(rx.has_changed(), Ok(false));

    tx.send(1).unwrap();
    tx.send(2).unwrap();

    assert_eq!(rx.has_changed(), Ok(true));
    assert_eq!(pollster::block_on(rx.recv()).unwrap(), 2);
    assert_eq!(rx.has_changed(), Ok(false));
}

#[test]
fn equal_values_still_create_a_new_version() {
    let (tx, mut rx) = watch::channel(1);

    tx.send(1).unwrap();

    assert_eq!(rx.has_changed(), Ok(true));
    assert_eq!(pollster::block_on(rx.recv()).unwrap(), 1);
}

#[test]
fn get_does_not_consume_but_recv_does() {
    let (tx, mut rx) = watch::channel(0);
    tx.send(1).unwrap();

    assert_eq!(rx.get(), 1);
    assert_eq!(rx.has_changed(), Ok(true));
    assert_eq!(pollster::block_on(rx.recv()).unwrap(), 1);
    assert_eq!(rx.has_changed(), Ok(false));
}

#[test]
fn panicking_clone_leaves_the_update_unseen() {
    let panic_next = Arc::new(AtomicBool::new(false));
    let (tx, mut rx) = watch::channel(PanicOnceClone {
        value: 0,
        panic_next: panic_next.clone(),
    });
    tx.send(PanicOnceClone {
        value: 1,
        panic_next: panic_next.clone(),
    })
    .unwrap();
    panic_next.store(true, Ordering::Relaxed);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pollster::block_on(rx.recv())
    }));

    assert!(result.is_err());
    assert_eq!(rx.has_changed(), Ok(true));
    assert_eq!(pollster::block_on(rx.recv()).unwrap().value, 1);
}

#[test]
fn cloned_receivers_inherit_then_advance_independently() {
    let (tx, mut first) = watch::channel(0);
    tx.send(1).unwrap();
    let mut second = first.clone();

    assert_eq!(pollster::block_on(first.recv()).unwrap(), 1);
    assert_eq!(first.has_changed(), Ok(false));
    assert_eq!(second.has_changed(), Ok(true));
    assert_eq!(pollster::block_on(second.recv()).unwrap(), 1);

    tx.send(2).unwrap();
    assert_eq!(pollster::block_on(first.recv()).unwrap(), 2);
    assert_eq!(second.has_changed(), Ok(true));
}

#[test]
fn subscriptions_start_at_the_current_version() {
    let (tx, _rx) = watch::channel(0);
    tx.send(1).unwrap();
    let mut subscribed = tx.subscribe();

    assert_eq!(subscribed.get(), 1);
    assert_eq!(subscribed.has_changed(), Ok(false));

    tx.send(2).unwrap();
    assert_eq!(pollster::block_on(subscribed.recv()).unwrap(), 2);
}

#[test]
fn final_unseen_value_is_reported_before_disconnection() {
    let (tx, mut first) = watch::channel(0);
    let mut second = first.clone();
    tx.send(1).unwrap();
    drop(tx);

    assert!(first.is_disconnected());
    assert_eq!(first.has_changed(), Ok(true));
    assert_eq!(first.get(), 1);
    assert_eq!(first.has_changed(), Ok(true));
    assert_eq!(pollster::block_on(first.recv()).unwrap(), 1);
    assert_eq!(first.has_changed(), Err(watch::RecvError::Disconnected));

    assert_eq!(pollster::block_on(second.recv()).unwrap(), 1);
    assert_eq!(
        pollster::block_on(second.recv()),
        Err(watch::RecvError::Disconnected)
    );
}

#[test]
fn sending_without_receivers_returns_the_value_and_preserves_current() {
    let (tx, rx) = watch::channel(String::from("initial"));
    drop(rx);

    let error = tx.send(String::from("unsent")).unwrap_err();
    assert_eq!(error.as_inner(), "unsent");
    assert_eq!(error.into_inner(), "unsent");

    let mut replacement = tx.subscribe();
    assert_eq!(replacement.get(), "initial");
    assert_eq!(replacement.has_changed(), Ok(false));

    tx.send(String::from("accepted")).unwrap();
    assert_eq!(pollster::block_on(replacement.recv()).unwrap(), "accepted");
}

#[test]
fn send_replace_returns_previous_and_publishes_without_receivers() {
    let (tx, mut rx) = watch::channel(String::from("initial"));

    assert_eq!(tx.send_replace(String::from("first")), "initial");
    assert_eq!(pollster::block_on(rx.recv()).unwrap(), "first");

    drop(rx);
    assert_eq!(tx.send_replace(String::from("retained")), "first");

    let mut subscribed = tx.subscribe();
    assert_eq!(subscribed.get(), "retained");
    assert_eq!(subscribed.has_changed(), Ok(false));

    assert_eq!(tx.send_replace(String::from("next")), "retained");
    assert_eq!(pollster::block_on(subscribed.recv()).unwrap(), "next");
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
    assert_eq!(pollster::block_on(rx.recv()).unwrap(), 1);
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

    assert_eq!(pollster::block_on(rx.recv()).unwrap(), 1);
}

#[test]
fn cancelling_recv_after_wake_still_leaves_the_update_unseen() {
    let (tx, mut rx) = watch::channel(0);
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let mut recv = Box::pin(rx.recv());

    assert!(poll_with(recv.as_mut(), &waker).is_pending());
    tx.send(1).unwrap();
    assert_eq!(tracker.0.load(Ordering::Relaxed), 1);
    drop(recv);

    assert_eq!(pollster::block_on(rx.recv()).unwrap(), 1);
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
        Poll::Ready(Ok(()))
    );
    assert_eq!(
        poll_with(second_changed.as_mut(), &second_waker),
        Poll::Ready(Ok(()))
    );
    drop(first_changed);
    drop(second_changed);
    assert_eq!(first.get(), 1);
    assert_eq!(second.get(), 1);
}

#[test]
fn panicking_waker_does_not_skip_other_waiters() {
    let (tx, mut first) = watch::channel(0);
    let mut second = first.clone();
    let panicking = Waker::from(Arc::new(PanicWake));
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let tracked = Waker::from(tracker.clone());
    let mut first_changed = Box::pin(first.changed());
    let mut second_changed = Box::pin(second.changed());

    assert!(poll_with(first_changed.as_mut(), &panicking).is_pending());
    assert!(poll_with(second_changed.as_mut(), &tracked).is_pending());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tx.send(1)));
    assert!(result.is_err());
    assert_eq!(tracker.0.load(Ordering::Relaxed), 1);
    assert_eq!(
        poll_with(second_changed.as_mut(), &tracked),
        Poll::Ready(Ok(()))
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

    assert_eq!(pollster::block_on(second.recv()).unwrap(), 1);
    let second_tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let second_waker = Waker::from(second_tracker.clone());
    let mut second_changed = Box::pin(second.changed());
    assert!(poll_with(second_changed.as_mut(), &second_waker).is_pending());

    drop(first_changed);
    tx.send(2).unwrap();

    assert_eq!(second_tracker.0.load(Ordering::Relaxed), 1);
    assert_eq!(
        poll_with(second_changed.as_mut(), &second_waker),
        Poll::Ready(Ok(()))
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
                drop(callback_sender.subscribe());
            },
        ))))));
        let mut changed = Box::pin(rx.changed());

        assert!(poll_with(changed.as_mut(), &waker).is_pending());
        tx.send(1).unwrap();
        assert_eq!(poll_with(changed.as_mut(), &waker), Poll::Ready(Ok(())));
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
                drop(callback_sender.subscribe());
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
