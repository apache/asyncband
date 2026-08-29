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
use std::sync::Barrier;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;

use asyncband::completion;

struct NotClone(String);

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

struct ReentrantRejected {
    completer: Option<Arc<completion::Completer<ReentrantRejected>>>,
}

impl Drop for ReentrantRejected {
    fn drop(&mut self) {
        if let Some(completer) = self.completer.take() {
            drop(completer.complete(Self { completer: None }));
        }
    }
}

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

fn poll_with<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

#[test]
fn all_observers_borrow_the_same_non_clone_value() {
    let (completer, completion) = completion::channel();
    let first = completion.clone();
    let second = completion.clone();

    completer.complete(NotClone(String::from("ready"))).unwrap();

    let first_value = pollster::block_on(first.wait()).unwrap();
    let second_value = pollster::block_on(second.wait()).unwrap();
    let repeated = pollster::block_on(first.wait()).unwrap();
    let late = completion.clone();
    let late_value = pollster::block_on(late.wait()).unwrap();

    assert_eq!(first_value.0.as_str(), "ready");
    assert!(std::ptr::eq(first_value, second_value));
    assert!(std::ptr::eq(first_value, repeated));
    assert!(std::ptr::eq(first_value, late_value));
}

#[test]
fn rejected_completions_return_the_original_value() {
    let (completer, completion) = completion::channel();
    completer.complete(String::from("winner")).unwrap();

    let error = completer.complete(String::from("rejected")).unwrap_err();
    assert_eq!(error.as_inner(), "rejected");
    assert_eq!(error.into_inner(), "rejected");
    assert_eq!(pollster::block_on(completion.wait()).unwrap(), "winner");

    let (completer, completion) = completion::channel();
    drop(completion);
    assert_eq!(
        completer
            .complete(String::from("unobserved"))
            .unwrap_err()
            .into_inner(),
        "unobserved"
    );
}

#[test]
fn rejected_payloads_are_dropped_outside_the_completion_lock() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let (completer, _completion) = completion::channel();
        let completer = Arc::new(completer);
        completer
            .complete(ReentrantRejected { completer: None })
            .unwrap();

        drop(completer.complete(ReentrantRejected {
            completer: Some(completer.clone()),
        }));
        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("rejected payload destructor deadlocked against the completion lock");
    worker.join().unwrap();
}

#[test]
fn completer_drop_closes_every_observer_but_completed_drop_does_not() {
    let (completer, first) = completion::channel::<usize>();
    let second = first.clone();
    drop(completer);

    assert_eq!(
        pollster::block_on(first.wait()),
        Err(completion::WaitError::Closed)
    );
    assert_eq!(
        pollster::block_on(second.wait()),
        Err(completion::WaitError::Closed)
    );

    let (completer, completion) = completion::channel();
    completer.complete(42).unwrap();
    drop(completer);
    assert_eq!(pollster::block_on(completion.wait()), Ok(&42));
}

#[test]
fn dropping_one_observer_before_or_after_completion_does_not_affect_another() {
    let (completer, first) = completion::channel();
    let second = first.clone();
    drop(first);

    completer.complete(5).unwrap();
    assert_eq!(pollster::block_on(second.wait()), Ok(&5));

    let (completer, first) = completion::channel();
    let second = first.clone();
    completer.complete(6).unwrap();
    drop(first);

    assert_eq!(pollster::block_on(second.wait()), Ok(&6));
}

#[test]
fn completer_drop_wakes_all_registered_waits() {
    let (completer, first) = completion::channel::<usize>();
    let second = first.clone();
    let first_tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let second_tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let first_waker = Waker::from(first_tracker.clone());
    let second_waker = Waker::from(second_tracker.clone());
    let mut first_wait = Box::pin(first.wait());
    let mut second_wait = Box::pin(second.wait());

    assert!(poll_with(first_wait.as_mut(), &first_waker).is_pending());
    assert!(poll_with(second_wait.as_mut(), &second_waker).is_pending());
    drop(completer);

    assert_eq!(first_tracker.0.load(Ordering::Relaxed), 1);
    assert_eq!(second_tracker.0.load(Ordering::Relaxed), 1);
    assert_eq!(
        poll_with(first_wait.as_mut(), &first_waker),
        Poll::Ready(Err(completion::WaitError::Closed))
    );
    assert_eq!(
        poll_with(second_wait.as_mut(), &second_waker),
        Poll::Ready(Err(completion::WaitError::Closed))
    );
}

#[test]
fn payload_errors_remain_distinct_from_completion_closure() {
    let (completer, completion) = completion::channel::<Result<u8, &'static str>>();
    completer.complete(Err("domain error")).unwrap();
    assert_eq!(
        pollster::block_on(completion.wait()),
        Ok(&Err("domain error"))
    );

    let (completer, completion) = completion::channel::<Result<u8, &'static str>>();
    drop(completer);
    assert_eq!(
        pollster::block_on(completion.wait()),
        Err(completion::WaitError::Closed)
    );
}

#[test]
fn cancelling_a_wait_releases_only_its_waker() {
    let (completer, completion) = completion::channel();
    let cancelled_tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waiting_tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let cancelled_waker = Waker::from(cancelled_tracker.clone());
    let waiting_waker = Waker::from(waiting_tracker.clone());
    let baseline = Arc::strong_count(&cancelled_tracker);
    let mut cancelled = Box::pin(completion.wait());
    let mut waiting = Box::pin(completion.wait());

    assert!(poll_with(cancelled.as_mut(), &cancelled_waker).is_pending());
    assert!(poll_with(waiting.as_mut(), &waiting_waker).is_pending());
    assert_eq!(Arc::strong_count(&cancelled_tracker), baseline + 1);
    drop(cancelled);
    assert_eq!(Arc::strong_count(&cancelled_tracker), baseline);

    completer.complete(7).unwrap();
    assert_eq!(cancelled_tracker.0.load(Ordering::Relaxed), 0);
    assert_eq!(waiting_tracker.0.load(Ordering::Relaxed), 1);
    assert_eq!(
        poll_with(waiting.as_mut(), &waiting_waker),
        Poll::Ready(Ok(&7))
    );
}

#[test]
fn cancelling_after_wake_does_not_consume_the_shared_result() {
    let (completer, first) = completion::channel();
    let second = first.clone();
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let mut wait = Box::pin(first.wait());

    assert!(poll_with(wait.as_mut(), &waker).is_pending());
    completer.complete(9).unwrap();
    assert_eq!(tracker.0.load(Ordering::Relaxed), 1);
    drop(wait);

    assert_eq!(pollster::block_on(second.wait()), Ok(&9));
}

#[test]
fn cancellation_and_completer_drop_have_clean_orderings() {
    let (completer, completion) = completion::channel::<usize>();
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let baseline = Arc::strong_count(&tracker);
    let mut wait = Box::pin(completion.wait());

    assert!(poll_with(wait.as_mut(), &waker).is_pending());
    assert_eq!(Arc::strong_count(&tracker), baseline + 1);
    drop(wait);
    assert_eq!(Arc::strong_count(&tracker), baseline);
    drop(completer);
    assert_eq!(tracker.0.load(Ordering::Relaxed), 0);
    assert_eq!(
        pollster::block_on(completion.wait()),
        Err(completion::WaitError::Closed)
    );

    let (completer, completion) = completion::channel::<usize>();
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let baseline = Arc::strong_count(&tracker);
    let mut wait = Box::pin(completion.wait());

    assert!(poll_with(wait.as_mut(), &waker).is_pending());
    drop(completer);
    assert_eq!(tracker.0.load(Ordering::Relaxed), 1);
    assert_eq!(Arc::strong_count(&tracker), baseline);
    drop(wait);
    assert_eq!(Arc::strong_count(&tracker), baseline);
    assert_eq!(
        pollster::block_on(completion.wait()),
        Err(completion::WaitError::Closed)
    );
}

#[test]
fn completion_attempts_every_waker_after_one_panics() {
    let (completer, first) = completion::channel();
    let second = first.clone();
    let panicking = Waker::from(Arc::new(PanicWake));
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let tracked = Waker::from(tracker.clone());
    let mut first_wait = Box::pin(first.wait());
    let mut second_wait = Box::pin(second.wait());

    assert!(poll_with(first_wait.as_mut(), &panicking).is_pending());
    assert!(poll_with(second_wait.as_mut(), &tracked).is_pending());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| completer.complete(11)));
    assert!(result.is_err());
    assert_eq!(tracker.0.load(Ordering::Relaxed), 1);
    assert_eq!(
        poll_with(second_wait.as_mut(), &tracked),
        Poll::Ready(Ok(&11))
    );
}

#[test]
fn wake_callbacks_run_outside_the_completion_lock() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let (completer, completion) = completion::channel();
        let callback_completion = completion.clone();
        let waker = Waker::from(Arc::new(WakeCallback(Mutex::new(Some(Box::new(
            move || {
                drop(callback_completion);
            },
        ))))));
        let mut wait = Box::pin(completion.wait());

        assert!(poll_with(wait.as_mut(), &waker).is_pending());
        completer.complete(13).unwrap();
        assert_eq!(poll_with(wait.as_mut(), &waker), Poll::Ready(Ok(&13)));
        drop(wait);

        let (completer, completion) = completion::channel::<usize>();
        let callback_completion = completion.clone();
        let waker = Waker::from(Arc::new(WakeCallback(Mutex::new(Some(Box::new(
            move || {
                drop(callback_completion);
            },
        ))))));
        let mut wait = Box::pin(completion.wait());
        assert!(poll_with(wait.as_mut(), &waker).is_pending());
        drop(completer);
        assert_eq!(
            poll_with(wait.as_mut(), &waker),
            Poll::Ready(Err(completion::WaitError::Closed))
        );

        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("wake callback deadlocked against the completion lock");
    worker.join().unwrap();
}

#[test]
fn replaced_wakers_are_dropped_outside_the_completion_lock() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let (_completer, completion) = completion::channel::<usize>();
        let callback_completion = completion.clone();
        let old_waker = Waker::from(Arc::new(DropCallbackWake(Mutex::new(Some(Box::new(
            move || {
                drop(callback_completion);
            },
        ))))));
        let mut wait = Box::pin(completion.wait());
        assert!(poll_with(wait.as_mut(), &old_waker).is_pending());
        drop(old_waker);

        let replacement = Waker::from(Arc::new(TrackWake(AtomicUsize::new(0))));
        assert!(poll_with(wait.as_mut(), &replacement).is_pending());
        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("replaced waker destructor deadlocked against the completion lock");
    worker.join().unwrap();
}

#[test]
fn cancelled_wakers_are_dropped_outside_the_completion_lock() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let (_completer, completion) = completion::channel::<usize>();
        let callback_completion = completion.clone();
        let waker = Waker::from(Arc::new(DropCallbackWake(Mutex::new(Some(Box::new(
            move || {
                drop(callback_completion);
            },
        ))))));
        let mut wait = Box::pin(completion.wait());
        assert!(poll_with(wait.as_mut(), &waker).is_pending());
        drop(waker);
        drop(wait);
        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("cancelled waker destructor deadlocked against the completion lock");
    worker.join().unwrap();
}

#[test]
fn complete_and_final_observer_drop_linearize_cleanly() {
    for _ in 0..100 {
        let (completer, completion) = completion::channel();
        let barrier = Arc::new(Barrier::new(2));
        thread::scope(|scope| {
            let worker_barrier = barrier.clone();
            let worker = scope.spawn(move || {
                worker_barrier.wait();
                drop(completion);
            });

            barrier.wait();
            let result = completer.complete(17);
            worker.join().unwrap();
            if let Err(error) = result {
                assert_eq!(error.into_inner(), 17);
            }
        });
    }
}
