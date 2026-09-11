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

use std::cell::Cell;
use std::sync::Arc;
use std::sync::Barrier;
use std::task::Poll;
use std::task::Waker;
use std::thread;

use asyncband::blocking::FutureExt;
use asyncband::completion;
use tests_integration::PanicWake;
use tests_integration::WakeCounter;
use tests_integration::assert_completes_without_deadlock;
use tests_integration::poll_with;
use tests_integration::waker_on_drop;
use tests_integration::waker_on_wake;

struct NotClone(String);

#[test]
fn all_observers_borrow_the_same_non_clone_value() {
    let (completer, completion) = completion::new();
    let first = completion.clone();
    let second = completion.clone();
    drop(completion);

    assert!(completer.complete(NotClone(String::from("ready"))).is_ok());

    let first_value = FutureExt::block_on(first.wait()).unwrap();
    let second_value = FutureExt::block_on(second.wait()).unwrap();
    let repeated = FutureExt::block_on(first.wait()).unwrap();
    assert_eq!(first_value.0.as_str(), "ready");
    assert!(std::ptr::eq(first_value, second_value));
    assert!(std::ptr::eq(first_value, repeated));

    let late = first.clone();
    drop(first);
    let late_value = FutureExt::block_on(late.wait()).unwrap();
    assert!(std::ptr::eq(second_value, late_value));
}

#[test]
fn completer_transfers_a_send_only_value_between_threads() {
    let (completer, completion) = completion::new::<Cell<u8>>();

    let worker = thread::spawn(move || completer.complete(Cell::new(7)));

    assert_eq!(FutureExt::block_on(completion.wait()).unwrap().get(), 7);
    worker.join().unwrap().unwrap();
}

#[test]
fn complete_returns_the_value_when_no_observers_remain() {
    let (completer, completion) = completion::new();
    drop(completion);
    assert_eq!(
        completer.complete(String::from("unobserved")).unwrap_err(),
        "unobserved"
    );
}

#[test]
fn completed_payload_is_released_with_the_last_observer() {
    let payload = Arc::new(());
    let (completer, completion) = completion::new();

    completer.complete(payload.clone()).unwrap();
    assert_eq!(Arc::strong_count(&payload), 2);

    drop(completion);
    assert_eq!(Arc::strong_count(&payload), 1);
}

#[test]
fn abandonment_wakes_registered_waits_and_is_visible_to_late_observers() {
    let (completer, first) = completion::new::<usize>();
    let second = first.clone();
    let first_tracker = Arc::new(WakeCounter::default());
    let second_tracker = Arc::new(WakeCounter::default());
    let first_waker = Waker::from(first_tracker.clone());
    let second_waker = Waker::from(second_tracker.clone());
    let mut first_wait = Box::pin(first.wait());
    let mut second_wait = Box::pin(second.wait());

    assert!(poll_with(first_wait.as_mut(), &first_waker).is_pending());
    assert!(poll_with(second_wait.as_mut(), &second_waker).is_pending());
    drop(completer);

    assert_eq!(first_tracker.count(), 1);
    assert_eq!(second_tracker.count(), 1);
    assert!(matches!(
        poll_with(first_wait.as_mut(), &first_waker),
        Poll::Ready(Err(_))
    ));
    assert!(matches!(
        poll_with(second_wait.as_mut(), &second_waker),
        Poll::Ready(Err(_))
    ));

    let late = first.clone();
    assert!(FutureExt::block_on(late.wait()).is_err());
}

#[test]
fn payload_errors_remain_distinct_from_abandonment() {
    let (completer, completion) = completion::new::<Result<u8, &'static str>>();
    completer.complete(Err("domain error")).unwrap();
    assert_eq!(
        FutureExt::block_on(completion.wait()),
        Ok(&Err("domain error"))
    );

    let (completer, completion) = completion::new::<Result<u8, &'static str>>();
    drop(completer);
    assert!(FutureExt::block_on(completion.wait()).is_err());
}

#[test]
fn cancelling_a_wait_releases_only_its_waker() {
    let (completer, completion) = completion::new();
    let cancelled_tracker = Arc::new(WakeCounter::default());
    let waiting_tracker = Arc::new(WakeCounter::default());
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
    assert_eq!(cancelled_tracker.count(), 0);
    assert_eq!(waiting_tracker.count(), 1);
    assert_eq!(
        poll_with(waiting.as_mut(), &waiting_waker),
        Poll::Ready(Ok(&7))
    );
}

#[test]
fn cancelling_after_wake_does_not_consume_the_shared_result() {
    let (completer, first) = completion::new();
    let second = first.clone();
    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker.clone());
    let mut wait = Box::pin(first.wait());

    assert!(poll_with(wait.as_mut(), &waker).is_pending());
    completer.complete(9).unwrap();
    assert_eq!(tracker.count(), 1);
    drop(wait);

    assert_eq!(FutureExt::block_on(second.wait()), Ok(&9));
}

#[test]
fn cancelling_after_abandonment_does_not_retain_the_waker() {
    let (completer, completion) = completion::new::<usize>();
    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker.clone());
    let baseline = Arc::strong_count(&tracker);
    let mut wait = Box::pin(completion.wait());

    assert!(poll_with(wait.as_mut(), &waker).is_pending());
    drop(completer);
    assert_eq!(tracker.count(), 1);
    assert_eq!(Arc::strong_count(&tracker), baseline);
    drop(wait);
    assert_eq!(Arc::strong_count(&tracker), baseline);
    assert!(FutureExt::block_on(completion.wait()).is_err());
}

#[test]
fn completion_attempts_every_waker_after_one_panics() {
    let (completer, first) = completion::new();
    let second = first.clone();
    let panicking = Waker::from(Arc::new(PanicWake));
    let tracker = Arc::new(WakeCounter::default());
    let tracked = Waker::from(tracker.clone());
    let mut first_wait = Box::pin(first.wait());
    let mut second_wait = Box::pin(second.wait());

    assert!(poll_with(first_wait.as_mut(), &panicking).is_pending());
    assert!(poll_with(second_wait.as_mut(), &tracked).is_pending());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| completer.complete(11)));
    assert!(result.is_err());
    assert_eq!(tracker.count(), 1);
    assert_eq!(
        poll_with(second_wait.as_mut(), &tracked),
        Poll::Ready(Ok(&11))
    );
}

#[test]
fn wake_callbacks_run_outside_the_completion_lock() {
    assert_completes_without_deadlock(|| {
        let (completer, completion) = completion::new();
        let callback_completion = completion.clone();
        let waker = waker_on_wake(move || {
            assert_eq!(FutureExt::block_on(callback_completion.wait()), Ok(&13));
        });
        let mut wait = Box::pin(completion.wait());

        assert!(poll_with(wait.as_mut(), &waker).is_pending());
        completer.complete(13).unwrap();
        assert_eq!(poll_with(wait.as_mut(), &waker), Poll::Ready(Ok(&13)));
        drop(wait);

        let (completer, completion) = completion::new::<usize>();
        let callback_completion = completion.clone();
        let waker = waker_on_wake(move || {
            assert!(FutureExt::block_on(callback_completion.wait()).is_err());
        });
        let mut wait = Box::pin(completion.wait());
        assert!(poll_with(wait.as_mut(), &waker).is_pending());
        drop(completer);
        assert!(matches!(
            poll_with(wait.as_mut(), &waker),
            Poll::Ready(Err(_))
        ));
    });
}

#[test]
fn replaced_wakers_are_dropped_outside_the_completion_lock() {
    assert_completes_without_deadlock(|| {
        let (completer, completion) = completion::new::<usize>();
        let old_waker = waker_on_drop(move || drop(completer));
        let mut wait = Box::pin(completion.wait());
        assert!(poll_with(wait.as_mut(), &old_waker).is_pending());
        drop(old_waker);

        let tracker = Arc::new(WakeCounter::default());
        let replacement = Waker::from(tracker.clone());
        assert!(poll_with(wait.as_mut(), &replacement).is_pending());
        assert_eq!(tracker.count(), 1);
        assert!(matches!(
            poll_with(wait.as_mut(), &replacement),
            Poll::Ready(Err(_))
        ));
    });
}

#[test]
fn cancelled_wakers_are_dropped_outside_the_completion_lock() {
    assert_completes_without_deadlock(|| {
        let (completer, completion) = completion::new::<usize>();
        let waker = waker_on_drop(move || drop(completer));
        let mut wait = Box::pin(completion.wait());
        assert!(poll_with(wait.as_mut(), &waker).is_pending());
        drop(waker);
        drop(wait);
        assert!(FutureExt::block_on(completion.wait()).is_err());
    });
}

#[test]
fn complete_and_final_observer_drop_linearize_cleanly() {
    for _ in 0..100 {
        let (completer, completion) = completion::new();
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
                assert_eq!(error, 17);
            }
        });
    }
}
