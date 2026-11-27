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

use std::future::pending;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use asyncband::once::Once;
use tests_integration::WakeCounter;
use tests_integration::expect_ready;
use tests_integration::poll_once;
use tests_integration::poll_with;
use tokio::sync::Barrier;

#[tokio::test]
async fn call_once_runs_only_one_initializer() {
    let once = Once::new();
    let counter = AtomicUsize::new(0);

    assert!(!once.is_completed());

    once.call_once(async || {
        counter.fetch_add(1, Ordering::SeqCst);
    })
    .await;

    assert!(once.is_completed());
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    once.call_once(async || {
        counter.fetch_add(1, Ordering::SeqCst);
    })
    .await;

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_callers_run_one_initializer() {
    const TASKS: usize = 100;

    let once = Arc::new(Once::new());
    let counter = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(Barrier::new(TASKS + 1));
    let mut tasks = Vec::with_capacity(TASKS);

    for _ in 0..TASKS {
        let once = once.clone();
        let counter = counter.clone();
        let start = start.clone();
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            once.call_once(async || {
                counter.fetch_add(1, Ordering::SeqCst);
            })
            .await;
        }));
    }

    start.wait().await;
    for task in tasks {
        task.await.unwrap();
    }

    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert!(once.is_completed());
}

#[tokio::test]
async fn cancelled_initializer_can_be_retried() {
    let once = Once::new();
    let mut first = Box::pin(once.call_once(async || pending::<()>().await));

    assert!(poll_once(first.as_mut()).is_pending());
    drop(first);

    once.call_once(async || {}).await;
    assert!(once.is_completed());
}

#[tokio::test]
async fn panicked_initializer_can_be_retried() {
    let once = Arc::new(Once::new());
    let counter = Arc::new(AtomicUsize::new(0));

    let handle = tokio::spawn({
        let once = once.clone();
        let counter = counter.clone();
        async move {
            once.call_once(async || {
                counter.fetch_add(1, Ordering::SeqCst);
                panic!("boom");
            })
            .await;
        }
    });

    let error = handle.await.expect_err("initializer should panic");
    assert!(error.is_panic());

    once.call_once(async || {
        counter.fetch_add(1, Ordering::SeqCst);
    })
    .await;

    assert_eq!(counter.load(Ordering::SeqCst), 2);
    assert!(once.is_completed());
}

#[tokio::test]
async fn wait_observes_completion() {
    let once = Once::new();
    let mut waiting = pin!(once.wait());

    assert!(poll_once(waiting.as_mut()).is_pending());
    once.call_once(async || {}).await;
    assert!(poll_once(waiting.as_mut()).is_ready());

    let mut completed = pin!(once.wait());
    assert!(poll_once(completed.as_mut()).is_ready());
}

#[test]
fn completion_wakes_all_queued_callers_even_when_the_first_follower_stays_paused() {
    let once = Once::new();
    let (finish, ready) = tokio::sync::oneshot::channel();
    let mut leader = pin!(once.call_once(async || {
        ready.await.unwrap();
    }));
    let mut paused_follower = Box::pin(once.call_once(async || unreachable!()));
    let mut trailing_follower = pin!(once.call_once(async || unreachable!()));
    let mut observer = pin!(once.wait());
    let (paused_waker, paused_wakes) = WakeCounter::new();
    let (trailing_waker, trailing_wakes) = WakeCounter::new();
    let (observer_waker, observer_wakes) = WakeCounter::new();

    assert!(poll_once(leader.as_mut()).is_pending());
    assert!(poll_with(paused_follower.as_mut(), &paused_waker).is_pending());
    assert!(poll_with(trailing_follower.as_mut(), &trailing_waker).is_pending());
    assert!(poll_with(observer.as_mut(), &observer_waker).is_pending());

    finish.send(()).unwrap();
    expect_ready(poll_once(leader.as_mut()));

    assert_eq!(paused_wakes.count(), 1);
    assert_eq!(trailing_wakes.count(), 1);
    assert_eq!(observer_wakes.count(), 1);

    // `paused_follower` is neither polled nor dropped yet; trailing callers must still complete.
    expect_ready(poll_with(trailing_follower.as_mut(), &trailing_waker));
    expect_ready(poll_with(observer.as_mut(), &observer_waker));
    drop(paused_follower);
    assert_eq!(trailing_wakes.count(), 1);
}

#[test]
fn cancelled_leader_hands_off_fifo_without_waking_observers() {
    let once = Once::new();
    let mut first = Box::pin(once.call_once(async || pending::<()>().await));
    let mut second = Box::pin(once.call_once(async || unreachable!()));
    let mut third = Box::pin(once.call_once(async || unreachable!()));
    let mut observer = pin!(once.wait());
    let mut fourth = pin!(once.call_once(async || {}));
    let (second_waker, second_wakes) = WakeCounter::new();
    let (third_waker, third_wakes) = WakeCounter::new();
    let (observer_waker, observer_wakes) = WakeCounter::new();
    let (fourth_waker, fourth_wakes) = WakeCounter::new();

    // `second` occupies the inline slot while `third` and `observer` spill to overflow. Dropping
    // `second` vacates the inline slot, and `fourth` must still queue behind `third` rather than
    // jumping ahead through the empty inline slot.
    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_with(second.as_mut(), &second_waker).is_pending());
    assert!(poll_with(third.as_mut(), &third_waker).is_pending());
    assert!(poll_with(observer.as_mut(), &observer_waker).is_pending());
    drop(second);
    assert_eq!(second_wakes.count(), 0);
    assert!(poll_with(fourth.as_mut(), &fourth_waker).is_pending());

    drop(first);
    assert_eq!(third_wakes.count(), 1);
    assert_eq!(fourth_wakes.count(), 0);
    assert_eq!(observer_wakes.count(), 0);

    drop(third);
    assert_eq!(fourth_wakes.count(), 1);
    assert_eq!(observer_wakes.count(), 0);

    expect_ready(poll_with(fourth.as_mut(), &fourth_waker));
    assert_eq!(observer_wakes.count(), 1);
    expect_ready(poll_with(observer.as_mut(), &observer_waker));
}
