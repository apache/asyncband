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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;

use asyncband::blocking::FutureExt;
use asyncband::once::OnceCell;
use tests_integration::WakeCounter;
use tests_integration::assert_completes_without_deadlock;
use tests_integration::expect_ready;
use tests_integration::poll_once;
use tests_integration::poll_with;
use tokio::sync::Barrier;

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[tokio::test]
async fn dropping_the_cell_drops_its_value() {
    let dropped = Arc::new(AtomicBool::new(false));

    {
        let cell = OnceCell::new();
        cell.get_or_init(async || DropFlag(dropped.clone())).await;
        assert!(!dropped.load(Ordering::Acquire));
    }

    assert!(dropped.load(Ordering::Acquire));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_callers_publish_one_value() {
    const TASKS: usize = 100;

    let cell = Arc::new(OnceCell::new());
    let attempts = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(Barrier::new(TASKS + 1));
    let mut tasks = Vec::with_capacity(TASKS);

    for value in 0..TASKS {
        let cell = cell.clone();
        let attempts = attempts.clone();
        let start = start.clone();
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            *cell
                .get_or_init(async || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    [value; 32]
                })
                .await
        }));
    }

    start.wait().await;
    let expected = tasks.remove(0).await.unwrap();
    assert_eq!(expected, [expected[0]; 32]);
    for task in tasks {
        assert_eq!(task.await.unwrap(), expected);
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelled_initialization_can_be_retried() {
    let cell = OnceCell::new();
    let mut first = Box::pin(cell.get_or_init(async || pending::<u8>().await));

    assert!(poll_once(first.as_mut()).is_pending());
    drop(first);

    assert_eq!(*cell.get_or_init(async || 2).await, 2);
}

#[test]
fn cancelling_a_partial_waiter_does_not_start_another_initializer() {
    let cell = OnceCell::new();
    let mut first = Box::pin(cell.get_or_init(async || pending::<u8>().await));
    let mut cancelled = Box::pin(cell.get_or_init(async || unreachable!()));
    let mut next = pin!(cell.get_or_init(async || 2));
    let (waker, wakes) = WakeCounter::new();

    assert!(poll_once(first.as_mut()).is_pending());
    // This waiter holds the spare permit, but still cannot initialize the cell.
    assert!(poll_once(cancelled.as_mut()).is_pending());
    assert!(poll_with(next.as_mut(), &waker).is_pending());
    drop(cancelled);
    assert_eq!(wakes.count(), 0);
    assert!(poll_with(next.as_mut(), &waker).is_pending());
    assert_eq!(cell.get(), None);

    drop(first);
    assert_eq!(wakes.count(), 1);
    assert_eq!(expect_ready(poll_with(next.as_mut(), &waker)), &2);
}

#[test]
fn cancelling_a_granted_waiter_passes_initialization_to_the_next() {
    let cell = OnceCell::new();
    let mut first = Box::pin(cell.get_or_init(async || pending::<u8>().await));
    let mut cancelled = Box::pin(cell.get_or_init(async || unreachable!()));
    let mut next = pin!(cell.get_or_init(async || 3));
    let (cancelled_waker, cancelled_wakes) = WakeCounter::new();
    let (next_waker, next_wakes) = WakeCounter::new();

    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_with(cancelled.as_mut(), &cancelled_waker).is_pending());
    assert!(poll_with(next.as_mut(), &next_waker).is_pending());
    drop(first);
    assert_eq!(cancelled_wakes.count(), 1);
    assert_eq!(next_wakes.count(), 0);

    drop(cancelled);
    assert_eq!(next_wakes.count(), 1);
    assert_eq!(expect_ready(poll_with(next.as_mut(), &next_waker)), &3);
}

#[test]
fn failed_initialization_wakes_a_registered_retry() {
    let cell = OnceCell::new();
    let (finish, ready) = tokio::sync::oneshot::channel();
    let mut first = pin!(cell.get_or_try_init(async || {
        ready.await.unwrap();
        Err::<u8, _>("not ready")
    }));
    let mut next = pin!(cell.get_or_init(async || 2));
    let (waker, wakes) = WakeCounter::new();

    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_with(next.as_mut(), &waker).is_pending());
    finish.send(()).unwrap();
    assert_eq!(expect_ready(poll_once(first.as_mut())), Err("not ready"));
    assert_eq!(wakes.count(), 1);
    assert_eq!(expect_ready(poll_with(next.as_mut(), &waker)), &2);
}

#[test]
fn completed_initialization_survives_cancellation_of_a_notified_waiter() {
    let cell = OnceCell::new();
    let (finish, ready) = tokio::sync::oneshot::channel();
    let mut first = pin!(cell.get_or_init(async || {
        ready.await.unwrap();
        [42; 32]
    }));
    let mut cancelled = Box::pin(cell.get_or_init(async || unreachable!()));
    let mut next = pin!(cell.get_or_init(async || unreachable!()));
    let (old_waker, old_wakes) = WakeCounter::new();
    let (waker, wakes) = WakeCounter::new();

    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(cancelled.as_mut()).is_pending());
    assert!(poll_with(next.as_mut(), &old_waker).is_pending());
    assert!(poll_with(next.as_mut(), &waker).is_pending());
    finish.send(()).unwrap();
    assert_eq!(expect_ready(poll_once(first.as_mut())), &[42; 32]);
    assert_eq!(wakes.count(), 0);

    drop(cancelled);
    assert_eq!(old_wakes.count(), 0);
    assert_eq!(wakes.count(), 1);
    assert_eq!(expect_ready(poll_with(next.as_mut(), &waker)), &[42; 32]);
    assert_eq!(cell.get(), Some(&[42; 32]));
    assert_eq!(
        expect_ready(poll_once(pin!(cell.get_or_init(async || unreachable!())))),
        &[42; 32]
    );
}

#[test]
fn registered_waiter_observes_completion_from_another_thread() {
    assert_completes_without_deadlock(|| {
        let cell = OnceCell::new();
        let (finish, ready) = tokio::sync::oneshot::channel();
        let mut first = pin!(cell.get_or_init(async || {
            ready.await.unwrap();
            [42; 32]
        }));
        assert!(poll_once(first.as_mut()).is_pending());

        let (registered, registration) = mpsc::channel();
        thread::scope(|scope| {
            let cell = &cell;
            scope.spawn(move || {
                let mut waiter = pin!(cell.get_or_init(async || unreachable!()));
                assert!(poll_once(waiter.as_mut()).is_pending());
                registered.send(()).unwrap();
                assert_eq!(waiter.block_on(), &[42; 32]);
            });
            registration.recv().unwrap();
            finish.send(()).unwrap();
            assert_eq!(expect_ready(poll_once(first.as_mut())), &[42; 32]);
        });
    });
}

#[test]
fn taking_the_value_allows_reinitialization() {
    let mut cell = OnceCell::from_value(0);
    for value in 0..3 {
        assert_eq!(cell.take(), Some(value));
        assert_eq!(
            expect_ready(poll_once(pin!(cell.get_or_init(async || value + 1)))),
            &(value + 1)
        );
    }
}

#[tokio::test]
async fn failed_initialization_can_be_retried_and_success_is_cached() {
    let cell = OnceCell::new();
    let attempts = AtomicUsize::new(0);

    let error = cell
        .get_or_try_init(async || {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err::<u8, _>("not ready")
        })
        .await;
    assert_eq!(error, Err("not ready"));

    let value = cell
        .get_or_try_init(async || {
            attempts.fetch_add(1, Ordering::SeqCst);
            Ok::<_, &str>(2)
        })
        .await;
    assert_eq!(value, Ok(&2));

    let cached = cell
        .get_or_try_init(async || {
            attempts.fetch_add(1, Ordering::SeqCst);
            Ok::<_, &str>(3)
        })
        .await;
    assert_eq!(cached, Ok(&2));
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn exclusive_initialization_returns_mutable_access() {
    let mut cell: OnceCell<u32> = OnceCell::new();

    let failed = cell
        .get_mut_or_try_init(async || Err::<u32, _>("not ready"))
        .await;
    assert_eq!(failed, Err("not ready"));
    assert_eq!(cell.get_mut(), None);

    let value = cell.get_mut_or_init(async || 41).await;
    *value += 1;

    let value = tokio::spawn(async move { *cell.get_or_init(async || 0).await })
        .await
        .unwrap();
    assert_eq!(value, 42);
}
