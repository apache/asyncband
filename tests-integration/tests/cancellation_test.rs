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

use std::pin::pin;
use std::sync::Arc;
use std::sync::Barrier;

use asyncband::cancellation::CancellationSource;
use tests_integration::WakeCounter;
use tests_integration::poll_once;
use tests_integration::poll_with;

#[test]
fn request_reaches_current_and_late_waits() {
    let source = CancellationSource::new();
    let token = source.token();
    let observer = token.clone();
    let (first_waker, first_wakes) = WakeCounter::new();
    let (second_waker, second_wakes) = WakeCounter::new();
    let mut first = pin!(token.cancelled());
    let mut second = pin!(observer.cancelled());
    assert!(!token.is_cancelled());
    assert!(poll_with(first.as_mut(), &first_waker).is_pending());
    assert!(poll_with(second.as_mut(), &second_waker).is_pending());

    source.cancel();
    source.cancel();
    let late = source.token();
    drop(source);

    assert_eq!(first_wakes.count(), 1);
    assert_eq!(second_wakes.count(), 1);
    assert!(poll_once(first.as_mut()).is_ready());
    assert!(poll_once(second.as_mut()).is_ready());
    assert!(poll_once(pin!(late.cancelled())).is_ready());
    assert!(poll_once(pin!(token.cancelled())).is_ready());
    assert!(late.is_cancelled());
}

#[test]
fn dropping_source_neither_cancels_nor_wakes() {
    let source = CancellationSource::new();
    let token = source.token();
    let (waker, wakes) = WakeCounter::new();
    let baseline = Arc::strong_count(&wakes);
    let mut wait = Box::pin(token.cancelled_owned());
    assert!(poll_with(wait.as_mut(), &waker).is_pending());

    drop(source);

    assert!(!token.is_cancelled());
    assert_eq!(wakes.count(), 0);
    assert!(poll_with(wait.as_mut(), &waker).is_pending());
    assert!(poll_once(pin!(token.clone().cancelled())).is_pending());
    assert_eq!(Arc::strong_count(&wakes), baseline + 1);
    drop(wait);
    assert_eq!(Arc::strong_count(&wakes), baseline);
}

#[test]
fn source_can_issue_tokens_after_all_observers_are_dropped() {
    let source = CancellationSource::new();
    drop(source.token());
    assert!(!source.token().is_cancelled());

    source.cancel();

    let token = source.token();
    assert!(token.is_cancelled());
    assert!(poll_once(pin!(token.cancelled())).is_ready());
}

#[test]
fn dropping_wait_only_removes_its_registration() {
    let source = CancellationSource::new();
    let token = source.token();
    let (retired_waker, retired_wakes) = WakeCounter::new();
    let (remaining_waker, remaining_wakes) = WakeCounter::new();
    let (retry_waker, retry_wakes) = WakeCounter::new();
    let baseline = Arc::strong_count(&retired_wakes);
    let mut retired = Box::pin(token.cancelled());
    let mut remaining = pin!(token.cancelled());
    assert!(poll_with(retired.as_mut(), &retired_waker).is_pending());
    assert!(poll_with(remaining.as_mut(), &remaining_waker).is_pending());

    drop(retired);
    assert_eq!(Arc::strong_count(&retired_wakes), baseline);
    assert!(!token.is_cancelled());
    let mut retry = pin!(token.cancelled());
    assert!(poll_with(retry.as_mut(), &retry_waker).is_pending());
    source.cancel();

    assert_eq!(retired_wakes.count(), 0);
    assert_eq!(remaining_wakes.count(), 1);
    assert_eq!(retry_wakes.count(), 1);
    assert!(poll_once(remaining.as_mut()).is_ready());
    assert!(poll_once(retry.as_mut()).is_ready());
}

#[test]
fn owned_wait_outlives_its_token() {
    let source = CancellationSource::new();
    let token = source.token();
    let mut wait = pin!(token.cancelled_owned());
    drop(token);
    assert!(poll_once(wait.as_mut()).is_pending());

    source.cancel();

    assert!(poll_once(wait.as_mut()).is_ready());
}

#[test]
fn registration_racing_with_concurrent_requests_does_not_lose_wake() {
    for _ in 0..64 {
        let source = CancellationSource::new();
        let token = source.token();
        let start = Barrier::new(3);
        let (waker, wakes) = WakeCounter::new();
        let mut wait = pin!(token.cancelled());

        let first_poll = std::thread::scope(|scope| {
            for _ in 0..2 {
                scope.spawn(|| {
                    start.wait();
                    source.cancel();
                });
            }
            start.wait();
            poll_with(wait.as_mut(), &waker)
        });

        if first_poll.is_pending() {
            // A subsequent ready poll alone would not detect a missed executor notification.
            assert!(wakes.count() > 0);
            assert!(poll_once(wait.as_mut()).is_ready());
        }
        assert!(token.is_cancelled());
    }
}

#[tokio::test]
async fn cancellation_does_not_finish_a_task() {
    let source = CancellationSource::new();
    let wait = source.token().cancelled_owned();
    let (observed_tx, observed_rx) = tokio::sync::oneshot::channel();
    let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
    let worker = tokio::spawn(async move {
        wait.await;
        observed_tx.send(()).unwrap();
        cleanup_rx.await.unwrap();
    });

    source.cancel();
    observed_rx.await.unwrap();
    assert!(!worker.is_finished());
    cleanup_tx.send(()).unwrap();
    worker.await.unwrap();
}
