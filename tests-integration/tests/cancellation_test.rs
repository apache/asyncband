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
use std::sync::Barrier;
use std::thread;

use asyncband::cancellation::CancellationSource;
use tests_integration::TestTask;

#[test]
fn test_cancellation_is_sticky_and_idempotent() {
    let source = CancellationSource::new();
    let token = source.token();

    assert!(!source.is_cancelled());
    assert!(!token.is_cancelled());

    source.cancel();
    source.cancel();

    assert!(source.is_cancelled());
    assert!(token.is_cancelled());
    assert!(TestTask::new(token.cancelled()).poll().is_ready());
}

#[test]
fn test_cancellation_wakes_all_observers() {
    let source = CancellationSource::new();
    let token = source.token();
    let mut first = TestTask::new(token.cancelled());
    let mut second = TestTask::new(token.clone().cancelled_owned());

    assert!(first.poll().is_pending());
    assert!(second.poll().is_pending());

    source.cancel();

    assert!(first.is_woken());
    assert!(second.is_woken());
    assert!(first.poll().is_ready());
    assert!(second.poll().is_ready());
}

#[test]
fn test_dropping_one_wait_does_not_affect_other_observers() {
    let source = CancellationSource::new();
    let token = source.token();
    let mut cancelled_wait = TestTask::new(token.cancelled());
    let mut remaining_wait = TestTask::new(token.clone().cancelled_owned());

    assert!(cancelled_wait.poll().is_pending());
    assert!(remaining_wait.poll().is_pending());
    drop(cancelled_wait);

    source.cancel();

    assert!(remaining_wait.is_woken());
    assert!(remaining_wait.poll().is_ready());
    assert!(token.is_cancelled());
}

#[test]
fn test_owned_wait_outlives_token() {
    let source = CancellationSource::new();
    let token = source.token();
    let mut wait = TestTask::new(token.cancelled_owned());
    drop(token);

    assert!(wait.poll().is_pending());
    source.cancel();
    assert!(wait.is_woken());
    assert!(wait.poll().is_ready());
}

#[test]
fn test_dropping_uncancelled_source_does_not_cancel_or_wake_tokens() {
    let source = CancellationSource::new();
    let token = source.token();
    drop(source);

    assert!(!token.is_cancelled());
    let mut wait = TestTask::new(token.cancelled());
    assert!(wait.poll().is_pending());
    assert!(!wait.is_woken());
}

#[test]
fn test_racing_wait_registration_with_cancellation_does_not_lose_request() {
    let source = CancellationSource::new();
    let token = source.token();
    let barrier = Arc::new(Barrier::new(2));
    let waiter_barrier = barrier.clone();
    let mut wait = TestTask::new(token.cancelled_owned());

    let waiter = thread::spawn(move || {
        waiter_barrier.wait();
        let poll = wait.poll();
        (wait, poll)
    });

    barrier.wait();
    source.cancel();

    let (mut wait, poll) = waiter.join().unwrap();
    if poll.is_pending() {
        assert!(wait.is_woken());
    }
    assert!(wait.poll().is_ready());
}
