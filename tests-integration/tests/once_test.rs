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

use asyncband::once::Once;
use tests_integration::poll_once;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_callers_run_one_initializer() {
    const TASKS: usize = 100;

    let once = Arc::new(Once::new());
    let counter = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(tokio::sync::Barrier::new(TASKS + 1));
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
    let mut first = Box::pin(once.call_once(async || std::future::pending::<()>().await));

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
    let mut waiting = Box::pin(once.wait());

    assert!(poll_once(waiting.as_mut()).is_pending());
    once.call_once(async || {}).await;
    assert!(poll_once(waiting.as_mut()).is_ready());

    let mut completed = Box::pin(once.wait());
    assert!(poll_once(completed.as_mut()).is_ready());
}
