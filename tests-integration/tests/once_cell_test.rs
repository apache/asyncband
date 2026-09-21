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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use asyncband::once::OnceCell;
use tests_integration::poll_once;

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
    let start = Arc::new(tokio::sync::Barrier::new(TASKS + 1));
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
                    value
                })
                .await
        }));
    }

    start.wait().await;
    let expected = tasks.remove(0).await.unwrap();
    for task in tasks {
        assert_eq!(task.await.unwrap(), expected);
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelled_initialization_can_be_retried() {
    let cell = OnceCell::new();
    let mut first = Box::pin(cell.get_or_init(async || std::future::pending::<u8>().await));

    assert!(poll_once(first.as_mut()).is_pending());
    drop(first);

    assert_eq!(*cell.get_or_init(async || 2).await, 2);
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
