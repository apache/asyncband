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

use std::future::IntoFuture;

use asyncband::waitgroup::WaitGroup;
use tests_integration::poll_once;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waits_for_all_worker_handles() {
    let wg = WaitGroup::new();
    let mut tasks = vec![];
    for _ in 0..100 {
        let worker = wg.clone();
        tasks.push(tokio::spawn(async move {
            drop(worker);
        }));
    }

    wg.await;
    for task in tasks {
        task.await.unwrap();
    }
}

#[test]
fn wait_is_pending_until_the_last_handle_drops() {
    let wg = WaitGroup::new();
    let worker = wg.clone();
    let mut wait = Box::pin(wg.into_future());

    assert!(poll_once(wait.as_mut()).is_pending());
    drop(worker);
    assert!(poll_once(wait.as_mut()).is_ready());
}

#[test]
fn cancelling_one_wait_does_not_cancel_another() {
    let wg = WaitGroup::new();
    let worker = wg.clone();
    let first = wg.into_future();
    let mut second = Box::pin(first.clone());
    let mut first = Box::pin(first);

    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());
    drop(first);

    drop(worker);
    assert!(poll_once(second.as_mut()).is_ready());
}

#[test]
fn multiple_handles_can_wait_for_the_same_completion() {
    let wg = WaitGroup::new();
    let worker = wg.clone();
    let mut first = Box::pin(wg.clone().into_future());
    let mut second = Box::pin(wg.into_future());

    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());

    drop(worker);
    assert!(poll_once(first.as_mut()).is_ready());
    assert!(poll_once(second.as_mut()).is_ready());
}

#[test]
fn handle_clones_register_nested_work() {
    let wg = WaitGroup::new();
    let worker = wg.clone();
    let nested = worker.clone();
    let mut wait = Box::pin(wg.into_future());

    drop(worker);
    assert!(poll_once(wait.as_mut()).is_pending());
    drop(nested);
    assert!(poll_once(wait.as_mut()).is_ready());
}

#[test]
fn a_consumed_initial_handle_is_immediately_ready() {
    let mut wait = Box::pin(WaitGroup::new().into_future());

    assert!(poll_once(wait.as_mut()).is_ready());
}
