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

use super::Group;
use crate::test_support::poll_once;

#[tokio::test]
async fn panicked_work_removes_empty_entry() {
    let group = Arc::new(Group::<&str, String>::new());

    let group_clone = group.clone();
    let task = tokio::spawn(async move {
        group_clone
            .work("key", || async {
                panic!("oops");
            })
            .await
    });

    assert!(task.await.unwrap_err().is_panic());
    assert!(group.map.lock().is_empty());

    let result = group.work("key", || async { "success".to_owned() }).await;
    assert_eq!(result, "success");
}

#[tokio::test]
async fn cancelled_work_removes_empty_entry() {
    let group = Arc::new(Group::<&str, &str>::new());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();

    let group_clone = group.clone();
    let task = tokio::spawn(async move {
        group_clone
            .work("key", || async move {
                started_tx.send(()).unwrap();
                std::future::pending().await
            })
            .await
    });

    started_rx.await.unwrap();
    assert_eq!(group.map.lock().len(), 1);

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(group.map.lock().is_empty());
}

#[tokio::test]
async fn failed_try_work_removes_empty_entry() {
    let group = Group::new();

    let result = group
        .try_work("key", || async { Err::<&str, &str>("error") })
        .await;
    assert_eq!(result, Err("error"));
    assert!(group.map.lock().is_empty());

    let retry = group
        .try_work("key", || async { Ok::<&str, ()>("success") })
        .await;
    assert_eq!(retry, Ok("success"));
}

#[tokio::test]
async fn failed_try_work_preserves_entry_for_waiter_retry() {
    let group = Group::new();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let first = group.try_work("key", || async move {
        release_rx.await.unwrap();
        Err::<&str, &str>("fail")
    });
    tokio::pin!(first);
    assert!(poll_once(first.as_mut()).is_pending());

    let retry = group.try_work("key", || async { Ok::<&str, &str>("success") });
    tokio::pin!(retry);
    assert!(poll_once(retry.as_mut()).is_pending());

    release_tx.send(()).unwrap();
    assert_eq!(first.await, Err("fail"));

    assert_eq!(group.map.lock().len(), 1);
    assert_eq!(retry.await, Ok("success"));
    assert!(group.map.lock().is_empty());
}
