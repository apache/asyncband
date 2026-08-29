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

use super::OnceMap;
use super::ReadyIndex;
use crate::test_support::poll_once;

// These tests stay next to the implementation because they inspect private state.

#[test]
fn ready_index_bucket_count_is_bounded_by_shards() {
    let shard_amount = 8;
    let index = ReadyIndex::<usize, usize>::new(1_000_000, shard_amount);

    assert_eq!(index.buckets.len(), shard_amount * 4);
}

#[tokio::test]
async fn failed_compute_removes_empty_entry() {
    let map = OnceMap::new();

    let result: Result<i32, &str> = map.try_compute("key", async || Err("fail")).await;

    assert_eq!(result, Err("fail"));
    assert!(map.is_empty());
}

#[tokio::test]
async fn panicked_compute_removes_empty_entry() {
    let map = Arc::new(OnceMap::<&str, i32>::new());

    let map_clone = map.clone();
    let task = tokio::spawn(async move {
        map_clone
            .compute("key", async || {
                panic!("oops");
            })
            .await
    });

    assert!(task.await.unwrap_err().is_panic());
    assert!(map.is_empty());
}

#[tokio::test]
async fn cancelled_compute_removes_empty_entry() {
    let map = Arc::new(OnceMap::<&str, i32>::new());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();

    let map_clone = map.clone();
    let task = tokio::spawn(async move {
        map_clone
            .compute("key", async move || {
                started_tx.send(()).unwrap();
                std::future::pending().await
            })
            .await
    });

    started_rx.await.unwrap();
    assert_eq!(map.len(), 1);

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(map.is_empty());
}

#[tokio::test]
async fn failed_compute_preserves_entry_for_waiter_retry() {
    let map = OnceMap::new();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let first = map.try_compute("key", async move || {
        release_rx.await.unwrap();
        Err::<i32, &str>("fail")
    });
    tokio::pin!(first);
    assert!(poll_once(first.as_mut()).is_pending());

    let retry = map.try_compute("key", async || Ok::<i32, &str>(1));
    tokio::pin!(retry);
    assert!(poll_once(retry.as_mut()).is_pending());

    release_tx.send(()).unwrap();
    assert_eq!(first.await, Err("fail"));

    assert_eq!(map.len(), 1);
    assert_eq!(retry.await, Ok(1));
    assert_eq!(map.get("key"), Some(1));
}
