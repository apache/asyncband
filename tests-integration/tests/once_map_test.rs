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

use std::collections::hash_map::RandomState;
use std::hash::BuildHasherDefault;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use asyncband::once::OnceMap;

#[test]
fn constructors_and_default() {
    let _: OnceMap<String, i32> = OnceMap::default();
    let _: OnceMap<String, i32> = OnceMap::new();
    let _: OnceMap<String, i32> = OnceMap::with_capacity(10);
    let _: OnceMap<String, i32> = OnceMap::with_hasher(RandomState::new());
    let _: OnceMap<String, i32> = OnceMap::with_capacity_and_hasher(10, RandomState::new());

    let map: OnceMap<String, i32> = OnceMap::with_capacity(100);
    assert!(format!("{map:?}").contains("OnceMap"));
}

#[tokio::test]
async fn compute_caches_value() {
    let map = OnceMap::new();

    assert_eq!(map.compute("key", async || 1).await, 1);
    assert_eq!(map.compute("key", async || 2).await, 1);
}

#[tokio::test]
async fn concurrent_compute_runs_once() {
    let map = Arc::new(OnceMap::new());
    let count = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let map_clone = map.clone();
    let count_clone = count.clone();
    let first = tokio::spawn(async move {
        map_clone
            .compute("key", async move || {
                count_clone.fetch_add(1, Ordering::SeqCst);
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
                42
            })
            .await
    });

    started_rx.await.unwrap();
    let mut waiters = Vec::new();
    for _ in 0..9 {
        let map = map.clone();
        let count = count.clone();
        waiters.push(tokio::spawn(async move {
            map.compute("key", async move || {
                count.fetch_add(1, Ordering::SeqCst);
                42
            })
            .await
        }));
    }

    release_tx.send(()).unwrap();
    assert_eq!(first.await.unwrap(), 42);
    for waiter in waiters {
        assert_eq!(waiter.await.unwrap(), 42);
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_try_compute_can_be_retried_and_then_cached() {
    let map = OnceMap::new();

    let failed: Result<i32, &str> = map.try_compute("key", async || Err("fail")).await;
    assert_eq!(failed, Err("fail"));

    let success = map.try_compute("key", async || Ok::<i32, &str>(1)).await;
    assert_eq!(success, Ok(1));

    let cached = map.try_compute("key", async || Ok::<i32, &str>(2)).await;
    assert_eq!(cached, Ok(1));
}

#[tokio::test]
async fn get_remove_and_discard() {
    let map = OnceMap::<String, i32>::new();
    assert_eq!(map.get("key"), None);
    assert_eq!(map.remove("key"), None);

    map.compute("key".to_owned(), async || 1).await;
    assert_eq!(map.get("key"), Some(1));
    assert_eq!(map.remove("key"), Some(1));
    assert_eq!(map.get("key"), None);

    map.compute("key".to_owned(), async || 2).await;
    map.discard("key");
    assert_eq!(map.get("key"), None);
}

#[test]
fn discard_releases_the_removed_value() {
    #[derive(Clone)]
    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let map: OnceMap<_, _> = [(0, DropCounter(Arc::clone(&drops)))].into_iter().collect();

    map.discard(&0);

    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn remove_while_computing_detaches_entry() {
    let map = Arc::new(OnceMap::new());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let map_clone = map.clone();
    let task = tokio::spawn(async move {
        map_clone
            .compute("key", async move || {
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
                1
            })
            .await
    });

    started_rx.await.unwrap();
    assert_eq!(map.remove("key"), None);
    release_tx.send(()).unwrap();

    assert_eq!(task.await.unwrap(), 1);
    assert_eq!(map.get("key"), None);
}

#[tokio::test]
async fn get_returns_none_while_computing() {
    let map = Arc::new(OnceMap::new());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let map_clone = map.clone();
    let task = tokio::spawn(async move {
        map_clone
            .compute("key", async move || {
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
                1
            })
            .await
    });

    started_rx.await.unwrap();
    assert_eq!(map.get("key"), None);
    release_tx.send(()).unwrap();

    assert_eq!(task.await.unwrap(), 1);
    assert_eq!(map.get("key"), Some(1));
}

#[test]
fn from_iter_keeps_last_value_for_duplicate_key() {
    #[derive(Hash, PartialEq, Eq)]
    struct Key(&'static str);

    let map: OnceMap<_, _> = vec![(Key("a"), 1), (Key("b"), 2), (Key("a"), 3)]
        .into_iter()
        .collect();

    assert_eq!(map.get(&Key("a")), Some(3));
    assert_eq!(map.get(&Key("b")), Some(2));
    assert_eq!(map.get(&Key("c")), None);
}

#[tokio::test]
async fn supports_non_clone_keys_and_owned_values() {
    #[derive(Hash, PartialEq, Eq, Debug)]
    struct Key(i32);

    let map = OnceMap::new();
    let value = map.compute(Key(1), async || "value".to_owned()).await;

    assert_eq!(value, "value");
    assert_eq!(map.get(&Key(1)), Some("value".to_owned()));
}

#[tokio::test]
async fn ready_entries_with_colliding_hashes_remain_independent() {
    #[derive(Default)]
    struct ConstantHasher;

    impl Hasher for ConstantHasher {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, _bytes: &[u8]) {}
    }

    let map = OnceMap::with_hasher(BuildHasherDefault::<ConstantHasher>::default());
    assert_eq!(map.compute("first", async || 1).await, 1);
    assert_eq!(map.compute("second", async || 2).await, 2);
    assert_eq!(map.get("first"), Some(1));
    assert_eq!(map.get("second"), Some(2));

    map.discard("first");
    assert_eq!(map.get("first"), None);
    assert_eq!(map.get("second"), Some(2));
}
