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

use std::hash::BuildHasherDefault;
use std::hash::Hasher;
use std::sync::Arc;

use super::Lookup;
use super::OnceMap;
use super::TrieState;
use crate::test_support::poll_once;

// These tests stay next to the implementation because they inspect private state.

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

#[test]
fn reader_snapshot_does_not_keep_an_abandoned_entry_registered() {
    let map = OnceMap::<&str, i32>::new();
    let Lookup::Pending(entry) = map.get_or_insert("key") else {
        unreachable!()
    };
    let root = map.entries.root.get().unwrap();
    let snapshot = root.node.slot(entry.hash, 0).state.load_owned().unwrap();
    assert!(matches!(snapshot.as_ref(), TrieState::Leaf(_)));

    map.cleanup_abandoned_entry(entry);

    assert!(map.is_empty());
    drop(snapshot);
}

#[test]
fn concurrent_deep_reads_survive_replacement_and_removal() {
    const ITERATIONS: usize = if cfg!(miri) { 32 } else { 2_000 };
    const KEYS: u64 = 16;

    #[derive(Default)]
    struct IdentityHasher(u64);

    impl Hasher for IdentityHasher {
        fn finish(&self) -> u64 {
            self.0
        }

        fn write(&mut self, bytes: &[u8]) {
            self.0 = bytes
                .iter()
                .fold(0, |hash, byte| hash.rotate_left(8) ^ u64::from(*byte));
        }

        fn write_u64(&mut self, value: u64) {
            self.0 = value;
        }
    }

    let map: OnceMap<u64, u64, BuildHasherDefault<IdentityHasher>> =
        (0..KEYS).map(|key| (key << 32, key << 32)).collect();

    std::thread::scope(|scope| {
        for _ in 0..2 {
            scope.spawn(|| {
                for iteration in 0..ITERATIONS {
                    let key = (iteration as u64 % KEYS) << 32;
                    if let Some(value) = map.get(&key) {
                        assert_eq!(value, key);
                    }
                    std::thread::yield_now();
                }
            });
        }

        for iteration in 0..ITERATIONS {
            let key = (iteration as u64 % KEYS) << 32;
            map.discard(&key);
            map.insert(key, key);
            std::thread::yield_now();
        }
    });

    for key in (0..KEYS).map(|key| key << 32) {
        assert_eq!(map.get(&key), Some(key));
    }
}
