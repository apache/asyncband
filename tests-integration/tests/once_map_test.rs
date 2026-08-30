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

use std::borrow::Borrow;
use std::cell::Cell;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasherDefault;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use asyncband::once::OnceMap;
use tests_integration::poll_once;

#[test]
fn constructors_and_default() {
    let _: OnceMap<String, i32> = OnceMap::default();
    let _: OnceMap<String, i32> = OnceMap::new();
    let _: OnceMap<String, i32> = OnceMap::with_hasher(RandomState::new());

    let map: OnceMap<String, i32> = OnceMap::new();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_growth_keeps_every_entry() {
    let map = Arc::new(OnceMap::new());
    let mut workers = Vec::new();
    for worker in 0..8 {
        let map = Arc::clone(&map);
        workers.push(tokio::spawn(async move {
            for offset in 0..128 {
                let key = worker * 128 + offset;
                assert_eq!(map.compute(key, async move || key * 2).await, key * 2);
            }
        }));
    }

    for worker in workers {
        worker.await.unwrap();
    }
    for key in 0..1024 {
        assert_eq!(map.get(&key), Some(key * 2));
    }
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

#[tokio::test]
async fn discard_releases_the_removed_key_and_value_after_growth() {
    struct Key {
        value: usize,
        drops: Arc<AtomicUsize>,
    }

    impl Borrow<usize> for Key {
        fn borrow(&self) -> &usize {
            &self.value
        }
    }

    impl PartialEq for Key {
        fn eq(&self, other: &Self) -> bool {
            self.value == other.value
        }
    }

    impl Eq for Key {}

    impl Hash for Key {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.value.hash(state);
        }
    }

    impl Drop for Key {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let key_drops = Arc::new(AtomicUsize::new(0));
    let value_drops = Arc::new(AtomicUsize::new(0));
    let value = Arc::new(DropCounter(Arc::clone(&value_drops)));
    let map = OnceMap::new();
    map.compute(
        Key {
            value: 1,
            drops: Arc::clone(&key_drops),
        },
        async || Arc::clone(&value),
    )
    .await;
    drop(value);

    let filler_key_drops = Arc::new(AtomicUsize::new(0));
    let filler_value_drops = Arc::new(AtomicUsize::new(0));
    for value in 2..=64 {
        map.compute(
            Key {
                value,
                drops: Arc::clone(&filler_key_drops),
            },
            async || Arc::new(DropCounter(Arc::clone(&filler_value_drops))),
        )
        .await;
    }

    map.discard(&1);

    assert_eq!(key_drops.load(Ordering::SeqCst), 1);
    assert_eq!(value_drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn remove_while_computing_allows_a_new_generation() {
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
    assert_eq!(map.compute("key", async || 2).await, 2);
    release_tx.send(()).unwrap();

    assert_eq!(task.await.unwrap(), 1);
    assert_eq!(map.get("key"), Some(2));
}

#[test]
fn remove_detaches_waiters_from_a_new_generation() {
    let map = OnceMap::new();
    let release = Cell::new(false);

    let mut leader = std::pin::pin!(map.compute("key", async || {
        std::future::poll_fn(|_| {
            if release.get() {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await;
        1
    }));
    assert!(poll_once(leader.as_mut()).is_pending());

    let mut waiter = std::pin::pin!(map.compute("key", async || 2));
    assert!(poll_once(waiter.as_mut()).is_pending());

    assert_eq!(map.remove("key"), None);
    let mut replacement = std::pin::pin!(map.compute("key", async || 3));
    assert_eq!(poll_once(replacement.as_mut()), std::task::Poll::Ready(3));

    release.set(true);
    assert_eq!(poll_once(leader.as_mut()), std::task::Poll::Ready(1));
    assert_eq!(poll_once(waiter.as_mut()), std::task::Poll::Ready(1));
    assert_eq!(map.get("key"), Some(3));
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

#[test]
fn entries_remain_accessible_as_the_map_grows() {
    let map: OnceMap<usize, usize> = (0..1024).map(|key| (key, key * 2)).collect();

    for key in 0..1024 {
        assert_eq!(map.get(&key), Some(key * 2));
    }

    for key in (0..1024).step_by(3) {
        assert_eq!(map.remove(&key), Some(key * 2));
    }
    for key in 0..1024 {
        let expected = (key % 3 != 0).then_some(key * 2);
        assert_eq!(map.get(&key), expected);
    }
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
async fn entries_with_colliding_hashes_remain_independent() {
    #[derive(Default)]
    struct ConstantHasher;

    impl Hasher for ConstantHasher {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, _bytes: &[u8]) {}
    }

    let map = OnceMap::with_hasher(BuildHasherDefault::<ConstantHasher>::default());
    for key in 0..32 {
        assert_eq!(map.compute(key, async move || key * 2).await, key * 2);
    }
    for key in 0..32 {
        assert_eq!(map.get(&key), Some(key * 2));
    }

    for key in (0..32).step_by(2) {
        map.discard(&key);
    }
    for key in 0..32 {
        let expected = (key % 2 != 0).then_some(key * 2);
        assert_eq!(map.get(&key), expected);
    }
}

#[tokio::test]
async fn entries_with_a_long_common_hash_prefix_remain_accessible() {
    #[derive(Default)]
    struct UpperBitsHasher(u64);

    impl Hasher for UpperBitsHasher {
        fn finish(&self) -> u64 {
            self.0 << 32
        }

        fn write(&mut self, bytes: &[u8]) {
            self.0 = bytes
                .iter()
                .fold(0, |hash, byte| hash.rotate_left(8) ^ u64::from(*byte));
        }

        fn write_usize(&mut self, value: usize) {
            self.0 = value as u64;
        }
    }

    let map = OnceMap::with_hasher(BuildHasherDefault::<UpperBitsHasher>::default());
    for key in 0..32 {
        assert_eq!(map.compute(key, async move || key).await, key);
    }
    for key in 0..32 {
        assert_eq!(map.get(&key), Some(key));
    }
}
