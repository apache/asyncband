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
use std::sync::atomic::Ordering;
use std::task::Poll;

use super::Lookup;
use super::OnceMap;
use super::ReadBarrier;
use crate::test_support::poll_once;

// These tests stay next to the implementation because they inspect private state.

fn entry_count<K, V, S>(map: &OnceMap<K, V, S>) -> usize {
    let ready = map.readers.read(|| {
        let mut len = 0;
        // SAFETY: The read barrier protects the current table and every referenced entry.
        unsafe { map.ready.for_each(|_| len += 1) };
        len
    });
    let pending = map.write.lock().pending.len();
    ready + pending
}

#[derive(Default)]
struct ConstantHasher;

impl Hasher for ConstantHasher {
    fn finish(&self) -> u64 {
        0
    }

    fn write(&mut self, _bytes: &[u8]) {}
}

#[tokio::test]
async fn failed_compute_removes_empty_entry() {
    let map = OnceMap::new();

    let result: Result<i32, &str> = map.try_compute("key", async || Err("fail")).await;

    assert_eq!(result, Err("fail"));
    assert_eq!(entry_count(&map), 0);
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
    assert_eq!(entry_count(&map), 0);
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
    assert_eq!(entry_count(&map), 1);

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(entry_count(&map), 0);
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

    assert_eq!(entry_count(&map), 1);
    assert_eq!(retry.await, Ok(1));
    assert_eq!(map.get("key"), Some(1));
}

#[test]
fn abandoned_pending_entry_is_removed_when_last_caller_leaves() {
    let map = OnceMap::<&str, i32>::new();
    let Lookup::Pending(entry) = map.get_or_insert("key") else {
        unreachable!()
    };

    map.cleanup_abandoned_entry(entry);

    assert_eq!(entry_count(&map), 0);
}

#[test]
fn colliding_ready_entries_can_be_unlinked_independently() {
    let map: OnceMap<usize, usize, BuildHasherDefault<ConstantHasher>> =
        (0..4).map(|key| (key, key * 2)).collect();

    map.discard(&1);
    map.discard(&3);

    assert_eq!(map.get(&0), Some(0));
    assert_eq!(map.get(&1), None);
    assert_eq!(map.get(&2), Some(4));
    assert_eq!(map.get(&3), None);
}

#[test]
fn colliding_pending_entries_are_tracked_independently() {
    let map: OnceMap<usize, usize, BuildHasherDefault<ConstantHasher>> = OnceMap::default();
    let Lookup::Pending(first) = map.get_or_insert(1) else {
        unreachable!()
    };
    let Lookup::Pending(first_waiter) = map.get_or_insert(1) else {
        unreachable!()
    };
    let Lookup::Pending(second) = map.get_or_insert(2) else {
        unreachable!()
    };

    assert!(Arc::ptr_eq(&first, &first_waiter));
    assert!(!Arc::ptr_eq(&first, &second));

    drop(first_waiter);
    map.cleanup_abandoned_entry(first);
    map.cleanup_abandoned_entry(second);
    assert_eq!(entry_count(&map), 0);
}

#[test]
fn writer_waits_for_an_active_reader() {
    let barrier = Arc::new(ReadBarrier::new());
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);

    let reader = {
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.read(|| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
        })
    };
    entered_rx.recv().unwrap();

    let (blocked_tx, blocked_rx) = std::sync::mpsc::sync_channel(0);
    let writer = {
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            let blocked = barrier.block();
            blocked_tx.send(()).unwrap();
            drop(blocked);
        })
    };
    while !barrier.blocked.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }
    assert!(blocked_rx.try_recv().is_err());

    release_tx.send(()).unwrap();
    blocked_rx.recv().unwrap();
    reader.join().unwrap();
    writer.join().unwrap();
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
            let mut compute = std::pin::pin!(map.compute(key, async move || key));
            assert_eq!(poll_once(compute.as_mut()), Poll::Ready(key));
            std::thread::yield_now();
        }
    });

    for key in (0..KEYS).map(|key| key << 32) {
        assert_eq!(map.get(&key), Some(key));
    }
}
