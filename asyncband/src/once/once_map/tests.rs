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

use std::cell::Cell;
use std::future::poll_fn;
use std::hash::BuildHasherDefault;
use std::hash::Hasher;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use super::Lookup;
use super::OnceMap;
use crate::test_support::poll_once;

// These tests stay next to the implementation because they inspect private state.

impl<K, V, S> OnceMap<K, V, S> {
    fn len(&self) -> usize {
        self.entries.lock().len()
    }
}

#[derive(Default)]
struct ConstantHasher;

impl Hasher for ConstantHasher {
    fn finish(&self) -> u64 {
        0
    }

    fn write(&mut self, _bytes: &[u8]) {}
}

#[test]
fn failed_compute_removes_empty_entry() {
    let map = OnceMap::new();

    let mut compute = pin!(map.try_compute("key", async || Err::<i32, &str>("fail")));

    assert_eq!(poll_once(compute.as_mut()), Poll::Ready(Err("fail")));
    assert_eq!(map.len(), 0);
}

#[test]
fn panicked_compute_removes_empty_entry() {
    let map = OnceMap::<&str, i32>::new();

    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut compute = pin!(map.compute("key", async || {
            panic!("oops");
        }));
        let _ = poll_once(compute.as_mut());
    }));

    assert!(result.is_err());
    assert_eq!(map.len(), 0);
}

#[test]
fn cancelled_compute_removes_empty_entry() {
    let map = OnceMap::<&str, i32>::new();

    {
        let mut compute = pin!(map.compute("key", async || std::future::pending::<i32>().await));
        assert!(poll_once(compute.as_mut()).is_pending());
        assert_eq!(map.len(), 1);
    }

    assert_eq!(map.len(), 0);
}

#[test]
fn pending_computation_does_not_block_another_key() {
    let map = OnceMap::new();
    {
        let mut pending = std::pin::pin!(
            map.compute("pending", async || { std::future::pending::<i32>().await })
        );
        assert!(poll_once(pending.as_mut()).is_pending());

        let mut ready = std::pin::pin!(map.compute("ready", async || 1));
        assert_eq!(poll_once(ready.as_mut()), std::task::Poll::Ready(1));
    }

    assert_eq!(map.len(), 1);
}

#[test]
fn failed_compute_preserves_entry_for_waiter_retry() {
    let map = OnceMap::new();
    let released = Cell::new(false);

    let first = map.try_compute("key", async || {
        poll_fn(|_| {
            released
                .get()
                .then_some(())
                .map_or(Poll::Pending, Poll::Ready)
        })
        .await;
        Err::<i32, &str>("fail")
    });
    let mut first = pin!(first);
    assert!(poll_once(first.as_mut()).is_pending());

    let retry = map.try_compute("key", async || Ok::<i32, &str>(1));
    let mut retry = pin!(retry);
    assert!(poll_once(retry.as_mut()).is_pending());

    released.set(true);
    assert_eq!(poll_once(first.as_mut()), Poll::Ready(Err("fail")));

    assert_eq!(map.len(), 1);
    assert_eq!(poll_once(retry.as_mut()), Poll::Ready(Ok(1)));
    assert_eq!(map.get("key"), Some(1));
}

#[test]
fn abandoned_pending_entry_is_removed_when_last_caller_leaves() {
    let map = OnceMap::<&str, i32>::new();
    let Lookup::Pending(entry) = map.get_or_insert("key") else {
        unreachable!()
    };

    map.cleanup_abandoned_entry(entry);

    assert_eq!(map.len(), 0);
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
    assert_eq!(map.len(), 0);
}
