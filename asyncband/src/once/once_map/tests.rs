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
use std::cell::RefCell;
use std::future::pending;
use std::future::poll_fn;
use std::hash::BuildHasherDefault;
use std::hash::Hasher;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::pin;
use std::rc::Rc;
use std::task::Poll;

use super::*;
use crate::test_support::KeyWithDrop;
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
        let mut compute = pin!(map.compute("key", async || pending::<i32>().await));
        assert!(poll_once(compute.as_mut()).is_pending());
        assert_eq!(map.len(), 1);
    }

    assert_eq!(map.len(), 0);
}

#[test]
fn cancelled_compute_removes_empty_entry_during_get() {
    let map = OnceMap::<&str, i32>::new();
    let mut compute = Box::pin(map.compute("key", async || pending::<i32>().await));
    assert!(poll_once(compute.as_mut()).is_pending());

    // A concurrent `get` holds its lookup result after releasing the table lock. Cancel the
    // computation inside that window.
    let held_by_get = map.find_entry(map.hasher.hash_one("key"), |entry| entry.key == "key");
    drop(compute);
    drop(held_by_get);

    assert_eq!(map.len(), 0);
}

#[test]
fn pending_computation_does_not_block_another_key() {
    let map = OnceMap::new();
    {
        let mut pending = pin!(map.compute("pending", async || { pending::<i32>().await }));
        assert!(poll_once(pending.as_mut()).is_pending());

        let mut ready = pin!(map.compute("ready", async || 1));
        assert_eq!(poll_once(ready.as_mut()), Poll::Ready(1));
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

    assert!(Arc::ptr_eq(first.entry(), first_waiter.entry()));
    assert!(!Arc::ptr_eq(first.entry(), second.entry()));

    drop(first_waiter);
    drop(first);
    drop(second);
    assert_eq!(map.len(), 0);
}

#[test]
fn duplicate_key_panic_cleans_up_pending_compute() {
    for fallible in [false, true] {
        for cancel_during_drop in [false, true] {
            let map = Rc::new(OnceMap::<KeyWithDrop, i32>::new());
            let first_map = Rc::clone(&map);
            let mut first = Box::pin(async move {
                first_map
                    .compute(
                        KeyWithDrop {
                            key: "key",
                            on_drop: None,
                        },
                        async || pending().await,
                    )
                    .await
            });
            assert!(poll_once(first.as_mut()).is_pending());
            let first = Rc::new(RefCell::new(Some(first)));
            let dropping_first = Rc::clone(&first);
            let dropping_map = Rc::clone(&map);
            let key = KeyWithDrop {
                key: "key",
                on_drop: Some(Box::new(move || {
                    // Reenter the map and optionally cancel its last other caller before unwinding.
                    assert_eq!(dropping_map.len(), 1);
                    if cancel_during_drop {
                        drop(dropping_first.borrow_mut().take());
                    }
                    panic!("duplicate key destructor");
                })),
            };
            let result = catch_unwind(AssertUnwindSafe(|| {
                if fallible {
                    let _ = poll_once(pin!(map.try_compute(key, async || Ok::<_, ()>(1))));
                } else {
                    let _ = poll_once(pin!(map.compute(key, async || 1)));
                }
            }));
            assert!(result.is_err());
            drop(first.borrow_mut().take());
            assert_eq!(map.len(), 0);
        }
    }
}

#[test]
fn duplicate_key_panic_drops_cached_value_clone() {
    for fallible in [false, true] {
        let map = Rc::new(OnceMap::new());
        let value = Arc::new(());
        assert!(
            poll_once(pin!(map.compute(
                KeyWithDrop {
                    key: "key",
                    on_drop: None
                },
                async || Arc::clone(&value),
            )))
            .is_ready()
        );
        let dropping_map = Rc::clone(&map);
        let key = KeyWithDrop {
            key: "key",
            on_drop: Some(Box::new(move || {
                dropping_map.discard("key");
                panic!("duplicate key destructor");
            })),
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            if fallible {
                let _ = poll_once(pin!(
                    map.try_compute(key, async || Ok::<_, ()>(Arc::clone(&value)))
                ));
            } else {
                let _ = poll_once(pin!(map.compute(key, async || Arc::clone(&value))));
            }
        }));
        assert!(result.is_err());
        assert_eq!(map.len(), 0);
        assert_eq!(Arc::strong_count(&value), 1);
    }
}
