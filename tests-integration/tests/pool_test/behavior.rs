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
use std::convert::Infallible;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::time::Instant;

use asyncband::pool::QueueStrategy;
use asyncband::pool::bounded;
use asyncband::pool::unbounded;

use super::support::Manager;

#[test]
#[should_panic(expected = "bounded pool max_size must be greater than zero")]
fn bounded_pool_rejects_zero_capacity() {
    bounded::Pool::new(bounded::PoolConfig::new(0), Manager::default());
}

#[test]
fn bounded_construction_allocates_idle_storage_lazily() {
    let pool = bounded::Pool::new(bounded::PoolConfig::new(usize::MAX), Manager::default());

    assert_eq!(pool.status().max_size, usize::MAX);
    assert_eq!(pool.status().current_size, 0);
    assert_eq!(pool.status().idle_count, 0);
}

#[tokio::test]
async fn bounded_last_used_tracks_the_end_of_a_checkout() {
    let pool = bounded::Pool::new(bounded::PoolConfig::new(1), Manager::default());

    let object = pool.get().await.unwrap();
    let before_return = Instant::now();
    drop(object);

    let object = pool.get().await.unwrap();
    assert!(object.status().last_used() >= before_return);
    assert_eq!(object.status().recycle_count(), 1);
}

#[tokio::test]
async fn retain_invokes_detachment_hook_once_per_removed_object() {
    let manager = Manager::default();
    let pool = bounded::Pool::new(bounded::PoolConfig::new(4), manager.clone());

    let mut objects = vec![];
    for _ in 0..4 {
        objects.push(pool.get().await.unwrap());
    }
    drop(objects);

    let mut result = pool.retain(|object, _status| *object % 2 == 0);
    result.removed.sort_unstable();

    assert_eq!(result.retained, 2);
    assert_eq!(result.removed, [1001, 1003]);
    assert_eq!(manager.detached(), [1, 3]);
    assert_eq!(pool.status().current_size, 2);
    assert_eq!(pool.status().idle_count, 2);
}

#[test]
fn manual_pool_try_get_tracks_return_time() {
    let pool = unbounded::Pool::<usize>::never_manage(unbounded::PoolConfig::default());
    assert!(pool.try_get().is_none());

    pool.extend_one(42);
    let object = pool.try_get().unwrap();
    assert_eq!(object.status().recycle_count(), 1);

    let before_return = Instant::now();
    drop(object);

    let object = pool.try_get().unwrap();
    assert!(object.status().last_used() >= before_return);
    assert_eq!(object.status().recycle_count(), 2);
}

#[tokio::test]
async fn manual_pool_accepts_a_local_factory_and_reuses_its_object() {
    let pool = unbounded::Pool::<Vec<u8>>::never_manage(unbounded::PoolConfig::default());
    let created = Cell::new(0);
    let create = async || {
        created.set(created.get() + 1);
        Ok::<_, Infallible>(vec![1, 2, 3])
    };

    let mut object = pool.get_or_create(&create).await.unwrap();
    object.push(4);
    drop(object);

    let object = pool.get_or_create(&create).await.unwrap();
    assert_eq!(*object, [1, 2, 3, 4]);
    assert_eq!(created.get(), 1);
}

#[test]
fn manual_pool_honors_fifo_and_lifo_order() {
    fn drain(strategy: QueueStrategy) -> Vec<usize> {
        let config = unbounded::PoolConfig::new().with_queue_strategy(strategy);
        let pool = unbounded::Pool::<usize>::never_manage(config);
        pool.extend([1, 2, 3]);

        (0..3).map(|_| pool.try_get().unwrap().detach()).collect()
    }

    assert_eq!(drain(QueueStrategy::Fifo), [1, 2, 3]);
    assert_eq!(drain(QueueStrategy::Lifo), [3, 2, 1]);
}

#[test]
fn retain_predicate_panic_preserves_pool_ownership() {
    let pool = unbounded::Pool::<usize>::never_manage(unbounded::PoolConfig::default());
    pool.extend([1, 2, 3, 4]);

    let result = catch_unwind(AssertUnwindSafe(|| {
        pool.retain(|object, _status| {
            assert_ne!(*object, 3, "predicate panic");
            *object % 2 == 0
        });
    }));
    assert!(result.is_err());
    assert_eq!(pool.status().current_size, 4);
    assert_eq!(pool.status().idle_count, 4);

    let mut objects = (0..4)
        .map(|_| pool.try_get().unwrap().detach())
        .collect::<Vec<_>>();
    objects.sort_unstable();
    assert_eq!(objects, [1, 2, 3, 4]);
}
