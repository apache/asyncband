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

use std::task::Poll;

use asyncband::pool::bounded::Pool;
use asyncband::pool::bounded::PoolConfig;
use tests_integration::WakeCounter;
use tests_integration::expect_ready;
use tests_integration::poll_once;
use tests_integration::poll_with;

use super::support::Manager;
use super::support::ManagerError;
use super::support::ready;

#[test]
fn targets_account_for_idle_objects_and_checked_out_capacity() {
    for capacity in [1, 4] {
        for checked_out in 0..=capacity {
            for idle in 0..=capacity - checked_out {
                for target in [0, 1, capacity, usize::MAX] {
                    let manager = Manager::default();
                    let pool = Pool::new(PoolConfig::new(capacity), manager.clone());
                    let active: Vec<_> = (0..checked_out)
                        .map(|_| ready(pool.get()).unwrap())
                        .collect();
                    assert_eq!(ready(pool.replenish_to(idle)), Ok(idle));
                    let expected_idle = target.min(capacity - checked_out).max(idle);
                    assert_eq!(ready(pool.replenish_to(target)), Ok(expected_idle - idle));
                    assert_eq!(pool.status().idle_count, expected_idle);
                    assert_eq!(pool.status().current_size, checked_out + expected_idle);
                    assert_eq!(manager.created(), checked_out + expected_idle);
                    drop(active);
                    assert_eq!(pool.status().idle_count, manager.created());
                }
            }
        }
    }
}

#[test]
fn creation_failure_keeps_completed_work_and_allows_a_retry() {
    let manager = Manager::default();
    manager.pause_create().send(Ok(())).unwrap();
    manager.pause_create().send(Err(ManagerError)).unwrap();
    let pool = Pool::new(PoolConfig::new(3), manager.clone());

    assert_eq!(ready(pool.replenish_to(3)), Err(ManagerError));
    assert_eq!(pool.status().current_size, 1);
    assert_eq!(pool.status().idle_count, 1);
    assert_eq!(manager.created(), 2);

    assert_eq!(ready(pool.replenish_to(3)), Ok(2));
    let mut ids: Vec<_> = (0..3)
        .map(|_| ready(pool.get()).unwrap().detach())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, [1000, 1002, 1003]);
}

#[test]
fn in_flight_creation_reserves_capacity_against_other_replenishers() {
    let manager = Manager::default();
    let pool = Pool::new(PoolConfig::new(3), manager.clone());
    assert_eq!(ready(pool.replenish_to(1)), Ok(1));

    let creation = manager.pause_create();
    let mut first = Box::pin(pool.replenish_to(3));
    assert!(poll_once(first.as_mut()).is_pending());
    assert_eq!(manager.created(), 2);
    assert_eq!(ready(pool.replenish_to(3)), Ok(0));
    assert_eq!(manager.created(), 2);

    creation.send(Ok(())).unwrap();
    assert_eq!(poll_once(first.as_mut()), Poll::Ready(Ok(2)));
    assert_eq!(pool.status().current_size, 3);
    assert_eq!(pool.status().idle_count, 3);
}

#[test]
fn replenished_object_wakes_a_checkout_waiting_for_capacity() {
    let manager = Manager::default();
    let pool = Pool::new(PoolConfig::new(2), manager.clone());
    let held = ready(pool.get()).unwrap();
    let creation = manager.pause_create();
    let mut replenish = Box::pin(pool.replenish_to(2));
    assert!(poll_once(replenish.as_mut()).is_pending());

    let (waker, wakes) = WakeCounter::new();
    let mut checkout = Box::pin(pool.get());
    assert!(poll_with(checkout.as_mut(), &waker).is_pending());
    assert_eq!(manager.created(), 2);
    creation.send(Ok(())).unwrap();
    assert_eq!(poll_once(replenish.as_mut()), Poll::Ready(Ok(1)));
    assert!(wakes.count() > 0);
    let acquired = expect_ready(poll_with(checkout.as_mut(), &waker)).unwrap();
    assert_ne!(*held, *acquired);
    assert_eq!(manager.created(), 2);
    assert_eq!(pool.status().idle_count, 0);
    drop((held, acquired));
    assert_eq!(pool.status().idle_count, 2);
}

#[test]
fn cancellation_keeps_finished_objects_and_releases_unfilled_reservations() {
    for completed in [0, 1] {
        let manager = Manager::default();
        for _ in 0..completed {
            manager.pause_create().send(Ok(())).unwrap();
        }
        let creation = manager.pause_create();
        let pool = Pool::new(PoolConfig::new(3), manager.clone());
        let mut replenish = Box::pin(pool.replenish_to(3));
        assert!(poll_once(replenish.as_mut()).is_pending());
        assert_eq!(pool.status().current_size, completed);
        assert_eq!(pool.status().idle_count, completed);

        let mut active: Vec<_> = (0..completed).map(|_| ready(pool.get()).unwrap()).collect();
        let (waker, wakes) = WakeCounter::new();
        let mut checkout = Box::pin(pool.get());
        assert!(poll_with(checkout.as_mut(), &waker).is_pending());
        drop(replenish);
        assert!(creation.is_closed());
        assert!(wakes.count() > 0);
        let acquired = expect_ready(poll_with(checkout.as_mut(), &waker)).unwrap();
        active.push(acquired);
        for _ in active.len()..3 {
            active.push(ready(pool.get()).unwrap());
        }
        assert_eq!(pool.status().current_size, 3);
        assert_eq!(pool.status().idle_count, 0);
        assert!(manager.detached().is_empty());
        assert!(poll_once(Box::pin(pool.get()).as_mut()).is_pending());
        drop(active);
        assert_eq!(pool.status().idle_count, 3);
    }
}
