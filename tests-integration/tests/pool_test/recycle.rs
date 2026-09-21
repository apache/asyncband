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

use asyncband::pool::RecycleCancelledStrategy;
use tests_integration::WakeCounter;
use tests_integration::expect_ready;
use tests_integration::poll_once;
use tests_integration::poll_with;

use super::support::Manager;
use super::support::ManagerError;
use super::support::ready;

// Both pool variants promise the same object lifecycle during recycle cancellation.
macro_rules! recycle_contract {
    () => {
        #[test]
        fn cancelled_validation_applies_the_configured_ownership_policy() {
            for strategy in [None, Some(RecycleCancelledStrategy::ReturnToPool)] {
                let manager = Manager::default();
                let pool = pool(1, manager.clone(), strategy);
                let original = ready(pool.get()).unwrap();
                let id = *original;
                drop(original);

                let mut returned_at = None;
                pool.retain(|_, status| {
                    returned_at = Some(status.last_used());
                    true
                });

                // Repeating the cancellation checks that restored objects stay reusable.
                let attempts = if strategy.is_some() { 4 } else { 1 };
                for _ in 0..attempts {
                    let validation = manager.pause_recycle();
                    let mut checkout = Box::pin(pool.get());
                    assert!(poll_once(checkout.as_mut()).is_pending());
                    assert_eq!(pool.status().idle_count, 0);
                    drop(checkout);
                    assert!(validation.is_closed());

                    let retained = usize::from(strategy.is_some());
                    assert_eq!(pool.status().current_size, retained);
                    assert_eq!(pool.status().idle_count, retained);
                    assert_eq!(manager.created(), 1);
                }

                if strategy.is_some() {
                    assert!(manager.detached().is_empty());
                    pool.retain(|object, status| {
                        assert_eq!(*object, id);
                        assert_eq!(Some(status.last_used()), returned_at);
                        assert_eq!(status.recycle_count(), 0);
                        true
                    });
                } else {
                    assert_eq!(manager.detached(), [id]);
                }
                let replacement = ready(pool.get()).unwrap();
                if strategy.is_some() {
                    assert_eq!(*replacement, id);
                    assert_eq!(manager.created(), 1);
                    assert_eq!(replacement.status().recycle_count(), 1);
                } else {
                    assert_ne!(*replacement, id);
                    assert_eq!(manager.created(), 2);
                }
                assert_eq!(pool.status().current_size, 1);
                assert_eq!(pool.status().idle_count, 0);
            }
        }

        #[test]
        fn recycle_completion_wakes_checkout_and_rejection_replaces_the_object() {
            for reject in [false, true] {
                let manager = Manager::default();
                let pool = pool(
                    1,
                    manager.clone(),
                    Some(RecycleCancelledStrategy::ReturnToPool),
                );
                drop(ready(pool.get()).unwrap());
                let validation = manager.pause_recycle();
                let (waker, wakes) = WakeCounter::new();
                let mut checkout = Box::pin(pool.get());
                assert!(poll_with(checkout.as_mut(), &waker).is_pending());
                validation
                    .send(if reject { Err(ManagerError) } else { Ok(()) })
                    .unwrap();
                assert!(wakes.count() > 0);

                let object = expect_ready(poll_with(checkout.as_mut(), &waker)).unwrap();
                assert_eq!(*object, usize::from(reject));
                assert_eq!(manager.created(), 1 + usize::from(reject));
                assert_eq!(manager.detached(), if reject { vec![0] } else { vec![] });
                drop(object);
                assert_eq!(pool.status().current_size, 1);
                assert_eq!(pool.status().idle_count, 1);
            }
        }

        #[test]
        fn cancelling_one_validation_leaves_other_idle_objects_available() {
            let manager = Manager::default();
            let pool = pool(
                3,
                manager.clone(),
                Some(RecycleCancelledStrategy::ReturnToPool),
            );
            let objects: Vec<_> = (0..3).map(|_| ready(pool.get()).unwrap()).collect();
            let mut ids: Vec<_> = objects.iter().map(|object| **object).collect();
            drop(objects);

            for _ in 0..6 {
                let validation = manager.pause_recycle();
                let mut checkout = Box::pin(pool.get());
                assert!(poll_once(checkout.as_mut()).is_pending());
                assert_eq!(pool.status().idle_count, 2);
                drop(checkout);
                assert!(validation.is_closed());
                assert_eq!(pool.status().idle_count, 3);
            }

            let objects: Vec<_> = (0..3).map(|_| ready(pool.get()).unwrap()).collect();
            let mut recycled: Vec<_> = objects.iter().map(|object| **object).collect();
            ids.sort_unstable();
            recycled.sort_unstable();
            assert_eq!(recycled, ids);
            assert_eq!(manager.created(), 3);
            assert!(manager.detached().is_empty());
        }
    };
}

mod bounded {
    use std::sync::Arc;

    use asyncband::pool::bounded::Pool;
    use asyncband::pool::bounded::PoolConfig;

    use super::*;

    fn pool(
        capacity: usize,
        manager: Manager,
        strategy: Option<RecycleCancelledStrategy>,
    ) -> Arc<Pool<Manager>> {
        let mut config = PoolConfig::new(capacity);
        if let Some(strategy) = strategy {
            config = config.with_recycle_cancelled_strategy(strategy);
        }
        Pool::new(config, manager)
    }

    recycle_contract!();
}

mod unbounded {
    use std::sync::Arc;

    use asyncband::pool::unbounded::Pool;
    use asyncband::pool::unbounded::PoolConfig;

    use super::*;

    fn pool(
        _capacity: usize,
        manager: Manager,
        strategy: Option<RecycleCancelledStrategy>,
    ) -> Arc<Pool<usize, Manager>> {
        let mut config = PoolConfig::new();
        if let Some(strategy) = strategy {
            config = config.with_recycle_cancelled_strategy(strategy);
        }
        Pool::new(config, manager)
    }

    recycle_contract!();
}
