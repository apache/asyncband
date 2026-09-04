// Copyright 2025 FastLabs Developers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file contains code ported from Fastpool 1.1.1.
// The incorporated code has been modified for use in Apache Asyncband.
// See the project LICENSE file for the exact upstream revision and source path.

use std::future::Future;
use std::future::poll_fn;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;

use asyncband::pool::ManageObject;
use asyncband::pool::ObjectStatus;
use asyncband::pool::RecycleCancelledStrategy;

#[derive(Default)]
struct Controls {
    created: AtomicUsize,
    recycle_ready: AtomicBool,
    reject_recycle: AtomicBool,
}

struct ControlledRecycleManager {
    controls: Arc<Controls>,
}

impl ManageObject for ControlledRecycleManager {
    type Object = usize;
    type Error = ();

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        Ok(self.controls.created.fetch_add(1, Ordering::Relaxed))
    }

    async fn is_recyclable(
        &self,
        _object: &mut Self::Object,
        _status: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        poll_fn(|_| {
            if !self.controls.recycle_ready.load(Ordering::Acquire) {
                Poll::Pending
            } else if self.controls.reject_recycle.load(Ordering::Relaxed) {
                Poll::Ready(Err(()))
            } else {
                Poll::Ready(Ok(()))
            }
        })
        .await
    }
}

fn manager() -> (ControlledRecycleManager, Arc<Controls>) {
    let controls = Arc::new(Controls::default());
    (
        ControlledRecycleManager {
            controls: controls.clone(),
        },
        controls,
    )
}

fn poll_and_cancel(future: impl Future) {
    let mut future = pin!(future);
    assert!(tests_integration::poll_once(future.as_mut()).is_pending());
}

mod bounded_tests {
    use asyncband::pool::bounded::Pool;
    use asyncband::pool::bounded::PoolConfig;

    use super::*;

    #[tokio::test]
    async fn cancellation_detaches_by_default() {
        let (manager, controls) = manager();
        let pool = Pool::new(PoolConfig::new(1), manager);

        let object = pool.get().await.unwrap();
        assert_eq!(*object, 0);
        drop(object);

        poll_and_cancel(pool.get());
        assert_eq!(pool.status().current_size, 0);
        assert_eq!(pool.status().idle_count, 0);

        let object = pool.get().await.unwrap();
        assert_eq!(*object, 1);
        assert_eq!(controls.created.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn cancellation_can_restore_the_idle_object() {
        let (manager, controls) = manager();
        let config = PoolConfig::new(1)
            .with_recycle_cancelled_strategy(RecycleCancelledStrategy::ReturnToPool);
        let pool = Pool::new(config, manager);

        let object = pool.get().await.unwrap();
        drop(object);
        let mut last_used_before = None;
        pool.retain(|_, status| {
            last_used_before = Some(status.last_used());
            true
        });

        poll_and_cancel(pool.get());
        assert_eq!(pool.status().current_size, 1);
        assert_eq!(pool.status().idle_count, 1);

        let mut last_used_after = None;
        pool.retain(|_, status| {
            last_used_after = Some(status.last_used());
            true
        });
        assert_eq!(last_used_after, last_used_before);

        controls.recycle_ready.store(true, Ordering::Release);
        let object = pool.get().await.unwrap();
        assert_eq!(*object, 0);
        assert_eq!(controls.created.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn repeated_cancellation_does_not_shrink_a_restoring_pool() {
        let (manager, _) = manager();
        let config = PoolConfig::new(3)
            .with_recycle_cancelled_strategy(RecycleCancelledStrategy::ReturnToPool);
        let pool = Pool::new(config, manager);

        let objects = [
            pool.get().await.unwrap(),
            pool.get().await.unwrap(),
            pool.get().await.unwrap(),
        ];
        drop(objects);

        for _ in 0..5 {
            poll_and_cancel(pool.get());
        }
        assert_eq!(pool.status().current_size, 3);
        assert_eq!(pool.status().idle_count, 3);
    }

    #[tokio::test]
    async fn rejected_recycle_detaches_even_when_cancellation_would_restore() {
        let (manager, controls) = manager();
        let config = PoolConfig::new(1)
            .with_recycle_cancelled_strategy(RecycleCancelledStrategy::ReturnToPool);
        let pool = Pool::new(config, manager);

        let object = pool.get().await.unwrap();
        drop(object);
        controls.reject_recycle.store(true, Ordering::Relaxed);
        controls.recycle_ready.store(true, Ordering::Release);

        let object = pool.get().await.unwrap();
        assert_eq!(*object, 1);
        assert_eq!(controls.created.load(Ordering::Relaxed), 2);
        assert_eq!(pool.status().current_size, 1);
    }
}

mod unbounded_tests {
    use asyncband::pool::unbounded::Pool;
    use asyncband::pool::unbounded::PoolConfig;

    use super::*;

    #[tokio::test]
    async fn cancellation_detaches_by_default() {
        let (manager, controls) = manager();
        let pool = Pool::new(PoolConfig::default(), manager);

        let object = pool.get().await.unwrap();
        assert_eq!(*object, 0);
        drop(object);

        poll_and_cancel(pool.get());
        assert_eq!(pool.status().current_size, 0);
        assert_eq!(pool.status().idle_count, 0);

        let object = pool.get().await.unwrap();
        assert_eq!(*object, 1);
        assert_eq!(controls.created.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn cancellation_can_restore_the_idle_object() {
        let (manager, controls) = manager();
        let config = PoolConfig::new()
            .with_recycle_cancelled_strategy(RecycleCancelledStrategy::ReturnToPool);
        let pool = Pool::new(config, manager);

        let object = pool.get().await.unwrap();
        drop(object);
        poll_and_cancel(pool.get());

        assert_eq!(pool.status().current_size, 1);
        assert_eq!(pool.status().idle_count, 1);
        controls.recycle_ready.store(true, Ordering::Release);

        let object = pool.get().await.unwrap();
        assert_eq!(*object, 0);
        assert_eq!(controls.created.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn repeated_cancellation_does_not_shrink_a_restoring_pool() {
        let (manager, _) = manager();
        let config = PoolConfig::new()
            .with_recycle_cancelled_strategy(RecycleCancelledStrategy::ReturnToPool);
        let pool = Pool::new(config, manager);

        let objects = [
            pool.get().await.unwrap(),
            pool.get().await.unwrap(),
            pool.get().await.unwrap(),
        ];
        drop(objects);

        for _ in 0..5 {
            poll_and_cancel(pool.get());
        }
        assert_eq!(pool.status().current_size, 3);
        assert_eq!(pool.status().idle_count, 3);
    }
}
