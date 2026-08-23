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

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use asyncband::pool::ManageObject;
use asyncband::pool::ObjectStatus;
use asyncband::pool::RecycleCancelledStrategy;

struct SlowRecycleManager {
    created_count: Arc<AtomicUsize>,
    recycle_delay: Duration,
}

impl SlowRecycleManager {
    fn new(created_count: Arc<AtomicUsize>, recycle_delay: Duration) -> Self {
        Self {
            created_count,
            recycle_delay,
        }
    }
}

impl ManageObject for SlowRecycleManager {
    type Object = usize;
    type Error = Infallible;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        let id = self.created_count.fetch_add(1, Ordering::SeqCst);
        Ok(id)
    }

    async fn is_recyclable(
        &self,
        _o: &mut Self::Object,
        _status: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        tokio::time::sleep(self.recycle_delay).await;
        Ok(())
    }
}

mod bounded_tests {
    use asyncband::pool::bounded::Pool;
    use asyncband::pool::bounded::PoolConfig;

    use super::*;

    /// Test default behavior (Detach): cancelled get() calls detach objects from the pool.
    #[tokio::test]
    async fn test_default_detach_behavior() {
        const MAX_SIZE: usize = 1;
        let created_count = Arc::new(AtomicUsize::new(0));
        let manager = SlowRecycleManager::new(created_count.clone(), Duration::from_millis(100));
        let pool = Pool::new(PoolConfig::new(MAX_SIZE), manager);

        let obj = pool.get().await.unwrap();
        assert_eq!(*obj, 0);
        assert_eq!(pool.status().current_size, 1);

        drop(obj);
        assert_eq!(pool.status().current_size, 1);
        assert_eq!(pool.status().idle_count, 1);

        let timeout_result = tokio::time::timeout(Duration::from_millis(10), pool.get()).await;
        assert!(timeout_result.is_err(), "Should have timed out");

        tokio::time::sleep(Duration::from_millis(10)).await;

        let status = pool.status();
        assert_eq!(
            status.current_size, 0,
            "Pool size should be 0 after cancelled get() with Detach behavior"
        );

        let obj = pool.get().await.unwrap();
        assert_eq!(*obj, 1, "Should be a new object (id=1)");
        assert_eq!(
            created_count.load(Ordering::SeqCst),
            2,
            "Two objects should have been created"
        );
    }

    /// Test ReturnToPool behavior: cancelled get() calls return objects to the pool.
    #[tokio::test]
    async fn test_return_to_pool_behavior() {
        const MAX_SIZE: usize = 1;
        let created_count = Arc::new(AtomicUsize::new(0));
        let manager = SlowRecycleManager::new(created_count.clone(), Duration::from_millis(100));
        let config = PoolConfig::new(MAX_SIZE)
            .with_recycle_cancelled_strategy(RecycleCancelledStrategy::ReturnToPool);
        let pool = Pool::new(config, manager);

        let obj = pool.get().await.unwrap();
        assert_eq!(*obj, 0);
        assert_eq!(pool.status().current_size, 1);
        assert_eq!(created_count.load(Ordering::SeqCst), 1);

        drop(obj);
        assert_eq!(pool.status().current_size, 1);
        assert_eq!(pool.status().idle_count, 1);

        let timeout_result = tokio::time::timeout(Duration::from_millis(10), pool.get()).await;
        assert!(timeout_result.is_err(), "Should have timed out");

        tokio::time::sleep(Duration::from_millis(10)).await;

        let status = pool.status();
        assert_eq!(
            status.current_size, 1,
            "Pool size should be preserved after cancelled get() with ReturnToPool behavior"
        );
        assert_eq!(
            status.idle_count, 1,
            "Object should be back in idle state after cancelled get()"
        );

        assert_eq!(
            created_count.load(Ordering::SeqCst),
            1,
            "No extra objects should be created"
        );

        let obj = pool.get().await.unwrap();
        assert_eq!(*obj, 0, "Should get the same object back");
    }

    /// Test that multiple cancelled get() calls with ReturnToPool don't shrink the pool.
    #[tokio::test]
    async fn test_multiple_cancelled_gets_with_return_to_pool() {
        const MAX_SIZE: usize = 3;
        let created_count = Arc::new(AtomicUsize::new(0));
        let manager = SlowRecycleManager::new(created_count.clone(), Duration::from_millis(100));
        let config = PoolConfig::new(MAX_SIZE)
            .with_recycle_cancelled_strategy(RecycleCancelledStrategy::ReturnToPool);
        let pool = Pool::new(config, manager);

        let obj1 = pool.get().await.unwrap();
        let obj2 = pool.get().await.unwrap();
        let obj3 = pool.get().await.unwrap();

        drop((obj1, obj2, obj3));
        assert_eq!(pool.status().current_size, 3);
        assert_eq!(pool.status().idle_count, 3);

        for _ in 0..5 {
            let _ = tokio::time::timeout(Duration::from_millis(10), pool.get()).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let status = pool.status();
        assert_eq!(
            status.current_size, 3,
            "Pool size should be preserved after multiple cancelled gets"
        );
    }

    /// Test that cancelling `get()` releases its user-count registration.
    #[tokio::test]
    async fn test_cancelled_get_releases_user_count() {
        let created_count = Arc::new(AtomicUsize::new(0));
        let manager = SlowRecycleManager::new(created_count, Duration::from_millis(100));
        let pool = Pool::new(PoolConfig::new(1), manager);

        let object = pool.get().await.unwrap();
        drop(object);

        let mut get = Box::pin(pool.get());
        assert!(tests_integration::poll_once(get.as_mut()).is_pending());
        drop(get);

        assert_eq!(pool.status().wait_count, 0);
    }

    /// Test that a failed create call releases its user-count registration.
    #[tokio::test]
    async fn test_failed_create_releases_user_count() {
        struct FailingManager;

        impl ManageObject for FailingManager {
            type Object = ();
            type Error = ();

            async fn create(&self) -> Result<Self::Object, Self::Error> {
                Err(())
            }

            async fn is_recyclable(
                &self,
                _o: &mut Self::Object,
                _status: &ObjectStatus,
            ) -> Result<(), Self::Error> {
                Ok(())
            }
        }

        let pool = Pool::new(PoolConfig::new(1), FailingManager);
        assert!(pool.get().await.is_err());

        let status = pool.status();
        assert_eq!(status.current_size, 0);
        assert_eq!(status.wait_count, 0);
    }

    /// Test that failed is_recyclable always properly detaches objects.
    #[tokio::test]
    async fn test_failed_recyclable_still_detaches() {
        const MAX_SIZE: usize = 1;
        let created_count = Arc::new(AtomicUsize::new(0));
        let manager = SlowRecycleManager::new(created_count.clone(), Duration::from_millis(10));
        let config = PoolConfig::new(MAX_SIZE)
            .with_recycle_cancelled_strategy(RecycleCancelledStrategy::ReturnToPool);
        let pool = Pool::new(config, manager);

        let obj = pool.get().await.unwrap();
        assert_eq!(*obj, 0);
        drop(obj);
        assert_eq!(pool.status().current_size, 1);
        assert_eq!(pool.status().idle_count, 1);

        let obj = pool.get().await.unwrap();
        assert_eq!(*obj, 0, "Should get the recycled object");
        assert_eq!(
            obj.status().recycle_count(),
            1,
            "Should have been recycled once"
        );
    }
}

mod unbounded_tests {
    use asyncband::pool::unbounded::Pool;
    use asyncband::pool::unbounded::PoolConfig;

    use super::*;

    /// Test default behavior (Detach): cancelled get() calls detach objects from the unbounded
    /// pool.
    #[tokio::test]
    async fn test_default_detach_behavior() {
        let created_count = Arc::new(AtomicUsize::new(0));
        let manager = SlowRecycleManager::new(created_count.clone(), Duration::from_millis(100));
        let pool = Pool::new(PoolConfig::default(), manager);

        let obj = pool.get().await.unwrap();
        assert_eq!(*obj, 0);
        assert_eq!(pool.status().current_size, 1);

        drop(obj);
        assert_eq!(pool.status().current_size, 1);
        assert_eq!(pool.status().idle_count, 1);

        let timeout_result = tokio::time::timeout(Duration::from_millis(10), pool.get()).await;
        assert!(timeout_result.is_err(), "Should have timed out");

        tokio::time::sleep(Duration::from_millis(10)).await;

        let status = pool.status();
        assert_eq!(
            status.current_size, 0,
            "Pool size should be 0 after cancelled get() with Detach behavior"
        );

        let obj = pool.get().await.unwrap();
        assert_eq!(*obj, 1, "Should be a new object (id=1)");
        assert_eq!(
            created_count.load(Ordering::SeqCst),
            2,
            "Two objects should have been created"
        );
    }

    /// Test ReturnToPool behavior: cancelled get() calls return objects to the unbounded pool.
    #[tokio::test]
    async fn test_return_to_pool_behavior() {
        let created_count = Arc::new(AtomicUsize::new(0));
        let manager = SlowRecycleManager::new(created_count.clone(), Duration::from_millis(100));
        let config = PoolConfig::new()
            .with_recycle_cancelled_strategy(RecycleCancelledStrategy::ReturnToPool);
        let pool = Pool::new(config, manager);

        let obj = pool.get().await.unwrap();
        assert_eq!(*obj, 0);
        assert_eq!(pool.status().current_size, 1);
        assert_eq!(created_count.load(Ordering::SeqCst), 1);

        drop(obj);
        assert_eq!(pool.status().current_size, 1);
        assert_eq!(pool.status().idle_count, 1);

        let timeout_result = tokio::time::timeout(Duration::from_millis(10), pool.get()).await;
        assert!(timeout_result.is_err(), "Should have timed out");

        tokio::time::sleep(Duration::from_millis(10)).await;

        let status = pool.status();
        assert_eq!(
            status.current_size, 1,
            "Pool size should be preserved after cancelled get() with ReturnToPool behavior"
        );
        assert_eq!(
            status.idle_count, 1,
            "Object should be back in idle state after cancelled get()"
        );

        // Verify we can still get the same object
        let obj = pool.get().await.unwrap();
        assert_eq!(*obj, 0, "Should get the same object back");
    }

    /// Test that multiple cancelled get() calls with ReturnToPool don't shrink the unbounded pool.
    #[tokio::test]
    async fn test_multiple_cancelled_gets_with_return_to_pool() {
        let created_count = Arc::new(AtomicUsize::new(0));
        let manager = SlowRecycleManager::new(created_count.clone(), Duration::from_millis(100));
        let config = PoolConfig::new()
            .with_recycle_cancelled_strategy(RecycleCancelledStrategy::ReturnToPool);
        let pool = Pool::new(config, manager);

        let obj1 = pool.get().await.unwrap();
        let obj2 = pool.get().await.unwrap();
        let obj3 = pool.get().await.unwrap();

        drop((obj1, obj2, obj3));
        assert_eq!(pool.status().current_size, 3);
        assert_eq!(pool.status().idle_count, 3);

        for _ in 0..5 {
            let _ = tokio::time::timeout(Duration::from_millis(10), pool.get()).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let status = pool.status();
        assert_eq!(
            status.current_size, 3,
            "Pool size should be preserved after multiple cancelled gets"
        );
    }
}
