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

// Portions ported from Fastpool 1.1.1 at commit e4c65f1:
// Copyright 2025 FastLabs Developers
// https://github.com/fast/fastpool/tree/e4c65f1

use std::convert::Infallible;
use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;

use asyncband::pool::ManageObject;
use asyncband::pool::ObjectStatus;
use asyncband::pool::bounded::Pool;
use asyncband::pool::bounded::PoolConfig;

#[tokio::test]
async fn test_replenish_to() {
    #[derive(Default)]
    struct Manager;

    impl ManageObject for Manager {
        type Object = ();
        type Error = Infallible;

        async fn create(&self) -> Result<Self::Object, Self::Error> {
            Ok(())
        }

        async fn is_recyclable(
            &self,
            _o: &mut Self::Object,
            _status: &ObjectStatus,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    const MAX_SIZE: usize = 2;

    fn make_default() -> Arc<Pool<Manager>> {
        Pool::new(PoolConfig::new(MAX_SIZE), Manager)
    }

    for i in 0..5 {
        let pool = make_default();
        let n = pool.replenish_to(i).await.unwrap();
        assert_eq!(n, i.min(MAX_SIZE));
    }

    // stage one idle object
    {
        let pool = make_default();
        pool.get().await.unwrap();
        let n = pool.replenish_to(2).await.unwrap();
        assert_eq!(n, 1);
    }

    // stage two idle objects
    {
        let pool = make_default();
        let o1 = pool.get().await.unwrap();
        let o2 = pool.get().await.unwrap();
        drop((o1, o2));

        let n = pool.replenish_to(2).await.unwrap();
        assert_eq!(n, 0);
    }
}

#[derive(Debug, PartialEq, Eq)]
struct CreateError;

struct FailingManager {
    calls: Arc<AtomicUsize>,
}

impl ManageObject for FailingManager {
    type Object = usize;
    type Error = CreateError;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        let call = self.calls.fetch_add(1, Ordering::Relaxed);
        if call == 1 {
            Err(CreateError)
        } else {
            Ok(call)
        }
    }

    async fn is_recyclable(
        &self,
        _object: &mut Self::Object,
        _status: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn replenish_to_reports_create_errors_and_keeps_prior_objects() {
    let pool = Pool::new(
        PoolConfig::new(2),
        FailingManager {
            calls: Arc::new(AtomicUsize::new(0)),
        },
    );

    assert_eq!(pool.replenish_to(2).await, Err(CreateError));
    assert_eq!(pool.status().current_size, 1);
    assert_eq!(pool.status().idle_count, 1);

    assert_eq!(pool.replenish_to(2).await, Ok(1));
    assert_eq!(pool.status().current_size, 2);
    assert_eq!(pool.status().idle_count, 2);
}

struct ControlledManager {
    calls: Arc<AtomicUsize>,
    allow_create: Arc<AtomicBool>,
}

impl ManageObject for ControlledManager {
    type Object = usize;
    type Error = Infallible;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        let call = self.calls.fetch_add(1, Ordering::Relaxed);
        if call != 0 {
            poll_fn(|_| {
                if self.allow_create.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        }
        Ok(call)
    }

    async fn is_recyclable(
        &self,
        _object: &mut Self::Object,
        _status: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn concurrent_replenish_to_calls_respect_capacity() {
    let calls = Arc::new(AtomicUsize::new(0));
    let allow_create = Arc::new(AtomicBool::new(false));
    let pool = Pool::new(
        PoolConfig::new(2),
        ControlledManager {
            calls: calls.clone(),
            allow_create: allow_create.clone(),
        },
    );

    assert_eq!(pool.replenish_to(1).await, Ok(1));

    let mut first = Box::pin(pool.replenish_to(2));
    assert!(tests_integration::poll_once(first.as_mut()).is_pending());

    let mut second = Box::pin(pool.replenish_to(2));
    assert_eq!(
        tests_integration::poll_once(second.as_mut()),
        Poll::Ready(Ok(0))
    );

    allow_create.store(true, Ordering::Release);
    assert_eq!(
        tests_integration::poll_once(first.as_mut()),
        Poll::Ready(Ok(1))
    );
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert_eq!(pool.status().current_size, 2);
    assert_eq!(pool.status().idle_count, 2);
}

#[tokio::test]
async fn concurrent_get_and_replenish_to_respect_capacity() {
    let calls = Arc::new(AtomicUsize::new(0));
    let allow_create = Arc::new(AtomicBool::new(false));
    let pool = Pool::new(
        PoolConfig::new(2),
        ControlledManager {
            calls: calls.clone(),
            allow_create: allow_create.clone(),
        },
    );

    let first = pool.get().await.unwrap();

    let mut replenish = Box::pin(pool.replenish_to(2));
    assert!(tests_integration::poll_once(replenish.as_mut()).is_pending());

    let mut get = Box::pin(pool.get());
    assert!(tests_integration::poll_once(get.as_mut()).is_pending());
    assert_eq!(pool.status().current_size, 1);

    allow_create.store(true, Ordering::Release);
    assert_eq!(
        tests_integration::poll_once(replenish.as_mut()),
        Poll::Ready(Ok(1))
    );

    let second = match tests_integration::poll_once(get.as_mut()) {
        Poll::Ready(Ok(object)) => object,
        _ => panic!("get should consume the replenished object"),
    };
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert_eq!(pool.status().current_size, 2);
    assert_eq!(pool.status().idle_count, 0);

    drop((first, second));
    assert_eq!(pool.status().idle_count, 2);
}

struct BlockingManager {
    allow_create: Arc<AtomicBool>,
}

impl ManageObject for BlockingManager {
    type Object = ();
    type Error = Infallible;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        poll_fn(|_| {
            if self.allow_create.load(Ordering::Acquire) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        Ok(())
    }

    async fn is_recyclable(
        &self,
        _object: &mut Self::Object,
        _status: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn replenish_to_respects_max_size_with_active_and_idle_objects() {
    let pool = Pool::new(
        PoolConfig::new(2),
        BlockingManager {
            allow_create: Arc::new(AtomicBool::new(true)),
        },
    );

    assert_eq!(pool.replenish_to(2).await, Ok(2));
    let active = pool.get().await.unwrap();
    assert_eq!(pool.status().current_size, 2);
    assert_eq!(pool.status().idle_count, 1);

    assert_eq!(pool.replenish_to(usize::MAX).await, Ok(0));
    assert_eq!(pool.status().current_size, 2);
    assert_eq!(pool.status().idle_count, 1);

    drop(active);
    assert_eq!(pool.status().idle_count, 2);
}

#[tokio::test]
async fn cancelling_replenish_to_releases_reserved_capacity() {
    let allow_create = Arc::new(AtomicBool::new(false));
    let pool = Pool::new(
        PoolConfig::new(1),
        BlockingManager {
            allow_create: allow_create.clone(),
        },
    );

    let mut replenish = Box::pin(pool.replenish_to(1));
    assert!(tests_integration::poll_once(replenish.as_mut()).is_pending());
    drop(replenish);

    allow_create.store(true, Ordering::Release);
    let mut get = Box::pin(pool.get());
    assert!(tests_integration::poll_once(get.as_mut()).is_ready());
    assert_eq!(pool.status().idle_count, 1);
}
