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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use asyncband::singleflight::Group;
use tests_integration::poll_once;

#[tokio::test]
async fn work_returns_value() {
    let group = Group::new();
    let res = group.work("key", || async { "val" }).await;
    assert_eq!(res, "val");
}

#[tokio::test]
async fn supports_non_clone_key() {
    #[derive(Hash, PartialEq, Eq)]
    struct Key(&'static str);

    let group = Group::new();
    let res = group.work(Key("key"), || async { "val" }).await;
    assert_eq!(res, "val");
}

#[tokio::test]
async fn concurrent_work_is_coalesced() {
    let group = Group::new();
    let counter = AtomicUsize::new(0);
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let leader = group.work("key", || async {
        counter.fetch_add(1, Ordering::SeqCst);
        release_rx.await.unwrap();
        "val"
    });
    tokio::pin!(leader);
    assert!(poll_once(leader.as_mut()).is_pending());

    let mut waiters = (0..9)
        .map(|_| {
            Box::pin(group.work("key", || async {
                counter.fetch_add(1, Ordering::SeqCst);
                "other"
            }))
        })
        .collect::<Vec<_>>();
    for waiter in &mut waiters {
        assert!(poll_once(waiter.as_mut()).is_pending());
    }
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    release_tx.send(()).unwrap();
    assert_eq!(leader.await, "val");
    for waiter in waiters {
        assert_eq!(waiter.await, "val");
    }
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn pending_work_does_not_block_other_keys() {
    let group = Group::new();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let first = group.work("key1", || async {
        release_rx.await.unwrap();
        "val1"
    });
    tokio::pin!(first);
    assert!(poll_once(first.as_mut()).is_pending());

    assert_eq!(group.work("key2", || async { "val2" }).await, "val2");

    release_tx.send(()).unwrap();
    assert_eq!(first.await, "val1");
}

#[tokio::test]
async fn forget_detaches_in_flight_work() {
    let group = Group::new();
    let counter = AtomicUsize::new(0);
    let (first_release_tx, first_release_rx) = tokio::sync::oneshot::channel();
    let (second_release_tx, second_release_rx) = tokio::sync::oneshot::channel();

    let first = group.work("key".to_owned(), || async {
        counter.fetch_add(1, Ordering::SeqCst);
        first_release_rx.await.unwrap();
        "val1"
    });
    tokio::pin!(first);
    assert!(poll_once(first.as_mut()).is_pending());

    group.forget("key");

    let second = group.work("key".to_owned(), || async {
        counter.fetch_add(1, Ordering::SeqCst);
        second_release_rx.await.unwrap();
        "val2"
    });
    tokio::pin!(second);
    assert!(poll_once(second.as_mut()).is_pending());
    assert_eq!(counter.load(Ordering::SeqCst), 2);

    first_release_tx.send(()).unwrap();
    second_release_tx.send(()).unwrap();
    assert_eq!(first.await, "val1");
    assert_eq!(second.await, "val2");
}

#[tokio::test]
async fn try_work_returns_value_without_caching_it() {
    let group = Group::new();
    let res = group
        .try_work("key", || async { Ok::<&str, ()>("val") })
        .await;
    assert_eq!(res, Ok("val"));

    let res2 = group
        .try_work("key", || async { Ok::<&str, ()>("val2") })
        .await;
    assert_eq!(res2, Ok("val2"));
}

#[tokio::test]
async fn concurrent_try_work_is_coalesced() {
    let group = Group::new();
    let counter = AtomicUsize::new(0);
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let leader = group.try_work("key", || async {
        counter.fetch_add(1, Ordering::SeqCst);
        release_rx.await.unwrap();
        Ok::<&str, ()>("val")
    });
    tokio::pin!(leader);
    assert!(poll_once(leader.as_mut()).is_pending());

    let mut waiters = (0..9)
        .map(|_| {
            Box::pin(group.try_work("key", || async {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<&str, ()>("other")
            }))
        })
        .collect::<Vec<_>>();
    for waiter in &mut waiters {
        assert!(poll_once(waiter.as_mut()).is_pending());
    }
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    release_tx.send(()).unwrap();
    assert_eq!(leader.await, Ok("val"));
    for waiter in waiters {
        assert_eq!(waiter.await, Ok("val"));
    }
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}
