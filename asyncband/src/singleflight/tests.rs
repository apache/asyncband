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

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::poll_once;
use crate::singleflight::Group;

#[tokio::test]
async fn test_simple() {
    let group = Group::new();
    let res = group.work("key", || async { "val" }).await;
    assert_eq!(res, "val");
}

#[tokio::test]
async fn test_non_clone_key() {
    #[derive(Hash, PartialEq, Eq)]
    struct Key(&'static str);

    let group = Group::new();
    let res = group.work(Key("key"), || async { "val" }).await;
    assert_eq!(res, "val");
}

#[tokio::test]
async fn test_coalescing() {
    let group = Arc::new(Group::new());
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..10 {
        let group = group.clone();
        let counter = counter.clone();
        handles.push(tokio::spawn(async move {
            group
                .work("key", || async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    counter.fetch_add(1, Ordering::SeqCst);
                    "val"
                })
                .await
        }));
    }

    for handle in handles {
        assert_eq!(handle.await.unwrap(), "val");
    }

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_multiple_keys() {
    let group = Arc::new(Group::new());
    let counter = Arc::new(AtomicUsize::new(0));

    let g1 = group.clone();
    let c1 = counter.clone();
    let h1 = tokio::spawn(async move {
        g1.work("key1", || async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            c1.fetch_add(1, Ordering::SeqCst);
            "val1"
        })
        .await
    });

    let g2 = group.clone();
    let c2 = counter.clone();
    let h2 = tokio::spawn(async move {
        g2.work("key2", || async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            c2.fetch_add(1, Ordering::SeqCst);
            "val2"
        })
        .await
    });

    assert_eq!(h1.await.unwrap(), "val1");
    assert_eq!(h2.await.unwrap(), "val2");
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_forget() {
    let group = Arc::new(Group::new());
    let counter = Arc::new(AtomicUsize::new(0));

    let g1 = group.clone();
    let c1 = counter.clone();
    let h1 = tokio::spawn(async move {
        g1.work("key".to_owned(), || async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            c1.fetch_add(1, Ordering::SeqCst);
            "val1"
        })
        .await
    });

    // Wait a bit to ensure the first call is established
    tokio::time::sleep(Duration::from_millis(10)).await;
    group.forget("key");

    let g2 = group.clone();
    let c2 = counter.clone();
    let h2 = tokio::spawn(async move {
        g2.work("key".to_owned(), || async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            c2.fetch_add(1, Ordering::SeqCst);
            "val2"
        })
        .await
    });

    assert_eq!(h1.await.unwrap(), "val1");
    assert_eq!(h2.await.unwrap(), "val2");
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_panic_safe() {
    let group = Arc::new(Group::<&str, String>::new());

    // Task that panics
    let g1 = group.clone();
    let h1 = tokio::spawn(async move {
        g1.work("key", || async {
            panic!("oops");
        })
        .await
    });

    // Wait for h1 to panic and exit
    let err = h1.await.unwrap_err();
    assert!(err.is_panic());
    assert!(group.map.lock().is_empty());

    // Next task should succeed (new attempt)
    let res = group.work("key", || async { "success".to_string() }).await;
    assert_eq!(res, "success");
}

#[tokio::test]
async fn test_cancelled_work_removes_empty_entry() {
    let group = Arc::new(Group::<&str, &str>::new());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();

    let group_clone = group.clone();
    let task = tokio::spawn(async move {
        group_clone
            .work("key", || async move {
                started_tx.send(()).unwrap();
                std::future::pending().await
            })
            .await
    });

    started_rx.await.unwrap();
    assert_eq!(group.map.lock().len(), 1);

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(group.map.lock().is_empty());
}

#[tokio::test]
async fn test_try_work_simple() {
    let group = Group::new();
    let res = group
        .try_work("key", || async { Ok::<&str, ()>("val") })
        .await;
    assert_eq!(res, Ok("val"));

    // Should be removed from map, so next call executes again
    let res2 = group
        .try_work("key", || async { Ok::<&str, ()>("val2") })
        .await;
    assert_eq!(res2, Ok("val2"));
}

#[tokio::test]
async fn test_try_work_coalescing() {
    let group = Arc::new(Group::new());
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..10 {
        let group = group.clone();
        let counter = counter.clone();
        handles.push(tokio::spawn(async move {
            group
                .try_work("key", || async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok::<&str, ()>("val")
                })
                .await
        }));
    }

    for handle in handles {
        assert_eq!(handle.await.unwrap(), Ok("val"));
    }

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_try_work_failure() {
    let group = Group::new();
    let res = group
        .try_work("key", || async { Err::<&str, &str>("error") })
        .await;
    assert_eq!(res, Err("error"));
    assert!(group.map.lock().is_empty());

    // Retry should work
    let res2 = group
        .try_work("key", || async { Ok::<&str, ()>("success") })
        .await;
    assert_eq!(res2, Ok("success"));
}

#[tokio::test]
async fn test_try_work_wait_and_retry() {
    let group = Group::new();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let first = group.try_work("key", || async move {
        release_rx.await.unwrap();
        Err::<&str, &str>("fail")
    });
    tokio::pin!(first);
    assert!(poll_once(first.as_mut()).is_pending());

    let retry = group.try_work("key", || async { Ok::<&str, &str>("success") });
    tokio::pin!(retry);
    assert!(poll_once(retry.as_mut()).is_pending());

    release_tx.send(()).unwrap();
    assert_eq!(first.await, Err("fail"));

    // The failed caller must not remove the cell while an existing waiter can still retry it.
    assert_eq!(group.map.lock().len(), 1);
    assert_eq!(retry.await, Ok("success"));
    assert!(group.map.lock().is_empty());
}
