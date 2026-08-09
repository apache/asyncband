// Copyright 2024 tison <wander4096@gmail.com>
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

use std::collections::hash_map::RandomState;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use crate::once::OnceMap;

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn test_default_and_constructors() {
    let _map: OnceMap<String, i32> = OnceMap::default();
    let _: OnceMap<String, i32> = OnceMap::new();
    let _: OnceMap<String, i32> = OnceMap::with_capacity(10);
    let _: OnceMap<String, i32> = OnceMap::with_hasher(RandomState::new());
    let _: OnceMap<String, i32> = OnceMap::with_capacity_and_hasher(10, RandomState::new());

    // Check capacity (indirectly via debug or just ensure it runs)
    let map: OnceMap<String, i32> = OnceMap::with_capacity(100);
    assert!(format!("{:?}", map).contains("OnceMap"));
}

#[tokio::test]
async fn test_compute() {
    let map = OnceMap::new();
    let v = map.compute("key", async || 1).await;
    assert_eq!(v, 1);
    let v = map.compute("key", async || 2).await;
    assert_eq!(v, 1);
}

#[tokio::test]
async fn test_compute_concurrent() {
    let map = Arc::new(OnceMap::new());
    let cnt = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for _ in 0..10 {
        let map = map.clone();
        let cnt = cnt.clone();
        handles.push(tokio::spawn(async move {
            map.compute("key", async move || {
                cnt.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                42
            })
            .await
        }));
    }

    for h in handles {
        assert_eq!(h.await.unwrap(), 42);
    }
    assert_eq!(cnt.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_try_compute() {
    let map = OnceMap::new();

    // Fail first
    let res: Result<i32, &str> = map.try_compute("key", async || Err("fail")).await;
    assert_eq!(res, Err("fail"));
    assert!(map.map.lock().is_empty());

    // Success then
    let res: Result<i32, &str> = map.try_compute("key", async || Ok::<i32, &str>(1)).await;
    assert_eq!(res, Ok(1));

    // Cached
    let res: Result<i32, &str> = map.try_compute("key", async || Ok::<i32, &str>(2)).await;
    assert_eq!(res, Ok(1));
}

#[tokio::test]
async fn test_panicked_compute_removes_empty_entry() {
    let map = Arc::new(OnceMap::<&str, i32>::new());

    let map_clone = map.clone();
    let task = tokio::spawn(async move {
        map_clone
            .compute("key", async || {
                panic!("oops");
            })
            .await
    });

    assert!(task.await.unwrap_err().is_panic());
    assert!(map.map.lock().is_empty());
}

#[tokio::test]
async fn test_cancelled_compute_removes_empty_entry() {
    let map = Arc::new(OnceMap::<&str, i32>::new());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();

    let map_clone = map.clone();
    let task = tokio::spawn(async move {
        map_clone
            .compute("key", async move || {
                started_tx.send(()).unwrap();
                std::future::pending().await
            })
            .await
    });

    started_rx.await.unwrap();
    assert_eq!(map.map.lock().len(), 1);

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(map.map.lock().is_empty());
}

#[tokio::test]
async fn test_try_compute_concurrent_failure_then_success() {
    let map = OnceMap::new();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let first = map.try_compute("key", async move || {
        release_rx.await.unwrap();
        Err::<i32, &str>("fail")
    });
    tokio::pin!(first);
    assert!(poll_once(first.as_mut()).is_pending());

    let retry = map.try_compute("key", async || Ok::<i32, &str>(1));
    tokio::pin!(retry);
    assert!(poll_once(retry.as_mut()).is_pending());

    release_tx.send(()).unwrap();
    assert_eq!(first.await, Err("fail"));

    // The failed caller must not remove the cell while an existing waiter can still retry it.
    assert_eq!(map.map.lock().len(), 1);
    assert_eq!(retry.await, Ok(1));
    assert_eq!(map.get("key"), Some(1));
}

#[tokio::test]
async fn test_get_remove() {
    let map = OnceMap::new();
    assert_eq!(map.get("key"), None);
    assert_eq!(map.remove("key"), None);

    map.compute("key", async || 1).await;
    assert_eq!(map.get("key"), Some(1));

    let v = map.remove("key");
    assert_eq!(v, Some(1));
    assert_eq!(map.get("key"), None);

    map.compute("key", async || 2).await;
    map.discard("key");
    assert_eq!(map.get("key"), None);
}

#[tokio::test]
async fn test_remove_while_computing() {
    let map = Arc::new(OnceMap::new());
    let map_clone = map.clone();

    let t1 = tokio::spawn(async move {
        map_clone
            .compute("key", async || {
                tokio::time::sleep(Duration::from_millis(100)).await;
                1
            })
            .await
    });

    // Give t1 time to insert the cell and start "computing"
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Remove should return None because value is not ready
    // And it removes the cell from the map.
    assert_eq!(map.remove("key"), None);

    // t1 finishes. It returns 1.
    assert_eq!(t1.await.unwrap(), 1);

    // The map should be empty now (key was removed)
    assert_eq!(map.get("key"), None);
}

#[tokio::test]
async fn test_get_while_computing() {
    let map = Arc::new(OnceMap::new());
    let map_clone = map.clone();

    let t1 = tokio::spawn(async move {
        map_clone
            .compute("key", async || {
                tokio::time::sleep(Duration::from_millis(50)).await;
                1
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(map.get("key"), None);

    assert_eq!(t1.await.unwrap(), 1);
    assert_eq!(map.get("key"), Some(1));
}

#[tokio::test]
async fn test_from_iter() {
    let map: OnceMap<_, _> = vec![("a", 1), ("b", 2)].into_iter().collect();
    assert_eq!(map.get("a"), Some(1));
    assert_eq!(map.get("b"), Some(2));
    assert_eq!(map.get("c"), None);
}

#[tokio::test]
async fn test_complex_key_value() {
    #[derive(Hash, PartialEq, Eq, Debug)]
    struct Key(i32);

    let map = OnceMap::new();
    let v = map.compute(Key(1), async || "value".to_string()).await;
    assert_eq!(v, "value");

    assert_eq!(map.get(&Key(1)), Some("value".to_string()));
}
