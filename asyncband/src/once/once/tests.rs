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

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio_test::assert_ready;

use super::*;
use crate::latch::Latch;
use crate::test_runtime;

#[tokio::test]
async fn test_call_once_runs_only_once() {
    static ONCE: Once = Once::new();
    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    assert!(!ONCE.is_completed());

    ONCE.call_once(async || {
        COUNTER.fetch_add(1, Ordering::SeqCst);
    })
    .await;

    assert!(ONCE.is_completed());
    assert_eq!(COUNTER.load(Ordering::SeqCst), 1);

    // Second call should not run the closure
    ONCE.call_once(async || {
        COUNTER.fetch_add(1, Ordering::SeqCst);
    })
    .await;

    assert_eq!(COUNTER.load(Ordering::SeqCst), 1);
}

#[test]
fn test_once_multi_task() {
    static ONCE: Once = Once::new();
    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    test_runtime().block_on(async {
        const N: usize = 100;

        let latch = Arc::new(Latch::new(N as u32));
        let mut handles = Vec::with_capacity(N);

        for _ in 0..N {
            let latch = latch.clone();
            handles.push(tokio::spawn(async move {
                ONCE.call_once(async || {
                    COUNTER.fetch_add(1, Ordering::SeqCst);
                })
                .await;
                latch.count_down();
            }));
        }

        latch.wait().await;

        for handle in handles {
            handle.await.unwrap();
        }

        // Only one task should have incremented the counter
        assert_eq!(COUNTER.load(Ordering::SeqCst), 1);
        assert!(ONCE.is_completed());
    });
}

#[tokio::test]
async fn test_once_cancelled() {
    static ONCE: Once = Once::new();
    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    let handle1 = tokio::spawn(async {
        let fut = ONCE.call_once(async || {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            COUNTER.fetch_add(1, Ordering::SeqCst);
        });
        let timeout = tokio::time::timeout(Duration::from_millis(1), fut).await;
        assert!(timeout.is_err());
    });

    let handle2 = tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        ONCE.call_once(async || {
            COUNTER.fetch_add(10, Ordering::SeqCst);
        })
        .await;
    });

    handle1.await.unwrap();
    handle2.await.unwrap();

    // The second task should have run since the first was cancelled
    assert_eq!(COUNTER.load(Ordering::SeqCst), 10);
    assert!(ONCE.is_completed());
}

#[tokio::test]
async fn test_once_debug() {
    let once = Once::new();
    let debug_str = format!("{:?}", once);
    assert!(debug_str.contains("Once"));
    assert!(debug_str.contains("done"));
    assert!(debug_str.contains("false"));

    once.call_once(async || {}).await;

    let debug_str = format!("{:?}", once);
    assert!(debug_str.contains("true"));
}

#[tokio::test]
async fn test_once_default() {
    let once = Once::default();
    assert!(!once.is_completed());
}

#[tokio::test]
async fn test_once_retry_after_panic() {
    static ONCE: Once = Once::new();
    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    let handle = tokio::spawn(async {
        ONCE.call_once(async || {
            COUNTER.fetch_add(1, Ordering::SeqCst);
            panic!("boom");
        })
        .await;
    });

    let err = handle.await.expect_err("once init should panic");
    assert!(err.is_panic());

    ONCE.call_once(async || {
        COUNTER.fetch_add(1, Ordering::SeqCst);
    })
    .await;

    assert_eq!(COUNTER.load(Ordering::SeqCst), 2);
    assert!(ONCE.is_completed());
}

#[tokio::test]
async fn test_once_wait() {
    // wait after call_once completed
    {
        let once = Once::new();
        once.call_once(async || {}).await;
        assert_ready!(tokio_test::task::spawn(once.wait()).poll());
    }

    // wait before call_once completed
    {
        static ONCE: Once = Once::new();
        let handle = tokio::spawn(async {
            ONCE.wait().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        ONCE.call_once(async || {}).await;
        handle.await.unwrap();
    }
}
