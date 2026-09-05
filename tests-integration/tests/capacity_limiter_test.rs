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

use std::pin::pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;

use asyncband::capacity_limiter::AlreadyBorrowed;
use asyncband::capacity_limiter::CapacityLimiter;
use asyncband::capacity_limiter::TryAcquireError;
use tests_integration::poll_once;

#[test]
fn zero_capacity_admits_nobody() {
    let limiter = CapacityLimiter::new(0);

    assert_eq!(limiter.total(), 0);
    assert_eq!(
        limiter.try_acquire("a").unwrap_err(),
        TryAcquireError::NoCapacity
    );
}

#[test]
fn capacity_bounds_concurrent_borrowers() {
    let limiter = CapacityLimiter::new(2);

    let a = limiter.try_acquire("a").unwrap();
    let b = limiter.try_acquire("b").unwrap();
    assert_eq!(limiter.borrowed(), 2);
    assert_eq!(limiter.available(), 0);
    assert_eq!(
        limiter.try_acquire("c").unwrap_err(),
        TryAcquireError::NoCapacity
    );

    drop(a);
    let c = limiter.try_acquire("c").unwrap();
    assert_eq!(c.borrower(), &"c");

    drop(b);
    drop(c);
    assert_eq!(limiter.borrowed(), 0);
    assert_eq!(limiter.available(), 2);
}

#[test]
fn one_token_per_borrower() {
    let limiter = CapacityLimiter::new(4);
    let held = limiter.try_acquire("a").unwrap();

    // Capacity is available, so the rejection is about identity rather than capacity.
    assert!(limiter.available() > 0);
    assert_eq!(
        limiter.try_acquire("a").unwrap_err(),
        TryAcquireError::AlreadyBorrowed
    );

    drop(held);
    assert!(limiter.try_acquire("a").is_ok());
}

#[tokio::test]
async fn duplicate_acquire_fails_without_waiting() {
    let limiter = CapacityLimiter::new(1);
    let _held = limiter.acquire("a").await.unwrap();

    // No capacity is left, yet this resolves immediately instead of queueing, because the identity
    // conflict is detected before the borrower enters the queue.
    assert_eq!(limiter.acquire("a").await.unwrap_err(), AlreadyBorrowed);
    assert_eq!(limiter.waiting(), 0);
}

#[tokio::test]
async fn queued_borrower_proceeds_after_release() {
    let limiter = Arc::new(CapacityLimiter::new(1));
    let held = limiter.try_acquire(1u32).unwrap();

    let waiter = {
        let limiter = limiter.clone();
        tokio::spawn(async move {
            let permit = limiter.acquire(2u32).await.unwrap();
            *permit.borrower()
        })
    };

    // Let the spawned task reach the queue before capacity is returned.
    while limiter.waiting() == 0 {
        tokio::task::yield_now().await;
    }
    assert_eq!(limiter.borrowed(), 1);

    drop(held);
    assert_eq!(waiter.await.unwrap(), 2);
    assert_eq!(limiter.borrowed(), 0);
}

#[test]
fn cancelled_acquire_releases_the_identity() {
    let limiter = CapacityLimiter::new(0);

    {
        let mut acquire = pin!(limiter.acquire("a"));
        assert!(poll_once(acquire.as_mut()).is_pending());
        assert_eq!(limiter.waiting(), 1);

        // A second acquisition by the same borrower is rejected while the first one is queued.
        assert_eq!(
            limiter.try_acquire("a").unwrap_err(),
            TryAcquireError::AlreadyBorrowed
        );
    }

    assert_eq!(limiter.waiting(), 0);

    // After cancellation the borrower is free to try again.
    limiter.set_total(1);
    assert!(limiter.try_acquire("a").is_ok());
}

#[test]
fn cancelled_acquire_returns_capacity_to_the_next_borrower() {
    let limiter = CapacityLimiter::new(1);
    let held = limiter.try_acquire("a").unwrap();

    {
        let mut queued = pin!(limiter.acquire("b"));
        assert!(poll_once(queued.as_mut()).is_pending());

        // "b" is handed the released token while it is still suspended, and is then cancelled.
        drop(held);
    }

    // The token must return to the limiter rather than vanish with the cancelled future.
    assert_eq!(limiter.available(), 1);
    assert_eq!(limiter.borrowed(), 0);
    assert_eq!(limiter.waiting(), 0);
    assert!(limiter.try_acquire("c").is_ok());
}

#[test]
fn growing_the_total_admits_more_borrowers() {
    let limiter = CapacityLimiter::new(1);
    let _a = limiter.try_acquire("a").unwrap();
    assert_eq!(
        limiter.try_acquire("b").unwrap_err(),
        TryAcquireError::NoCapacity
    );

    limiter.set_total(3);
    assert_eq!(limiter.total(), 3);

    let _b = limiter.try_acquire("b").unwrap();
    let _c = limiter.try_acquire("c").unwrap();
    assert_eq!(limiter.borrowed(), 3);
    assert_eq!(limiter.available(), 0);
}

#[test]
fn growing_the_total_wakes_a_queued_borrower() {
    let limiter = CapacityLimiter::new(0);
    let mut queued = pin!(limiter.acquire("a"));
    assert!(poll_once(queued.as_mut()).is_pending());

    limiter.set_total(1);

    let permit = match poll_once(queued.as_mut()) {
        Poll::Ready(result) => result.expect("the queued borrower holds no other token"),
        Poll::Pending => panic!("raising the total must admit the queued borrower"),
    };
    assert_eq!(permit.borrower(), &"a");
    assert_eq!(limiter.borrowed(), 1);
    assert_eq!(limiter.waiting(), 0);
}

#[test]
fn shrinking_the_total_never_revokes_borrowed_tokens() {
    let limiter = CapacityLimiter::new(3);
    let a = limiter.try_acquire("a").unwrap();
    let b = limiter.try_acquire("b").unwrap();
    let c = limiter.try_acquire("c").unwrap();

    limiter.set_total(1);
    assert_eq!(limiter.total(), 1);
    assert_eq!(limiter.borrowed(), 3);
    assert_eq!(limiter.available(), 0);

    // The first two releases repay the deficit instead of freeing capacity.
    drop(a);
    assert_eq!(limiter.available(), 0);
    drop(b);
    assert_eq!(limiter.available(), 0);

    drop(c);
    assert_eq!(limiter.available(), 1);
    assert_eq!(limiter.borrowed(), 0);
}

#[test]
fn shrinking_to_zero_blocks_further_admission() {
    let limiter = CapacityLimiter::new(1);
    limiter.set_total(0);

    assert_eq!(limiter.total(), 0);
    assert_eq!(
        limiter.try_acquire("a").unwrap_err(),
        TryAcquireError::NoCapacity
    );
}

#[tokio::test]
async fn queued_borrowers_are_served_in_order() {
    let limiter = Arc::new(CapacityLimiter::new(1));
    let held = limiter.try_acquire(0usize).unwrap();

    let order = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for borrower in 1..=4usize {
        let task_limiter = limiter.clone();
        let order = order.clone();

        handles.push(tokio::spawn(async move {
            let permit = task_limiter.acquire(borrower).await.unwrap();
            order.lock().unwrap().push(*permit.borrower());
        }));

        // Queue the borrowers one at a time so the expected order is well defined.
        while limiter.waiting() < borrower {
            tokio::task::yield_now().await;
        }
    }

    drop(held);
    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(*order.lock().unwrap(), vec![1, 2, 3, 4]);
    assert_eq!(limiter.waiting(), 0);
    assert_eq!(limiter.borrowed(), 0);
}

#[test]
fn borrower_may_be_any_hashable_identity() {
    let limiter = CapacityLimiter::new(2);

    let name: Arc<str> = Arc::from("tenant-a");
    let first = limiter.try_acquire(name.clone()).unwrap();
    assert_eq!(first.borrower().as_ref(), "tenant-a");

    // A distinct allocation with equal contents is the same borrower.
    let same: Arc<str> = Arc::from("tenant-a");
    assert_eq!(
        limiter.try_acquire(same).unwrap_err(),
        TryAcquireError::AlreadyBorrowed
    );

    let other: Arc<str> = Arc::from("tenant-b");
    assert!(limiter.try_acquire(other).is_ok());
}

#[test]
fn errors_describe_their_cause() {
    assert_eq!(
        AlreadyBorrowed.to_string(),
        "borrower already holds a token from this limiter"
    );
    assert_eq!(
        TryAcquireError::NoCapacity.to_string(),
        "no capacity available"
    );
    assert_eq!(
        TryAcquireError::AlreadyBorrowed.to_string(),
        "borrower already holds a token from this limiter"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_contention_preserves_the_capacity_bound() {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    let limiter = Arc::new(CapacityLimiter::new(2));
    let active = Arc::new(AtomicUsize::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for borrower in 0..8 {
        let limiter = limiter.clone();
        let active = active.clone();
        tasks.spawn(async move {
            for _ in 0..100 {
                let permit = limiter.acquire(borrower).await.unwrap();
                assert!(active.fetch_add(1, Ordering::SeqCst) < 2);
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::SeqCst);
                drop(permit);
            }
        });
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await
    .expect("queued work must finish");
    assert_eq!(limiter.available(), 2);
    assert_eq!(limiter.borrowed(), 0);
    assert_eq!(limiter.waiting(), 0);
}
