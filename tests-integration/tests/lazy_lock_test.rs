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

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use asyncband::once::LazyLock;
use asyncband::once::LazyLockFuture;
use tokio::sync::Notify;

#[tokio::test]
/// Ensure that multiple concurrent calls to a successful `force` only run the
/// initializer once.
async fn force_runs_initializer_once() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let lazy = Arc::new(LazyLock::<u32, _>::new({
        let attempts = attempts.clone();
        async move || {
            attempts.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            42
        }
    }));

    let mut tasks = Vec::new();
    for _ in 0..32 {
        let lazy = lazy.clone();
        tasks.push(tokio::spawn(async move { *LazyLock::force(&lazy).await }));
    }

    for task in tasks {
        assert_eq!(task.await.unwrap(), 42);
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
/// Ensure that cancellation of a `force` call does not prevent future calls from
/// rerunning the initializer.
async fn cancellation_restarts_initialization() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let lazy = Arc::new(LazyLock::<u32, _>::new({
        let attempts = attempts.clone();
        let started = started.clone();
        async move || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                started.notify_one();
                std::future::pending::<()>().await;
            }
            42
        }
    }));

    let task = {
        let lazy = lazy.clone();
        tokio::spawn(async move { *LazyLock::force(&lazy).await })
    };
    started.notified().await;
    assert_eq!(LazyLock::get(&lazy), None);

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    assert_eq!(*LazyLock::force(&lazy).await, 42);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
/// Validates falliabile initialization. If the initializer returns an error,
/// the value is not stored and future calls may retry it.
async fn queued_callers_retry_after_errors() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let lazy = Arc::new(LazyLock::<u32, _>::new({
        let attempts = attempts.clone();
        async move || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            if attempt < 2 { Err("retry") } else { Ok(42) }
        }
    }));

    let mut tasks = Vec::new();
    for _ in 0..3 {
        let lazy = lazy.clone();
        tasks.push(tokio::spawn(async move {
            LazyLock::try_force(&lazy).await.copied()
        }));
    }

    let mut errors = 0;
    let mut successes = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(42) => successes += 1,
            Err("retry") => errors += 1,
            result => panic!("unexpected result: {result:?}"),
        }
    }

    assert_eq!(errors, 2);
    assert_eq!(successes, 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(LazyLock::try_force(&lazy).await, Ok(&42));
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
/// Ensure that a panic in the initializer permanently poisons the lock, preventing future calls
/// from succeeding.
async fn panic_permanently_poisons_lock() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let lazy = Arc::new(LazyLock::<u32, _>::new({
        let attempts = attempts.clone();
        async move || {
            attempts.fetch_add(1, Ordering::SeqCst);
            panic!("initializer panic");
        }
    }));

    let first = {
        let lazy = lazy.clone();
        tokio::spawn(async move {
            let _ = LazyLock::force(&lazy).await;
        })
    };
    assert!(first.await.unwrap_err().is_panic());
    assert_eq!(LazyLock::get(&lazy), None);

    let second = {
        let lazy = lazy.clone();
        tokio::spawn(async move {
            let _ = LazyLock::force(&lazy).await;
        })
    };
    assert!(second.await.unwrap_err().is_panic());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    let lazy = Arc::try_unwrap(lazy).ok().unwrap();
    let result = std::panic::catch_unwind(|| LazyLock::into_inner(lazy));
    assert!(result.is_err());
}

#[tokio::test]
/// Ensure that `force_mut` and `try_force_mut` can be used to mutate the value
/// after it has been initialized.
async fn mutable_force_updates_value() {
    let mut lazy = LazyLock::<u32, _>::new(async || 41);
    *LazyLock::force_mut(&mut lazy).await += 1;
    assert_eq!(LazyLock::get(&lazy), Some(&42));

    let mut fallible = LazyLock::<u32, _>::new(async || Ok::<_, ()>(41));
    *LazyLock::try_force_mut(&mut fallible).await.unwrap() += 1;
    assert_eq!(LazyLock::get(&fallible), Some(&42));
}

#[tokio::test]
/// Ensure that `into_inner` returns the value if it has been initialized, or
/// returns the initializer if it has not been initialized.
async fn into_inner_returns_value_or_initializer() {
    let lazy = LazyLock::<u32, _>::new(async || 42);
    let initializer = LazyLock::into_inner(lazy).unwrap_err();
    assert_eq!(initializer().await, 42);

    let lazy = LazyLock::<u32, _>::new(async || 42);
    LazyLock::force(&lazy).await;
    assert!(matches!(LazyLock::into_inner(lazy), Ok(42)));
}

#[tokio::test]
/// Validates that `Debug` and `Default` trait implementations work as
/// expected.
async fn default_from_and_debug_match_lazy_lock() {
    let lazy = LazyLock::<u32>::default();
    assert_eq!(format!("{lazy:?}"), "LazyLock(<uninit>)");
    assert_eq!(LazyLock::force(&lazy).await, &0);
    assert_eq!(format!("{lazy:?}"), "LazyLock(0)");

    let lazy: LazyLock<u32> = LazyLock::from(42);
    assert_eq!(LazyLock::get(&lazy), Some(&42));
}

#[tokio::test]
/// Ensure that the initializer does not need to be `Sync` in order for the `LazyLock` to be
/// `Sync`. This is important for cases where the initializer captures non-`Sync` state, such as a
/// `Cell`. This is guaranteed by the internal mutex.
async fn initializer_need_not_be_sync() {
    fn assert_sync<T: Sync>(_: &T) {}

    let count = Cell::new(0);
    let lazy = LazyLock::<u32, _>::new(async move || {
        count.set(count.get() + 1);
        count.get()
    });

    assert_sync(&lazy);
    assert_eq!(LazyLock::force(&lazy).await, &1);
}

fn static_initializer() -> LazyLockFuture<u32> {
    Box::pin(async { 42 })
}

static STATIC_LAZY: LazyLock<u32> = LazyLock::new(static_initializer);

#[tokio::test]
async fn default_initializer_type_supports_statics() {
    assert_eq!(LazyLock::force(&STATIC_LAZY).await, &42);
}

fn fallible_static_initializer() -> LazyLockFuture<Result<u32, &'static str>> {
    Box::pin(async { Ok(42) })
}

type FallibleStaticInitializer = fn() -> LazyLockFuture<Result<u32, &'static str>>;

static FALLIBLE_STATIC_LAZY: LazyLock<u32, FallibleStaticInitializer> =
    LazyLock::new(fallible_static_initializer);

#[tokio::test]
async fn one_type_supports_fallible_statics() {
    assert_eq!(LazyLock::try_force(&FALLIBLE_STATIC_LAZY).await, Ok(&42));
}
