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

use asyncband::once::LazyCell;
use asyncband::once::LazyCellFuture;
use tokio::sync::Notify;

#[tokio::test]
/// Ensure that multiple concurrent calls to a successful `force` only run the
/// initializer once.
async fn force_runs_initializer_once() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let lazy = Arc::new(LazyCell::<u32, _>::new({
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
        tasks.push(tokio::spawn(async move { *LazyCell::force(&lazy).await }));
    }

    for task in tasks {
        assert_eq!(task.await.unwrap(), 42);
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
/// Ensure that cancellation preserves the active initialization future.
async fn cancellation_resumes_initialization() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let lazy = Arc::new(LazyCell::<u32, _>::new({
        let attempts = attempts.clone();
        let started = started.clone();
        let resume = resume.clone();
        async move || {
            attempts.fetch_add(1, Ordering::SeqCst);
            started.notify_one();
            resume.notified().await;
            42
        }
    }));

    let task = {
        let lazy = lazy.clone();
        tokio::spawn(async move { *LazyCell::force(&lazy).await })
    };
    started.notified().await;
    assert_eq!(LazyCell::get(&lazy), None);

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    resume.notify_one();
    assert_eq!(*LazyCell::force(&lazy).await, 42);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
/// Ensure unrelated task unwinding does not poison a pending attempt.
async fn unrelated_unwind_does_not_poison_cell() {
    let started = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let lazy = Arc::new(LazyCell::<u32, _>::new({
        let started = started.clone();
        let resume = resume.clone();
        async move || {
            started.notify_one();
            resume.notified().await;
            42
        }
    }));

    let task = {
        let lazy = lazy.clone();
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = LazyCell::force(&lazy) => {}
                _ = async {
                    started.notified().await;
                    panic!("unrelated panic");
                } => {}
            }
        })
    };
    assert!(task.await.unwrap_err().is_panic());

    resume.notify_one();
    assert_eq!(LazyCell::force(&lazy).await, &42);
}

#[tokio::test]
/// Ensure that dropping the cell drops a suspended initialization future.
async fn dropping_cell_drops_suspended_attempt() {
    let held = Arc::new(());
    let weak = Arc::downgrade(&held);
    let started = Arc::new(Notify::new());
    let lazy = Arc::new(LazyCell::<u32, _>::new({
        let started = started.clone();
        async move || {
            started.notify_one();
            std::future::pending::<()>().await;
            drop(held);
            42
        }
    }));

    let task = {
        let lazy = lazy.clone();
        tokio::spawn(async move { *LazyCell::force(&lazy).await })
    };
    started.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    drop(Arc::try_unwrap(lazy).ok().unwrap());
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
/// Validates fallible initialization. If the initializer returns an error,
/// the value is not stored and future calls may retry it.
async fn queued_callers_retry_after_errors() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let lazy = Arc::new(LazyCell::<u32, _>::new({
        let attempts = attempts.clone();
        move || {
            let attempts = attempts.clone();
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                if attempt < 2 { Err("retry") } else { Ok(42) }
            }
        }
    }));

    let mut tasks = Vec::new();
    for _ in 0..3 {
        let lazy = lazy.clone();
        tasks.push(tokio::spawn(async move {
            LazyCell::try_force(&lazy).await.copied()
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
    assert_eq!(LazyCell::try_force(&lazy).await, Ok(&42));
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
/// Ensure that fallible attempts can receive different call-time arguments.
async fn call_time_arguments_are_used_for_retries() {
    let lazy = LazyCell::<u32, _>::new(
        async |(value, succeed): (u32, bool)| {
            if succeed { Ok(value) } else { Err("retry") }
        },
    );

    assert_eq!(
        LazyCell::try_force_with(&lazy, (1, false)).await,
        Err("retry")
    );
    assert_eq!(LazyCell::try_force_with(&lazy, (42, true)).await, Ok(&42));
}

#[tokio::test]
/// Ensure that a resumed attempt keeps its original call-time arguments.
async fn cancellation_preserves_active_arguments() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let lazy = Arc::new(LazyCell::<u32, _>::new(
        async |(value, attempts, started, resume): (
            u32,
            Arc<AtomicUsize>,
            Arc<Notify>,
            Arc<Notify>,
        )| {
            attempts.fetch_add(1, Ordering::SeqCst);
            started.notify_one();
            resume.notified().await;
            Ok::<_, &'static str>(value)
        },
    ));

    let task = {
        let lazy = lazy.clone();
        let attempts = attempts.clone();
        let started = started.clone();
        let resume = resume.clone();
        tokio::spawn(async move {
            LazyCell::try_force_with(&lazy, (41, attempts, started, resume))
                .await
                .copied()
        })
    };
    started.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    resume.notify_one();
    assert_eq!(
        LazyCell::try_force_with(
            &lazy,
            (99, attempts.clone(), started.clone(), resume.clone())
        )
        .await,
        Ok(&41)
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

struct NotifyOnDrop(Option<Arc<Notify>>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        if let Some(notify) = self.0.take() {
            notify.notify_one();
        }
    }
}

#[tokio::test]
/// Ensure unused caller arguments are dropped before an attempt resumes.
async fn unused_arguments_are_dropped_before_resume() {
    let started = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    let lazy = Arc::new(LazyCell::<u32, _>::new({
        let started = started.clone();
        let resume = resume.clone();
        move |(value, _): (u32, NotifyOnDrop)| {
            let started = started.clone();
            let resume = resume.clone();
            async move {
                started.notify_one();
                resume.notified().await;
                Ok::<_, ()>(value)
            }
        }
    }));

    let first = {
        let lazy = lazy.clone();
        tokio::spawn(async move {
            LazyCell::try_force_with(&lazy, (41, NotifyOnDrop(None)))
                .await
                .copied()
        })
    };
    started.notified().await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());

    let second = {
        let lazy = lazy.clone();
        let dropped = dropped.clone();
        tokio::spawn(async move {
            LazyCell::try_force_with(&lazy, (99, NotifyOnDrop(Some(dropped))))
                .await
                .copied()
        })
    };

    dropped.notified().await;
    resume.notify_one();
    assert_eq!(second.await.unwrap(), Ok(41));
}

#[tokio::test]
/// Ensure that a panic in the initializer permanently poisons the cell, preventing future calls
/// from succeeding.
async fn panic_permanently_poisons_cell() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let lazy = Arc::new(LazyCell::<u32, _>::new({
        let attempts = attempts.clone();
        async move || {
            attempts.fetch_add(1, Ordering::SeqCst);
            panic!("initializer panic");
        }
    }));

    let first = {
        let lazy = lazy.clone();
        tokio::spawn(async move {
            let _ = LazyCell::force(&lazy).await;
        })
    };
    assert!(first.await.unwrap_err().is_panic());
    assert_eq!(LazyCell::get(&lazy), None);

    let second = {
        let lazy = lazy.clone();
        tokio::spawn(async move {
            let _ = LazyCell::force(&lazy).await;
        })
    };
    assert!(second.await.unwrap_err().is_panic());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    let lazy = Arc::try_unwrap(lazy).ok().unwrap();
    let result = std::panic::catch_unwind(|| LazyCell::into_inner(lazy));
    assert!(result.is_err());
}

#[tokio::test]
/// Ensure that `force_mut` and `try_force_mut` can be used to mutate the value
/// after it has been initialized.
async fn mutable_force_updates_value() {
    let mut lazy = LazyCell::<u32, _>::new(async || 41);
    *LazyCell::force_mut(&mut lazy).await += 1;
    assert_eq!(LazyCell::get(&lazy), Some(&42));

    let mut fallible = LazyCell::<u32, _>::new(async || Ok::<_, ()>(41));
    *LazyCell::try_force_mut(&mut fallible).await.unwrap() += 1;
    assert_eq!(LazyCell::get(&fallible), Some(&42));

    let mut with_args = LazyCell::<u32, _>::new(async |value| Ok::<_, ()>(value));
    *LazyCell::try_force_mut_with(&mut with_args, 41)
        .await
        .unwrap() += 1;
    assert_eq!(LazyCell::get(&with_args), Some(&42));

    let mut infallible_with_args = LazyCell::<u32, _>::new(async |value| value);
    *LazyCell::force_mut_with(&mut infallible_with_args, 41).await += 1;
    assert_eq!(LazyCell::get(&infallible_with_args), Some(&42));
}

#[tokio::test]
/// Ensure that `into_inner` returns the value if it has been initialized, or
/// returns the initializer if it has not been initialized.
async fn into_inner_returns_value_or_initializer() {
    let lazy = LazyCell::<u32, _>::new(async || 42);
    let initializer = LazyCell::into_inner(lazy).unwrap_err();
    assert_eq!(initializer().await, 42);

    let lazy = LazyCell::<u32, _>::new(async || 42);
    LazyCell::force(&lazy).await;
    assert!(matches!(LazyCell::into_inner(lazy), Ok(42)));
}

#[tokio::test]
/// Validates that `Debug` and `Default` trait implementations work as
/// expected.
async fn default_from_and_debug_match_lazy_cell() {
    let lazy = LazyCell::<u32>::default();
    assert_eq!(format!("{lazy:?}"), "LazyCell(<uninit>)");
    assert_eq!(LazyCell::force(&lazy).await, &0);
    assert_eq!(format!("{lazy:?}"), "LazyCell(0)");

    let lazy: LazyCell<u32> = LazyCell::from(42);
    assert_eq!(LazyCell::get(&lazy), Some(&42));
}

#[tokio::test]
/// Ensure that the initializer does not need to be `Sync` in order for the `LazyCell` to be
/// `Sync`. This is important for cases where the initializer captures non-`Sync` state, such as a
/// `Cell`. This is guaranteed by the internal mutex.
async fn initializer_need_not_be_sync() {
    fn assert_sync<T: Sync>(_: &T) {}

    let count = Cell::new(0);
    let lazy = LazyCell::<u32, _>::new(async move || {
        count.set(count.get() + 1);
        count.get()
    });

    assert_sync(&lazy);
    assert_eq!(LazyCell::force(&lazy).await, &1);
}

fn static_initializer() -> LazyCellFuture<u32> {
    Box::pin(async { 42 })
}

static STATIC_LAZY: LazyCell<u32> = LazyCell::new(static_initializer);

#[tokio::test]
async fn default_initializer_type_supports_statics() {
    assert_eq!(LazyCell::force(&STATIC_LAZY).await, &42);
}

fn fallible_static_initializer() -> LazyCellFuture<Result<u32, &'static str>> {
    Box::pin(async { Ok(42) })
}

type FallibleStaticInitializer = fn() -> LazyCellFuture<Result<u32, &'static str>>;

static FALLIBLE_STATIC_LAZY: LazyCell<u32, FallibleStaticInitializer> =
    LazyCell::new(fallible_static_initializer);

#[tokio::test]
async fn one_type_supports_fallible_statics() {
    assert_eq!(LazyCell::try_force(&FALLIBLE_STATIC_LAZY).await, Ok(&42));
}
