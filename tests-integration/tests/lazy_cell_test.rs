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
use std::future::Ready;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use asyncband::once::LazyCell;
use tokio::sync::Notify;

#[tokio::test]
async fn initializer_starts_when_force_is_polled() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let lazy = LazyCell::<u32, _>::new({
        let attempts = attempts.clone();
        move || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { 42 }
        }
    });

    let force = LazyCell::force(&lazy);
    assert_eq!(attempts.load(Ordering::SeqCst), 0);
    drop(force);
    assert_eq!(attempts.load(Ordering::SeqCst), 0);

    assert_eq!(LazyCell::force(&lazy).await, &42);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_force_runs_initializer_once() {
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
async fn cancellation_preserves_initialization_future() {
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
async fn from_future_does_not_poll_until_forced() {
    let polls = Arc::new(AtomicUsize::new(0));
    let lazy = LazyCell::from_future({
        let polls = polls.clone();
        async move {
            polls.fetch_add(1, Ordering::SeqCst);
            42
        }
    });

    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(LazyCell::force(&lazy).await, &42);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn from_future_cancellation_preserves_the_same_future() {
    let executions = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let lazy = Arc::new(LazyCell::from_future({
        let executions = executions.clone();
        let started = started.clone();
        let resume = resume.clone();
        async move {
            executions.fetch_add(1, Ordering::SeqCst);
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
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    resume.notify_one();
    assert_eq!(*LazyCell::force(&lazy).await, 42);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unrelated_unwind_does_not_poison_pending_attempt() {
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
async fn result_is_cached_as_the_value() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let lazy = LazyCell::<Result<u32, &'static str>, _>::new({
        let attempts = attempts.clone();
        async move || {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err("cached")
        }
    });

    assert_eq!(LazyCell::force(&lazy).await, &Err("cached"));
    assert_eq!(LazyCell::force(&lazy).await, &Err("cached"));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn initializer_poll_panic_permanently_poisons_cell() {
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

    let mut lazy = Arc::try_unwrap(lazy).ok().unwrap();
    let third = tokio::spawn(async move {
        let _ = LazyCell::force_mut(&mut lazy).await;
    });
    assert!(third.await.unwrap_err().is_panic());
}

#[tokio::test]
async fn initializer_creation_panic_permanently_poisons_cell() {
    let lazy = Arc::new(LazyCell::<u32, _>::new(|| -> std::future::Ready<u32> {
        panic!("initializer creation panic")
    }));

    let first = {
        let lazy = lazy.clone();
        tokio::spawn(async move {
            let _ = LazyCell::force(&lazy).await;
        })
    };
    assert!(first.await.unwrap_err().is_panic());

    let second = tokio::spawn(async move {
        let _ = LazyCell::force(&lazy).await;
    });
    assert!(second.await.unwrap_err().is_panic());
}

#[tokio::test]
async fn force_mut_updates_value() {
    let mut lazy = LazyCell::<u32, _>::new(async || 41);
    *LazyCell::force_mut(&mut lazy).await += 1;
    assert_eq!(LazyCell::get(&lazy), Some(&42));
}

#[tokio::test]
async fn force_mut_resumes_a_started_attempt() {
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
            let _ = LazyCell::force(&lazy).await;
        })
    };
    started.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    let mut lazy = Arc::try_unwrap(lazy).ok().unwrap();
    resume.notify_one();
    assert_eq!(LazyCell::force_mut(&mut lazy).await, &mut 42);
}

#[tokio::test]
async fn default_and_value_constructors_match_lazy_cell() {
    let lazy = LazyCell::<u32>::default();
    assert_eq!(format!("{lazy:?}"), "LazyCell(<uninit>)");
    assert_eq!(LazyCell::force(&lazy).await, &0);
    assert_eq!(format!("{lazy:?}"), "LazyCell(0)");

    let local = LazyCell::<Rc<u32>>::default();
    assert_eq!(**LazyCell::force(&local).await, 0);

    let lazy = const { LazyCell::from_value(42) };
    assert_eq!(LazyCell::get(&lazy), Some(&42));
}

#[tokio::test]
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

fn static_initializer() -> Ready<u32> {
    std::future::ready(42)
}

static STATIC_LAZY: LazyCell<u32, fn() -> Ready<u32>> = LazyCell::new(static_initializer);

#[tokio::test]
async fn function_pointer_initializer_supports_statics() {
    assert_eq!(LazyCell::force(&STATIC_LAZY).await, &42);
}
