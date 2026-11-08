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
use std::future::pending;
use std::future::poll_fn;
use std::future::ready;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Poll;
use std::thread;

use asyncband::blocking::FutureExt;
use asyncband::once::LazyCell;
use tests_integration::WakeCounter;
use tests_integration::assert_completes_without_deadlock;
use tests_integration::expect_ready;
use tests_integration::poll_once;
use tests_integration::poll_with;
use tokio::sync::Notify;

struct DropPanic;

impl Drop for DropPanic {
    fn drop(&mut self) {
        panic!("future drop panic");
    }
}

#[tokio::test]
async fn initializer_starts_when_force_is_polled() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let lazy = LazyCell::new({
        let attempts = attempts.clone();
        move || {
            attempts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { 42 })
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
    let lazy = Arc::new(LazyCell::new({
        let attempts = attempts.clone();
        move || {
            Box::pin(async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                42
            })
        }
    }));

    let mut tasks = vec![];
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
    let lazy = Arc::new(LazyCell::new({
        let attempts = attempts.clone();
        let started = started.clone();
        let resume = resume.clone();
        move || {
            Box::pin(async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                started.notify_one();
                resume.notified().await;
                42
            })
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
    let lazy = pin!(lazy);

    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(LazyCell::force_pin(lazy.as_ref()).await, &42);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn from_future_cancellation_preserves_the_same_future() {
    let executions = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let lazy = Arc::pin(LazyCell::from_future({
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
        tokio::spawn(async move { *LazyCell::force_pin(lazy.as_ref()).await })
    };
    started.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    resume.notify_one();
    assert_eq!(*LazyCell::force_pin(lazy.as_ref()).await, 42);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unrelated_unwind_does_not_poison_pending_attempt() {
    let started = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let lazy = Arc::new(LazyCell::new({
        let started = started.clone();
        let resume = resume.clone();
        move || {
            Box::pin(async move {
                started.notify_one();
                resume.notified().await;
                42
            })
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
    let lazy = Arc::new(LazyCell::new({
        let started = started.clone();
        move || {
            Box::pin(async move {
                started.notify_one();
                pending::<()>().await;
                drop(held);
                42
            })
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
    let lazy = LazyCell::new({
        let attempts = attempts.clone();
        move || {
            Box::pin(async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<u32, _>("cached")
            })
        }
    });

    assert_eq!(LazyCell::force(&lazy).await, &Err("cached"));
    assert_eq!(LazyCell::force(&lazy).await, &Err("cached"));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn initializer_poll_panic_permanently_poisons_cell() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let lazy = Arc::new(LazyCell::new({
        let attempts = attempts.clone();
        move || {
            Box::pin(async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                panic!("initializer panic");
            })
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
async fn completed_future_drop_panic_permanently_poisons_cell() {
    let lazy = Arc::new(LazyCell::new(|| {
        let guard = DropPanic;
        poll_fn(move |_| {
            let _ = &guard;
            Poll::Ready(42)
        })
    }));

    let first = {
        let lazy = lazy.clone();
        tokio::spawn(async move {
            let _ = LazyCell::force(&lazy).await;
        })
    };
    assert!(first.await.unwrap_err().is_panic());
    assert_eq!(LazyCell::get(&lazy), None);

    let second = tokio::spawn(async move {
        let _ = LazyCell::force(&lazy).await;
    });
    assert!(second.await.unwrap_err().is_panic());
}

#[tokio::test]
async fn initializer_creation_panic_permanently_poisons_cell() {
    let lazy = Arc::new(LazyCell::new(|| -> Ready<u32> {
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
async fn force_mut_resumes_a_started_attempt() {
    let started = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let lazy = Arc::new(LazyCell::new({
        let started = started.clone();
        let resume = resume.clone();
        move || {
            Box::pin(async move {
                started.notify_one();
                resume.notified().await;
                42
            })
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
    *LazyCell::force_mut(&mut lazy).await += 1;
    assert_eq!(LazyCell::get(&lazy), Some(&43));
}

#[tokio::test]
async fn value_constructors_support_local_and_const_values() {
    let lazy = LazyCell::<u32, Ready<u32>>::default();
    assert_eq!(LazyCell::force(&lazy).await, &0);

    let local = LazyCell::<Rc<u32>, Ready<Rc<u32>>>::default();
    assert_eq!(**LazyCell::force(&local).await, 0);

    let lazy = const { LazyCell::from_value(42) };
    assert_eq!(LazyCell::get(&lazy), Some(&42));
}

#[tokio::test]
async fn initializer_need_not_be_sync() {
    fn assert_sync<T: Sync>(_: &T) {}

    let count = Cell::new(0);
    let lazy = LazyCell::new(move || {
        Box::pin(async move {
            count.set(count.get() + 1);
            count.get()
        })
    });

    assert_sync(&lazy);
    assert_eq!(LazyCell::force(&lazy).await, &1);
}

fn static_initializer() -> Ready<u32> {
    ready(42)
}

static STATIC_LAZY: LazyCell<u32, Ready<u32>> = LazyCell::new(static_initializer);

#[tokio::test]
async fn function_pointer_initializer_supports_statics() {
    assert_eq!(LazyCell::force(&STATIC_LAZY).await, &42);
}

#[tokio::test]
async fn inline_future_supports_local_non_send_state() {
    let local = Rc::new(41);
    let lazy = LazyCell::new({
        let local = Rc::clone(&local);
        async move || {
            tokio::task::yield_now().await;
            *local + 1
        }
    });
    let mut lazy = pin!(lazy);

    assert_eq!(*LazyCell::force_pin(lazy.as_ref()).await, 42);
    assert_eq!(LazyCell::get(&lazy), Some(&42));

    *LazyCell::force_pin_mut(lazy.as_mut()).await += 1;
    assert_eq!(LazyCell::get(&lazy), Some(&43));
}

#[tokio::test]
async fn cancellation_preserves_inline_initialization_future() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let lazy = LazyCell::new({
        let attempts = Arc::clone(&attempts);
        let started = Arc::clone(&started);
        let resume = Arc::clone(&resume);
        async move || {
            attempts.fetch_add(1, Ordering::SeqCst);
            started.notify_one();
            resume.notified().await;
            42
        }
    });
    let lazy = pin!(lazy);

    tokio::select! {
        biased;
        _ = LazyCell::force_pin(lazy.as_ref()) => panic!("initializer unexpectedly completed"),
        _ = started.notified() => {}
    }

    resume.notify_one();
    assert_eq!(*LazyCell::force_pin(lazy.as_ref()).await, 42);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn queued_caller_resumes_the_same_future_after_the_driver_is_cancelled() {
    let starts = Cell::new(0);
    let (finish, ready) = tokio::sync::oneshot::channel();
    let lazy = pin!(LazyCell::new({
        let starts = &starts;
        async move || {
            starts.set(starts.get() + 1);
            ready.await.unwrap();
            42
        }
    }));
    let mut driver = Box::pin(LazyCell::force_pin(lazy.as_ref()));
    let mut queued = pin!(LazyCell::force_pin(lazy.as_ref()));
    let (waker, wakes) = WakeCounter::new();

    assert!(poll_once(driver.as_mut()).is_pending());
    assert!(poll_with(queued.as_mut(), &waker).is_pending());
    drop(driver);
    assert_eq!(wakes.take(), 1);

    assert!(poll_with(queued.as_mut(), &waker).is_pending());
    finish.send(()).unwrap();
    assert_eq!(wakes.take(), 1);
    assert_eq!(expect_ready(poll_with(queued.as_mut(), &waker)), &42);
    assert_eq!(starts.get(), 1);
}

#[test]
fn queued_caller_observes_poison_after_the_driver_panics() {
    let (finish, ready) = tokio::sync::oneshot::channel::<()>();
    let lazy = pin!(LazyCell::new(async move || -> u8 {
        ready.await.unwrap();
        panic!("initializer panic");
    }));
    let mut driver = pin!(LazyCell::force_pin(lazy.as_ref()));
    let mut queued = pin!(LazyCell::force_pin(lazy.as_ref()));
    let (waker, wakes) = WakeCounter::new();

    assert!(poll_once(driver.as_mut()).is_pending());
    assert!(poll_with(queued.as_mut(), &waker).is_pending());
    finish.send(()).unwrap();
    let panic = catch_unwind(AssertUnwindSafe(|| poll_once(driver.as_mut()))).unwrap_err();
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"initializer panic"));
    assert_eq!(wakes.count(), 1);

    let panic = catch_unwind(AssertUnwindSafe(|| poll_with(queued.as_mut(), &waker))).unwrap_err();
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"LazyCell instance has previously been poisoned")
    );
    assert_eq!(LazyCell::get(&lazy), None);
}

#[test]
fn cancelling_a_partial_waiter_does_not_start_another_driver() {
    let (finish, ready) = tokio::sync::oneshot::channel();
    let lazy = pin!(LazyCell::from_future(async move {
        ready.await.unwrap();
        2
    }));
    let mut driver = Box::pin(LazyCell::force_pin(lazy.as_ref()));
    let mut cancelled = Box::pin(LazyCell::force_pin(lazy.as_ref()));
    let mut next = pin!(LazyCell::force_pin(lazy.as_ref()));
    let (waker, wakes) = WakeCounter::new();

    assert!(poll_once(driver.as_mut()).is_pending());
    // This waiter holds the spare permit, but still cannot drive the attempt.
    assert!(poll_once(cancelled.as_mut()).is_pending());
    assert!(poll_with(next.as_mut(), &waker).is_pending());
    drop(cancelled);
    assert_eq!(wakes.count(), 0);
    assert!(poll_with(next.as_mut(), &waker).is_pending());

    drop(driver);
    assert_eq!(wakes.take(), 1);
    finish.send(()).unwrap();
    assert_eq!(expect_ready(poll_with(next.as_mut(), &waker)), &2);
}

#[test]
fn cancelling_a_granted_waiter_passes_the_attempt_to_the_next() {
    let (finish, ready) = tokio::sync::oneshot::channel();
    let lazy = pin!(LazyCell::from_future(async move {
        ready.await.unwrap();
        3
    }));
    let mut driver = Box::pin(LazyCell::force_pin(lazy.as_ref()));
    let mut cancelled = Box::pin(LazyCell::force_pin(lazy.as_ref()));
    let mut next = pin!(LazyCell::force_pin(lazy.as_ref()));
    let (cancelled_waker, cancelled_wakes) = WakeCounter::new();
    let (next_waker, next_wakes) = WakeCounter::new();

    assert!(poll_once(driver.as_mut()).is_pending());
    assert!(poll_with(cancelled.as_mut(), &cancelled_waker).is_pending());
    assert!(poll_with(next.as_mut(), &next_waker).is_pending());
    drop(driver);
    assert_eq!(cancelled_wakes.count(), 1);
    assert_eq!(next_wakes.count(), 0);

    drop(cancelled);
    assert_eq!(next_wakes.take(), 1);
    finish.send(()).unwrap();
    assert_eq!(expect_ready(poll_with(next.as_mut(), &next_waker)), &3);
}

#[test]
fn queued_caller_observes_completion_from_another_thread() {
    assert_completes_without_deadlock(|| {
        let (finish, ready) = tokio::sync::oneshot::channel();
        let lazy = pin!(LazyCell::new(async move || {
            ready.await.unwrap();
            [42; 32]
        }));
        let lazy = lazy.as_ref();
        let mut driver = pin!(LazyCell::force_pin(lazy));
        assert!(poll_once(driver.as_mut()).is_pending());

        let (registered, registration) = mpsc::channel();
        thread::scope(|scope| {
            scope.spawn(move || {
                let mut waiter = pin!(LazyCell::force_pin(lazy));
                assert!(poll_once(waiter.as_mut()).is_pending());
                registered.send(()).unwrap();
                assert_eq!(waiter.block_on(), &[42; 32]);
            });
            registration.recv().unwrap();
            finish.send(()).unwrap();
            assert_eq!(expect_ready(poll_once(driver.as_mut())), &[42; 32]);
        });
    });
}
