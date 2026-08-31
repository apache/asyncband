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

use std::future::Future;
use std::panic;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;

use asyncband::condvar::Condvar;
use asyncband::mutex::Mutex;
use tests_integration::poll_once;
use tests_integration::test_runtime;
use tokio::task::JoinHandle;

fn expect_ready<T>(poll: Poll<T>) -> T {
    match poll {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("future should be ready"),
    }
}

fn poll_with<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("wake panic");
    }
}

struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct NotifyOnDrop(Arc<Condvar>);

impl Wake for NotifyOnDrop {
    fn wake(self: Arc<Self>) {}
}

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[test]
fn predicate_preserves_state_when_notification_precedes_wait() {
    test_runtime().block_on(async {
        let mutex = Mutex::new(false);
        let condvar = Condvar::new();

        {
            let mut ready = mutex.lock().await;
            *ready = true;
            condvar.notify_one();
        }

        // The notification itself was not buffered. The predicate is the durable state, so a task
        // that arrives later observes it and does not wait.
        let ready = condvar
            .wait_while(mutex.lock().await, |ready| !*ready)
            .await;
        assert!(*ready);
    });
}

#[test]
fn notify_one_is_not_buffered() {
    test_runtime().block_on(async {
        let mutex = Mutex::new(());
        let condvar = Condvar::new();

        for _ in 0..3 {
            condvar.notify_one();
        }

        let mut wait = Box::pin(condvar.wait(mutex.lock().await));
        assert!(poll_once(wait.as_mut()).is_pending());
    });
}

#[test]
fn notify_all_is_not_buffered() {
    test_runtime().block_on(async {
        let mutex = Mutex::new(());
        let condvar = Condvar::new();

        condvar.notify_all();

        let mut wait = Box::pin(condvar.wait(mutex.lock().await));
        assert!(poll_once(wait.as_mut()).is_pending());
    });
}

#[test]
fn notify_one_wakes_one_waiter_at_a_time() {
    test_runtime().block_on(async {
        let mutex = Mutex::new(());
        let condvar = Condvar::new();

        let mut first = Box::pin(condvar.wait(mutex.lock().await));
        assert!(poll_once(first.as_mut()).is_pending());

        let mut second = Box::pin(condvar.wait(mutex.lock().await));
        assert!(poll_once(second.as_mut()).is_pending());

        condvar.notify_one();
        drop(expect_ready(poll_once(first.as_mut())));
        assert!(poll_once(second.as_mut()).is_pending());

        condvar.notify_one();
        drop(expect_ready(poll_once(second.as_mut())));
    });
}

#[test]
fn notify_all_wakes_current_waiters_using_a_predicate_loop() {
    test_runtime().block_on(async {
        const WAITERS: usize = 10;

        #[derive(Default)]
        struct State {
            ready: bool,
            waiting: usize,
        }

        let pair = Arc::new((Mutex::new(State::default()), Condvar::new()));
        let mut tasks: Vec<JoinHandle<()>> = vec![];

        for _ in 0..WAITERS {
            let pair = pair.clone();
            tasks.push(tokio::spawn(async move {
                let (mutex, condvar) = &*pair;
                let mut state = mutex.lock().await;
                state.waiting += 1;
                let state = condvar.wait_while(state, |state| !state.ready).await;
                assert!(state.ready);
            }));
        }

        let (mutex, condvar) = &*pair;
        loop {
            let state = mutex.lock().await;
            if state.waiting == WAITERS {
                break;
            }
            drop(state);
            tokio::task::yield_now().await;
        }

        // Seeing every task's `waiting` update while holding this mutex also means every task has
        // registered with the condition variable before releasing the same mutex.
        {
            let mut state = mutex.lock().await;
            state.ready = true;
            condvar.notify_all();
        }

        for task in tasks {
            task.await.unwrap();
        }
    });
}

#[test]
fn notify_all_attempts_every_waker_after_one_panics() {
    let mutex = Mutex::new(());
    let condvar = Condvar::new();
    let panicking = Waker::from(Arc::new(PanicWake));
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let tracked = Waker::from(counter.clone());

    let mut first = Box::pin(condvar.wait(mutex.try_lock().unwrap()));
    assert!(poll_with(first.as_mut(), &panicking).is_pending());
    let mut second = Box::pin(condvar.wait(mutex.try_lock().unwrap()));
    assert!(poll_with(second.as_mut(), &tracked).is_pending());

    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| condvar.notify_all()));
    assert!(result.is_err());
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);

    drop(expect_ready(poll_once(first.as_mut())));
    drop(expect_ready(poll_once(second.as_mut())));
}

#[test]
fn cancelling_waiter_drops_its_waker_outside_the_waiter_lock() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let mutex = Mutex::new(());
        let condvar = Arc::new(Condvar::new());
        let waker = Waker::from(Arc::new(NotifyOnDrop(condvar.clone())));
        let mut wait = Box::pin(condvar.wait(mutex.try_lock().unwrap()));

        assert!(poll_with(wait.as_mut(), &waker).is_pending());
        drop(waker);
        drop(wait);
        finished_tx.send(()).unwrap();
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("dropping a cancelled waiter deadlocked against the condvar waiter lock");
    worker.join().unwrap();
}

#[test]
fn cancelling_notified_waiter_passes_notify_one_to_next_waiter() {
    test_runtime().block_on(async {
        let mutex = Mutex::new(());
        let condvar = Condvar::new();

        let mut first = Box::pin(condvar.wait(mutex.lock().await));
        assert!(poll_once(first.as_mut()).is_pending());

        let mut second = Box::pin(condvar.wait(mutex.lock().await));
        assert!(poll_once(second.as_mut()).is_pending());

        let held = mutex.lock().await;
        condvar.notify_one();

        // The first waiter consumes the notification, then blocks while reacquiring the mutex.
        assert!(poll_once(first.as_mut()).is_pending());
        drop(first);
        drop(held);

        // Cancelling the selected waiter passes the notification to an existing waiter.
        drop(expect_ready(poll_once(second.as_mut())));
    });
}

#[test]
fn cancelling_only_notified_waiter_does_not_buffer_notify_one() {
    test_runtime().block_on(async {
        let mutex = Mutex::new(());
        let condvar = Condvar::new();

        let mut first = Box::pin(condvar.wait(mutex.lock().await));
        assert!(poll_once(first.as_mut()).is_pending());

        let held = mutex.lock().await;
        condvar.notify_one();
        assert!(poll_once(first.as_mut()).is_pending());
        drop(first);
        drop(held);

        let mut late = Box::pin(condvar.wait(mutex.lock().await));
        assert!(poll_once(late.as_mut()).is_pending());
    });
}

#[test]
fn cancelled_notify_all_waiter_does_not_wake_late_waiter() {
    test_runtime().block_on(async {
        let mutex = Mutex::new(());
        let condvar = Condvar::new();

        let mut current = Box::pin(condvar.wait(mutex.lock().await));
        assert!(poll_once(current.as_mut()).is_pending());
        condvar.notify_all();

        let mut late = Box::pin(condvar.wait(mutex.lock().await));
        assert!(poll_once(late.as_mut()).is_pending());
        drop(current);

        assert!(poll_once(late.as_mut()).is_pending());
        condvar.notify_one();
        drop(expect_ready(poll_once(late.as_mut())));
    });
}

#[test]
fn wait_owned_reacquires_the_mutex() {
    test_runtime().block_on(async {
        let mutex = Arc::new(Mutex::new(0));
        let condvar = Condvar::new();

        let mut wait = Box::pin(condvar.wait_owned(mutex.clone().lock_owned().await));
        assert!(poll_once(wait.as_mut()).is_pending());
        condvar.notify_one();

        let mut guard = expect_ready(poll_once(wait.as_mut()));
        *guard = 1;
        drop(guard);
        assert_eq!(*mutex.lock().await, 1);
    });
}
