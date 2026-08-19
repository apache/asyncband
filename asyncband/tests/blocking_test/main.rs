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
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread;
use std::time::Duration;

use asyncband::blocking::FutureExt as _;
use asyncband::blocking::block_on;

// Long timeouts in this test target are watchdogs for detecting a stalled test process, not
// assertions about elapsed-time precision.
const TEST_WATCHDOG: Duration = Duration::from_secs(5);

#[test]
fn ready_future_returns_from_function_and_extension_method() {
    assert_eq!(block_on(async { 42 }), 42);
    assert_eq!(async { 42 }.block_on(), 42);
}

#[test]
fn wait_timeout_polls_before_checking_the_deadline() {
    assert_eq!(async { 42 }.wait_timeout(Duration::ZERO), Some(42));
    assert_eq!(
        std::future::pending::<()>().wait_timeout(Duration::ZERO),
        None
    );
}

#[test]
fn pending_future_times_out() {
    assert_eq!(
        std::future::pending::<()>().wait_timeout(Duration::from_millis(1)),
        None
    );
}

#[test]
fn wake_notification_resumes_a_timed_wait() {
    let (completion, future, polls) = controlled_future();
    let producer = thread::spawn(move || {
        polls.recv().unwrap();
        completion.complete(7);
    });

    assert_eq!(future.wait_timeout(TEST_WATCHDOG), Some(7));
    producer.join().unwrap();
}

#[test]
fn nested_waits_use_independent_notifications() {
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let mut outer_polled = false;
        let output = block_on(std::future::poll_fn(|outer_context| {
            if outer_polled {
                Poll::Ready(42)
            } else {
                outer_polled = true;
                outer_context.waker().wake_by_ref();

                let mut inner_polled = false;
                let inner_output = std::future::poll_fn(|inner_context| {
                    if inner_polled {
                        Poll::Ready(7)
                    } else {
                        inner_polled = true;
                        inner_context.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .wait_timeout(TEST_WATCHDOG);
                assert_eq!(inner_output, Some(7));
                Poll::Pending
            }
        }));
        done_tx.send(output).unwrap();
    });

    assert_eq!(
        done_rx
            .recv_timeout(TEST_WATCHDOG)
            .expect("nested waits shared or lost a notification"),
        42
    );
    worker.join().unwrap();
}

#[test]
fn block_on_preserves_the_current_threads_park_token() {
    let (completion, future, polls) = controlled_future();
    let (worker_thread_tx, worker_thread_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        worker_thread_tx.send(thread::current()).unwrap();
        assert_eq!(block_on(future), 7);

        thread::park();
        done_tx.send(()).unwrap();
    });

    let worker_thread = worker_thread_rx.recv().unwrap();
    polls.recv_timeout(TEST_WATCHDOG).unwrap();

    worker_thread.unpark();
    completion.wake();
    polls.recv_timeout(TEST_WATCHDOG).unwrap();
    completion.complete(7);

    done_rx
        .recv_timeout(TEST_WATCHDOG)
        .expect("block_on consumed an unrelated thread park token");
    worker.join().unwrap();
}

struct Shared<T> {
    state: Mutex<State<T>>,
}

struct State<T> {
    output: Option<T>,
    waker: Option<Waker>,
}

struct Completion<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Completion<T> {
    fn wake(&self) {
        let waker = self.shared.state.lock().unwrap().waker.clone();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn complete(&self, output: T) {
        let waker = {
            let mut state = self.shared.state.lock().unwrap();
            state.output = Some(output);
            state.waker.take()
        };

        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

struct ControlledFuture<T> {
    shared: Arc<Shared<T>>,
    polls: mpsc::Sender<()>,
}

impl<T> Future for ControlledFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut state = this.shared.state.lock().unwrap();

        if let Some(output) = state.output.take() {
            return Poll::Ready(output);
        }

        state.waker = Some(cx.waker().clone());
        drop(state);
        this.polls.send(()).unwrap();
        Poll::Pending
    }
}

fn controlled_future<T>() -> (Completion<T>, ControlledFuture<T>, mpsc::Receiver<()>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            output: None,
            waker: None,
        }),
    });
    let (polls_tx, polls_rx) = mpsc::channel();

    (
        Completion {
            shared: shared.clone(),
        },
        ControlledFuture {
            shared,
            polls: polls_tx,
        },
        polls_rx,
    )
}
