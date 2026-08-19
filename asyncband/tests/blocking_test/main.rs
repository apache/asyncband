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
use std::future::pending;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread;
use std::time::Duration;

use asyncband::blocking::BlockingExt as _;
use asyncband::blocking::Timeout;
use asyncband::blocking::block_on;
use asyncband::blocking::block_on_for;

#[test]
fn ready_future_returns_from_function_and_extension_method() {
    assert_eq!(block_on(async { 42 }), 42);
    assert_eq!(async { 42 }.block_on(), 42);
}

#[test]
fn wake_notification_resumes_the_parked_thread() {
    let (completion, future, first_poll) = controlled_future();
    let producer = thread::spawn(move || {
        first_poll.recv().unwrap();
        completion.complete(7);
    });

    assert_eq!(block_on(future), 7);
    producer.join().unwrap();
}

#[test]
fn bounded_wait_returns_ready_output_from_function_and_extension_method() {
    assert_eq!(block_on_for(async { 42 }, Duration::ZERO), Ok(42));
    assert_eq!(async { 42 }.block_on_for(Duration::ZERO), Ok(42));
}

#[test]
fn bounded_wait_resumes_after_a_cross_thread_wake() {
    let (completion, future, first_poll) = controlled_future();
    let producer = thread::spawn(move || {
        first_poll.recv().unwrap();
        completion.complete(7);
    });

    assert_eq!(block_on_for(future, Duration::from_secs(1)), Ok(7));
    producer.join().unwrap();
}

#[test]
fn zero_wait_times_out_a_pending_future() {
    assert_eq!(block_on_for(pending::<()>(), Duration::ZERO), Err(Timeout));
}

#[test]
fn unrepresentable_deadline_does_not_expire_immediately() {
    let (completion, future, first_poll) = controlled_future();
    let producer = thread::spawn(move || {
        first_poll.recv().unwrap();
        completion.complete(7);
    });

    assert_eq!(block_on_for(future, Duration::MAX), Ok(7));
    producer.join().unwrap();
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
    fn complete(self, output: T) {
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
    first_poll: Option<mpsc::Sender<()>>,
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

        if let Some(first_poll) = this.first_poll.take() {
            first_poll.send(()).unwrap();
        }

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
    let (first_poll_tx, first_poll_rx) = mpsc::channel();

    (
        Completion {
            shared: shared.clone(),
        },
        ControlledFuture {
            shared,
            first_poll: Some(first_poll_tx),
        },
        first_poll_rx,
    )
}
