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
use std::future::Future;
use std::future::poll_fn;
use std::pin::Pin;
use std::pin::pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

struct BenchTask;

// This deliberately uses `Wake` rather than `Waker::noop()` so waiter registration and wake-up
// exercise the reference-counting work performed by runtime task wakers.
#[allow(clippy::manual_noop_waker)]
impl Wake for BenchTask {
    fn wake(self: Arc<Self>) {}
}

static BENCH_WAKER: LazyLock<Waker> = LazyLock::new(|| Waker::from(Arc::new(BenchTask)));

pub(super) fn bench_context() -> Context<'static> {
    Context::from_waker(&BENCH_WAKER)
}

pub(super) fn poll_ready<F: Future>(future: F, context: &mut Context<'_>) -> F::Output {
    let mut future = pin!(future);
    poll_pinned_ready(future.as_mut(), context)
}

pub(super) fn poll_pinned_ready<F: Future>(
    mut future: Pin<&mut F>,
    context: &mut Context<'_>,
) -> F::Output {
    match future.as_mut().poll(context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("benchmark future should be ready"),
    }
}

pub(super) fn poll_pending<F: Future>(mut future: Pin<&mut F>, context: &mut Context<'_>) {
    assert!(future.as_mut().poll(context).is_pending());
}

// Move the input into the benchmark output so Divan drops it outside the timed section.
#[inline]
pub(super) fn defer_input_drop<I, O>(input: I, output: O) -> (I, O) {
    (input, output)
}

pub(super) async fn wait_until_open(gate: &Cell<bool>) {
    poll_fn(|_| {
        if gate.get() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await
}
