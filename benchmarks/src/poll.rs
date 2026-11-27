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
use std::pin::Pin;
use std::pin::pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

struct BenchTask;

// This deliberately uses `Wake` rather than `Waker::noop()` so waiter registration and wake-up
// exercise the reference-counting work performed by runtime task wakers.
#[expect(
    clippy::manual_noop_waker,
    reason = "exercises Arc reference-counting overhead of runtime task wakers"
)]
impl Wake for BenchTask {
    fn wake(self: Arc<Self>) {}
}

static BENCH_WAKER: LazyLock<Waker> = LazyLock::new(|| Waker::from(Arc::new(BenchTask)));

/// Returns a `Context` backed by a reference-counted no-op `Wake` task.
#[inline]
pub fn bench_context() -> Context<'static> {
    Context::from_waker(&BENCH_WAKER)
}

/// Polls `future` once, asserting that it resolves immediately to `Poll::Ready`.
pub fn poll_ready<F: Future>(future: F, context: &mut Context<'_>) -> F::Output {
    let mut future = pin!(future);
    poll_pinned_ready(future.as_mut(), context)
}

/// Polls a pinned `future` once, asserting that it resolves immediately to `Poll::Ready`.
pub fn poll_pinned_ready<F: Future>(
    mut future: Pin<&mut F>,
    context: &mut Context<'_>,
) -> F::Output {
    match future.as_mut().poll(context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("benchmark future should be ready"),
    }
}

/// Polls a pinned `future` once, asserting that it remains `Poll::Pending`.
pub fn poll_pending<F: Future>(mut future: Pin<&mut F>, context: &mut Context<'_>) {
    assert!(future.as_mut().poll(context).is_pending());
}

static NEXT_THREAD_SLOT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static THREAD_SLOT: usize = NEXT_THREAD_SLOT.fetch_add(1, Ordering::Relaxed);
    static THREAD_TICKET: Cell<usize> = const { Cell::new(0) };
}

/// Returns a thread-local wrapping ticket spread across threads without shared atomic contention.
pub fn next_ticket() -> usize {
    let slot = THREAD_SLOT.with(|slot| *slot);
    let ticket = THREAD_TICKET.with(|ticket| {
        let current = ticket.get();
        ticket.set(current.wrapping_add(1));
        current
    });
    slot.wrapping_mul(0x9e37_79b9usize).wrapping_add(ticket)
}
