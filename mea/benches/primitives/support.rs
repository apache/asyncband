// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;
use std::pin::Pin;
use std::pin::pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

pub(super) fn noop_context() -> Context<'static> {
    Context::from_waker(Waker::noop())
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
