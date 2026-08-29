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

// Asyncband returns an owning Arc snapshot, while Tokio returns a read-lock-backed Ref from its
// borrow methods and returns () from changed. These benchmarks immediately read a usize and release
// either representation. The changed adapter also reads Tokio's current value so both sides finish
// with the observed value, but their ownership and lock-lifetime contracts remain intentionally
// different.

use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use divan::Bencher;
use divan::black_box;

use super::adapters::Asyncband;
use super::adapters::Tokio;
use super::adapters::Watch;
use crate::support::bench_context;
use crate::support::poll_ready;

const RECEIVER_COUNTS: &[usize] = &[1, 2, 4, 8, 32];

fn poll_erased_pending(
    mut future: Pin<&mut dyn Future<Output = usize>>,
    context: &mut Context<'_>,
) {
    assert!(future.as_mut().poll(context).is_pending());
}

fn poll_erased_ready(
    mut future: Pin<&mut dyn Future<Output = usize>>,
    context: &mut Context<'_>,
) -> usize {
    match future.as_mut().poll(context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("benchmark future should be ready"),
    }
}

#[divan::bench(types = [Asyncband, Tokio])]
fn borrow_current<C: Watch>(bencher: Bencher) {
    let (sender, mut receivers) = C::channel(1);
    let receiver = receivers.pop().unwrap();
    bencher.bench_local(|| black_box(C::borrow(&receiver)));
    black_box(sender);
}

#[divan::bench(types = [Asyncband, Tokio])]
fn send_and_borrow<C: Watch>(bencher: Bencher) {
    let (sender, mut receivers) = C::channel(1);
    let receiver = receivers.pop().unwrap();
    bencher.bench_local(|| {
        C::send(&sender, black_box(1));
        black_box(C::borrow(&receiver))
    });
}

#[divan::bench(types = [Asyncband, Tokio])]
fn send_and_borrow_and_update<C: Watch>(bencher: Bencher) {
    let (sender, mut receivers) = C::channel(1);
    let mut receiver = receivers.pop().unwrap();
    bencher.bench_local(|| {
        C::send(&sender, black_box(1));
        black_box(C::borrow_and_update(&mut receiver))
    });
}

#[divan::bench(types = [Asyncband, Tokio])]
fn ready_changed<C: Watch>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(1);
    let mut receiver = receivers.pop().unwrap();
    bencher.bench_local(|| {
        C::send(&sender, black_box(1));
        black_box(poll_ready(C::changed(&mut receiver), &mut context))
    });
}

#[divan::bench(types = [Asyncband, Tokio], args = RECEIVER_COUNTS)]
fn notify_pending_fanout<C: Watch>(bencher: Bencher, receiver_count: usize) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(receiver_count);

    bencher.bench_local(|| {
        let mut changed = receivers.iter_mut().map(C::changed).collect::<Vec<_>>();
        for future in &mut changed {
            poll_erased_pending(future.as_mut(), &mut context);
        }

        C::send(&sender, black_box(1));
        for mut future in changed {
            black_box(poll_erased_ready(future.as_mut(), &mut context));
        }
    });
}
