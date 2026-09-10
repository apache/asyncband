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

// Asyncband clones the current value while Tokio returns a read-lock-backed Ref. These benchmarks
// immediately read a usize, so both sides finish with an owned value and release any lock before
// continuing. The recv adapter combines Tokio's changed and borrow_and_update operations to match
// Asyncband's owned receive contract.

use std::pin::pin;

use divan::Bencher;
use divan::black_box;

use super::adapters::Asyncband;
use super::adapters::Tokio;
use super::adapters::Watch;
use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;
use crate::support::poll_ready;

const RECEIVER_COUNTS: &[usize] = &[2, 4, 8, 32];

#[divan::bench(types = [Asyncband, Tokio])]
fn get_current<C: Watch>(bencher: Bencher) {
    let (sender, mut receivers) = C::channel(1);
    let receiver = receivers.pop().unwrap();
    bencher.bench_local(|| black_box(C::get(&receiver)));
    black_box(sender);
}

#[divan::bench(types = [Asyncband, Tokio])]
fn send_and_get<C: Watch>(bencher: Bencher) {
    let (sender, mut receivers) = C::channel(1);
    let receiver = receivers.pop().unwrap();
    bencher.bench_local(|| {
        C::send(&sender, black_box(1));
        black_box(C::get(&receiver))
    });
}

#[divan::bench(types = [Asyncband, Tokio])]
fn ready_recv<C: Watch>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(1);
    let mut receiver = receivers.pop().unwrap();
    bencher.bench_local(|| {
        C::send(&sender, black_box(1));
        black_box(poll_ready(C::recv(&mut receiver), &mut context))
    });
}

#[divan::bench(types = [Asyncband, Tokio])]
fn ready_changed<C: Watch>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(1);
    let mut receiver = receivers.pop().unwrap();
    bencher.bench_local(|| {
        C::send(&sender, black_box(1));
        poll_ready(C::changed(&mut receiver), &mut context);
    });
}

#[divan::bench(types = [Asyncband, Tokio])]
fn notify_pending<C: Watch>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(1);
    let mut receiver = receivers.pop().unwrap();
    bencher.bench_local(|| {
        let mut changed = pin!(C::changed(&mut receiver));
        poll_pending(changed.as_mut(), &mut context);
        C::send(&sender, black_box(1));
        poll_pinned_ready(changed.as_mut(), &mut context);
    });
}

#[divan::bench(types = [Asyncband, Tokio], args = RECEIVER_COUNTS)]
fn notify_pending_fanout<C: Watch>(bencher: Bencher, receiver_count: usize) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(receiver_count);

    bencher.bench_local(|| {
        let mut changed = receivers
            .iter_mut()
            .map(|receiver| Box::pin(C::changed(receiver)))
            .collect::<Vec<_>>();
        for future in &mut changed {
            poll_pending(future.as_mut(), &mut context);
        }

        C::send(&sender, black_box(1));
        for mut future in changed {
            poll_pinned_ready(future.as_mut(), &mut context);
        }
    });
}
