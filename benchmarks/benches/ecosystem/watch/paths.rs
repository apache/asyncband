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

// Both adapters return an owned `usize`. The Tokio adapter's `recv` combines `changed` and
// `borrow_and_update`.

use std::hint::black_box;

use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::implementations;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::poll_ready;

use crate::watch::adapters::Asyncband;
use crate::watch::adapters::Tokio;
use crate::watch::adapters::Watch;

fn get_current<C: Watch>(bencher: Bencher) {
    let (sender, mut receivers) = C::channel(1);
    let receiver = receivers.pop().unwrap();
    bencher.bench_local(|| black_box(C::get(&receiver)));
    black_box(sender);
}

fn publish_then_receive<C: Watch>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(1);
    let mut receiver = receivers.pop().unwrap();
    bencher.bench_local(|| {
        C::send(&sender, black_box(1));
        black_box(poll_ready(C::recv(&mut receiver), &mut context))
    });
}

fn register_publish_observe_fanout<C: Watch>(bencher: Bencher, receiver_count: usize) {
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

pub fn register(runner: &mut Runner) {
    runner.compare(
        "watch/snapshot",
        75_000_000,
        implementations!([Asyncband, Tokio], |C, b| get_current::<C>(b)),
    );
    runner.compare(
        "watch/publish-then-receive",
        18_000_000,
        implementations!([Asyncband, Tokio], |C, b| publish_then_receive::<C>(b)),
    );
    runner.compare(
        "watch/notify-4-waiters",
        1_700_000,
        implementations!(
            [Asyncband, Tokio],
            |C, b| register_publish_observe_fanout::<C>(b, 4)
        ),
    );
}
