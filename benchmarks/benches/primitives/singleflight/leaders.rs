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
use std::hint::black_box;

use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::primitive;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::poll_ready;
use benchmarks::support::thread_slot;
use benchmarks::support::wait_until_open;

use crate::singleflight::support::BATCH_SIZES;
use crate::singleflight::support::BenchGroup;
use crate::singleflight::support::BenchOwnedGroup;
use crate::singleflight::support::THREAD_COUNTS;

// Reuse the group so table capacity survives completed calls.
fn leader_success(bencher: Bencher) {
    let group = BenchGroup::default();
    let mut context = bench_context();
    bencher.bench_local(|| {
        black_box(poll_ready(
            group.work(black_box(0), || async { black_box(1) }),
            &mut context,
        ))
    });
}

fn leader_success_owned_1k(bencher: Bencher) {
    let group = BenchOwnedGroup::default();
    let mut context = bench_context();
    bencher.bench_local(|| {
        black_box(poll_ready(
            group.work(black_box(0), || async { black_box(vec![1_u8; 1024]) }),
            &mut context,
        ))
    });
}

// Suspend all leaders so their distinct-key entries coexist.
fn distinct_keys(bencher: Bencher, caller_count: usize) {
    let group = BenchGroup::default();
    let mut context = bench_context();
    bencher.bench_local(|| {
        let gate = Cell::new(false);
        let mut calls = (0..caller_count)
            .map(|key| {
                let gate = &gate;
                Box::pin(group.work(key, move || async move {
                    wait_until_open(gate).await;
                    black_box(key)
                }))
            })
            .collect::<Vec<_>>();
        for call in &mut calls {
            poll_pending(call.as_mut(), &mut context);
        }

        gate.set(true);
        for mut call in calls {
            black_box(poll_pinned_ready(call.as_mut(), &mut context));
        }
    });
}

// Each worker reuses its own key. Group removes completed calls, so every round starts a leader.
fn distributed_leaders(bencher: Bencher) {
    let group = BenchGroup::default();
    bencher.bench(|| {
        let key = thread_slot();
        let mut context = bench_context();
        black_box(poll_ready(
            group.work(black_box(key), || async move { black_box(key) }),
            &mut context,
        ))
    });
}

fn leader_error(bencher: Bencher) {
    let group = BenchGroup::default();
    let mut context = bench_context();
    bencher.bench_local(|| {
        black_box(poll_ready(
            group.try_work(black_box(0), || async { Err::<usize, ()>(()) }),
            &mut context,
        ))
    });
}

pub fn register(runner: &mut Runner) {
    primitive!(runner, leader_success, 3_000_000);
    primitive!(runner, leader_success_owned_1k, 3_000_000);
    for &arg in BATCH_SIZES {
        primitive!(runner, distinct_keys(arg), (2_000_000 / arg).max(1));
    }
    for &threads in THREAD_COUNTS {
        primitive!(runner, distributed_leaders, 1_000_000, threads = threads);
    }
    primitive!(runner, leader_error, 3_000_000);
}
