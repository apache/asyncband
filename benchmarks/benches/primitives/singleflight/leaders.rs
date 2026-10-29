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
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::poll_ready;
use benchmarks::support::wait_until_open;

use crate::singleflight::support::BATCH_SIZES;
use crate::singleflight::support::BenchGroup;
use crate::singleflight::support::THREAD_COUNTS;
use crate::singleflight::support::unique_thread_key;

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

// Unique keys isolate shared-table contention from same-key coalescing.
fn distributed_leaders(bencher: Bencher) {
    let group = BenchGroup::default();
    bencher.bench(|| {
        let key = unique_thread_key();
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
    runner.case(
        concat!(module_path!(), "::leader_success"),
        3_000_000,
        1,
        leader_success,
    );
    for &arg in BATCH_SIZES {
        runner.case(
            format!("{}/{arg}", concat!(module_path!(), "::distinct_keys")),
            (2_000_000 / arg).max(1),
            1,
            |bencher| distinct_keys(bencher, arg),
        );
    }
    for &threads in THREAD_COUNTS {
        runner.case(
            format!(
                "{}/threads={threads}",
                concat!(module_path!(), "::distributed_leaders")
            ),
            1_000_000,
            threads,
            distributed_leaders,
        );
    }
    runner.case(
        concat!(module_path!(), "::leader_error"),
        3_000_000,
        1,
        leader_error,
    );
}
