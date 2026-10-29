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
use std::pin::pin;

use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::wait_until_open;

use crate::singleflight::support::BATCH_SIZES;
use crate::singleflight::support::BenchGroup;

// Measures the complete successful fan-in: one leader is suspended, every duplicate joins it, and
// all callers receive the cloned result after the leader completes.
fn complete_coalesced(bencher: Bencher, caller_count: usize) {
    let group = BenchGroup::default();
    let mut context = bench_context();
    bencher.bench_local(|| {
        let gate = Cell::new(false);
        let mut leader = pin!(group.work(0, || async {
            wait_until_open(&gate).await;
            black_box(1usize)
        }));
        poll_pending(leader.as_mut(), &mut context);

        let mut duplicates = (1..caller_count)
            .map(|_| Box::pin(group.work(0, || async { unreachable!() })))
            .collect::<Vec<_>>();
        for duplicate in &mut duplicates {
            poll_pending(duplicate.as_mut(), &mut context);
        }

        gate.set(true);
        black_box(poll_pinned_ready(leader.as_mut(), &mut context));
        for mut duplicate in duplicates {
            black_box(poll_pinned_ready(duplicate.as_mut(), &mut context));
        }
    });
}

// The first leader fails; each waiter then retries with an immediately ready initializer.
// Those retries complete sequentially, so they do not coalesce with each other.
fn sequential_retries_after_leader_error(bencher: Bencher, caller_count: usize) {
    let group = BenchGroup::default();
    let mut context = bench_context();
    bencher.bench_local(|| {
        let gate = Cell::new(false);
        let mut leader = pin!(group.try_work(0, || async {
            wait_until_open(&gate).await;
            Err::<usize, ()>(())
        }));
        poll_pending(leader.as_mut(), &mut context);

        let mut retries = (1..caller_count)
            .map(|_| Box::pin(group.try_work(0, || async { Ok::<usize, ()>(1) })))
            .collect::<Vec<_>>();
        for retry in &mut retries {
            poll_pending(retry.as_mut(), &mut context);
        }

        gate.set(true);
        let _ = black_box(poll_pinned_ready(leader.as_mut(), &mut context));
        for mut retry in retries {
            let _ = black_box(poll_pinned_ready(retry.as_mut(), &mut context));
        }
    });
}

pub fn register(runner: &mut Runner) {
    for &arg in BATCH_SIZES {
        runner.case(
            format!("{}/{arg}", concat!(module_path!(), "::complete_coalesced")),
            (3_000_000 / arg).max(1),
            1,
            |bencher| complete_coalesced(bencher, arg),
        );
    }
    for &arg in BATCH_SIZES {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::sequential_retries_after_leader_error")
            ),
            (3_000_000 / arg).max(1),
            1,
            |bencher| sequential_retries_after_leader_error(bencher, arg),
        );
    }
}
