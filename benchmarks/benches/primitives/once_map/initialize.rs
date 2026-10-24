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

use crate::once_map::support::BATCH_SIZES;
use crate::once_map::support::BenchMap;
use crate::once_map::support::READY_ENTRY_COUNTS;
use crate::once_map::support::ready_map;

const ABSENT_KEY: usize = usize::MAX;

// Reuse the ready working set and remove the new entry after each successful initialization.
// Rebuilding the whole map on every round would hide the cost of the absent-key path.
fn initialize_remove_cycle(bencher: Bencher, ready_entries: usize) {
    let map = ready_map(ready_entries);
    let mut context = bench_context();
    bencher.bench_local(|| {
        black_box(poll_ready(
            map.compute(black_box(ABSENT_KEY), || async { black_box(1) }),
            &mut context,
        ));
        black_box(map.remove(&ABSENT_KEY));
    });
}

// Deterministically polls every caller while the leader is suspended. This measures actual
// same-key coalescing rather than relying on OS threads to overlap by chance.
fn initialize_same_key(bencher: Bencher, caller_count: usize) {
    let mut context = bench_context();
    bencher.bench_local(|| {
        let map = BenchMap::default();
        let gate = Cell::new(false);
        let mut calls = (0..caller_count)
            .map(|_| {
                Box::pin(map.compute(0, || async {
                    wait_until_open(&gate).await;
                    black_box(1usize)
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

// Uses the same scheduling scaffold as initialize_same_key, but every caller initializes a
// different key. Their difference isolates coalescing from independent cold-key bookkeeping.
fn initialize_distinct_keys(bencher: Bencher, caller_count: usize) {
    let mut context = bench_context();
    bencher.bench_local(|| {
        let map = BenchMap::default();
        let gate = Cell::new(false);
        let mut calls = (0..caller_count)
            .map(|key| {
                let gate = &gate;
                Box::pin(map.compute(key, move || async move {
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

// Failed initialization leaves the key absent, allowing retries on the same map.
fn initialize_error(bencher: Bencher, ready_entries: usize) {
    let map = ready_map(ready_entries);
    let mut context = bench_context();
    bencher.bench_local(|| {
        black_box(poll_ready(
            map.try_compute(black_box(ABSENT_KEY), || async { Err::<usize, ()>(()) }),
            &mut context,
        ))
    });
}

pub fn register(runner: &mut Runner) {
    for &arg in READY_ENTRY_COUNTS {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::initialize_remove_cycle")
            ),
            3_000_000,
            1,
            |bencher| initialize_remove_cycle(bencher, arg),
        );
    }
    for &arg in BATCH_SIZES {
        runner.case(
            format!("{}/{arg}", concat!(module_path!(), "::initialize_same_key")),
            (3_000_000 / arg).max(1),
            1,
            |bencher| initialize_same_key(bencher, arg),
        );
    }
    for &arg in BATCH_SIZES {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::initialize_distinct_keys")
            ),
            (2_000_000 / arg).max(1),
            1,
            |bencher| initialize_distinct_keys(bencher, arg),
        );
    }
    for &arg in READY_ENTRY_COUNTS {
        runner.case(
            format!("{}/{arg}", concat!(module_path!(), "::initialize_error")),
            3_000_000,
            1,
            |bencher| initialize_error(bencher, arg),
        );
    }
}
