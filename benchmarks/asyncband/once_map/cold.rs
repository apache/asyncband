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

use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::support::BATCH_SAMPLE_SIZE;
use super::support::BATCH_SIZES;
use super::support::BenchMap;
use super::support::FAST_SAMPLE_SIZE;
use super::support::READY_ENTRY_COUNTS;
use super::support::ready_map;
use crate::support::bench_context;
use crate::support::defer_input_drop;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;
use crate::support::poll_ready;
use crate::support::wait_until_open;

const ABSENT_KEY: usize = usize::MAX;

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn construct_default(bencher: Bencher) {
    bencher.bench_local(|| black_box(BenchMap::default()));
}

// Measures the complete absent-key path: lookup, pending entry creation, publication, and return.
// Input construction and map destruction stay outside the timed section.
#[divan::bench(args = READY_ENTRY_COUNTS, sample_size = BATCH_SAMPLE_SIZE)]
fn initialize_success(bencher: Bencher, ready_entries: usize) {
    let mut context = bench_context();
    bencher
        .with_inputs(|| ready_map(ready_entries))
        .bench_local_values(|map| {
            let result = black_box(poll_ready(
                map.compute(black_box(ABSENT_KEY), || async { black_box(1) }),
                &mut context,
            ));
            defer_input_drop(map, result)
        });
}

// A failed initializer leaves no value behind, so the same long-lived map can exercise a stable
// retryable miss without mixing map construction or deletion into the measurement.
#[divan::bench(args = READY_ENTRY_COUNTS, sample_size = FAST_SAMPLE_SIZE)]
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

// Deterministically polls every caller while the leader is suspended. This measures actual
// same-key coalescing rather than relying on OS threads to overlap by chance.
#[divan::bench(args = BATCH_SIZES, sample_size = BATCH_SAMPLE_SIZE)]
fn initialize_same_key(bencher: Bencher, caller_count: usize) {
    let mut context = bench_context();
    bencher
        .with_inputs(BenchMap::default)
        .counter(ItemsCount::new(caller_count))
        .bench_local_values(|map| {
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

            defer_input_drop(map, ())
        });
}

// Uses the same scheduling scaffold as initialize_same_key, but every caller initializes a
// different key. Their difference isolates coalescing from independent cold-key bookkeeping.
#[divan::bench(args = BATCH_SIZES, sample_size = BATCH_SAMPLE_SIZE)]
fn initialize_distinct_keys(bencher: Bencher, caller_count: usize) {
    let mut context = bench_context();
    bencher
        .with_inputs(BenchMap::default)
        .counter(ItemsCount::new(caller_count))
        .bench_local_values(|map| {
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

            defer_input_drop(map, ())
        });
}
