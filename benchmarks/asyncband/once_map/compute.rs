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

use super::support::BenchMap;
use super::support::CONTENDED_ENTRY_COUNTS;
use super::support::CONTENDED_THREAD_SLOTS;
use super::support::THREAD_COUNTS;
use super::support::preloaded_map;
use crate::support::bench_context;
use crate::support::defer_input_drop;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;
use crate::support::poll_ready;
use crate::support::spin_poll_ready;
use crate::support::thread_slot_ticket;
use crate::support::wait_until_open;

const CACHED_ENTRY_COUNTS: &[usize] = &[0, 64, 1024];
const COMPUTATION_COUNTS: &[usize] = &[2, 9, 33];
const MISS_KEYSPACE_SIZE: usize = 1 << 16;

enum MixedInput {
    Hit(usize),
    Miss(usize),
}

#[divan::bench]
fn construct_default(bencher: Bencher) {
    bencher.bench_local(BenchMap::default);
}

fn miss_key(cached_entries: usize, slot: usize, ticket: usize) -> usize {
    // Workers start at different phases but traverse the same keyspace.
    let thread_offset =
        slot % CONTENDED_THREAD_SLOTS * (MISS_KEYSPACE_SIZE / CONTENDED_THREAD_SLOTS);
    cached_entries + (thread_offset + ticket) % MISS_KEYSPACE_SIZE
}

#[divan::bench]
fn compute_vacant(bencher: Bencher) {
    let mut context = bench_context();
    bencher
        .with_inputs(BenchMap::default)
        .bench_local_values(|map| {
            let result = black_box(poll_ready(
                map.compute(black_box(0), || async { black_box(1) }),
                &mut context,
            ));
            defer_input_drop(map, result)
        });
}

#[divan::bench]
fn compute_occupied(bencher: Bencher) {
    let mut context = bench_context();
    bencher
        .with_inputs(|| [(0, 1)].into_iter().collect::<BenchMap>())
        .bench_local_values(|map| {
            let result = black_box(poll_ready(
                map.compute(black_box(0), || async { black_box(2) }),
                &mut context,
            ));
            defer_input_drop(map, result)
        });
}

#[divan::bench(args = CACHED_ENTRY_COUNTS)]
fn try_compute_error(bencher: Bencher, cached_entries: usize) {
    let mut context = bench_context();
    bencher
        .with_inputs(|| {
            (0..cached_entries)
                .map(|key| (key, key))
                .collect::<BenchMap>()
        })
        .bench_local_values(|map| {
            let result = black_box(poll_ready(
                map.try_compute(black_box(usize::MAX), || async { Err::<usize, ()>(()) }),
                &mut context,
            ));
            defer_input_drop(map, result)
        });
}

#[divan::bench(args = COMPUTATION_COUNTS)]
fn coalesced_compute_batch(bencher: Bencher, computation_count: usize) {
    let mut context = bench_context();

    bencher
        .with_inputs(BenchMap::default)
        .bench_local_values(|map| {
            let gate = Cell::new(false);
            let mut computations = (0..computation_count)
                .map(|_| {
                    Box::pin(map.compute(0, || async {
                        wait_until_open(&gate).await;
                        black_box(1usize)
                    }))
                })
                .collect::<Vec<_>>();
            for computation in &mut computations {
                poll_pending(computation.as_mut(), &mut context);
            }

            gate.set(true);
            for mut computation in computations {
                black_box(poll_pinned_ready(computation.as_mut(), &mut context));
            }

            defer_input_drop(map, ())
        });
}

#[divan::bench(args = COMPUTATION_COUNTS)]
fn independent_compute_batch(bencher: Bencher, computation_count: usize) {
    let mut context = bench_context();

    bencher
        .with_inputs(BenchMap::default)
        .bench_local_values(|map| {
            let gate = Cell::new(false);
            let mut computations = (0..computation_count)
                .map(|key| {
                    Box::pin(map.compute(key, || async {
                        wait_until_open(&gate).await;
                        black_box(1usize)
                    }))
                })
                .collect::<Vec<_>>();
            for computation in &mut computations {
                poll_pending(computation.as_mut(), &mut context);
            }

            gate.set(true);
            for mut computation in computations {
                black_box(poll_pinned_ready(computation.as_mut(), &mut context));
            }

            defer_input_drop(map, ())
        });
}

#[divan::bench(threads = THREAD_COUNTS)]
fn contended_compute_hit_same_key(bencher: Bencher) {
    let map = [(0, 1)].into_iter().collect::<BenchMap>();

    bencher.bench(|| {
        let mut context = bench_context();
        black_box(spin_poll_ready(
            map.compute(black_box(0), || async { unreachable!() }),
            &mut context,
        ))
    });
}

#[divan::bench(threads = THREAD_COUNTS, args = CONTENDED_ENTRY_COUNTS)]
fn contended_compute_hit_disjoint(bencher: Bencher, cached_entries: usize) {
    let map = preloaded_map(cached_entries);

    bencher
        .with_inputs(|| {
            let (slot, ticket) = thread_slot_ticket();
            (slot + ticket * CONTENDED_THREAD_SLOTS) % cached_entries
        })
        .bench_values(|key| {
            let mut context = bench_context();
            black_box(spin_poll_ready(
                map.compute(black_box(key), || async { unreachable!() }),
                &mut context,
            ))
        });
}

#[divan::bench(threads = THREAD_COUNTS, args = CONTENDED_ENTRY_COUNTS)]
fn contended_compute_miss_churn(bencher: Bencher, cached_entries: usize) {
    let map = preloaded_map(cached_entries);

    bencher
        .with_inputs(|| {
            let (slot, ticket) = thread_slot_ticket();
            miss_key(cached_entries, slot, ticket)
        })
        .bench_values(|key| {
            let mut context = bench_context();
            let value = spin_poll_ready(
                map.compute(black_box(key), || async move { key }),
                &mut context,
            );
            map.discard(&key);
            black_box(value)
        });
}

#[divan::bench(threads = THREAD_COUNTS, args = CONTENDED_ENTRY_COUNTS)]
fn contended_compute_mixed(bencher: Bencher, cached_entries: usize) {
    let map = preloaded_map(cached_entries);

    bencher
        .with_inputs(|| {
            let (slot, ticket) = thread_slot_ticket();
            if (slot + ticket) % 2 == 0 {
                MixedInput::Hit((slot + ticket / 2 * CONTENDED_THREAD_SLOTS) % cached_entries)
            } else {
                MixedInput::Miss(miss_key(cached_entries, slot, ticket / 2))
            }
        })
        .bench_values(|input| {
            let mut context = bench_context();
            match input {
                MixedInput::Hit(key) => black_box(spin_poll_ready(
                    map.compute(black_box(key), || async { unreachable!() }),
                    &mut context,
                )),
                MixedInput::Miss(key) => {
                    let value = spin_poll_ready(
                        map.compute(black_box(key), || async move { key }),
                        &mut context,
                    );
                    map.discard(&key);
                    black_box(value)
                }
            }
        });
}
