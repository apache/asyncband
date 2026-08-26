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

use asyncband::once::OnceMap;
use divan::Bencher;
use divan::black_box;

use super::support::CONTENDED_ENTRY_COUNTS;
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
use crate::support::yield_polls;

const CACHED_ENTRY_COUNTS: &[usize] = &[0, 64, 1024];
const WAITER_COUNTS: &[usize] = &[1, 8, 32];
const MISS_KEY_SPAN: usize = 1 << 16;
const COALESCED_LEADER_POLLS: usize = 32;

#[divan::bench]
fn compute_vacant(bencher: Bencher) {
    let mut context = bench_context();
    bencher
        .with_inputs(OnceMap::<usize, usize>::new)
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
        .with_inputs(|| [(0, 1)].into_iter().collect::<OnceMap<_, _>>())
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
                .collect::<OnceMap<_, _>>()
        })
        .bench_local_values(|map| {
            let result = black_box(poll_ready(
                map.try_compute(black_box(usize::MAX), || async { Err::<usize, ()>(()) }),
                &mut context,
            ));
            defer_input_drop(map, result)
        });
}

#[divan::bench(args = WAITER_COUNTS)]
fn coalesced_compute_batch(bencher: Bencher, waiter_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let map = OnceMap::<usize, usize>::new();
        let gate = Cell::new(false);
        let mut leader = Box::pin(map.compute(0, || async {
            wait_until_open(&gate).await;
            black_box(1usize)
        }));
        poll_pending(leader.as_mut(), &mut context);

        let mut waiters = (0..waiter_count)
            .map(|_| Box::pin(map.compute(0, || async { unreachable!() })))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }

        gate.set(true);
        black_box(poll_pinned_ready(leader.as_mut(), &mut context));
        drop(leader);
        for mut waiter in waiters {
            black_box(poll_pinned_ready(waiter.as_mut(), &mut context));
        }
    });
}

#[divan::bench(threads = THREAD_COUNTS)]
fn contended_compute_hit_same_key(bencher: Bencher) {
    let map = [(0, 1)].into_iter().collect::<OnceMap<_, _>>();

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

    bencher.bench(|| {
        let mut context = bench_context();
        let (slot, ticket) = thread_slot_ticket();
        let key = (slot + ticket) % cached_entries;
        black_box(spin_poll_ready(
            map.compute(black_box(key), || async { unreachable!() }),
            &mut context,
        ))
    });
}

#[divan::bench(threads = THREAD_COUNTS, args = CONTENDED_ENTRY_COUNTS)]
fn contended_compute_miss_churn(bencher: Bencher, cached_entries: usize) {
    let map = preloaded_map(cached_entries);

    bencher.bench(|| {
        let mut context = bench_context();
        let (slot, ticket) = thread_slot_ticket();
        let key = cached_entries + slot * MISS_KEY_SPAN + ticket % MISS_KEY_SPAN;
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

    bencher.bench(|| {
        let mut context = bench_context();
        let (slot, ticket) = thread_slot_ticket();
        if ticket % 2 == 0 {
            let key = (slot + ticket / 2) % cached_entries;
            black_box(spin_poll_ready(
                map.compute(black_box(key), || async { unreachable!() }),
                &mut context,
            ))
        } else {
            let key = cached_entries + slot * MISS_KEY_SPAN + (ticket / 2) % MISS_KEY_SPAN;
            let value = spin_poll_ready(
                map.compute(black_box(key), || async move { key }),
                &mut context,
            );
            map.discard(&key);
            black_box(value)
        }
    });
}

// The leader stays in flight for several polls so calls on other threads coalesce as duplicate
// waiters, and discards the key while in flight so every cycle re-runs the vacant-leader path
// instead of settling into steady-state hits.
#[divan::bench(threads = THREAD_COUNTS)]
fn contended_compute_coalesced(bencher: Bencher) {
    let map = OnceMap::<usize, usize>::new();

    bencher.bench(|| {
        let mut context = bench_context();
        black_box(spin_poll_ready(
            map.compute(black_box(0), || async {
                yield_polls(COALESCED_LEADER_POLLS).await;
                map.discard(&0);
                black_box(1)
            }),
            &mut context,
        ))
    });
}
