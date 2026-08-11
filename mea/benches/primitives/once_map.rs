// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use divan::Bencher;
use divan::black_box;
use mea::once::OnceMap;

use super::support::bench_context;
use super::support::defer_input_drop;
use super::support::poll_pending;
use super::support::poll_pinned_ready;
use super::support::poll_ready;
use super::support::wait_until_open;

const CACHED_ENTRY_COUNTS: &[usize] = &[0, 64, 1024];
const WAITER_COUNTS: &[usize] = &[1, 8, 32];

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
use std::cell::Cell;
