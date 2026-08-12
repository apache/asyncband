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

use asyncband::once::OnceMap;
use divan::Bencher;
use divan::black_box;

use super::support::defer_input_drop;
use super::support::noop_context;
use super::support::poll_ready;

const CACHED_ENTRY_COUNTS: &[usize] = &[0, 64, 1024];

#[divan::bench]
fn compute_vacant(bencher: Bencher) {
    let mut context = noop_context();
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
    let mut context = noop_context();
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
    let mut context = noop_context();
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
