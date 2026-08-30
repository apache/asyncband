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

use divan::Bencher;
use divan::black_box;

use super::support::CONTENDED_SAMPLE_SIZE;
use super::support::HOT_ENTRY_COUNTS;
use super::support::READY_ENTRY_COUNTS;
use super::support::THREAD_COUNTS;
use super::support::distributed_absent_key;
use super::support::distributed_ready_key;
use super::support::ready_map;
use crate::support::bench_context;
use crate::support::spin_poll_ready;

#[divan::bench(threads = THREAD_COUNTS, sample_size = CONTENDED_SAMPLE_SIZE)]
fn get_hit_same_key(bencher: Bencher) {
    let map = ready_map(1);
    bencher.bench(|| black_box(map.get(black_box(&0))));
}

#[divan::bench(
    threads = THREAD_COUNTS,
    args = HOT_ENTRY_COUNTS,
    sample_size = CONTENDED_SAMPLE_SIZE
)]
fn get_hit_distributed(bencher: Bencher, ready_entries: usize) {
    let map = ready_map(ready_entries);
    bencher
        .with_inputs(|| distributed_ready_key(ready_entries))
        .bench_values(|key| black_box(map.get(black_box(&key))));
}

// Negative get is still a read-only steady-state path: it traverses the ready index but never
// creates pending state. Varying absent hashes avoids measuring one unusually cache-hot miss.
#[divan::bench(
    threads = THREAD_COUNTS,
    args = READY_ENTRY_COUNTS,
    sample_size = CONTENDED_SAMPLE_SIZE
)]
fn get_miss_distributed(bencher: Bencher, ready_entries: usize) {
    let map = ready_map(ready_entries);
    bencher
        .with_inputs(|| distributed_absent_key(ready_entries))
        .bench_values(|key| black_box(map.get(black_box(&key))));
}

#[divan::bench(threads = THREAD_COUNTS, sample_size = CONTENDED_SAMPLE_SIZE)]
fn compute_hit_same_key(bencher: Bencher) {
    let map = ready_map(1);
    bencher.bench(|| {
        let mut context = bench_context();
        black_box(spin_poll_ready(
            map.compute(black_box(0), || async { unreachable!() }),
            &mut context,
        ))
    });
}

#[divan::bench(
    threads = THREAD_COUNTS,
    args = HOT_ENTRY_COUNTS,
    sample_size = CONTENDED_SAMPLE_SIZE
)]
fn compute_hit_distributed(bencher: Bencher, ready_entries: usize) {
    let map = ready_map(ready_entries);
    bencher
        .with_inputs(|| distributed_ready_key(ready_entries))
        .bench_values(|key| {
            let mut context = bench_context();
            black_box(spin_poll_ready(
                map.compute(black_box(key), || async { unreachable!() }),
                &mut context,
            ))
        });
}
