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

use super::support::BATCH_SAMPLE_SIZE;
use super::support::FAST_SAMPLE_SIZE;
use super::support::NONEMPTY_ENTRY_COUNTS;
use super::support::READY_ENTRY_COUNTS;
use super::support::ready_map;
use crate::support::defer_input_drop;

#[divan::bench(args = NONEMPTY_ENTRY_COUNTS, sample_size = BATCH_SAMPLE_SIZE)]
fn discard_hit(bencher: Bencher, ready_entries: usize) {
    let key = ready_entries / 2;
    bencher
        .with_inputs(|| ready_map(ready_entries))
        .bench_local_values(|map| {
            map.discard(black_box(&key));
            defer_input_drop(map, ())
        });
}

#[divan::bench(args = NONEMPTY_ENTRY_COUNTS, sample_size = BATCH_SAMPLE_SIZE)]
fn remove_hit(bencher: Bencher, ready_entries: usize) {
    let key = ready_entries / 2;
    bencher
        .with_inputs(|| ready_map(ready_entries))
        .bench_local_values(|map| {
            let removed = black_box(map.remove(black_box(&key)));
            defer_input_drop(map, removed)
        });
}

#[divan::bench(args = READY_ENTRY_COUNTS, sample_size = FAST_SAMPLE_SIZE)]
fn discard_miss(bencher: Bencher, ready_entries: usize) {
    let map = ready_map(ready_entries);
    bencher.bench_local(|| map.discard(black_box(&usize::MAX)));
}

#[divan::bench(args = READY_ENTRY_COUNTS, sample_size = FAST_SAMPLE_SIZE)]
fn remove_miss(bencher: Bencher, ready_entries: usize) {
    let map = ready_map(ready_entries);
    bencher.bench_local(|| black_box(map.remove(black_box(&usize::MAX))));
}
