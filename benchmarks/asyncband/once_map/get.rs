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

use super::support::CONTENDED_ENTRY_COUNTS;
use super::support::CONTENDED_THREAD_SLOTS;
use super::support::THREAD_COUNTS;
use super::support::ready_map;
use crate::support::thread_slot_ticket;

#[divan::bench(threads = THREAD_COUNTS)]
fn contended_get_hit_same_key(bencher: Bencher) {
    let map = ready_map(1);

    bencher.bench(|| black_box(map.get(black_box(&0))));
}

#[divan::bench(threads = THREAD_COUNTS, args = CONTENDED_ENTRY_COUNTS)]
fn contended_get_hit_disjoint(bencher: Bencher, cached_entries: usize) {
    let map = ready_map(cached_entries);

    bencher
        .with_inputs(|| {
            let (slot, ticket) = thread_slot_ticket();
            (slot + ticket * CONTENDED_THREAD_SLOTS) % cached_entries
        })
        .bench_values(|key| black_box(map.get(black_box(&key))));
}
