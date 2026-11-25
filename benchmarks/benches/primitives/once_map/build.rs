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

use std::hint::black_box;

use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::primitive;

use crate::once_map::support::BenchMap;
use crate::once_map::support::HOT_ENTRY_COUNTS;

fn collect_distinct_keys(bencher: Bencher, entries: usize) {
    let items = (0..entries).map(|key| (key, key)).collect::<Vec<_>>();
    bencher.bench_local(|| black_box(items.iter().copied().collect::<BenchMap>()));
}

// Every key appears twice, so half of the items replace an entry inserted earlier in the iterator.
fn collect_duplicate_keys(bencher: Bencher, entries: usize) {
    let items = (0..entries)
        .map(|item| (item % (entries / 2), item))
        .collect::<Vec<_>>();
    bencher.bench_local(|| black_box(items.iter().copied().collect::<BenchMap>()));
}

pub fn register(runner: &mut Runner) {
    for &arg in HOT_ENTRY_COUNTS {
        primitive!(runner, collect_distinct_keys(arg), (6_000_000 / arg).max(1));
    }
    for &arg in HOT_ENTRY_COUNTS {
        primitive!(
            runner,
            collect_duplicate_keys(arg),
            (6_000_000 / arg).max(1)
        );
    }
}
