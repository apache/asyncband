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
use super::support::BenchMap;
use super::support::HOT_ENTRY_COUNTS;

// Builds a map from distinct keys. The input vector is prepared outside the timed section and the
// map is dropped after it.
#[divan::bench(args = HOT_ENTRY_COUNTS, sample_size = BATCH_SAMPLE_SIZE)]
fn collect_distinct_keys(bencher: Bencher, entries: usize) {
    bencher
        .with_inputs(|| (0..entries).map(|key| (key, key)).collect::<Vec<_>>())
        .bench_local_values(|items| black_box(items.into_iter().collect::<BenchMap>()));
}

// Every key appears twice, so half of the items replace an entry inserted moments earlier.
#[divan::bench(args = HOT_ENTRY_COUNTS, sample_size = BATCH_SAMPLE_SIZE)]
fn collect_duplicate_keys(bencher: Bencher, entries: usize) {
    bencher
        .with_inputs(|| {
            (0..entries)
                .map(|item| (item % (entries / 2), item))
                .collect::<Vec<_>>()
        })
        .bench_local_values(|items| black_box(items.into_iter().collect::<BenchMap>()));
}
