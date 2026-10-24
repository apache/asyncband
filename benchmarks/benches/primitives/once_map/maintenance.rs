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
use benchmarks::support::bench_context;
use benchmarks::support::poll_ready;

use crate::once_map::support::NONEMPTY_ENTRY_COUNTS;
use crate::once_map::support::READY_ENTRY_COUNTS;
use crate::once_map::support::ready_map;

// Restore the removed middle entry so every round sees the same populated working set.
fn remove_reinitialize_cycle(bencher: Bencher, ready_entries: usize) {
    let key = ready_entries / 2;
    let map = ready_map(ready_entries);
    let mut context = bench_context();
    bencher.bench_local(|| {
        black_box(map.remove(black_box(&key)));
        black_box(poll_ready(map.compute(key, || async { key }), &mut context));
    });
}

fn remove_miss(bencher: Bencher, ready_entries: usize) {
    let map = ready_map(ready_entries);
    bencher.bench_local(|| black_box(map.remove(black_box(&usize::MAX))));
}

pub fn register(runner: &mut Runner) {
    for &arg in NONEMPTY_ENTRY_COUNTS {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::remove_reinitialize_cycle")
            ),
            3_000_000,
            1,
            |bencher| remove_reinitialize_cycle(bencher, arg),
        );
    }
    for &arg in READY_ENTRY_COUNTS {
        runner.case(
            format!("{}/{arg}", concat!(module_path!(), "::remove_miss")),
            48_000_000,
            1,
            |bencher| remove_miss(bencher, arg),
        );
    }
}
