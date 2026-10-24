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

use crate::once_map::support::HOT_ENTRY_COUNTS;
use crate::once_map::support::READY_ENTRY_COUNTS;
use crate::once_map::support::THREAD_COUNTS;
use crate::once_map::support::distributed_absent_key;
use crate::once_map::support::distributed_ready_key;
use crate::once_map::support::ready_map;

fn get_hit_same_key(bencher: Bencher) {
    let map = ready_map(1);
    bencher.bench(|| black_box(map.get(black_box(&0))));
}

fn get_hit_distributed(bencher: Bencher, ready_entries: usize) {
    let map = ready_map(ready_entries);
    bencher.bench(|| {
        let key = distributed_ready_key(ready_entries);
        black_box(map.get(black_box(&key)))
    });
}

// Negative get is still a read-only steady-state path: it traverses the ready index but never
// creates pending state. Varying absent hashes avoids measuring one unusually cache-hot miss.
fn get_miss_distributed(bencher: Bencher, ready_entries: usize) {
    let map = ready_map(ready_entries);
    bencher.bench(|| {
        let key = distributed_absent_key(ready_entries);
        black_box(map.get(black_box(&key)))
    });
}

fn compute_hit_same_key(bencher: Bencher) {
    let map = ready_map(1);
    bencher.bench(|| {
        let mut context = bench_context();
        black_box(poll_ready(
            map.compute(black_box(0), || async { unreachable!() }),
            &mut context,
        ))
    });
}

fn compute_hit_distributed(bencher: Bencher, ready_entries: usize) {
    let map = ready_map(ready_entries);
    bencher.bench(|| {
        let key = distributed_ready_key(ready_entries);
        let mut context = bench_context();
        black_box(poll_ready(
            map.compute(black_box(key), || async { unreachable!() }),
            &mut context,
        ))
    });
}

pub fn register(runner: &mut Runner) {
    for &threads in THREAD_COUNTS {
        runner.case(
            format!(
                "{}/threads={threads}",
                concat!(module_path!(), "::get_hit_same_key")
            ),
            16_000_000,
            threads,
            get_hit_same_key,
        );
    }
    for &arg in HOT_ENTRY_COUNTS {
        for &threads in THREAD_COUNTS {
            runner.case(
                format!(
                    "{}/{arg}/threads={threads}",
                    concat!(module_path!(), "::get_hit_distributed")
                ),
                8_000_000,
                threads,
                |bencher| get_hit_distributed(bencher, arg),
            );
        }
    }
    for &arg in READY_ENTRY_COUNTS {
        for &threads in THREAD_COUNTS {
            runner.case(
                format!(
                    "{}/{arg}/threads={threads}",
                    concat!(module_path!(), "::get_miss_distributed")
                ),
                16_000_000,
                threads,
                |bencher| get_miss_distributed(bencher, arg),
            );
        }
    }
    for &threads in THREAD_COUNTS {
        runner.case(
            format!(
                "{}/threads={threads}",
                concat!(module_path!(), "::compute_hit_same_key")
            ),
            10_000_000,
            threads,
            compute_hit_same_key,
        );
    }
    for &arg in HOT_ENTRY_COUNTS {
        for &threads in THREAD_COUNTS {
            runner.case(
                format!(
                    "{}/{arg}/threads={threads}",
                    concat!(module_path!(), "::compute_hit_distributed")
                ),
                4_000_000,
                threads,
                |bencher| compute_hit_distributed(bencher, arg),
            );
        }
    }
}
