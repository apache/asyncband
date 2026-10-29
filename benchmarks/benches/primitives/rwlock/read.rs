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

use asyncband::rwlock::RwLock;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;
use benchmarks::support::poll_ready;

fn eight_reads_then_one_write(bencher: Bencher) {
    const READS_PER_WRITE: usize = 8;

    let lock = RwLock::new(0usize);
    let mut context = bench_context();

    bencher.bench_local(|| {
        for _ in 0..READS_PER_WRITE {
            let guard = poll_ready(lock.read(), &mut context);
            black_box(*guard);
        }
        let mut guard = poll_ready(lock.write(), &mut context);
        *guard = black_box(guard.wrapping_add(1));
        black_box(*guard)
    });
}

fn read_reuse(bencher: Bencher) {
    let lock = RwLock::new(0usize);
    let mut context = bench_context();

    bencher.bench_local(|| {
        let guard = poll_ready(lock.read(), &mut context);
        black_box(*guard)
    });
}

pub fn register(runner: &mut Runner) {
    runner.case(
        concat!(module_path!(), "::eight_reads_then_one_write"),
        3_000_000,
        1,
        eight_reads_then_one_write,
    );
    runner.case(
        concat!(module_path!(), "::read_reuse"),
        30_000_000,
        1,
        read_reuse,
    );
}
