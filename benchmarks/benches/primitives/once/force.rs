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

use std::future::ready;
use std::hint::black_box;
use std::pin::pin;

use asyncband::once::LazyCell;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::primitive;
use benchmarks::support::bench_context;
use benchmarks::support::poll_ready;

fn initialize_cell(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let cell = pin!(LazyCell::new(|| ready(black_box(1usize))));
        let value = poll_ready(LazyCell::force_pin(cell.as_ref()), &mut context);
        black_box(*value)
    });
}

fn initialize_cell_from_future(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let cell = pin!(LazyCell::from_future(ready(black_box(1usize))));
        let value = poll_ready(LazyCell::force_pin(cell.as_ref()), &mut context);
        black_box(*value)
    });
}

fn initialized_cell(bencher: Bencher) {
    let mut context = bench_context();
    let cell = LazyCell::from_value(1usize);

    bencher.bench_local(|| {
        let value = poll_ready(LazyCell::force(black_box(&cell)), &mut context);
        black_box(*value)
    });
}

pub fn register(runner: &mut Runner) {
    primitive!(runner, initialize_cell, 7_000_000);
    primitive!(runner, initialize_cell_from_future, 7_000_000);
    primitive!(runner, initialized_cell, 85_000_000);
}
