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

use std::cell::Cell;
use std::future::ready;
use std::hint::black_box;
use std::pin::pin;

use asyncband::once::LazyCell;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::primitive;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::poll_ready;
use benchmarks::support::wait_until_open;

const WAITER_COUNTS: &[usize] = &[1, 4, 16];

fn force_lazy(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let cell = LazyCell::new(|| ready(black_box(1usize)));
        let value = poll_ready(LazyCell::force(&cell), &mut context);
        black_box(*value)
    });
}

fn force_lazy_from_future(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let cell = LazyCell::from_future(ready(black_box(1usize)));
        let value = poll_ready(LazyCell::force(&cell), &mut context);
        black_box(*value)
    });
}

fn forced_lazy(bencher: Bencher) {
    let mut context = bench_context();
    let cell = LazyCell::from_value(1usize);

    bencher.bench_local(|| {
        let value = poll_ready(LazyCell::force(black_box(&cell)), &mut context);
        black_box(*value)
    });
}

fn force_lazy_waiter_batch(bencher: Bencher, waiter_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let gate = Cell::new(false);
        let cell = pin!(LazyCell::from_future(async {
            wait_until_open(&gate).await;
            black_box(1usize)
        }));
        let mut leader = Box::pin(LazyCell::force_pin(cell.as_ref()));
        poll_pending(leader.as_mut(), &mut context);

        let mut waiters = (0..waiter_count)
            .map(|_| Box::pin(LazyCell::force_pin(cell.as_ref())))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }

        gate.set(true);
        let value = poll_pinned_ready(leader.as_mut(), &mut context);
        black_box(*value);
        drop(leader);
        for mut waiter in waiters {
            let value = poll_pinned_ready(waiter.as_mut(), &mut context);
            black_box(*value);
        }
    });
}

pub fn register(runner: &mut Runner) {
    primitive!(runner, force_lazy, 7_000_000);
    primitive!(runner, force_lazy_from_future, 7_000_000);
    primitive!(runner, forced_lazy, 30_000_000);
    for &arg in WAITER_COUNTS {
        primitive!(
            runner,
            force_lazy_waiter_batch(arg),
            (4_000_000 / arg).max(1)
        );
    }
}
