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
use std::pin::pin;

use asyncband::once::Once;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::poll_ready;

const WAITER_COUNTS: &[usize] = &[1, 4, 16];

fn complete_waiter(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let once = Once::new();
        let mut wait = pin!(once.wait());
        poll_pending(wait.as_mut(), &mut context);

        poll_ready(once.call_once(async || {}), &mut context);
        poll_pinned_ready(wait.as_mut(), &mut context);
        black_box(once.is_completed())
    });
}

fn complete_waiter_batch(bencher: Bencher, waiter_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let once = Once::new();
        let mut waiters = (0..waiter_count)
            .map(|_| Box::pin(once.wait()))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }

        poll_ready(once.call_once(async || {}), &mut context);
        for mut waiter in waiters {
            poll_pinned_ready(waiter.as_mut(), &mut context);
        }
        black_box(once.is_completed())
    });
}

fn cancel_pending_wait(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let once = Once::new();
        {
            let mut wait = pin!(once.wait());
            poll_pending(wait.as_mut(), &mut context);
        }
        black_box(once)
    });
}

pub fn register(runner: &mut Runner) {
    runner.case(
        concat!(module_path!(), "::complete_waiter"),
        3_000_000,
        1,
        complete_waiter,
    );
    for &arg in WAITER_COUNTS {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::complete_waiter_batch")
            ),
            (5_000_000 / arg).max(1),
            1,
            |bencher| complete_waiter_batch(bencher, arg),
        );
    }
    runner.case(
        concat!(module_path!(), "::cancel_pending_wait"),
        6_000_000,
        1,
        cancel_pending_wait,
    );
}
