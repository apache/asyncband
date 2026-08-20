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

use std::pin::pin;

use asyncband::latch::Latch;
use divan::Bencher;
use divan::black_box;

use super::support::bench_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;

const WORKER_COUNTS: &[usize] = &[1, 8, 32];

#[divan::bench]
fn cancel_pending(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let latch = Latch::new(1);
        {
            let mut wait = pin!(latch.wait());
            poll_pending(wait.as_mut(), &mut context);
        }
        black_box(latch)
    });
}

#[divan::bench]
fn wake_waiter(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let latch = Latch::new(1);
        let mut wait = pin!(latch.wait());
        poll_pending(wait.as_mut(), &mut context);

        latch.count_down();
        poll_pinned_ready(wait.as_mut(), &mut context);
        black_box(latch.count())
    });
}

#[divan::bench(args = WORKER_COUNTS)]
fn worker_fan_in(bencher: Bencher, worker_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let latch = Latch::new(worker_count as u32);
        let mut wait = pin!(latch.wait());
        poll_pending(wait.as_mut(), &mut context);

        for _ in 0..worker_count {
            latch.count_down();
        }
        poll_pinned_ready(wait.as_mut(), &mut context);
        black_box(latch.count())
    });
}

#[divan::bench(args = WORKER_COUNTS)]
fn waiter_fan_out(bencher: Bencher, waiter_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let latch = Latch::new(1);
        let mut waiters = (0..waiter_count)
            .map(|_| Box::pin(latch.wait()))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }

        latch.count_down();
        for mut waiter in waiters {
            poll_pinned_ready(waiter.as_mut(), &mut context);
        }
        black_box(latch.count())
    });
}
