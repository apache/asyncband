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

use std::future::IntoFuture;
use std::hint::black_box;
use std::pin::Pin;

use asyncband::waitgroup::WaitGroup;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::primitive;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_ready;

const WAITER_COUNTS: &[usize] = &[1, 4, 16];

fn complete_waiter_batch(bencher: Bencher, waiter_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let root = WaitGroup::new();
        let worker = root.clone();
        let wait = root.into_future();
        let mut waiters = (0..waiter_count).map(|_| wait.clone()).collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(Pin::new(waiter), &mut context);
        }

        drop(worker);
        for waiter in waiters {
            poll_ready(waiter, &mut context);
        }
        black_box(())
    });
}

pub fn register(runner: &mut Runner) {
    for &arg in WAITER_COUNTS {
        primitive!(runner, complete_waiter_batch(arg), (5_000_000 / arg).max(1));
    }
}
