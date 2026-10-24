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
use std::sync::Arc;

use asyncband::semaphore::Semaphore;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;

fn fulfill_debt_repeatedly(bencher: Bencher) {
    const CYCLES: usize = 64;

    bencher.bench_local(|| {
        let semaphore = Semaphore::new(0);
        for _ in 0..CYCLES {
            semaphore.reduce_permits(black_box(1));
            semaphore.release(black_box(1));
        }
        black_box(semaphore.available_permits())
    });
}

fn release_to_waiters(bencher: Bencher, waiter_count: usize) {
    bencher.bench_local(|| {
        let (semaphore, waiters) = {
            let semaphore = Arc::new(Semaphore::new(0));
            let mut context = bench_context();
            let mut waiters = (0..waiter_count)
                .map(|_| Box::pin(semaphore.clone().acquire_owned(1)))
                .collect::<Vec<_>>();
            for waiter in &mut waiters {
                poll_pending(waiter.as_mut(), &mut context);
            }
            (semaphore, waiters)
        };
        // Include registration, release, wake callbacks, and cancellation in each round.
        semaphore.release(black_box(waiter_count));
        drop((semaphore, waiters));
    });
}

pub fn register(runner: &mut Runner) {
    runner.case(
        concat!(module_path!(), "::fulfill_debt_repeatedly"),
        300_000,
        1,
        fulfill_debt_repeatedly,
    );
    for &arg in &[1, 2, 32, 256] {
        runner.case(
            format!("{}/{arg}", concat!(module_path!(), "::release_to_waiters")),
            (4_000_000 / arg).max(1),
            1,
            |bencher| release_to_waiters(bencher, arg),
        );
    }
}
