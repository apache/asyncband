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
use std::sync::Arc;

use asyncband::semaphore::Semaphore;
use divan::Bencher;
use divan::black_box;

use super::support::bench_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;
use super::support::poll_ready;

const QUEUE_DEPTHS: &[usize] = &[1, 8, 32];

#[divan::bench]
fn cancel_pending_acquire(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let semaphore = Semaphore::new(0);
        {
            let mut acquire = pin!(semaphore.acquire(black_box(1)));
            poll_pending(acquire.as_mut(), &mut context);
        }
        black_box(semaphore.available_permits())
    });
}

#[divan::bench]
fn handoff_permit(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let semaphore = Semaphore::new(0);
        let mut acquire = pin!(semaphore.acquire(black_box(1)));
        poll_pending(acquire.as_mut(), &mut context);

        semaphore.release(black_box(1));
        let permit = poll_pinned_ready(acquire.as_mut(), &mut context);
        permit.forget();

        black_box(semaphore.available_permits())
    });
}

#[divan::bench]
fn fulfill_debt_repeatedly(bencher: Bencher) {
    const CYCLES: usize = 64;

    bencher.bench_local(|| {
        let semaphore = Semaphore::new(0);
        for _ in 0..CYCLES {
            semaphore.forget_exact(black_box(1));
            semaphore.release(black_box(1));
        }
        black_box(semaphore.available_permits())
    });
}

#[divan::bench]
fn release(bencher: Bencher) {
    bencher
        .with_inputs(|| Semaphore::new(0))
        .bench_local_values(|semaphore| {
            semaphore.release(black_box(1));
            black_box(semaphore)
        });
}

#[divan::bench]
fn try_acquire_release(bencher: Bencher) {
    let semaphore = Semaphore::new(1);

    bencher.bench_local(|| {
        drop(black_box(semaphore.try_acquire(black_box(1)).unwrap()));
    });
}

#[divan::bench]
fn owned_try_acquire_release(bencher: Bencher) {
    let semaphore = Arc::new(Semaphore::new(8));

    bencher.bench_local(|| {
        let permit = semaphore
            .clone()
            .try_acquire_owned(black_box(2))
            .expect("permits must be available");
        black_box(permit.permits());
        drop(permit);
    });
}

#[divan::bench]
fn owned_try_acquire_rejected(bencher: Bencher) {
    let semaphore = Arc::new(Semaphore::new(8));
    let held = semaphore
        .clone()
        .try_acquire_owned(8)
        .expect("all permits must be available");

    bencher.bench_local(|| black_box(semaphore.clone().try_acquire_owned(black_box(1)).is_none()));

    drop(held);
}

#[divan::bench(args = QUEUE_DEPTHS)]
fn cancel_pending_owned_batch(bencher: Bencher, queue_depth: usize) {
    let semaphore = Arc::new(Semaphore::new(0));
    let mut context = bench_context();

    bencher.bench_local(|| {
        let mut waiters = (0..queue_depth)
            .map(|index| Box::pin(semaphore.clone().acquire_owned(1 << (index % 3))))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }
        drop(waiters);
        black_box(semaphore.available_permits())
    });
}

#[divan::bench(args = QUEUE_DEPTHS)]
fn queued_owned_burst(bencher: Bencher, queue_depth: usize) {
    const PERMITS: usize = 8;

    let mut context = bench_context();
    bencher.bench_local(|| {
        let semaphore = Arc::new(Semaphore::new(PERMITS));
        let held = poll_ready(semaphore.clone().acquire_owned(PERMITS), &mut context);
        let mut waiters = (0..queue_depth)
            .map(|index| Box::pin(semaphore.clone().acquire_owned(1 << (index % 3))))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }

        drop(held);
        for mut waiter in waiters {
            let permit = poll_pinned_ready(waiter.as_mut(), &mut context);
            black_box(permit.permits());
            drop(permit);
        }
        black_box(semaphore.available_permits())
    });
}
