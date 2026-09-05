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

use asyncband::capacity_limiter::CapacityLimiter;
use asyncband::semaphore::Semaphore;
use divan::Bencher;
use divan::black_box;

use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;

// Instances are reused; construction is outside the timed operation.
// The semaphore is a cost reference with no borrower-identity contract.

#[divan::bench]
fn semaphore_try_acquire_release(bencher: Bencher) {
    let semaphore = Semaphore::new(1);

    bencher.bench_local(|| {
        drop(black_box(
            semaphore
                .try_acquire(black_box(1))
                .expect("capacity is available"),
        ));
    });
}

#[divan::bench]
fn limiter_try_acquire_release(bencher: Bencher) {
    let limiter = CapacityLimiter::new(1);

    bencher.bench_local(|| {
        drop(black_box(
            limiter
                .try_acquire(black_box(7usize))
                .expect("capacity is available"),
        ));
    });
}

#[divan::bench]
fn limiter_try_acquire_release_arc_str(bencher: Bencher) {
    let limiter = CapacityLimiter::new(1);
    let borrower: Arc<str> = Arc::from("tenant-0000000001");

    bencher.bench_local(|| {
        drop(black_box(
            limiter
                .try_acquire(black_box(borrower.clone()))
                .expect("capacity is available"),
        ));
    });
}

#[divan::bench]
fn semaphore_handoff_reused(bencher: Bencher) {
    let limiter = Semaphore::new(1);
    let mut context = bench_context();
    bencher.bench_local(|| {
        let held = limiter.try_acquire(black_box(1)).unwrap();
        let mut acquire = pin!(limiter.acquire(black_box(1)));
        poll_pending(acquire.as_mut(), &mut context);
        drop(held);
        drop(poll_pinned_ready(acquire.as_mut(), &mut context));
    });
}

#[divan::bench]
fn limiter_handoff_reused(bencher: Bencher) {
    let limiter = CapacityLimiter::new(1);
    let mut context = bench_context();
    bencher.bench_local(|| {
        let held = limiter.try_acquire(black_box(7usize)).unwrap();
        let mut acquire = pin!(limiter.acquire(black_box(8usize)));
        poll_pending(acquire.as_mut(), &mut context);
        drop(held);
        drop(poll_pinned_ready(acquire.as_mut(), &mut context).unwrap());
    });
}

#[divan::bench]
fn semaphore_cancel_pending_reused(bencher: Bencher) {
    let limiter = Semaphore::new(0);
    let mut context = bench_context();
    bencher.bench_local(|| {
        let mut acquire = pin!(limiter.acquire(black_box(1)));
        poll_pending(acquire.as_mut(), &mut context);
    });
}

#[divan::bench]
fn limiter_cancel_pending_reused(bencher: Bencher) {
    let limiter = CapacityLimiter::new(0);
    let mut context = bench_context();
    bencher.bench_local(|| {
        let mut acquire = pin!(limiter.acquire(black_box(7usize)));
        poll_pending(acquire.as_mut(), &mut context);
    });
}
