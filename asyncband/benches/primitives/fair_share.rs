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

use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;
use std::pin::pin;

use asyncband::admission::FairShare;
use divan::Bencher;
use divan::black_box;

use super::support::bench_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;
use super::support::poll_ready;

type DeterministicFairShare = FairShare<usize, BuildHasherDefault<DefaultHasher>>;

const QUEUE_DEPTHS: &[usize] = &[1, 8, 32];

fn fair_share() -> DeterministicFairShare {
    FairShare::with_hasher(1, BuildHasherDefault::default())
}

#[divan::bench]
fn cancel_pending(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let admission = fair_share();
        let held = poll_ready(admission.acquire(black_box(0usize)), &mut context);

        {
            let mut pending = pin!(admission.acquire(black_box(1usize)));
            poll_pending(pending.as_mut(), &mut context);
        }

        assert_eq!(admission.num_waiters(), 0);
        drop(held);
        black_box(admission.available_permits())
    });
}

#[divan::bench]
fn handoff(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let admission = fair_share();
        let held = poll_ready(admission.acquire(black_box(0usize)), &mut context);
        let mut pending = pin!(admission.acquire(black_box(1usize)));
        poll_pending(pending.as_mut(), &mut context);

        drop(held);
        let permit = poll_pinned_ready(pending.as_mut(), &mut context);
        black_box(&permit);
        drop(permit);

        black_box(admission.available_permits())
    });
}

#[divan::bench(args = QUEUE_DEPTHS)]
fn handoff_batch(bencher: Bencher, queue_depth: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let admission = fair_share();
        let held = poll_ready(admission.acquire(black_box(0usize)), &mut context);
        let mut waiters = (0..queue_depth)
            .map(|key| Box::pin(admission.acquire(black_box(key + 1))))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }

        drop(held);
        for mut waiter in waiters {
            let permit = poll_pinned_ready(waiter.as_mut(), &mut context);
            black_box(&permit);
            drop(permit);
        }
        black_box(admission.available_permits())
    });
}

#[divan::bench(args = QUEUE_DEPTHS)]
fn cancel_pending_batch(bencher: Bencher, queue_depth: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let admission = fair_share();
        let held = poll_ready(admission.acquire(black_box(0usize)), &mut context);
        let mut waiters = (0..queue_depth)
            .map(|key| Box::pin(admission.acquire(black_box(key + 1))))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }

        drop(waiters);
        assert_eq!(admission.num_waiters(), 0);
        drop(held);
        black_box(admission.available_permits())
    });
}
