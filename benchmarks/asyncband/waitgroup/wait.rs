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
use std::pin::pin;

use asyncband::waitgroup::WaitGroup;
use divan::Bencher;
use divan::black_box;

use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;

const WORKER_COUNTS: &[usize] = &[1, 8, 32];
const THREAD_COUNTS: &[usize] = &[1, 2, 8, 32];
const CONTENDED_SAMPLE_SIZE: u32 = 256;

#[divan::bench]
fn ready_empty(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let mut wait = pin!(WaitGroup::new().into_future());
        poll_pinned_ready(wait.as_mut(), &mut context);
        black_box(())
    });
}

#[divan::bench]
fn worker_round_trip(bencher: Bencher) {
    let root = WaitGroup::new();

    bencher.bench_local(|| black_box(root.clone()));
}

#[divan::bench(threads = THREAD_COUNTS, sample_size = CONTENDED_SAMPLE_SIZE)]
fn worker_round_trip_contended(bencher: Bencher) {
    let root = WaitGroup::new();

    bencher.bench(|| black_box(root.clone()));
}

#[divan::bench]
fn cancel_pending(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let root = WaitGroup::new();
        let worker = root.clone();
        {
            let mut wait = pin!(root.into_future());
            poll_pending(wait.as_mut(), &mut context);
        }
        drop(worker);
        black_box(())
    });
}

#[divan::bench]
fn complete_waiter(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let root = WaitGroup::new();
        let worker = root.clone();
        let mut wait = pin!(root.into_future());
        poll_pending(wait.as_mut(), &mut context);

        drop(worker);
        poll_pinned_ready(wait.as_mut(), &mut context);
        black_box(())
    });
}

#[divan::bench(args = WORKER_COUNTS)]
fn complete_worker_batch(bencher: Bencher, worker_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let root = WaitGroup::new();
        let workers = (0..worker_count).map(|_| root.clone()).collect::<Vec<_>>();
        let mut wait = pin!(root.into_future());
        poll_pending(wait.as_mut(), &mut context);

        drop(workers);
        poll_pinned_ready(wait.as_mut(), &mut context);
        black_box(())
    });
}

#[divan::bench(args = WORKER_COUNTS)]
fn complete_waiter_batch(bencher: Bencher, waiter_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let root = WaitGroup::new();
        let worker = root.clone();
        let wait = root.into_future();
        let mut waiters = (0..waiter_count)
            .map(|_| Box::pin(wait.clone()))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }

        drop(worker);
        for mut waiter in waiters {
            poll_pinned_ready(waiter.as_mut(), &mut context);
        }
        black_box(())
    });
}
