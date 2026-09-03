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

use std::future::Future;
use std::pin::pin;

use divan::Bencher;
use divan::black_box;

use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;

const WORKER_COUNTS: &[usize] = &[1, 8, 32];

struct Asyncband;
struct WaitgroupRs;

trait WaitGroup {
    type Group;
    type Worker: Clone;
    type Wait: Future<Output = ()>;

    fn new() -> Self::Group;
    fn worker(group: &Self::Group) -> Self::Worker;
    fn wait(group: Self::Group) -> Self::Wait;
}

impl WaitGroup for Asyncband {
    type Group = asyncband::waitgroup::WaitGroup;
    type Worker = asyncband::waitgroup::Worker;
    type Wait = asyncband::waitgroup::Wait;

    fn new() -> Self::Group {
        Self::Group::new()
    }

    fn worker(group: &Self::Group) -> Self::Worker {
        group.worker()
    }

    fn wait(group: Self::Group) -> Self::Wait {
        group.wait()
    }
}

impl WaitGroup for WaitgroupRs {
    type Group = waitgroup::WaitGroup;
    type Worker = waitgroup::Worker;
    type Wait = waitgroup::WaitGroupFuture;

    fn new() -> Self::Group {
        Self::Group::new()
    }

    fn worker(group: &Self::Group) -> Self::Worker {
        group.worker()
    }

    fn wait(group: Self::Group) -> Self::Wait {
        group.wait()
    }
}

#[divan::bench(types = [Asyncband, WaitgroupRs])]
fn ready_empty<C: WaitGroup>(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let mut wait = pin!(C::wait(C::new()));
        poll_pinned_ready(wait.as_mut(), &mut context);
    });
}

#[divan::bench(types = [Asyncband, WaitgroupRs])]
fn complete_waiter<C: WaitGroup>(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let group = C::new();
        let worker = C::worker(&group);
        let mut wait = pin!(C::wait(group));
        poll_pending(wait.as_mut(), &mut context);

        drop(worker);
        poll_pinned_ready(wait.as_mut(), &mut context);
    });
}

#[divan::bench(types = [Asyncband, WaitgroupRs])]
fn cancel_pending<C: WaitGroup>(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let group = C::new();
        let worker = C::worker(&group);
        {
            let mut wait = pin!(C::wait(group));
            poll_pending(wait.as_mut(), &mut context);
        }
        drop(worker);
    });
}

#[divan::bench(types = [Asyncband, WaitgroupRs])]
fn worker_round_trip<C: WaitGroup>(bencher: Bencher) {
    let group = C::new();

    bencher.bench_local(|| black_box(C::worker(&group)));
}

#[divan::bench(types = [Asyncband, WaitgroupRs])]
fn nested_worker_round_trip<C: WaitGroup>(bencher: Bencher) {
    let group = C::new();
    let worker = C::worker(&group);

    bencher.bench_local(|| black_box(worker.clone()));
}

#[divan::bench(types = [Asyncband, WaitgroupRs], args = WORKER_COUNTS)]
fn worker_batch<C: WaitGroup>(bencher: Bencher, worker_count: usize) {
    bencher.bench_local(|| {
        let group = C::new();
        let workers = (0..worker_count)
            .map(|_| C::worker(&group))
            .collect::<Vec<_>>();
        black_box(workers);
        black_box(group);
    });
}

#[divan::bench(types = [Asyncband, WaitgroupRs], args = WORKER_COUNTS)]
fn nested_worker_batch<C: WaitGroup>(bencher: Bencher, worker_count: usize) {
    bencher.bench_local(|| {
        let group = C::new();
        let worker = C::worker(&group);
        let workers = (0..worker_count)
            .map(|_| worker.clone())
            .collect::<Vec<_>>();
        black_box(workers);
        black_box(worker);
        black_box(group);
    });
}

#[divan::bench(types = [Asyncband, WaitgroupRs], args = WORKER_COUNTS)]
fn complete_worker_batch<C: WaitGroup>(bencher: Bencher, worker_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let group = C::new();
        let workers = (0..worker_count)
            .map(|_| C::worker(&group))
            .collect::<Vec<_>>();
        let mut wait = pin!(C::wait(group));
        poll_pending(wait.as_mut(), &mut context);

        drop(workers);
        poll_pinned_ready(wait.as_mut(), &mut context);
    });
}
