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
use std::future::IntoFuture;
use std::pin::pin;

use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::implementations;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;

mod drain;

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
    type Worker = asyncband::waitgroup::WaitGroup;
    type Wait = asyncband::waitgroup::Wait;

    fn new() -> Self::Group {
        Self::Group::new()
    }

    fn worker(group: &Self::Group) -> Self::Worker {
        group.clone()
    }

    fn wait(group: Self::Group) -> Self::Wait {
        group.into_future()
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

// A parent can discover children after the coordinator has started waiting.
fn nested_workers<C: WaitGroup>(bencher: Bencher) {
    let mut context = bench_context();
    bencher.bench_local(|| {
        let group = C::new();
        let parent = C::worker(&group);
        let mut wait = pin!(C::wait(group));
        poll_pending(wait.as_mut(), &mut context);
        let children: [_; 4] = std::array::from_fn(|_| parent.clone());
        drop(parent);
        poll_pending(wait.as_mut(), &mut context);
        drop(children);
        poll_pinned_ready(wait.as_mut(), &mut context);
    });
}

fn wait_after_work<C: WaitGroup>(bencher: Bencher) {
    let mut context = bench_context();
    bencher.bench_local(|| {
        let group = C::new();
        let worker = C::worker(&group);
        drop(worker);
        let mut wait = pin!(C::wait(group));
        poll_pinned_ready(wait.as_mut(), &mut context);
    });
}

fn scheduled_workers<C: WaitGroup>(bencher: Bencher)
where
    C::Worker: Send + 'static,
{
    let runtime = benchmarks::channels::runtime(4);
    bencher.bench_local(|| {
        runtime.block_on(async {
            let group = C::new();
            let tasks: [_; 4] = std::array::from_fn(|_| {
                let worker = C::worker(&group);
                tokio::spawn(async move { drop(worker) })
            });
            C::wait(group).await;
            for task in tasks {
                task.await.unwrap();
            }
        })
    });
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "waitgroup/complete-waiter",
        3_500_000,
        implementations!([Asyncband, WaitgroupRs], |C, b| complete_waiter::<C>(b)),
    );
    runner.compare(
        "waitgroup/complete-4-workers",
        2_600_000,
        implementations!([Asyncband, WaitgroupRs], |C, b| complete_worker_batch::<C>(
            b, 4
        )),
    );
    runner.compare(
        "waitgroup/wait-after-work",
        5_500_000,
        implementations!([Asyncband, WaitgroupRs], |C, b| wait_after_work::<C>(b)),
    );
    runner.compare(
        "waitgroup/nested-4-workers",
        3_000_000,
        implementations!([Asyncband, WaitgroupRs], |C, b| nested_workers::<C>(b)),
    );
    runner.compare(
        "waitgroup/tasks-4-workers-4w",
        25_000,
        implementations!([Asyncband, WaitgroupRs], |C, b| scheduled_workers::<C>(b)),
    );
    drain::register(runner);
}
