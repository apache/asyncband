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
use std::hint::black_box;
use std::pin::pin;

use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;

const WORKER_COUNTS: &[usize] = &[1, 4, 16];

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

fn ready_empty<C: WaitGroup>(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let mut wait = pin!(C::wait(C::new()));
        poll_pinned_ready(wait.as_mut(), &mut context);
    });
}

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

pub fn register(runner: &mut Runner) {
    register_implementation::<Asyncband>(runner, "Asyncband");
    register_implementation::<WaitgroupRs>(runner, "WaitgroupRs");
}

fn register_implementation<C>(runner: &mut Runner, implementation: &str)
where
    C: WaitGroup,
{
    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::complete_waiter")
        ),
        6_000_000,
        1,
        complete_waiter::<C>,
    );

    for &arg in WORKER_COUNTS {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::complete_worker_batch")
            ),
            (10_000_000 / arg).max(1),
            1,
            |bencher| complete_worker_batch::<C>(bencher, arg),
        );
    }
    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::ready_empty")
        ),
        10_000_000,
        1,
        ready_empty::<C>,
    );

    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::cancel_pending")
        ),
        6_000_000,
        1,
        cancel_pending::<C>,
    );

    for &arg in WORKER_COUNTS {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::worker_batch")
            ),
            (10_000_000 / arg).max(1),
            1,
            |bencher| worker_batch::<C>(bencher, arg),
        );
    }
    for &arg in WORKER_COUNTS {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::nested_worker_batch")
            ),
            (10_000_000 / arg).max(1),
            1,
            |bencher| nested_worker_batch::<C>(bencher, arg),
        );
    }
}
