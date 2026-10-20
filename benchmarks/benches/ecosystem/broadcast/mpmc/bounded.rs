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

// Tokio broadcast overwrites at capacity, so it cannot participate in lossless backpressure cases.

use std::hint::black_box;

use benchmarks::channels::runtime;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;
use tokio::runtime::Builder;

use crate::broadcast::mpmc::adapters::AsyncBroadcast;
use crate::broadcast::mpmc::adapters::Asyncband;
use crate::broadcast::mpmc::adapters::BoundedBroadcastMpmc;
use crate::broadcast::mpmc::support::BACKPRESSURE_SHAPE;
use crate::broadcast::mpmc::support::BOUNDED_SHAPES;
use crate::broadcast::mpmc::support::BoundedConcurrent;
use crate::broadcast::mpmc::support::BoundedShape;
use crate::broadcast::mpmc::support::BoundedTasks;
use crate::broadcast::mpmc::support::ROUND_TRIP_CAPACITY;

// Send-then-receive pairing keeps at most one message retained, so these never reach capacity.
fn try_send_receive<C: BoundedBroadcastMpmc>(bencher: Bencher) {
    let (sender, mut receivers) = C::channel(ROUND_TRIP_CAPACITY, 1);
    let mut receiver = receivers.pop().unwrap();

    bencher.bench_local(|| {
        C::try_send(&sender, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver).unwrap())
    });
}

fn ready_send_receive<C: BoundedBroadcastMpmc>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(ROUND_TRIP_CAPACITY, 1);
    let mut receiver = receivers.pop().unwrap();

    bencher.bench_local(|| {
        C::send_ready(&sender, black_box(usize::MAX), &mut context);
        black_box(C::recv_ready(&mut receiver, &mut context))
    });
}

// Finish and join each fixture before starting the next round.
fn thread_batch<C: BoundedBroadcastMpmc>(bencher: Bencher, shape: BoundedShape) {
    bencher.bench_local(|| {
        let mut input = BoundedConcurrent::new::<C>(shape);
        BoundedConcurrent::run(&mut input)
    });
}

fn task_batch<C: BoundedBroadcastMpmc>(bencher: Bencher, shape: BoundedShape) {
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    bencher.bench_local(|| {
        let mut tasks = BoundedTasks::new::<C>(&runtime, shape);
        tasks.run(&runtime)
    });
}

fn capacity_one_task_handoff<C: BoundedBroadcastMpmc>(bencher: Bencher) {
    let runtime = runtime(4);
    bencher.bench_local(|| {
        let mut tasks = BoundedTasks::new::<C>(&runtime, BACKPRESSURE_SHAPE);
        tasks.run(&runtime)
    });
}

pub fn register(runner: &mut Runner) {
    register_implementation::<Asyncband>(runner, "Asyncband");
    register_implementation::<AsyncBroadcast>(runner, "AsyncBroadcast");
}

fn register_implementation<C>(runner: &mut Runner, implementation: &str)
where
    C: BoundedBroadcastMpmc,
{
    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::try_send_receive")
        ),
        9_000_000,
        1,
        try_send_receive::<C>,
    );

    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::ready_send_receive")
        ),
        10_000_000,
        1,
        ready_send_receive::<C>,
    );

    for &arg in BOUNDED_SHAPES {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::thread_batch")
            ),
            128,
            1,
            |bencher| thread_batch::<C>(bencher, arg),
        );
    }
    for &arg in BOUNDED_SHAPES {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::task_batch")
            ),
            320,
            1,
            |bencher| task_batch::<C>(bencher, arg),
        );
    }
    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::capacity_one_task_handoff")
        ),
        80,
        1,
        capacity_one_task_handoff::<C>,
    );
}
