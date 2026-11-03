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

use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::implementations;
use benchmarks::support::bench_context;

use crate::mpsc::adapters::AsyncChannel;
use crate::mpsc::adapters::Asyncband;
use crate::mpsc::adapters::BoundedMpsc;
use crate::mpsc::adapters::Flume;
use crate::mpsc::adapters::Tokio;
use crate::mpsc::support::BOUNDED_CAPACITY;
use crate::mpsc::support::Bounded;
use crate::mpsc::support::RepeatedBatch;
use crate::mpsc::support::RepeatedTasks;
use crate::mpsc::support::TaskShape;

fn try_send_receive<C: BoundedMpsc>(bencher: Bencher) {
    let (sender, mut receiver) = C::channel(BOUNDED_CAPACITY);

    bencher.bench_local(|| {
        C::try_send(&sender, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver))
    });
}

fn ready_send_receive<C: BoundedMpsc>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receiver) = C::channel(BOUNDED_CAPACITY);

    bencher.bench_local(|| {
        C::send_ready(&sender, black_box(usize::MAX), &mut context);
        black_box(C::recv_ready(&mut receiver, &mut context))
    });
}

fn reused_threads<C: BoundedMpsc>(bencher: Bencher, producer_count: usize) {
    let mut batch = RepeatedBatch::<Bounded<C>>::new(producer_count);
    batch.run();
    bencher.bench_local(|| batch.run());
}

fn reused_tasks<C: BoundedMpsc, const CAPACITY: usize>(
    bencher: Bencher,
    TaskShape { producers, workers }: TaskShape,
) {
    let mut batch = RepeatedTasks::<Bounded<C, CAPACITY>>::new(producers, workers);
    batch.run();
    bencher.bench_local(|| batch.run());
}

fn reused_tasks_inline_1k<C: BoundedMpsc<[u8; 1024]>, const CAPACITY: usize>(
    bencher: Bencher,
    TaskShape { producers, workers }: TaskShape,
) {
    let mut batch = RepeatedTasks::<Bounded<C, CAPACITY, [u8; 1024]>>::new(producers, workers);
    batch.run();
    bencher.bench_local(|| batch.run());
}

fn capacity_one_task_handoff<C: BoundedMpsc>(bencher: Bencher) {
    let mut batch = RepeatedTasks::<Bounded<C, 1>>::new(4, 4);
    batch.run();
    bencher.bench_local(|| batch.run());
}

fn external_receiver<C: BoundedMpsc>(bencher: Bencher, producers: usize) {
    let mut batch = RepeatedTasks::<Bounded<C>>::external_receiver(producers, 4);
    batch.run();
    bencher.bench_local(|| batch.run());
}

fn compare_tasks<const CAPACITY: usize>(
    runner: &mut Runner,
    producers: usize,
    workers: usize,
    rounds: usize,
) {
    let executor = if workers == 0 {
        String::from("current")
    } else {
        format!("{workers}w")
    };
    let name = format!("mpsc/bounded/tasks-{producers}p1c-cap{CAPACITY}-{executor}");
    let shape = TaskShape { producers, workers };
    runner.compare(
        &name,
        rounds,
        implementations!(
            [Asyncband, Tokio, AsyncChannel, Flume],
            |C, b| reused_tasks::<C, CAPACITY>(b, shape)
        ),
    );
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "mpsc/bounded/try-roundtrip",
        30_000_000,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |C, b| {
            try_send_receive::<C>(b)
        }),
    );
    runner.compare(
        "mpsc/bounded/ready-roundtrip",
        24_000_000,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |C, b| {
            ready_send_receive::<C>(b)
        }),
    );
    runner.compare(
        "mpsc/bounded/threads-4p1c-cap64",
        89,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |C, b| {
            reused_threads::<C>(b, 4)
        }),
    );
    compare_tasks::<64>(runner, 1, 4, 896);
    compare_tasks::<64>(runner, 4, 4, 320);
    compare_tasks::<1024>(runner, 1, 4, 430);
    compare_tasks::<1024>(runner, 4, 4, 320);
    compare_tasks::<64>(runner, 1, 0, 1_200);
    compare_tasks::<1024>(runner, 1, 0, 1_200);
    compare_tasks::<64>(runner, 4, 0, 1_100);
    runner.compare(
        "mpsc/bounded/tasks-4p1c-cap64-4w-inline1k",
        26,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |C, b| {
            reused_tasks_inline_1k::<C, 64>(
                b,
                TaskShape {
                    producers: 4,
                    workers: 4,
                },
            )
        }),
    );
    runner.compare(
        "mpsc/bounded/tasks-4p1c-cap1-4w",
        160,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |C, b| {
            capacity_one_task_handoff::<C>(b)
        }),
    );
    runner.compare(
        "mpsc/bounded/external-receiver-1p1c-cap64-4w",
        128,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |C, b| {
            external_receiver::<C>(b, 1)
        }),
    );
}
