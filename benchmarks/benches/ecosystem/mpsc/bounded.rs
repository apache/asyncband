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
use benchmarks::support::bench_context;

use crate::mpsc::adapters::AsyncChannel;
use crate::mpsc::adapters::Asyncband;
use crate::mpsc::adapters::BoundedMpsc;
use crate::mpsc::adapters::Flume;
use crate::mpsc::adapters::Tokio;
use crate::mpsc::support::BOUNDED_CAPACITY;
use crate::mpsc::support::Bounded;
use crate::mpsc::support::PARALLEL_TASK_SHAPES;
use crate::mpsc::support::PRODUCER_COUNTS;
use crate::mpsc::support::RepeatedBatch;
use crate::mpsc::support::RepeatedTasks;
use crate::mpsc::support::TASK_SHAPES;
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

fn clone_drop_sender<C: BoundedMpsc>(bencher: Bencher) {
    let (sender, _receiver) = C::channel(BOUNDED_CAPACITY);
    bencher.bench_local(|| drop(black_box(sender.clone())));
}

fn external_receiver<C: BoundedMpsc>(bencher: Bencher, producers: usize) {
    let mut batch = RepeatedTasks::<Bounded<C>>::external_receiver(producers, 4);
    batch.run();
    bencher.bench_local(|| batch.run());
}

fn external_receiver_inline<C: BoundedMpsc<[u8; 1024]>>(bencher: Bencher, producers: usize) {
    let mut batch =
        RepeatedTasks::<Bounded<C, BOUNDED_CAPACITY, [u8; 1024]>>::external_receiver(producers, 4);
    batch.run();
    bencher.bench_local(|| batch.run());
}

pub fn register(runner: &mut Runner) {
    register_implementation::<Asyncband>(runner, "Asyncband");
    register_implementation::<Tokio>(runner, "Tokio");
    register_implementation::<AsyncChannel>(runner, "AsyncChannel");
    register_implementation::<Flume>(runner, "Flume");
}

fn register_implementation<C>(runner: &mut Runner, implementation: &str)
where
    C: BoundedMpsc + BoundedMpsc<[u8; 1024]>,
{
    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::try_send_receive")
        ),
        30_000_000,
        1,
        try_send_receive::<C>,
    );

    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::ready_send_receive")
        ),
        20_000_000,
        1,
        ready_send_receive::<C>,
    );

    for &arg in PRODUCER_COUNTS {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::reused_threads")
            ),
            128,
            1,
            |bencher| reused_threads::<C>(bencher, arg),
        );
    }
    for &arg in TASK_SHAPES {
        runner.case(
            format!(
                "{}/{implementation}/64/{arg}",
                concat!(module_path!(), "::reused_tasks")
            ),
            512,
            1,
            |bencher| reused_tasks::<C, 64>(bencher, arg),
        );
        runner.case(
            format!(
                "{}/{implementation}/1024/{arg}",
                concat!(module_path!(), "::reused_tasks")
            ),
            512,
            1,
            |bencher| reused_tasks::<C, 1024>(bencher, arg),
        );
    }
    for &arg in PARALLEL_TASK_SHAPES {
        runner.case(
            format!(
                "{}/{implementation}/64/{arg}",
                concat!(module_path!(), "::reused_tasks_inline_1k")
            ),
            64,
            1,
            |bencher| reused_tasks_inline_1k::<C, 64>(bencher, arg),
        );
        runner.case(
            format!(
                "{}/{implementation}/1024/{arg}",
                concat!(module_path!(), "::reused_tasks_inline_1k")
            ),
            64,
            1,
            |bencher| reused_tasks_inline_1k::<C, 1024>(bencher, arg),
        );
    }
    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::capacity_one_task_handoff")
        ),
        64,
        1,
        capacity_one_task_handoff::<C>,
    );

    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::clone_drop_sender")
        ),
        96_000_000,
        1,
        clone_drop_sender::<C>,
    );

    for &arg in &[1, 8] {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::external_receiver")
            ),
            128,
            1,
            |bencher| external_receiver::<C>(bencher, arg),
        );
    }
    for &arg in &[1, 8] {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::external_receiver_inline")
            ),
            64,
            1,
            |bencher| external_receiver_inline::<C>(bencher, arg),
        );
    }
}
