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
use std::future::poll_fn;
use std::hint::black_box;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;
use tokio::runtime::Builder;
use tokio::sync::Notify;

use crate::mpsc::adapters::AsyncChannel;
use crate::mpsc::adapters::Asyncband;
use crate::mpsc::adapters::Flume;
use crate::mpsc::adapters::Tokio;
use crate::mpsc::adapters::UnboundedMpsc;
use crate::mpsc::support::BATCH_MESSAGES;
use crate::mpsc::support::PRODUCER_COUNTS;
use crate::mpsc::support::RepeatedBatch;
use crate::mpsc::support::RepeatedTasks;
use crate::mpsc::support::TASK_SHAPES;
use crate::mpsc::support::TaskShape;
use crate::mpsc::support::Unbounded;

fn ready_send_receive<C: UnboundedMpsc>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receiver) = C::channel();

    bencher.bench_local(|| {
        C::send(&sender, black_box(usize::MAX));
        black_box(C::recv_ready(&mut receiver, &mut context))
    });
}

fn try_send_receive<C: UnboundedMpsc>(bencher: Bencher) {
    let (sender, mut receiver) = C::channel();

    bencher.bench_local(|| {
        C::send(&sender, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver))
    });
}

fn burst_drain<C: UnboundedMpsc>(bencher: Bencher, messages: usize) {
    repeated_bursts::<C, _, _>(bencher, messages, 0, || usize::MAX);
}

fn burst_drain_inline<C: UnboundedMpsc<[u8; SIZE]>, const SIZE: usize>(
    bencher: Bencher,
    messages: usize,
) {
    repeated_bursts::<C, _, _>(bencher, messages, 0, || [1; SIZE]);
}

fn burst_drain_boxed<C: UnboundedMpsc<Box<[u8; 1024]>>>(bencher: Bencher, messages: usize) {
    // Include payload allocation and destruction to compare the complete boxed-message lifecycle.
    repeated_bursts::<C, _, _>(bencher, messages, 0, || Box::new([1; 1024]));
}

fn burst_with_backlog<C: UnboundedMpsc>(bencher: Bencher, messages: usize) {
    repeated_bursts::<C, _, _>(bencher, messages, messages / 2, || usize::MAX);
}

fn repeated_bursts<C: UnboundedMpsc<T>, T, F: Fn() -> T>(
    bencher: Bencher,
    messages: usize,
    backlog: usize,
    make_value: F,
) {
    // Keep one channel alive across rounds so allocation reuse and reclamation are measured.
    let (sender, mut receiver) = C::channel();
    for _ in 0..backlog {
        C::send(&sender, make_value());
    }
    let mut run = || {
        for _ in 0..messages {
            C::send(&sender, black_box(make_value()));
        }
        for _ in 0..messages {
            black_box(C::try_recv(&mut receiver));
        }
    };
    // Measure recurring bursts after the initial allocation, including a deliberately retained
    // backlog where requested. Do not require an extra `try_recv` call to trigger reclamation.
    run();
    bencher.bench_local(run);
}

fn reused_threads<C: UnboundedMpsc>(bencher: Bencher, producer_count: usize) {
    let mut batch = RepeatedBatch::<Unbounded<C>>::new(producer_count);
    batch.run();
    bencher.bench_local(|| batch.run());
}

fn reused_tasks<C: UnboundedMpsc>(bencher: Bencher, TaskShape { producers, workers }: TaskShape) {
    let mut batch = RepeatedTasks::<Unbounded<C>>::new(producers, workers);
    batch.run();
    bencher.bench_local(|| batch.run());
}

fn check_inline_message(value: [u8; 1024], expected: &mut [u64]) -> usize {
    let value = black_box(value);
    let producer = u64::from_le_bytes(value[..8].try_into().unwrap()) as usize;
    let sequence = u64::from_le_bytes(value[8..16].try_into().unwrap());
    assert_eq!(sequence, expected[producer]);
    expected[producer] += 1;
    usize::from(value[1023])
}

fn clone_drop_sender<C: UnboundedMpsc>(bencher: Bencher) {
    let (sender, _receiver) = C::channel();
    bencher.bench_local(|| drop(black_box(sender.clone())));
}

fn scheduled_bursts_inline<C: UnboundedMpsc<[u8; 1024]>>(
    bencher: Bencher,
    (producers, burst_messages): (usize, usize),
) {
    assert_eq!(BATCH_MESSAGES % burst_messages, 0);
    assert_eq!(burst_messages % producers, 0);
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    let (sender, mut receiver) = C::channel();
    let start: Vec<_> = (0..producers).map(|_| Arc::new(Notify::new())).collect();
    let stop = Arc::new(AtomicBool::new(false));
    let workers: Vec<_> = start
        .iter()
        .enumerate()
        .map(|(producer, start)| {
            let sender = sender.clone();
            let start = start.clone();
            let stop = stop.clone();
            runtime.spawn(async move {
                let mut sequence = 0u64;
                loop {
                    start.notified().await;
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    for _ in 0..burst_messages / producers {
                        let mut value = [1; 1024];
                        value[..8].copy_from_slice(&(producer as u64).to_le_bytes());
                        value[8..16].copy_from_slice(&sequence.to_le_bytes());
                        C::send(&sender, black_box(value));
                        sequence += 1;
                    }
                }
            })
        })
        .collect();
    drop(sender);

    let mut expected = vec![0u64; producers];
    let mut run = || {
        runtime.block_on(async {
            let mut checksum = 0;
            for _ in 0..BATCH_MESSAGES / burst_messages {
                let first = {
                    // Exclude Tokio's cooperative-budget Pending from the initial empty probe.
                    // Retain the same `recv_async` future so its channel registration drives the
                    // wake.
                    let mut receive =
                        pin!(tokio::task::unconstrained(C::recv_async(&mut receiver)));
                    let mut released = false;
                    poll_fn(|cx| {
                        let result = receive.as_mut().poll(cx);
                        if !released {
                            assert!(
                                result.is_pending(),
                                "each burst must start with an empty wait"
                            );
                            released = true;
                            for producer in &start {
                                producer.notify_one();
                            }
                        }
                        result
                    })
                    .await
                };
                checksum += check_inline_message(first, &mut expected);
                for _ in 1..burst_messages {
                    checksum +=
                        check_inline_message(C::recv_async(&mut receiver).await, &mut expected);
                }
            }
            assert_eq!(checksum, BATCH_MESSAGES);
            assert!(expected.iter().all(|count| *count == expected[0]));
            black_box(checksum)
        })
    };
    // Reuse the channel and tasks, including across bursts. Timing includes producer release,
    // channel wakeups, concurrent payload movement, and storage reclamation, not task creation.
    run();
    bencher.bench_local(run);

    stop.store(true, Ordering::Release);
    for producer in &start {
        producer.notify_one();
    }
    runtime.block_on(async {
        for worker in workers {
            worker.await.expect("benchmark producer panicked");
        }
    });
}

pub fn register(runner: &mut Runner) {
    register_implementation::<Asyncband>(runner, "Asyncband");
    register_implementation::<Tokio>(runner, "Tokio");
    register_implementation::<AsyncChannel>(runner, "AsyncChannel");
    register_implementation::<Flume>(runner, "Flume");
}

fn register_implementation<C>(runner: &mut Runner, implementation: &str)
where
    C: UnboundedMpsc
        + UnboundedMpsc<[u8; 64]>
        + UnboundedMpsc<[u8; 1024]>
        + UnboundedMpsc<Box<[u8; 1024]>>,
{
    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::ready_send_receive")
        ),
        30_000_000,
        1,
        ready_send_receive::<C>,
    );

    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::try_send_receive")
        ),
        30_000_000,
        1,
        try_send_receive::<C>,
    );

    for &arg in &[32, 1024, 65_536] {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::burst_drain")
            ),
            (32_000_000 / arg).max(1),
            1,
            |bencher| burst_drain::<C>(bencher, arg),
        );
    }
    {
        let &arg = &1024;
        runner.case(
            format!(
                "{}/{implementation}/64/{arg}",
                concat!(module_path!(), "::burst_drain_inline")
            ),
            (8_000_000 / arg).max(1),
            1,
            |bencher| burst_drain_inline::<C, 64>(bencher, arg),
        );
        runner.case(
            format!(
                "{}/{implementation}/1024/{arg}",
                concat!(module_path!(), "::burst_drain_inline")
            ),
            (8_000_000 / arg).max(1),
            1,
            |bencher| burst_drain_inline::<C, 1024>(bencher, arg),
        );
    }
    {
        let &arg = &1024;
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::burst_drain_boxed")
            ),
            (5_000_000 / arg).max(1),
            1,
            |bencher| burst_drain_boxed::<C>(bencher, arg),
        );
    }
    for &arg in &[1024, 65_536] {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::burst_with_backlog")
            ),
            (32_000_000 / arg).max(1),
            1,
            |bencher| burst_with_backlog::<C>(bencher, arg),
        );
    }
    for &arg in PRODUCER_COUNTS {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::reused_threads")
            ),
            320,
            1,
            |bencher| reused_threads::<C>(bencher, arg),
        );
    }
    for &arg in TASK_SHAPES {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::reused_tasks")
            ),
            512,
            1,
            |bencher| reused_tasks::<C>(bencher, arg),
        );
    }
    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::clone_drop_sender")
        ),
        96_000_000,
        1,
        clone_drop_sender::<C>,
    );

    for &arg in &[(1, 64), (8, 64), (1, 1024), (8, 1024)] {
        runner.case(
            format!(
                "{}/{implementation}/{arg:?}",
                concat!(module_path!(), "::scheduled_bursts_inline")
            ),
            64,
            1,
            |bencher| scheduled_bursts_inline::<C>(bencher, arg),
        );
    }
}
