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
use benchmarks::implementations;
use tokio::runtime::Builder;
use tokio::sync::Notify;

use crate::mpsc::adapters::AsyncChannel;
use crate::mpsc::adapters::Asyncband;
use crate::mpsc::adapters::Flume;
use crate::mpsc::adapters::Tokio;
use crate::mpsc::adapters::UnboundedMpsc;
use crate::mpsc::support::BATCH_MESSAGES;
use crate::mpsc::support::RepeatedTasks;
use crate::mpsc::support::TaskShape;
use crate::mpsc::support::Unbounded;

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
    runner.compare(
        "mpsc/unbounded/burst-1024",
        41_000,
        implementations!(
            [Asyncband, Tokio, AsyncChannel, Flume],
            |C, b| burst_drain::<C>(b, 1024)
        ),
    );
    runner.compare(
        "mpsc/unbounded/burst-1024-retain-512",
        41_000,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |C, b| {
            burst_with_backlog::<C>(b, 1024)
        }),
    );
    runner.compare(
        "mpsc/unbounded/burst-1024-inline1k",
        5_400,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |C, b| {
            burst_drain_inline::<C, 1024>(b, 1024)
        }),
    );
    runner.compare(
        "mpsc/unbounded/burst-1024-boxed1k",
        5_700,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |C, b| {
            burst_drain_boxed::<C>(b, 1024)
        }),
    );
    runner.compare(
        "mpsc/unbounded/tasks-4p1c-4w",
        690,
        implementations!(
            [Asyncband, Tokio, AsyncChannel, Flume],
            |C, b| reused_tasks::<C>(
                b,
                TaskShape {
                    producers: 4,
                    workers: 4
                }
            )
        ),
    );
    runner.compare(
        "mpsc/unbounded/scheduled-1p1c-bursts64-inline1k",
        64,
        implementations!([Asyncband, Tokio, AsyncChannel, Flume], |C, b| {
            scheduled_bursts_inline::<C>(b, (1, 64))
        }),
    );
}
