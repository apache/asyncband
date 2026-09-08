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
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::adapters::AsyncChannel;
use super::adapters::Asyncband;
use super::adapters::Flume;
use super::adapters::Tokio;
use super::adapters::UnboundedMpsc;
use super::support::BATCH_MESSAGES;
use super::support::ConcurrentBatch;
use super::support::PRODUCER_COUNTS;
use super::support::RepeatedBatch;
use super::support::RepeatedTasks;
use super::support::Unbounded;
use crate::support::bench_context;

#[divan::bench(types = [Asyncband, Tokio, AsyncChannel, Flume])]
fn ready_round_trip<C: UnboundedMpsc>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receiver) = C::channel();

    bencher.bench_local(|| {
        C::send(&sender, black_box(usize::MAX));
        black_box(C::recv_ready(&mut receiver, &mut context))
    });
}

#[divan::bench(types = [Asyncband, Tokio, AsyncChannel, Flume])]
fn try_round_trip<C: UnboundedMpsc>(bencher: Bencher) {
    let (sender, mut receiver) = C::channel();

    bencher.bench_local(|| {
        C::send(&sender, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver))
    });
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Flume],
    args = PRODUCER_COUNTS,
    sample_count = 20,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn concurrent<C: UnboundedMpsc>(bencher: Bencher, producer_count: usize) {
    bencher
        .with_inputs(|| ConcurrentBatch::<Unbounded<C>>::new(producer_count))
        .bench_local_refs(|batch| batch.run());
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Flume],
    args = [32, 1024, 65_536],
    sample_count = 20,
    sample_size = 1,
)]
fn burst_drain<C: UnboundedMpsc>(bencher: Bencher, messages: usize) {
    repeated_bursts::<C, _, _>(bencher, messages, 0, || usize::MAX);
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Flume],
    consts = [64, 1024],
    args = [32, 1024, 65_536],
    sample_count = 20,
    sample_size = 1,
)]
fn burst_drain_inline<C: UnboundedMpsc<[u8; SIZE]>, const SIZE: usize>(
    bencher: Bencher,
    messages: usize,
) {
    repeated_bursts::<C, _, _>(bencher, messages, 0, || [1; SIZE]);
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Flume],
    args = [32, 1024, 65_536],
    sample_count = 20,
    sample_size = 1,
)]
fn burst_drain_boxed<C: UnboundedMpsc<Box<[u8; 1024]>>>(bencher: Bencher, messages: usize) {
    // Include payload allocation and destruction to compare the complete boxed-message lifecycle.
    repeated_bursts::<C, _, _>(bencher, messages, 0, || Box::new([1; 1024]));
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Flume],
    args = [1024, 65_536],
    sample_count = 20,
    sample_size = 1,
)]
fn burst_with_backlog<C: UnboundedMpsc>(bencher: Bencher, messages: usize) {
    repeated_bursts::<C, _, _>(bencher, messages, messages / 2, || usize::MAX);
}

fn repeated_bursts<C: UnboundedMpsc<T>, T, F: Fn() -> T>(
    bencher: Bencher,
    messages: usize,
    backlog: usize,
    make_value: F,
) {
    // Keep one channel alive across samples so allocation reuse and reclamation are measured.
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
    // backlog where requested. Do not require an extra empty receive to trigger reclamation.
    run();
    bencher.counter(ItemsCount::new(messages)).bench_local(run);
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Flume],
    args = PRODUCER_COUNTS,
    sample_count = 50,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn sustained<C: UnboundedMpsc>(bencher: Bencher, producer_count: usize) {
    let mut batch = RepeatedBatch::<Unbounded<C>>::new(producer_count);
    batch.run();
    bencher.bench_local(|| batch.run());
}

#[divan::bench(types = [Asyncband, Tokio, AsyncChannel, Flume])]
fn clone_drop_sender<C: UnboundedMpsc>(bencher: Bencher) {
    let (sender, _receiver) = C::channel();
    bencher.bench_local(|| drop(black_box(sender.clone())));
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Flume],
    args = [(1, 0), (4, 0), (1, 4), (4, 4), (8, 4)],
    sample_count = 50,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn scheduled<C: UnboundedMpsc>(bencher: Bencher, (producers, workers): (usize, usize)) {
    let mut batch = RepeatedTasks::<Unbounded<C>>::new(producers, workers);
    batch.run();
    bencher.bench_local(|| batch.run());
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Flume],
    args = [(1, 64), (4, 64), (8, 64), (1, 1024), (4, 1024), (8, 1024)],
    sample_count = 50,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn scheduled_bursts_inline<C: UnboundedMpsc<[u8; 1024]>>(
    bencher: Bencher,
    (producers, burst_messages): (usize, usize),
) {
    assert_eq!(BATCH_MESSAGES % burst_messages, 0);
    assert_eq!(burst_messages % producers, 0);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    let (sender, mut receiver) = C::channel();
    let start: Vec<_> = (0..producers)
        .map(|_| Arc::new(tokio::sync::Notify::new()))
        .collect();
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
                    // Retain the same receive future so its channel registration drives the wake.
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

fn check_inline_message(value: [u8; 1024], expected: &mut [u64]) -> usize {
    let value = black_box(value);
    let producer = u64::from_le_bytes(value[..8].try_into().unwrap()) as usize;
    let sequence = u64::from_le_bytes(value[8..16].try_into().unwrap());
    assert_eq!(sequence, expected[producer]);
    expected[producer] += 1;
    usize::from(value[1023])
}
