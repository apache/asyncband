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
use super::adapters::BoundedMpsc;
use super::adapters::Flume;
use super::adapters::Tokio;
use super::support::BATCH_MESSAGES;
use super::support::BOUNDED_CAPACITY;
use super::support::Bounded;
use super::support::ConcurrentBatch;
use super::support::PRODUCER_COUNTS;
use super::support::RepeatedBatch;
use super::support::RepeatedTasks;
use crate::support::bench_context;

#[divan::bench(types = [Asyncband, Tokio, AsyncChannel, Flume])]
fn try_round_trip<C: BoundedMpsc>(bencher: Bencher) {
    let (sender, mut receiver) = C::channel(BOUNDED_CAPACITY);

    bencher.bench_local(|| {
        C::try_send(&sender, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver))
    });
}

#[divan::bench(types = [Asyncband, Tokio, AsyncChannel, Flume])]
fn ready_round_trip<C: BoundedMpsc>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receiver) = C::channel(BOUNDED_CAPACITY);

    bencher.bench_local(|| {
        C::send_ready(&sender, black_box(usize::MAX), &mut context);
        black_box(C::recv_ready(&mut receiver, &mut context))
    });
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Flume],
    args = PRODUCER_COUNTS,
    sample_count = 20,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn concurrent<C: BoundedMpsc>(bencher: Bencher, producer_count: usize) {
    bencher
        .with_inputs(|| ConcurrentBatch::<Bounded<C>>::new(producer_count))
        .bench_local_refs(|batch| batch.run());
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Flume],
    args = PRODUCER_COUNTS,
    sample_count = 50,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn sustained<C: BoundedMpsc>(bencher: Bencher, producer_count: usize) {
    let mut batch = RepeatedBatch::<Bounded<C>>::new(producer_count);
    batch.run();
    bencher.bench_local(|| batch.run());
}

#[divan::bench(types = [Asyncband, Tokio, AsyncChannel, Flume])]
fn clone_drop_sender<C: BoundedMpsc>(bencher: Bencher) {
    let (sender, _receiver) = C::channel(BOUNDED_CAPACITY);
    bencher.bench_local(|| drop(black_box(sender.clone())));
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Flume],
    consts = [1, 4096],
    args = PRODUCER_COUNTS,
    sample_count = 50,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn sustained_capacity<C: BoundedMpsc, const CAPACITY: usize>(
    bencher: Bencher,
    producer_count: usize,
) {
    let mut batch = RepeatedBatch::<Bounded<C, CAPACITY>>::new(producer_count);
    batch.run();
    bencher.bench_local(|| batch.run());
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Flume],
    consts = [1, 64, 4096],
    args = [(1, 0), (4, 0), (1, 4), (4, 4), (8, 4)],
    sample_count = 50,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn scheduled<C: BoundedMpsc, const CAPACITY: usize>(
    bencher: Bencher,
    (producers, workers): (usize, usize),
) {
    let mut batch = RepeatedTasks::<Bounded<C, CAPACITY>>::new(producers, workers);
    batch.run();
    bencher.bench_local(|| batch.run());
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Flume],
    consts = [64, 4096],
    args = [1, 8],
    sample_count = 50,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn scheduled_inline<C: BoundedMpsc<[u8; 1024]>, const CAPACITY: usize>(
    bencher: Bencher,
    producers: usize,
) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    let (sender, mut receiver) = C::channel(CAPACITY);
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
                    for _ in 0..BATCH_MESSAGES / producers {
                        let mut value = [1; 1024];
                        value[..8].copy_from_slice(&(producer as u64).to_le_bytes());
                        value[8..16].copy_from_slice(&sequence.to_le_bytes());
                        C::send_async(&sender, black_box(value)).await;
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
            let first = {
                let mut receive = pin!(tokio::task::unconstrained(C::recv_async(&mut receiver)));
                let mut released = false;
                poll_fn(|cx| {
                    let result = receive.as_mut().poll(cx);
                    if !released {
                        assert!(result.is_pending(), "each sample starts with an empty wait");
                        released = true;
                        for producer in &start {
                            producer.notify_one();
                        }
                    }
                    result
                })
                .await
            };
            let mut value = first;
            for received in 0..BATCH_MESSAGES {
                let producer = u64::from_le_bytes(value[..8].try_into().unwrap()) as usize;
                let sequence = u64::from_le_bytes(value[8..16].try_into().unwrap());
                assert_eq!(sequence, expected[producer]);
                expected[producer] += 1;
                assert_eq!(black_box(value)[1023], 1);
                if received + 1 < BATCH_MESSAGES {
                    value = C::recv_async(&mut receiver).await;
                }
            }
            assert!(expected.iter().all(|count| *count == expected[0]));
        })
    };
    // Reuse tasks and the channel. Include the initial empty wait, backpressure, and payload
    // movement; verify per-producer order and payload integrity on every measured sample.
    run();
    bencher.bench_local(run);
    stop.store(true, Ordering::Release);
    for producer in &start {
        producer.notify_one();
    }
    runtime.block_on(async {
        for worker in workers {
            worker.await.unwrap();
        }
    });
}
