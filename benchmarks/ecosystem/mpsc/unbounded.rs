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

use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::adapters::AsyncChannel;
use super::adapters::AsyncUnboundedMpsc;
use super::adapters::Asyncband;
use super::adapters::Crossbeam;
use super::adapters::Flume;
use super::adapters::Tokio;
use super::adapters::UnboundedMpsc;
use super::support::BATCH_MESSAGES;
use super::support::ConcurrentBatch;
use super::support::PRODUCER_COUNTS;
use super::support::Unbounded;
use crate::support::bench_context;

const SEQUENTIAL_MESSAGES: usize = 5_000;
const ASYNC_PRODUCERS: usize = 5;
const ASYNC_MESSAGES_PER_PRODUCER: usize = 1_000;

#[divan::bench(types = [Asyncband, Tokio, AsyncChannel, Crossbeam, Flume])]
fn create<C: UnboundedMpsc>(bencher: Bencher) {
    bencher.bench_local(|| black_box(C::channel()));
}

#[divan::bench(types = [Asyncband, Tokio, AsyncChannel, Crossbeam, Flume])]
fn oneshot<C: UnboundedMpsc>(bencher: Bencher) {
    bencher.bench_local(|| {
        let (sender, mut receiver) = C::channel();
        C::send(&sender, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver))
    });
}

#[divan::bench(types = [Asyncband, Tokio, AsyncChannel, Crossbeam, Flume])]
fn ready_round_trip<C: UnboundedMpsc>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receiver) = C::channel();

    bencher.bench_local(|| {
        C::send(&sender, black_box(usize::MAX));
        black_box(C::recv_ready(&mut receiver, &mut context))
    });
}

#[divan::bench(types = [Asyncband, Tokio, AsyncChannel, Crossbeam, Flume])]
fn try_round_trip<C: UnboundedMpsc>(bencher: Bencher) {
    let (sender, mut receiver) = C::channel();

    bencher.bench_local(|| {
        C::send(&sender, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver))
    });
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Crossbeam, Flume],
    sample_count = 100,
    sample_size = 1,
    counter = ItemsCount::new(SEQUENTIAL_MESSAGES),
)]
fn sequential<C: UnboundedMpsc>(bencher: Bencher) {
    bencher.bench_local(|| {
        let (sender, mut receiver) = C::channel();

        for value in 0..SEQUENTIAL_MESSAGES {
            C::send(&sender, black_box(value));
        }
        for _ in 0..SEQUENTIAL_MESSAGES {
            black_box(C::try_recv(&mut receiver));
        }
    });
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Crossbeam, Flume],
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
    sample_count = 20,
    sample_size = 1,
    counter = ItemsCount::new(ASYNC_PRODUCERS * ASYNC_MESSAGES_PER_PRODUCER),
)]
fn async_contention<C: AsyncUnboundedMpsc>(bencher: Bencher) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .build()
        .unwrap();

    bencher.bench_local(|| {
        runtime.block_on(async {
            let (sender, mut receiver) = C::channel();
            let workers = (0..ASYNC_PRODUCERS)
                .map(|producer| {
                    let sender = sender.clone();
                    tokio::spawn(async move {
                        let first = producer * ASYNC_MESSAGES_PER_PRODUCER;
                        for offset in 0..ASYNC_MESSAGES_PER_PRODUCER {
                            C::send(&sender, black_box(first + offset));
                        }
                    })
                })
                .collect::<Vec<_>>();
            drop(sender);

            let mut checksum = 0usize;
            for _ in 0..ASYNC_PRODUCERS * ASYNC_MESSAGES_PER_PRODUCER {
                checksum = checksum.wrapping_add(C::recv_async(&mut receiver).await);
            }
            for worker in workers {
                worker.await.unwrap();
            }
            black_box(checksum)
        })
    });
}
