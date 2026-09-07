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
