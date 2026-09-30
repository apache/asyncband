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

use benchmarks::support::bench_context;
use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::adapters::AsyncBroadcast;
use super::adapters::Asyncband;
use super::adapters::BoundedBroadcastMpmc;
use super::support::BACKPRESSURE_SHAPE;
use super::support::BATCH_MESSAGES;
use super::support::BOUNDED_SHAPES;
use super::support::BoundedConcurrent;
use super::support::BoundedShape;
use super::support::BoundedTasks;
use super::support::ROUND_TRIP_CAPACITY;

// Send-then-receive pairing keeps at most one message retained, so these never reach capacity.
#[divan::bench(types = [Asyncband, AsyncBroadcast], sample_size = 512)]
fn try_send_receive<C: BoundedBroadcastMpmc>(bencher: Bencher) {
    let (sender, mut receivers) = C::channel(ROUND_TRIP_CAPACITY, 1);
    let mut receiver = receivers.pop().unwrap();

    bencher.bench_local(|| {
        C::try_send(&sender, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver).unwrap())
    });
}

#[divan::bench(types = [Asyncband, AsyncBroadcast], sample_size = 512)]
fn ready_send_receive<C: BoundedBroadcastMpmc>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(ROUND_TRIP_CAPACITY, 1);
    let mut receiver = receivers.pop().unwrap();

    bencher.bench_local(|| {
        C::send_ready(&sender, black_box(usize::MAX), &mut context);
        black_box(C::recv_ready(&mut receiver, &mut context))
    });
}

// Keep one fixture per sample so workers from other fixtures are not alive during timing.
#[divan::bench(
    types = [Asyncband, AsyncBroadcast],
    args = BOUNDED_SHAPES,
    sample_count = 10,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn thread_batch<C: BoundedBroadcastMpmc>(bencher: Bencher, shape: BoundedShape) {
    bencher
        .with_inputs(|| BoundedConcurrent::new::<C>(shape))
        .bench_local_refs(BoundedConcurrent::run);
}

#[divan::bench(
    types = [Asyncband, AsyncBroadcast],
    args = BOUNDED_SHAPES,
    sample_count = 10,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn task_batch<C: BoundedBroadcastMpmc>(bencher: Bencher, shape: BoundedShape) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    bencher
        .with_inputs(|| BoundedTasks::new::<C>(&runtime, shape))
        .bench_local_refs(|tasks| tasks.run(&runtime));
}

#[divan::bench(types = [Asyncband, AsyncBroadcast], sample_count = 10, sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES))]
fn capacity_one_task_handoff<C: BoundedBroadcastMpmc>(bencher: Bencher) {
    let runtime = benchmarks::channels::runtime(4);
    bencher
        .with_inputs(|| BoundedTasks::new::<C>(&runtime, BACKPRESSURE_SHAPE))
        .bench_local_refs(|tasks| tasks.run(&runtime));
}
