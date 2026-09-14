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

// Lossless bounded path: producers wait at capacity. Tokio is absent because it overwrites.

use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::adapters::AsyncBroadcast;
use super::adapters::Asyncband;
use super::adapters::AsyncbandMpmc;
use super::adapters::BoundedBroadcastSpmc;
use super::support::BATCH_MESSAGES;
use super::support::BOUNDED_SHAPES;
use super::support::BoundedConcurrent;
use super::support::BoundedShape;
use super::support::BoundedTasks;
use super::support::ROUND_TRIP_CAPACITY;
use crate::support::bench_context;

#[divan::bench(types = [Asyncband, AsyncbandMpmc, AsyncBroadcast], sample_size = 512)]
fn try_round_trip<C: BoundedBroadcastSpmc>(bencher: Bencher) {
    let (mut sender, mut receivers) = C::channel(ROUND_TRIP_CAPACITY, 1);
    let mut receiver = receivers.pop().unwrap();

    bencher.bench_local(|| {
        C::try_send(&mut sender, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver).unwrap())
    });
}

#[divan::bench(types = [Asyncband, AsyncbandMpmc, AsyncBroadcast], sample_size = 512)]
fn ready_round_trip<C: BoundedBroadcastSpmc>(bencher: Bencher) {
    let mut context = bench_context();
    let (mut sender, mut receivers) = C::channel(ROUND_TRIP_CAPACITY, 1);
    let mut receiver = receivers.pop().unwrap();

    bencher.bench_local(|| {
        C::send_ready(&mut sender, black_box(usize::MAX), &mut context);
        black_box(C::recv_ready(&mut receiver, &mut context))
    });
}

#[divan::bench(
    types = [Asyncband, AsyncbandMpmc, AsyncBroadcast],
    args = BOUNDED_SHAPES,
    sample_count = 10,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn concurrent<C: BoundedBroadcastSpmc>(bencher: Bencher, shape: BoundedShape) {
    bencher
        .with_inputs(|| BoundedConcurrent::new::<C>(shape))
        .bench_local_refs(BoundedConcurrent::run);
}

#[divan::bench(
    types = [Asyncband, AsyncbandMpmc, AsyncBroadcast],
    args = BOUNDED_SHAPES,
    sample_count = 10,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn scheduled<C: BoundedBroadcastSpmc>(bencher: Bencher, shape: BoundedShape) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    bencher
        .with_inputs(|| BoundedTasks::new::<C>(&runtime, shape))
        .bench_local_refs(|tasks| tasks.run(&runtime));
}
