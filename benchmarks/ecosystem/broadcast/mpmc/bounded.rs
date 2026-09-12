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

// Asyncband and async-broadcast are compared here because both are lossless and both make
// producers wait at capacity, so a small channel measures the same contract on each side.
//
// `tokio::sync::broadcast` is deliberately absent. It overwrites at capacity and reports `Lagged`
// rather than waiting, so it has no lossless bounded path to compare: it would be measuring the
// cheaper workload of dropping messages. It appears in `unbounded.rs` instead, where every peer is
// given room for the whole batch and the comparison is over their shared non-blocking path.
//
// Sweep capacity and producer/subscription counts independently. Capacity one measures the
// per-message handoff; larger backlogs allow several messages to be outstanding. How effectively
// that headroom is used depends on scheduling and the slowest subscription, not just fanout.

use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::adapters::AsyncBroadcast;
use super::adapters::Asyncband;
use super::adapters::BoundedBroadcastMpmc;
use super::support::BATCH_MESSAGES;
use super::support::BOUNDED_SHAPES;
use super::support::BoundedConcurrent;
use super::support::BoundedShape;
use super::support::BoundedTasks;
use super::support::ROUND_TRIP_CAPACITY;
use crate::support::bench_context;

// Send-then-receive pairing keeps at most one message retained, so these never reach capacity.
#[divan::bench(types = [Asyncband, AsyncBroadcast], sample_size = 512)]
fn try_round_trip<C: BoundedBroadcastMpmc>(bencher: Bencher) {
    let (sender, mut receivers) = C::channel(ROUND_TRIP_CAPACITY, 1);
    let mut receiver = receivers.pop().unwrap();

    bencher.bench_local(|| {
        C::try_send(&sender, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver).unwrap())
    });
}

#[divan::bench(types = [Asyncband, AsyncBroadcast], sample_size = 512)]
fn ready_round_trip<C: BoundedBroadcastMpmc>(bencher: Bencher) {
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
fn concurrent<C: BoundedBroadcastMpmc>(bencher: Bencher, shape: BoundedShape) {
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
fn scheduled<C: BoundedBroadcastMpmc>(bencher: Bencher, shape: BoundedShape) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    bencher
        .with_inputs(|| BoundedTasks::new::<C>(&runtime, shape))
        .bench_local_refs(|tasks| tasks.run(&runtime));
}
