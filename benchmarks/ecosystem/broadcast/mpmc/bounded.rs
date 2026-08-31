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
// `concurrent` sweeps capacity as well as producer and receiver counts, because the ratio of
// capacity to fanout is what decides how a bounded broadcast behaves. A backlog smaller than the
// fanout turns every message into a round trip — publish one value, wake every subscription, wait
// for the one slot to come back — while a roomy backlog lets both sides batch. The two regimes
// differ by an order of magnitude on every implementation measured, so reporting one capacity
// would describe half the channel.

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
use super::support::ROUND_TRIP_CAPACITY;
use crate::support::bench_context;

// Send-then-receive pairing keeps at most one message retained, so these never reach capacity.
#[divan::bench(types = [Asyncband, AsyncBroadcast])]
fn try_round_trip<C: BoundedBroadcastMpmc>(bencher: Bencher) {
    let (sender, mut receivers) = C::channel(ROUND_TRIP_CAPACITY, 1);
    let mut receiver = receivers.pop().unwrap();

    bencher.bench_local(|| {
        C::try_send(&sender, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver).unwrap())
    });
}

#[divan::bench(types = [Asyncband, AsyncBroadcast])]
fn ready_round_trip<C: BoundedBroadcastMpmc>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(ROUND_TRIP_CAPACITY, 1);
    let mut receiver = receivers.pop().unwrap();

    bencher.bench_local(|| {
        C::send_ready(&sender, black_box(usize::MAX), &mut context);
        black_box(C::recv_ready(&mut receiver, &mut context))
    });
}

// `sample_size = 1` is required: `BoundedConcurrent` spawns its workers once and they exit after a
// single pass, so a second `run` on the same value would block forever.
#[divan::bench(
    types = [Asyncband, AsyncBroadcast],
    args = BOUNDED_SHAPES,
    sample_count = 10,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn concurrent<C: BoundedBroadcastMpmc>(bencher: Bencher, shape: BoundedShape) {
    bencher
        .with_inputs(|| BoundedConcurrent::<C>::new(shape))
        .bench_local_refs(BoundedConcurrent::run);
}
