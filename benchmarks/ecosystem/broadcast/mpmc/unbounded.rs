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

// Asyncband is the only unbounded channel in this comparison. Tokio broadcast overwrites messages
// at capacity, while async-broadcast applies backpressure by default. Every bounded peer gets room
// for the entire measured batch, so these workloads compare their common lossless, non-blocking
// path rather than their different lag and capacity policies.

use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::adapters::AsyncBroadcast;
use super::adapters::Asyncband;
use super::adapters::BroadcastMpmc;
use super::adapters::Tokio;
use super::support::BATCH_MESSAGES;
use super::support::ConcurrentSend;
use super::support::Fanout;
use super::support::PRODUCER_COUNTS;
use super::support::RECEIVER_COUNTS;
use super::support::ROUND_TRIP_CAPACITY;
use crate::support::bench_context;

#[divan::bench(types = [Asyncband, Tokio, AsyncBroadcast])]
fn try_round_trip<C: BroadcastMpmc>(bencher: Bencher) {
    let (sender, mut receivers) = C::channel(ROUND_TRIP_CAPACITY, 1);
    let mut receiver = receivers.pop().unwrap();

    bencher.bench_local(|| {
        C::send(&sender, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver).unwrap())
    });
}

#[divan::bench(types = [Asyncband, Tokio, AsyncBroadcast])]
fn ready_round_trip<C: BroadcastMpmc>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(ROUND_TRIP_CAPACITY, 1);
    let mut receiver = receivers.pop().unwrap();

    bencher.bench_local(|| {
        C::send(&sender, black_box(usize::MAX));
        black_box(C::recv_ready(&mut receiver, &mut context))
    });
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncBroadcast],
    args = PRODUCER_COUNTS,
    sample_count = 20,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn concurrent_producers<C: BroadcastMpmc>(bencher: Bencher, producer_count: usize) {
    bencher
        .with_inputs(|| ConcurrentSend::<C>::new(producer_count))
        .bench_local_refs(ConcurrentSend::run);
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncBroadcast],
    args = RECEIVER_COUNTS,
    sample_count = 20,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn fanout<C: BroadcastMpmc>(bencher: Bencher, receiver_count: usize) {
    bencher
        .with_inputs(|| Fanout::<C>::new(receiver_count))
        .bench_local_refs(Fanout::run);
}
