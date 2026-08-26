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
use super::adapters::BoundedMpsc;
use super::adapters::Flume;
use super::adapters::Tokio;
use super::support::BATCH_MESSAGES;
use super::support::BOUNDED_CAPACITY;
use super::support::Bounded;
use super::support::ConcurrentBatch;
use super::support::PRODUCER_COUNTS;
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
