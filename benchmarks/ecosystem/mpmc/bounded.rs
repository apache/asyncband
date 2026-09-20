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
use divan::counter::ItemsCount;

use crate::channels::BATCH_MESSAGES;
use crate::channels::BOUNDED_CAPACITY;
use crate::channels::adapters::AsyncChannel;
use crate::channels::adapters::Bounded;
use crate::channels::adapters::BoundedMpmc;
use crate::channels::adapters::Flume;
use crate::channels::adapters::Mpmc;
use crate::channels::mpmc::TOPOLOGIES;
use crate::channels::mpmc::TaskBatch;
use crate::channels::mpmc::ThreadBatch;
use crate::channels::mpmc::Topology;
use crate::channels::runtime;

#[divan::bench(
    types = [Mpmc, AsyncChannel, Flume],
    args = TOPOLOGIES,
    sample_count = 20,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn blocking_threads<C: BoundedMpmc>(bencher: Bencher, topology: Topology) {
    bencher
        .with_inputs(|| ThreadBatch::new_bounded::<C>(BOUNDED_CAPACITY, topology))
        .bench_local_refs(|batch| batch.run());
}

#[divan::bench(
    types = [Mpmc, AsyncChannel, Flume],
    consts = [0, 4],
    args = TOPOLOGIES,
    sample_count = 20,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn tokio_tasks<C: BoundedMpmc, const WORKERS: usize>(bencher: Bencher, topology: Topology) {
    let runtime = runtime(WORKERS);
    bencher
        .with_inputs(|| TaskBatch::new::<Bounded<C>>(&runtime, topology))
        .bench_local_refs(|batch| runtime.block_on(batch.run()));
}
