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

use crate::mpmc_support::adapters::Asyncband;
use crate::mpmc_support::support::BATCH_MESSAGES;
use crate::mpmc_support::support::BOUNDED_CAPACITY;
use crate::mpmc_support::support::Bounded;
use crate::mpmc_support::support::TOPOLOGIES;
use crate::mpmc_support::support::TaskBatch;
use crate::mpmc_support::support::ThreadBatch;
use crate::mpmc_support::support::Topology;
use crate::mpmc_support::support::runtime;

#[divan::bench(
    args = TOPOLOGIES,
    sample_count = 20,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn blocking_threads(bencher: Bencher, topology: Topology) {
    bencher
        .with_inputs(|| ThreadBatch::new_bounded::<Asyncband>(BOUNDED_CAPACITY, topology))
        .bench_local_refs(|batch| batch.run());
}

#[divan::bench(
    consts = [0, 4],
    args = TOPOLOGIES,
    sample_count = 20,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn tokio_tasks<const WORKERS: usize>(bencher: Bencher, topology: Topology) {
    let runtime = runtime(WORKERS);
    bencher
        .with_inputs(|| TaskBatch::new::<Bounded<Asyncband>>(&runtime, topology))
        .bench_local_refs(|batch| runtime.block_on(batch.run()));
}
