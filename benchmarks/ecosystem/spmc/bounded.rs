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

use super::adapters::AsyncChannel;
use super::adapters::Bounded;
use super::adapters::Channel;
use super::adapters::Flume;
use super::adapters::Mpmc;
use super::adapters::Spmc;
use super::support::BATCH_MESSAGES;
use super::support::CONSUMERS;
use super::support::TaskBatch;
use super::support::runtime;

#[divan::bench(
    types = [Spmc, Mpmc, AsyncChannel, Flume],
    consts = [0, 4],
    args = CONSUMERS,
    sample_count = 20,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn tokio_tasks<C, const WORKERS: usize>(bencher: Bencher, consumers: usize)
where
    Bounded<C>: Channel,
{
    let runtime = runtime(WORKERS);
    bencher
        .with_inputs(|| TaskBatch::new::<Bounded<C>>(&runtime, consumers))
        .bench_local_refs(|batch| runtime.block_on(batch.run()));
}
