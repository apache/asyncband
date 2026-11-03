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

use benchmarks::channels::adapters::AsyncChannel;
use benchmarks::channels::adapters::AsyncbandMpmc;
use benchmarks::channels::adapters::AsyncbandSpmc;
use benchmarks::channels::adapters::Bounded;
use benchmarks::channels::adapters::Channel;
use benchmarks::channels::adapters::Flume;
use benchmarks::channels::runtime;
use benchmarks::channels::spmc::TaskBatch;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::implementations;

fn task_batch<C, const WORKERS: usize>(bencher: Bencher, consumers: usize)
where
    Bounded<C>: Channel,
{
    let runtime = runtime(WORKERS);
    bencher.bench_local(|| {
        let mut batch = TaskBatch::new::<Bounded<C>>(&runtime, consumers);
        runtime.block_on(batch.run())
    });
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "spmc/bounded/tasks-1p4c-cap64-4w",
        320,
        implementations!(
            [AsyncbandSpmc, AsyncbandMpmc, AsyncChannel, Flume],
            |C, b| task_batch::<C, 4>(b, 4)
        ),
    );
}
