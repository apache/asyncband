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
use benchmarks::channels::adapters::Flume;
use benchmarks::channels::adapters::Unbounded;
use benchmarks::channels::adapters::UnboundedMpmc;
use benchmarks::channels::mpmc::TaskBatch;
use benchmarks::channels::mpmc::ThreadBatch;
use benchmarks::channels::mpmc::Topology;
use benchmarks::channels::runtime;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::implementations;

fn thread_batch<C: UnboundedMpmc>(bencher: Bencher, topology: Topology) {
    bencher.bench_local(|| {
        let mut batch = ThreadBatch::new_unbounded::<C>(topology);
        batch.run()
    });
}

fn task_batch<C: UnboundedMpmc, const WORKERS: usize>(bencher: Bencher, topology: Topology) {
    let runtime = runtime(WORKERS);
    bencher.bench_local(|| {
        let mut batch = TaskBatch::new::<Unbounded<C>>(&runtime, topology);
        runtime.block_on(batch.run())
    });
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "mpmc/unbounded/tasks-2p2c-4w",
        330,
        implementations!([AsyncbandMpmc, AsyncChannel, Flume], |C, b| task_batch::<
            C,
            4,
        >(
            b,
            Topology {
                producers: 2,
                consumers: 2
            }
        )),
    );
    runner.compare(
        "mpmc/unbounded/threads-2p2c",
        280,
        implementations!(
            [AsyncbandMpmc, AsyncChannel, Flume],
            |C, b| thread_batch::<C>(
                b,
                Topology {
                    producers: 2,
                    consumers: 2
                }
            )
        ),
    );
}
