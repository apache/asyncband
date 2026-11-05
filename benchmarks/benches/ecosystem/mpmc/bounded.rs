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

use benchmarks::channels::BOUNDED_CAPACITY;
use benchmarks::channels::adapters::AsyncBoundedMpmc;
use benchmarks::channels::adapters::AsyncChannel;
use benchmarks::channels::adapters::AsyncbandMpmc;
use benchmarks::channels::adapters::AsyncbandMpmcReserve;
use benchmarks::channels::adapters::Bounded;
use benchmarks::channels::adapters::BoundedMpmc;
use benchmarks::channels::adapters::Crossbeam;
use benchmarks::channels::adapters::Flume;
use benchmarks::channels::mpmc::TaskBatch;
use benchmarks::channels::mpmc::ThreadBatch;
use benchmarks::channels::mpmc::Topology;
use benchmarks::channels::runtime;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::implementations;

const TWO_BY_TWO: Topology = Topology {
    producers: 2,
    consumers: 2,
};

fn thread_batch<C: BoundedMpmc>(bencher: Bencher, capacity: usize, topology: Topology) {
    bencher.bench_local(|| {
        let mut batch = ThreadBatch::new_bounded::<C>(capacity, topology);
        batch.run()
    });
}

/// Endpoints spin on the nonblocking operations, so this measures queue contention without
/// parking or wakeups.
fn spin_batch<C: BoundedMpmc>(bencher: Bencher, topology: Topology) {
    bencher.bench_local(|| {
        let mut batch = ThreadBatch::new_bounded_spinning::<C>(BOUNDED_CAPACITY, topology);
        batch.run()
    });
}

fn task_batch<C: AsyncBoundedMpmc, const WORKERS: usize>(bencher: Bencher, topology: Topology) {
    let runtime = runtime(WORKERS);
    bencher.bench_local(|| {
        let mut batch = TaskBatch::new::<Bounded<C>>(&runtime, topology);
        runtime.block_on(batch.run())
    });
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "mpmc/bounded/tasks-2p2c-cap64-4w",
        270,
        implementations!(
            [AsyncbandMpmc, AsyncbandMpmcReserve, AsyncChannel, Flume],
            |C, b| task_batch::<C, 4>(b, TWO_BY_TWO)
        ),
    );
    runner.compare(
        "mpmc/bounded/threads-2p2c-cap64",
        120,
        implementations!(
            [
                AsyncbandMpmc,
                AsyncbandMpmcReserve,
                AsyncChannel,
                Flume,
                Crossbeam
            ],
            |C, b| thread_batch::<C>(b, BOUNDED_CAPACITY, TWO_BY_TWO)
        ),
    );
    // One producer wakes a parked consumer for most messages.
    runner.compare(
        "mpmc/bounded/threads-1p8c-cap64",
        27,
        implementations!([AsyncbandMpmc, AsyncChannel, Flume, Crossbeam], |C, b| {
            thread_batch::<C>(
                b,
                BOUNDED_CAPACITY,
                Topology {
                    producers: 1,
                    consumers: 8,
                },
            )
        }),
    );
    // Every send waits for a receiver to free the only slot.
    runner.compare(
        "mpmc/bounded/threads-2p2c-cap1",
        4,
        implementations!([AsyncbandMpmc, AsyncChannel, Flume, Crossbeam], |C, b| {
            thread_batch::<C>(b, 1, TWO_BY_TWO)
        }),
    );
    runner.compare(
        "mpmc/bounded/spin-2p2c-cap64",
        100,
        implementations!([AsyncbandMpmc, AsyncChannel, Flume, Crossbeam], |C, b| {
            spin_batch::<C>(b, TWO_BY_TWO)
        }),
    );
}
