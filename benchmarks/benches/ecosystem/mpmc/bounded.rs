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
use benchmarks::channels::adapters::AsyncChannel;
use benchmarks::channels::adapters::Bounded;
use benchmarks::channels::adapters::BoundedMpmc;
use benchmarks::channels::adapters::Flume;
use benchmarks::channels::adapters::Mpmc;
use benchmarks::channels::mpmc::TOPOLOGIES;
use benchmarks::channels::mpmc::TaskBatch;
use benchmarks::channels::mpmc::ThreadBatch;
use benchmarks::channels::mpmc::Topology;
use benchmarks::channels::runtime;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;

fn thread_batch<C: BoundedMpmc>(bencher: Bencher, topology: Topology) {
    bencher.bench_local(|| {
        let mut batch = ThreadBatch::new_bounded::<C>(BOUNDED_CAPACITY, topology);
        batch.run()
    });
}

fn task_batch<C: BoundedMpmc, const WORKERS: usize>(bencher: Bencher, topology: Topology) {
    let runtime = runtime(WORKERS);
    bencher.bench_local(|| {
        let mut batch = TaskBatch::new::<Bounded<C>>(&runtime, topology);
        runtime.block_on(batch.run())
    });
}

pub fn register(runner: &mut Runner) {
    register_implementation::<Mpmc>(runner, "Mpmc");
    register_implementation::<AsyncChannel>(runner, "AsyncChannel");
    register_implementation::<Flume>(runner, "Flume");
}

fn register_implementation<C>(runner: &mut Runner, implementation: &str)
where
    C: BoundedMpmc,
{
    for &arg in TOPOLOGIES {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::thread_batch")
            ),
            32,
            1,
            |bencher| thread_batch::<C>(bencher, arg),
        );
    }
    for &arg in TOPOLOGIES {
        runner.case(
            format!(
                "{}/{implementation}/0/{arg}",
                concat!(module_path!(), "::task_batch")
            ),
            320,
            1,
            |bencher| task_batch::<C, 0>(bencher, arg),
        );
        runner.case(
            format!(
                "{}/{implementation}/4/{arg}",
                concat!(module_path!(), "::task_batch")
            ),
            320,
            1,
            |bencher| task_batch::<C, 4>(bencher, arg),
        );
    }
}
