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

// Bounded peers have room for the entire batch, excluding their differing overflow policies.

use std::hint::black_box;

use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;

use crate::broadcast::mpmc::adapters::AsyncBroadcast;
use crate::broadcast::mpmc::adapters::Asyncband;
use crate::broadcast::mpmc::adapters::BroadcastMpmc;
use crate::broadcast::mpmc::adapters::Tokio;
use crate::broadcast::mpmc::support::ConcurrentSend;
use crate::broadcast::mpmc::support::Fanout;
use crate::broadcast::mpmc::support::PRODUCER_COUNTS;
use crate::broadcast::mpmc::support::RECEIVER_COUNTS;
use crate::broadcast::mpmc::support::ROUND_TRIP_CAPACITY;

fn try_send_receive<C: BroadcastMpmc>(bencher: Bencher) {
    let (sender, mut receivers) = C::channel(ROUND_TRIP_CAPACITY, 1);
    let mut receiver = receivers.pop().unwrap();

    bencher.bench_local(|| {
        C::send(&sender, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver).unwrap())
    });
}

fn ready_send_receive<C: BroadcastMpmc>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(ROUND_TRIP_CAPACITY, 1);
    let mut receiver = receivers.pop().unwrap();

    bencher.bench_local(|| {
        C::send(&sender, black_box(usize::MAX));
        black_box(C::recv_ready(&mut receiver, &mut context))
    });
}

fn publish_batch_then_drain<C: BroadcastMpmc>(bencher: Bencher, producer_count: usize) {
    bencher.bench_local(|| {
        let mut input = ConcurrentSend::<C>::new(producer_count);
        ConcurrentSend::run(&mut input)
    });
}

fn publish_then_parallel_drain<C: BroadcastMpmc>(bencher: Bencher, receiver_count: usize) {
    bencher.bench_local(|| {
        let mut input = Fanout::<C>::new(receiver_count);
        Fanout::run(&mut input)
    });
}

pub fn register(runner: &mut Runner) {
    register_implementation::<Asyncband>(runner, "Asyncband");
    register_implementation::<Tokio>(runner, "Tokio");
    register_implementation::<AsyncBroadcast>(runner, "AsyncBroadcast");
}

fn register_implementation<C>(runner: &mut Runner, implementation: &str)
where
    C: BroadcastMpmc,
{
    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::try_send_receive")
        ),
        12_000_000,
        1,
        try_send_receive::<C>,
    );

    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::ready_send_receive")
        ),
        10_000_000,
        1,
        ready_send_receive::<C>,
    );

    for &arg in PRODUCER_COUNTS {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::publish_batch_then_drain")
            ),
            1_024,
            1,
            |bencher| publish_batch_then_drain::<C>(bencher, arg),
        );
    }
    for &arg in RECEIVER_COUNTS {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::publish_then_parallel_drain")
            ),
            640,
            1,
            |bencher| publish_then_parallel_drain::<C>(bencher, arg),
        );
    }
}
