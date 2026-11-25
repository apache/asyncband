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
use benchmarks::implementations;
use benchmarks::support::bench_context;

use crate::broadcast::mpmc::adapters::AsyncBroadcast;
use crate::broadcast::mpmc::adapters::Asyncband;
use crate::broadcast::mpmc::adapters::BroadcastMpmc;
use crate::broadcast::mpmc::adapters::Payload;
use crate::broadcast::mpmc::adapters::Shared;
use crate::broadcast::mpmc::adapters::Tokio;
use crate::broadcast::mpmc::support::BATCH_MESSAGES;
use crate::broadcast::mpmc::support::ConcurrentSend;
use crate::broadcast::mpmc::support::EXPECTED_BATCH_CHECKSUM;
use crate::broadcast::mpmc::support::Fanout;
use crate::broadcast::mpmc::support::ROUND_TRIP_CAPACITY;
use crate::broadcast::mpmc::support::ReusedFanout;
use crate::broadcast::mpmc::support::recv;

fn try_send_receive<C: BroadcastMpmc<M>, M: Payload>(bencher: Bencher) {
    let (sender, mut receivers) = C::channel(ROUND_TRIP_CAPACITY, 1);
    let mut receiver = receivers.pop().unwrap();

    bencher.bench_local(|| {
        C::send(&sender, black_box(M::new(usize::MAX)));
        black_box(M::output_sequence(C::try_recv(&mut receiver).unwrap()))
    });
}

fn ready_send_receive<C: BroadcastMpmc<M>, M: Payload>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(ROUND_TRIP_CAPACITY, 1);
    let mut receiver = receivers.pop().unwrap();

    bencher.bench_local(|| {
        C::send(&sender, black_box(M::new(usize::MAX)));
        black_box(M::output_sequence(C::recv_ready(
            &mut receiver,
            &mut context,
        )))
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

// Reusing one channel without worker threads isolates per-subscription reads from contention.
fn publish_then_sequential_drain<C: BroadcastMpmc<M>, M: Payload>(
    bencher: Bencher,
    receiver_count: usize,
) {
    let (sender, mut receivers) = C::channel(BATCH_MESSAGES, receiver_count);

    bencher.bench_local(|| {
        for value in 0..BATCH_MESSAGES {
            C::send(&sender, black_box(M::new(value)));
        }

        let mut total = 0usize;
        for receiver in &mut receivers {
            let mut checksum = 0usize;
            for _ in 0..BATCH_MESSAGES {
                checksum = checksum.wrapping_add(M::output_sequence(recv::<C, M>(receiver)));
            }
            assert_eq!(checksum, EXPECTED_BATCH_CHECKSUM);
            total = total.wrapping_add(checksum);
        }
        black_box(total)
    });
}

fn publish_then_reused_drain<C: BroadcastMpmc<M>, M: Payload>(
    bencher: Bencher,
    receiver_count: usize,
) {
    let mut input = ReusedFanout::<C, M>::new(receiver_count);
    bencher.bench_local(|| input.run());
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "broadcast/no-backpressure/try-roundtrip",
        9_100_000,
        implementations!(
            [Asyncband, Tokio, AsyncBroadcast],
            |C, b| try_send_receive::<C, usize>(b)
        ),
    );
    runner.compare(
        "broadcast/no-backpressure/try-roundtrip-shared",
        9_100_000,
        implementations!(
            [Asyncband, Tokio, AsyncBroadcast],
            |C, b| try_send_receive::<C, Shared<usize>>(b)
        ),
    );
    runner.compare(
        "broadcast/no-backpressure/ready-roundtrip",
        8_000_000,
        implementations!([Asyncband, Tokio, AsyncBroadcast], |C, b| {
            ready_send_receive::<C, usize>(b)
        }),
    );
    runner.compare(
        "broadcast/no-backpressure/ready-roundtrip-shared",
        8_000_000,
        implementations!([Asyncband, Tokio, AsyncBroadcast], |C, b| {
            ready_send_receive::<C, Shared<usize>>(b)
        }),
    );
    runner.compare(
        "broadcast/no-backpressure/publish-4p-drain-1r",
        810,
        implementations!([Asyncband, Tokio, AsyncBroadcast], |C, b| {
            publish_batch_then_drain::<C>(b, 4)
        }),
    );
    runner.compare(
        "broadcast/no-backpressure/drain-4r",
        500,
        implementations!([Asyncband, Tokio, AsyncBroadcast], |C, b| {
            publish_then_parallel_drain::<C>(b, 4)
        }),
    );
    runner.compare(
        "broadcast/no-backpressure/sequential-drain-4r",
        2_500,
        implementations!([Asyncband, Tokio, AsyncBroadcast], |C, b| {
            publish_then_sequential_drain::<C, usize>(b, 4)
        }),
    );
    runner.compare(
        "broadcast/no-backpressure/reused-drain-4r",
        2_500,
        implementations!([Asyncband, Tokio, AsyncBroadcast], |C, b| {
            publish_then_reused_drain::<C, usize>(b, 4)
        }),
    );
    runner.compare(
        "broadcast/no-backpressure/reused-drain-4r-inline1k",
        650,
        implementations!([Asyncband, Tokio, AsyncBroadcast], |C, b| {
            publish_then_reused_drain::<C, [u8; 1024]>(b, 4)
        }),
    );
    runner.compare(
        "broadcast/no-backpressure/reused-drain-4r-shared1k",
        650,
        implementations!([Asyncband, Tokio, AsyncBroadcast], |C, b| {
            publish_then_reused_drain::<C, Shared<[u8; 1024]>>(b, 4)
        }),
    );
}
