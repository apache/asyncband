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

// Drain each iteration to keep retained memory bounded across rounds.

use std::fmt;
use std::hint::black_box;
use std::pin::pin;

use asyncband::broadcast::mpmc;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;

const RECEIVER_COUNTS: &[usize] = &[1, 4, 16];

/// Retains subscription storage from `peak` receivers while only `live` remain.
#[derive(Clone, Copy)]
struct Fanout {
    peak: usize,
    live: usize,
}

impl fmt::Display for Fanout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "peak {} live {}", self.peak, self.live)
    }
}

const RECLAIM_FANOUTS: &[Fanout] = &[
    Fanout { peak: 1, live: 1 },
    Fanout { peak: 8, live: 8 },
    Fanout { peak: 8, live: 1 },
    Fanout { peak: 32, live: 32 },
    Fanout { peak: 32, live: 4 },
    Fanout { peak: 32, live: 1 },
    Fanout {
        peak: 256,
        live: 32,
    },
    Fanout { peak: 256, live: 1 },
];

// Two receivers read the shared payload; the second reclaims the slot.
fn send_and_try_recv_shared(bencher: Bencher) {
    let (sender, mut first) = mpmc::unbounded();
    let mut second = sender.subscribe();
    bencher.bench_local(|| {
        sender.send(black_box(1usize));
        black_box(first.try_recv().unwrap());
        black_box(second.try_recv().unwrap())
    });
}

// The `usize` benchmarks above hide the cost of receiving a payload that owns memory: cloning it
// requires an allocation rather than a register move.
fn payload() -> String {
    "x".repeat(64)
}

fn send_and_try_recv_owned(bencher: Bencher) {
    let (sender, mut receiver) = mpmc::unbounded();
    bencher.bench_local(|| {
        sender.send(black_box(payload()));
        black_box(receiver.try_recv().unwrap())
    });
}

fn send_and_try_recv_owned_shared(bencher: Bencher) {
    let (sender, mut first) = mpmc::unbounded();
    let mut second = sender.subscribe();
    bencher.bench_local(|| {
        sender.send(black_box(payload()));
        black_box(first.try_recv().unwrap());
        black_box(second.try_recv().unwrap())
    });
}

fn deliver_to_waiting_receiver(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receiver) = mpmc::unbounded();

    bencher.bench_local(|| {
        let mut recv = pin!(receiver.recv());
        poll_pending(recv.as_mut(), &mut context);

        sender.send(black_box(1usize));
        let value = poll_pinned_ready(recv.as_mut(), &mut context).unwrap();
        black_box(value)
    });
}

fn register_and_deliver_fanout(bencher: Bencher, receiver_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (sender, receiver) = mpmc::unbounded();
        drop(receiver);
        let mut receivers = (0..receiver_count)
            .map(|_| sender.subscribe())
            .collect::<Vec<_>>();
        let mut recvs = receivers
            .iter_mut()
            .map(|receiver| Box::pin(receiver.recv()))
            .collect::<Vec<_>>();
        for recv in &mut recvs {
            poll_pending(recv.as_mut(), &mut context);
        }

        sender.send(black_box(1usize));
        for mut recv in recvs {
            let value = poll_pinned_ready(recv.as_mut(), &mut context).unwrap();
            black_box(value);
        }
    });
}

fn send_without_receivers(bencher: Bencher) {
    let (sender, receiver) = mpmc::unbounded::<usize>();
    drop(receiver);
    bencher.bench_local(|| sender.send(black_box(1)));
}

fn try_recv_empty(bencher: Bencher) {
    let (sender, mut receiver) = mpmc::unbounded::<usize>();
    bencher.bench_local(|| black_box(receiver.try_recv()));
    black_box(sender);
}

// Compare reclamation with identical historical peaks but different live subscriber counts.
fn drain_with_receivers(bencher: Bencher, fanout: Fanout) {
    let (sender, receiver) = mpmc::unbounded();
    drop(receiver);
    let mut receivers = (0..fanout.peak)
        .map(|_| sender.subscribe())
        .collect::<Vec<_>>();
    // Dropping down to `live` leaves the arena holding a slot for every receiver that ever
    // existed.
    receivers.truncate(fanout.live);

    bencher.bench_local(|| {
        sender.send(black_box(1usize));
        for receiver in &mut receivers {
            black_box(receiver.try_recv().unwrap());
        }
    });
}

fn cancel_pending(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (sender, mut receiver) = mpmc::unbounded::<usize>();
        {
            let mut recv = pin!(receiver.recv());
            poll_pending(recv.as_mut(), &mut context);
        }
        black_box((sender, receiver))
    });
}

pub fn register(runner: &mut Runner) {
    runner.case(
        concat!(module_path!(), "::send_and_try_recv_shared"),
        6_000_000,
        1,
        send_and_try_recv_shared,
    );
    runner.case(
        concat!(module_path!(), "::send_and_try_recv_owned"),
        3_000_000,
        1,
        send_and_try_recv_owned,
    );
    runner.case(
        concat!(module_path!(), "::send_and_try_recv_owned_shared"),
        3_000_000,
        1,
        send_and_try_recv_owned_shared,
    );
    runner.case(
        concat!(module_path!(), "::deliver_to_waiting_receiver"),
        6_000_000,
        1,
        deliver_to_waiting_receiver,
    );
    for &arg in RECEIVER_COUNTS {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::register_and_deliver_fanout")
            ),
            (3_000_000 / arg).max(1),
            1,
            |bencher| register_and_deliver_fanout(bencher, arg),
        );
    }
    runner.case(
        concat!(module_path!(), "::send_without_receivers"),
        12_000_000,
        1,
        send_without_receivers,
    );
    runner.case(
        concat!(module_path!(), "::try_recv_empty"),
        60_000_000,
        1,
        try_recv_empty,
    );
    for &arg in RECLAIM_FANOUTS {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::drain_with_receivers")
            ),
            (10_000_000 / arg.live).max(1),
            1,
            |bencher| drain_with_receivers(bencher, arg),
        );
    }
    runner.case(
        concat!(module_path!(), "::cancel_pending"),
        3_000_000,
        1,
        cancel_pending,
    );
}
