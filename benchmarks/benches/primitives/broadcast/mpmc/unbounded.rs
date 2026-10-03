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

// Drain each iteration to keep retained memory bounded across samples.

use std::fmt;
use std::pin::pin;

use asyncband::broadcast::mpmc;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use divan::Bencher;
use divan::black_box;

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
#[divan::bench]
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

#[divan::bench]
fn send_and_try_recv_owned(bencher: Bencher) {
    let (sender, mut receiver) = mpmc::unbounded();
    bencher.bench_local(|| {
        sender.send(black_box(payload()));
        black_box(receiver.try_recv().unwrap())
    });
}

#[divan::bench]
fn send_and_try_recv_owned_shared(bencher: Bencher) {
    let (sender, mut first) = mpmc::unbounded();
    let mut second = sender.subscribe();
    bencher.bench_local(|| {
        sender.send(black_box(payload()));
        black_box(first.try_recv().unwrap());
        black_box(second.try_recv().unwrap())
    });
}

#[divan::bench]
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

#[divan::bench(args = RECEIVER_COUNTS)]
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

#[divan::bench]
fn send_without_receivers(bencher: Bencher) {
    let (sender, receiver) = mpmc::unbounded::<usize>();
    drop(receiver);
    bencher.bench_local(|| sender.send(black_box(1)));
}

#[divan::bench]
fn try_recv_empty(bencher: Bencher) {
    let (sender, mut receiver) = mpmc::unbounded::<usize>();
    bencher.bench_local(|| black_box(receiver.try_recv()));
    black_box(sender);
}

// Compare reclamation with identical historical peaks but different live subscriber counts.
#[divan::bench(args = RECLAIM_FANOUTS)]
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

#[divan::bench]
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
