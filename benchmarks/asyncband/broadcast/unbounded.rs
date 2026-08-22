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

// Every benchmark here must return the channel to a drained state on each iteration. Unlike the
// overflow policy, this channel has no capacity ceiling, so a timed loop that only sends would
// grow the retained backlog until the process runs out of memory.

use std::fmt;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::thread::JoinHandle;

use asyncband::broadcast::unbounded;
use divan::Bencher;
use divan::black_box;

use super::support::bench_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;

const RECEIVER_COUNTS: &[usize] = &[1, 8, 32];
const CONCURRENCY_COUNTS: &[usize] = &[1, 2, 4, 8];
const CONCURRENT_BATCH_SIZE: usize = 4096;

/// A channel that peaked at `peak` receivers and currently has `live` of them.
///
/// The two are measured separately because a dropped receiver leaves its slot behind: the reclaim
/// scan walks every slot the channel ever handed out, so a channel that shed receivers keeps
/// paying for the peak. Pairing each peak with a drained arena is what makes that visible.
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

struct ConcurrentSend {
    receiver: unbounded::Receiver<usize>,
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
}

impl ConcurrentSend {
    fn new(sender_count: usize) -> Self {
        let (sender, receiver) = unbounded::channel();
        let ready = Arc::new(Barrier::new(sender_count + 1));
        let start = Arc::new(Barrier::new(sender_count + 1));
        let done = Arc::new(Barrier::new(sender_count + 1));
        let sends_per_worker = CONCURRENT_BATCH_SIZE / sender_count;
        let mut workers = Vec::with_capacity(sender_count);

        for worker_index in 0..sender_count {
            let sender = sender.clone();
            let ready = ready.clone();
            let start = start.clone();
            let done = done.clone();
            workers.push(thread::spawn(move || {
                ready.wait();
                start.wait();
                let first = worker_index * sends_per_worker;
                for value in first..first + sends_per_worker {
                    sender.send(black_box(value));
                }
                done.wait();
            }));
        }
        drop(sender);
        ready.wait();

        Self {
            receiver,
            start,
            done,
            workers,
        }
    }

    // The drain is inside the measured region on purpose: it is what keeps the backlog bounded
    // across samples, and reclaiming the batch is part of the cost of an unbounded send.
    fn run(&mut self) {
        self.start.wait();
        self.done.wait();
        while let Ok(value) = self.receiver.try_recv() {
            black_box(value);
        }
    }
}

impl Drop for ConcurrentSend {
    fn drop(&mut self) {
        for worker in self.workers.drain(..) {
            worker.join().unwrap();
        }
    }
}

struct ConcurrentFanout {
    sender: unbounded::Sender<usize>,
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
}

impl ConcurrentFanout {
    fn new(receiver_count: usize) -> Self {
        let (sender, receiver) = unbounded::channel();
        let mut receivers = Vec::with_capacity(receiver_count);
        receivers.push(receiver);
        for _ in 1..receiver_count {
            receivers.push(sender.subscribe());
        }

        let ready = Arc::new(Barrier::new(receiver_count + 1));
        let start = Arc::new(Barrier::new(receiver_count + 1));
        let done = Arc::new(Barrier::new(receiver_count + 1));
        let mut workers = Vec::with_capacity(receiver_count);

        for mut receiver in receivers {
            let ready = ready.clone();
            let start = start.clone();
            let done = done.clone();
            workers.push(thread::spawn(move || {
                ready.wait();
                start.wait();
                let result = (0..CONCURRENT_BATCH_SIZE).try_for_each(|_| {
                    receiver.try_recv().map(|value| {
                        black_box(value);
                    })
                });
                done.wait();
                result.unwrap();
            }));
        }
        ready.wait();

        Self {
            sender,
            start,
            done,
            workers,
        }
    }

    fn run(&mut self) {
        for value in 0..CONCURRENT_BATCH_SIZE {
            self.sender.send(black_box(value));
        }
        self.start.wait();
        self.done.wait();
    }
}

impl Drop for ConcurrentFanout {
    fn drop(&mut self) {
        for worker in self.workers.drain(..) {
            worker.join().unwrap();
        }
    }
}

#[divan::bench]
fn send_without_receivers(bencher: Bencher) {
    let (sender, receiver) = unbounded::channel::<usize>();
    drop(receiver);
    bencher.bench_local(|| sender.send(black_box(1)));
}

#[divan::bench]
fn try_recv_empty(bencher: Bencher) {
    let (sender, mut receiver) = unbounded::channel::<usize>();
    bencher.bench_local(|| black_box(receiver.try_recv()));
    black_box(sender);
}

// A sole receiver takes ownership of the payload, so this path never clones the message.
#[divan::bench]
fn send_and_try_recv(bencher: Bencher) {
    let (sender, mut receiver) = unbounded::channel();
    bencher.bench_local(|| {
        sender.send(black_box(1usize));
        black_box(receiver.try_recv().unwrap())
    });
}

// With the payload shared, each receive clones it and the second one reclaims the slot.
#[divan::bench]
fn send_and_try_recv_shared(bencher: Bencher) {
    let (sender, mut first) = unbounded::channel();
    let mut second = sender.subscribe();
    bencher.bench_local(|| {
        sender.send(black_box(1usize));
        black_box(first.try_recv().unwrap());
        black_box(second.try_recv().unwrap())
    });
}

// The `usize` benchmarks above hide what a receive costs for a payload that owns memory: a clone
// there is an allocation, not a register move.
fn payload() -> String {
    "x".repeat(64)
}

#[divan::bench]
fn send_and_try_recv_owned(bencher: Bencher) {
    let (sender, mut receiver) = unbounded::channel();
    bencher.bench_local(|| {
        sender.send(black_box(payload()));
        black_box(receiver.try_recv().unwrap())
    });
}

#[divan::bench]
fn send_and_try_recv_owned_shared(bencher: Bencher) {
    let (sender, mut first) = unbounded::channel();
    let mut second = sender.subscribe();
    bencher.bench_local(|| {
        sender.send(black_box(payload()));
        black_box(first.try_recv().unwrap());
        black_box(second.try_recv().unwrap())
    });
}

// Measures the reclaim scan, which runs when the slowest cursor advances. Comparing a peak against
// the same peak drained down to fewer receivers shows what the slots left behind still cost.
#[divan::bench(args = RECLAIM_FANOUTS)]
fn drain_with_receivers(bencher: Bencher, fanout: Fanout) {
    let (sender, receiver) = unbounded::channel();
    drop(receiver);
    let mut receivers = (0..fanout.peak)
        .map(|_| sender.subscribe())
        .collect::<Vec<_>>();
    // Dropping down to `live` leaves the arena holding a slot for every receiver that ever existed.
    receivers.truncate(fanout.live);

    bencher.bench_local(|| {
        sender.send(black_box(1usize));
        for receiver in &mut receivers {
            black_box(receiver.try_recv().unwrap());
        }
    });
}

#[divan::bench(
    args = CONCURRENCY_COUNTS,
    sample_count = 50,
    sample_size = 1,
    counters = [CONCURRENT_BATCH_SIZE]
)]
fn concurrent_send_and_drain(bencher: Bencher, sender_count: usize) {
    bencher
        .with_inputs(|| ConcurrentSend::new(sender_count))
        .bench_local_refs(ConcurrentSend::run);
}

#[divan::bench(
    args = CONCURRENCY_COUNTS,
    sample_count = 50,
    sample_size = 1,
    counters = [CONCURRENT_BATCH_SIZE]
)]
fn concurrent_fanout(bencher: Bencher, receiver_count: usize) {
    bencher
        .with_inputs(|| ConcurrentFanout::new(receiver_count))
        .bench_local_refs(ConcurrentFanout::run);
}

#[divan::bench]
fn cancel_pending(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (sender, mut receiver) = unbounded::channel::<usize>();
        {
            let mut recv = pin!(receiver.recv());
            poll_pending(recv.as_mut(), &mut context);
        }
        black_box((sender, receiver))
    });
}

#[divan::bench]
fn deliver_to_waiter(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (sender, mut receiver) = unbounded::channel();
        let mut recv = pin!(receiver.recv());
        poll_pending(recv.as_mut(), &mut context);

        sender.send(black_box(1usize));
        let value = poll_pinned_ready(recv.as_mut(), &mut context).unwrap();
        black_box(value)
    });
}

#[divan::bench(args = RECEIVER_COUNTS)]
fn deliver_to_receiver_batch(bencher: Bencher, receiver_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (sender, receiver) = unbounded::channel();
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
