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

use std::fmt;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::thread::JoinHandle;

use asyncband::broadcast::mpmc;
use divan::Bencher;
use divan::black_box;

use super::support::bench_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;

const RECEIVER_COUNTS: &[usize] = &[1, 8, 32];
const SENDER_COUNTS: &[usize] = &[1, 2, 4, 8];
const CONCURRENT_BATCH_SIZE: usize = 4096;

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
    Fanout { peak: 32, live: 1 },
    Fanout {
        peak: 256,
        live: 32,
    },
    Fanout { peak: 256, live: 1 },
];

struct ConcurrentSend {
    receiver: mpmc::UnboundedReceiver<usize>,
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
}

struct ConcurrentBoundedSend {
    _receiver: mpmc::BoundedReceiver<usize>,
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
}

impl ConcurrentBoundedSend {
    fn new(sender_count: usize) -> Self {
        let (sender, receiver) = mpmc::bounded(CONCURRENT_BATCH_SIZE);
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
                    sender.try_send(black_box(value)).unwrap();
                }
                done.wait();
            }));
        }
        drop(sender);
        ready.wait();

        Self {
            _receiver: receiver,
            start,
            done,
            workers,
        }
    }

    fn run(&mut self) {
        self.start.wait();
        self.done.wait();
    }
}

impl Drop for ConcurrentBoundedSend {
    fn drop(&mut self) {
        for worker in self.workers.drain(..) {
            worker.join().unwrap();
        }
    }
}

impl ConcurrentSend {
    fn new(sender_count: usize) -> Self {
        let (sender, receiver) = mpmc::unbounded();
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
                    sender.send(black_box(value)).unwrap();
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

    fn run(&mut self) {
        self.start.wait();
        self.done.wait();
        for _ in 0..CONCURRENT_BATCH_SIZE {
            black_box(self.receiver.try_recv().unwrap());
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

#[divan::bench]
fn bounded_round_trip(bencher: Bencher) {
    let (sender, mut receiver) = mpmc::bounded(64);
    bencher.bench_local(|| {
        sender.try_send(black_box(1usize)).unwrap();
        black_box(receiver.try_recv().unwrap())
    });
}

#[divan::bench]
fn unbounded_round_trip(bencher: Bencher) {
    let (sender, mut receiver) = mpmc::unbounded();
    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        black_box(receiver.try_recv().unwrap())
    });
}

#[divan::bench]
fn unbounded_round_trip_two_subscriptions(bencher: Bencher) {
    let (sender, mut first) = mpmc::unbounded();
    let mut second = sender.subscribe();
    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        black_box(first.try_recv().unwrap());
        black_box(second.try_recv().unwrap())
    });
}

#[divan::bench]
fn deliver_to_waiting_receiver(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receiver) = mpmc::unbounded();

    bencher.bench_local(|| {
        let mut recv = Box::pin(receiver.recv());
        poll_pending(recv.as_mut(), &mut context);
        sender.send(black_box(1usize)).unwrap();
        black_box(poll_pinned_ready(recv.as_mut(), &mut context).unwrap())
    });
}

#[divan::bench(args = RECEIVER_COUNTS)]
fn fanout_round_trip(bencher: Bencher, receiver_count: usize) {
    let (sender, receiver) = mpmc::unbounded();
    let mut receivers = Vec::with_capacity(receiver_count);
    receivers.push(receiver);
    for _ in 1..receiver_count {
        receivers.push(sender.subscribe());
    }

    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        for receiver in &mut receivers {
            black_box(receiver.try_recv().unwrap());
        }
    });
}

#[divan::bench(args = RECLAIM_FANOUTS)]
fn reclaim_after_receiver_high_water(bencher: Bencher, fanout: Fanout) {
    let (sender, initial) = mpmc::unbounded();
    drop(initial);
    let mut receivers = (0..fanout.peak)
        .map(|_| sender.subscribe())
        .collect::<Vec<_>>();
    receivers.truncate(fanout.live);

    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        for receiver in &mut receivers {
            black_box(receiver.try_recv().unwrap());
        }
    });
}

#[divan::bench(
    args = SENDER_COUNTS,
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
    args = SENDER_COUNTS,
    sample_count = 50,
    sample_size = 1,
    counters = [CONCURRENT_BATCH_SIZE]
)]
fn concurrent_bounded_send(bencher: Bencher, sender_count: usize) {
    bencher
        .with_inputs(|| ConcurrentBoundedSend::new(sender_count))
        .bench_local_refs(ConcurrentBoundedSend::run);
}
