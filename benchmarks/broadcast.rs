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

use std::pin::pin;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::thread::JoinHandle;

use asyncband::broadcast::overflow;
use divan::Bencher;
use divan::black_box;

use super::support::bench_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;

const RECEIVER_COUNTS: &[usize] = &[1, 8, 32];
const CONCURRENCY_COUNTS: &[usize] = &[1, 2, 4, 8];
const CONCURRENT_BATCH_SIZE: usize = 4096;

struct ConcurrentSend {
    _receiver: overflow::Receiver<usize>,
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
}

impl ConcurrentSend {
    fn new(sender_count: usize) -> Self {
        let (sender, receiver) = overflow::channel(CONCURRENT_BATCH_SIZE);
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

impl Drop for ConcurrentSend {
    fn drop(&mut self) {
        for worker in self.workers.drain(..) {
            worker.join().unwrap();
        }
    }
}

struct ConcurrentFanout {
    sender: overflow::Sender<usize>,
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
}

impl ConcurrentFanout {
    fn new(receiver_count: usize) -> Self {
        let (sender, receiver) = overflow::channel(CONCURRENT_BATCH_SIZE);
        let mut receivers = Vec::with_capacity(receiver_count);
        receivers.push(receiver);
        for _ in 1..receiver_count {
            receivers.push(receivers[0].clone());
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
fn send_overwrite(bencher: Bencher) {
    let (sender, receiver) = overflow::channel::<usize>(1);
    bencher.bench_local(|| sender.send(black_box(1)));
    black_box(receiver);
}

#[divan::bench]
fn try_recv_empty(bencher: Bencher) {
    let (sender, mut receiver) = overflow::channel::<usize>(1);
    bencher.bench_local(|| black_box(receiver.try_recv()));
    black_box(sender);
}

#[divan::bench]
fn send_and_try_recv(bencher: Bencher) {
    let (sender, mut receiver) = overflow::channel(1);
    bencher.bench_local(|| {
        sender.send(black_box(1));
        black_box(receiver.try_recv().unwrap())
    });
}

#[divan::bench(
    args = CONCURRENCY_COUNTS,
    sample_count = 50,
    sample_size = 1,
    counters = [CONCURRENT_BATCH_SIZE]
)]
fn concurrent_send(bencher: Bencher, sender_count: usize) {
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
        let (sender, mut receiver) = overflow::channel::<usize>(1);
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
        let (sender, mut receiver) = overflow::channel(1);
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
        let (sender, receiver) = overflow::channel(1);
        let mut receivers = (0..receiver_count)
            .map(|_| receiver.resubscribe())
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
