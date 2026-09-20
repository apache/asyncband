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

use asyncband::mpmc;
use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::FAST_SAMPLE_SIZE;
use crate::channels::BATCH_MESSAGES;
use crate::channels::BOUNDED_CAPACITY;
use crate::channels::adapters::Bounded;
use crate::channels::adapters::Mpmc;
use crate::channels::mpmc::TOPOLOGIES;
use crate::channels::mpmc::TaskBatch;
use crate::channels::mpmc::ThreadBatch;
use crate::channels::mpmc::Topology;
use crate::channels::runtime;
use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;
use crate::support::poll_ready;

#[divan::bench(
    args = TOPOLOGIES,
    sample_count = 20,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn blocking_threads(bencher: Bencher, topology: Topology) {
    bencher
        .with_inputs(|| ThreadBatch::new_bounded::<Mpmc>(BOUNDED_CAPACITY, topology))
        .bench_local_refs(|batch| batch.run());
}

#[divan::bench(
    consts = [0, 4],
    args = TOPOLOGIES,
    sample_count = 20,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn tokio_tasks<const WORKERS: usize>(bencher: Bencher, topology: Topology) {
    let runtime = runtime(WORKERS);
    bencher
        .with_inputs(|| TaskBatch::new::<Bounded<Mpmc>>(&runtime, topology))
        .bench_local_refs(|batch| runtime.block_on(batch.run()));
}

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn try_send_then_try_recv(bencher: Bencher) {
    let (sender, receiver) = mpmc::bounded(1);
    bencher.bench_local(|| {
        sender.try_send(black_box(1usize)).unwrap();
        black_box(receiver.try_recv().unwrap())
    });
}

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn send_then_recv(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, receiver) = mpmc::bounded(1);
    bencher.bench_local(|| {
        poll_ready(sender.send(black_box(1usize)), &mut context).unwrap();
        black_box(poll_ready(receiver.recv(), &mut context).unwrap())
    });
}

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn wake_blocked_sender(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, receiver) = mpmc::bounded(1);
    sender.try_send(0usize).unwrap();
    bencher.bench_local(|| {
        let mut send = pin!(sender.send(black_box(1)));
        poll_pending(send.as_mut(), &mut context);
        black_box(receiver.try_recv().unwrap());
        poll_pinned_ready(send.as_mut(), &mut context).unwrap();
    });
}
