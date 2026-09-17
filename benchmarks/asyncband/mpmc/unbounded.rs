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
use crate::mpmc_support::adapters::Asyncband;
use crate::mpmc_support::support::BATCH_MESSAGES;
use crate::mpmc_support::support::TOPOLOGIES;
use crate::mpmc_support::support::TaskBatch;
use crate::mpmc_support::support::ThreadBatch;
use crate::mpmc_support::support::Topology;
use crate::mpmc_support::support::Unbounded;
use crate::mpmc_support::support::runtime;
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
        .with_inputs(|| ThreadBatch::new_unbounded::<Asyncband>(topology))
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
        .with_inputs(|| TaskBatch::new::<Unbounded<Asyncband>>(&runtime, topology))
        .bench_local_refs(|batch| runtime.block_on(batch.run()));
}

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn send_then_try_recv(bencher: Bencher) {
    let (sender, receiver) = mpmc::unbounded();
    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        black_box(receiver.try_recv().unwrap())
    });
}

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn send_then_recv(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, receiver) = mpmc::unbounded();
    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        black_box(poll_ready(receiver.recv(), &mut context).unwrap())
    });
}

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn wake_pending_receiver(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, receiver) = mpmc::unbounded();
    bencher.bench_local(|| {
        let mut recv = pin!(receiver.recv());
        poll_pending(recv.as_mut(), &mut context);
        sender.send(black_box(usize::MAX)).unwrap();
        black_box(poll_pinned_ready(recv.as_mut(), &mut context).unwrap())
    });
}

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn repoll_pending_receiver(bencher: Bencher) {
    let mut context = bench_context();
    let (_sender, receiver) = mpmc::unbounded::<usize>();
    let mut recv = pin!(receiver.recv());
    poll_pending(recv.as_mut(), &mut context);
    bencher.bench_local(|| poll_pending(recv.as_mut(), &mut context));
}
