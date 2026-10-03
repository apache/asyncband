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

use asyncband::spmc;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::poll_ready;
use divan::Bencher;
use divan::black_box;

use super::FAST_SAMPLE_SIZE;

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn try_send_then_try_recv(bencher: Bencher) {
    let (mut sender, receiver) = spmc::bounded(1);
    bencher.bench_local(|| {
        sender.try_send(black_box(1usize)).unwrap();
        black_box(receiver.try_recv().unwrap())
    });
}

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn send_then_recv(bencher: Bencher) {
    let mut context = bench_context();
    let (mut sender, receiver) = spmc::bounded(1);
    bencher.bench_local(|| {
        poll_ready(sender.send(black_box(1usize)), &mut context).unwrap();
        black_box(poll_ready(receiver.recv(), &mut context).unwrap())
    });
}

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn wake_blocked_sender(bencher: Bencher) {
    let mut context = bench_context();
    let (mut sender, receiver) = spmc::bounded(1);
    sender.try_send(0usize).unwrap();
    bencher.bench_local(|| {
        let mut send = pin!(sender.send(black_box(1)));
        poll_pending(send.as_mut(), &mut context);
        black_box(receiver.try_recv().unwrap());
        poll_pinned_ready(send.as_mut(), &mut context).unwrap();
    });
}

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn cancel_pending_send(bencher: Bencher) {
    let mut context = bench_context();
    let (mut sender, _receiver) = spmc::bounded(1);
    sender.try_send(0usize).unwrap();
    bencher.bench_local(|| {
        let mut send = pin!(sender.send(black_box(1)));
        poll_pending(send.as_mut(), &mut context);
    });
}

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn repoll_pending_send(bencher: Bencher) {
    let mut context = bench_context();
    let (mut sender, _receiver) = spmc::bounded(1);
    sender.try_send(0usize).unwrap();
    let mut send = pin!(sender.send(1));
    poll_pending(send.as_mut(), &mut context);
    bencher.bench_local(|| poll_pending(send.as_mut(), &mut context));
}

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn wake_pending_receiver(bencher: Bencher) {
    let mut context = bench_context();
    let (mut sender, receiver) = spmc::bounded(1);
    bencher.bench_local(|| {
        let mut recv = pin!(receiver.recv());
        poll_pending(recv.as_mut(), &mut context);
        sender.try_send(black_box(usize::MAX)).unwrap();
        black_box(poll_pinned_ready(recv.as_mut(), &mut context).unwrap())
    });
}
