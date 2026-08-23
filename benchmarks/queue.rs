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

use asyncband::mpmc;
use asyncband::spmc;
use asyncband::spsc;
use divan::Bencher;
use divan::black_box;

use super::support::bench_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;

#[divan::bench]
fn spsc_bounded_round_trip(bencher: Bencher) {
    let (mut sender, mut receiver) = spsc::bounded(64);
    bencher.bench_local(|| {
        sender.try_send(black_box(1usize)).unwrap();
        black_box(receiver.try_recv().unwrap())
    });
}

#[divan::bench]
fn spsc_unbounded_round_trip(bencher: Bencher) {
    let (mut sender, mut receiver) = spsc::unbounded();
    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        black_box(receiver.try_recv().unwrap())
    });
}

#[divan::bench]
fn spmc_unbounded_round_trip(bencher: Bencher) {
    let (mut sender, receiver) = spmc::unbounded();
    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        black_box(receiver.try_recv().unwrap())
    });
}

#[divan::bench]
fn mpmc_bounded_round_trip(bencher: Bencher) {
    let (sender, receiver) = mpmc::bounded(64);
    bencher.bench_local(|| {
        sender.try_send(black_box(1usize)).unwrap();
        black_box(receiver.try_recv().unwrap())
    });
}

#[divan::bench]
fn mpmc_unbounded_round_trip(bencher: Bencher) {
    let (sender, receiver) = mpmc::unbounded();
    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        black_box(receiver.try_recv().unwrap())
    });
}

#[divan::bench]
fn mpmc_deliver_to_waiting_receiver(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, receiver) = mpmc::unbounded();

    bencher.bench_local(|| {
        let mut recv = Box::pin(receiver.recv());
        poll_pending(recv.as_mut(), &mut context);
        sender.send(black_box(1usize)).unwrap();
        black_box(poll_pinned_ready(recv.as_mut(), &mut context).unwrap())
    });
}
