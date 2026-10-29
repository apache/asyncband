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

use std::hint::black_box;
use std::pin::pin;

use asyncband::mpmc;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::poll_ready;

fn send_then_try_recv(bencher: Bencher) {
    let (sender, receiver) = mpmc::unbounded();
    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        black_box(receiver.try_recv().unwrap())
    });
}

fn send_then_recv(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, receiver) = mpmc::unbounded();
    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        black_box(poll_ready(receiver.recv(), &mut context).unwrap())
    });
}

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

fn repoll_pending_receiver(bencher: Bencher) {
    let mut context = bench_context();
    let (_sender, receiver) = mpmc::unbounded::<usize>();
    let mut recv = pin!(receiver.recv());
    poll_pending(recv.as_mut(), &mut context);
    bencher.bench_local(|| poll_pending(recv.as_mut(), &mut context));
}

pub fn register(runner: &mut Runner) {
    runner.case(
        concat!(module_path!(), "::send_then_try_recv"),
        30_000_000,
        1,
        send_then_try_recv,
    );
    runner.case(
        concat!(module_path!(), "::send_then_recv"),
        24_000_000,
        1,
        send_then_recv,
    );
    runner.case(
        concat!(module_path!(), "::wake_pending_receiver"),
        9_000_000,
        1,
        wake_pending_receiver,
    );
    runner.case(
        concat!(module_path!(), "::repoll_pending_receiver"),
        40_000_000,
        1,
        repoll_pending_receiver,
    );
}
