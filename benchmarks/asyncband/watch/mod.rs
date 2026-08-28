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

use asyncband::watch;
use divan::Bencher;
use divan::black_box;

use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;
use crate::support::poll_ready;

const RECEIVER_COUNTS: &[usize] = &[1, 2, 4, 8, 32];

#[divan::bench]
fn borrow_current(bencher: Bencher) {
    let (sender, receiver) = watch::channel(1usize);
    bencher.bench_local(|| black_box(*receiver.borrow()));
    black_box(sender);
}

#[divan::bench]
fn send_and_borrow(bencher: Bencher) {
    let (sender, receiver) = watch::channel(0usize);
    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        black_box(*receiver.borrow())
    });
}

#[divan::bench]
fn send_and_borrow_and_update(bencher: Bencher) {
    let (sender, mut receiver) = watch::channel(0usize);
    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        black_box(*receiver.borrow_and_update())
    });
}

#[divan::bench]
fn ready_changed(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receiver) = watch::channel(0usize);
    bencher.bench_local(|| {
        sender.send(black_box(1usize)).unwrap();
        black_box(*poll_ready(receiver.changed(), &mut context).unwrap())
    });
}

#[divan::bench]
fn notify_pending(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receiver) = watch::channel(0usize);
    bencher.bench_local(|| {
        let mut changed = pin!(receiver.changed());
        poll_pending(changed.as_mut(), &mut context);
        sender.send(black_box(1usize)).unwrap();
        black_box(*poll_pinned_ready(changed.as_mut(), &mut context).unwrap())
    });
}

#[divan::bench(args = RECEIVER_COUNTS)]
fn notify_pending_fanout(bencher: Bencher, receiver_count: usize) {
    let mut context = bench_context();
    let (sender, first) = watch::channel(0usize);
    let mut receivers = Vec::with_capacity(receiver_count);
    receivers.push(first);
    receivers.extend((1..receiver_count).map(|_| sender.subscribe()));

    bencher.bench_local(|| {
        let mut changed = receivers
            .iter_mut()
            .map(|receiver| Box::pin(receiver.changed()))
            .collect::<Vec<_>>();
        for future in &mut changed {
            poll_pending(future.as_mut(), &mut context);
        }

        sender.send(black_box(1usize)).unwrap();
        for mut future in changed {
            black_box(*poll_pinned_ready(future.as_mut(), &mut context).unwrap());
        }
    });
}
