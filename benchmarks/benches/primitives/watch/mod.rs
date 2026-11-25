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

use asyncband::watch;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::primitive;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::poll_ready;

const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];
const WAITER_COUNTS: &[usize] = &[1, 4, 16];

struct Payload1k([u64; 128]);

impl Clone for Payload1k {
    #[inline(never)]
    fn clone(&self) -> Self {
        Self(black_box(self.0))
    }
}

fn has_changed(bencher: Bencher) {
    let (sender, receiver) = watch::channel(0usize);
    sender.send(1).unwrap();

    bencher.bench(|| black_box(black_box(&receiver).has_changed()));
    black_box(sender);
}

fn clone_drop_receiver(bencher: Bencher) {
    let (sender, receiver) = watch::channel(0usize);

    bencher.bench(|| drop(black_box(&receiver).clone()));
    black_box(sender);
}

fn get_word(bencher: Bencher) {
    let (sender, receiver) = watch::channel(1usize);

    bencher.bench(|| black_box(black_box(&receiver).get()));
    black_box(sender);
}

fn get_payload_1k(bencher: Bencher) {
    let (sender, receiver) = watch::channel(Payload1k([7; 128]));

    bencher.bench(|| black_box(black_box(&receiver).get().0[0]));
    black_box(sender);
}

fn publish_overwrite(bencher: Bencher) {
    let (sender, receiver) = watch::channel(0usize);

    bencher.bench_local(|| sender.send(black_box(1)).unwrap());
    black_box(receiver);
}

fn changed_ready(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receiver) = watch::channel(0usize);

    bencher.bench_local(|| {
        sender.send(black_box(1)).unwrap();
        poll_ready(receiver.changed(), &mut context).unwrap();
    });
}

fn notify_pending_fanout(bencher: Bencher, waiter_count: usize) {
    let mut context = bench_context();
    let (sender, first) = watch::channel(0usize);
    let mut receivers = Vec::with_capacity(waiter_count);
    receivers.push(first);
    receivers.extend((1..waiter_count).map(|_| sender.subscribe()));

    bencher.bench_local(|| {
        let mut waiters = receivers
            .iter_mut()
            .map(|receiver| Box::pin(receiver.changed()))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }

        sender.send(black_box(1)).unwrap();
        for mut waiter in waiters {
            poll_pinned_ready(waiter.as_mut(), &mut context).unwrap();
        }
    });
}

fn cancel_pending(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receiver) = watch::channel(0usize);

    bencher.bench_local(|| {
        let mut changed = pin!(receiver.changed());
        poll_pending(changed.as_mut(), &mut context);
    });
    black_box(sender);
}

pub fn register(runner: &mut Runner) {
    for &threads in THREAD_COUNTS {
        primitive!(runner, has_changed, 480_000_000, threads = threads);
    }
    for &threads in THREAD_COUNTS {
        primitive!(runner, clone_drop_receiver, 24_000_000, threads = threads);
    }
    for &threads in THREAD_COUNTS {
        primitive!(runner, get_word, 48_000_000, threads = threads);
    }
    for &threads in THREAD_COUNTS {
        primitive!(runner, get_payload_1k, 12_000_000, threads = threads);
    }
    primitive!(runner, publish_overwrite, 24_000_000);
    primitive!(runner, changed_ready, 24_000_000);
    for &arg in WAITER_COUNTS {
        primitive!(runner, notify_pending_fanout(arg), (4_000_000 / arg).max(1));
    }
    primitive!(runner, cancel_pending, 24_000_000);
}
