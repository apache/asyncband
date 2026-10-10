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
use std::task::Context;
use std::task::Wake;
use std::task::Waker;

use asyncband::event::ManualResetEvent;
use benchmarks::support::bench_context;
use benchmarks::support::defer_input_drop;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

const WAITER_COUNTS: &[usize] = &[1, 4, 16];
const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];
const CONTENDED_SAMPLE_SIZE: u32 = 256;

#[divan::bench(args = [false, true], threads = THREAD_COUNTS, sample_size = CONTENDED_SAMPLE_SIZE)]
fn is_set(bencher: Bencher, is_set: bool) {
    let event = ManualResetEvent::with_state(is_set);

    bencher.bench(|| black_box(black_box(&event).is_set()));
}

#[divan::bench(threads = THREAD_COUNTS, sample_size = CONTENDED_SAMPLE_SIZE)]
fn wait_already_set(bencher: Bencher) {
    let event = ManualResetEvent::with_state(true);

    bencher.bench(|| {
        let mut context = bench_context();
        let mut wait = pin!(event.wait());
        poll_pinned_ready(wait.as_mut(), &mut context);
        black_box(&event)
    });
}

#[divan::bench]
fn set_reset_cycle(bencher: Bencher) {
    let event = ManualResetEvent::new();

    bencher.bench_local(|| {
        event.set();
        event.reset();
        black_box(&event)
    });
}

#[divan::bench]
fn reused_waiter_handoff(bencher: Bencher) {
    let mut context = bench_context();
    let event = ManualResetEvent::new();

    bencher.bench_local(|| {
        let mut wait = pin!(event.wait());
        poll_pending(wait.as_mut(), &mut context);

        event.set();
        poll_pinned_ready(wait.as_mut(), &mut context);
        event.reset();
        black_box(&event)
    });
}

#[divan::bench(args = WAITER_COUNTS)]
fn waiter_fan_out(bencher: Bencher, waiter_count: usize) {
    let mut context = bench_context();

    bencher
        .counter(ItemsCount::new(waiter_count))
        .bench_local(|| {
            let event = ManualResetEvent::new();
            let mut waiters = (0..waiter_count)
                .map(|_| Box::pin(event.wait()))
                .collect::<Vec<_>>();
            for waiter in &mut waiters {
                poll_pending(waiter.as_mut(), &mut context);
            }

            event.set();
            for mut waiter in waiters {
                poll_pinned_ready(waiter.as_mut(), &mut context);
            }
            black_box(event)
        });
}

#[divan::bench]
fn cancel_pending(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let event = ManualResetEvent::new();
        {
            let mut wait = pin!(event.wait());
            poll_pending(wait.as_mut(), &mut context);
        }
        black_box(event)
    });
}

#[divan::bench]
fn cancel_pending_reused(bencher: Bencher) {
    let mut context = bench_context();
    let event = ManualResetEvent::new();

    bencher.bench_local(|| {
        let mut wait = pin!(event.wait());
        poll_pending(wait.as_mut(), &mut context);
    });
}

#[divan::bench]
fn repoll_pending(bencher: Bencher) {
    let mut context = bench_context();
    let event = ManualResetEvent::new();
    let mut wait = pin!(event.wait());
    poll_pending(wait.as_mut(), &mut context);

    bencher.bench_local(|| poll_pending(wait.as_mut(), &mut context));
}

#[divan::bench]
fn replace_pending_waker(bencher: Bencher) {
    struct Task;
    #[allow(clippy::manual_noop_waker)]
    impl Wake for Task {
        fn wake(self: Arc<Self>) {}
    }

    let wakers = [Waker::from(Arc::new(Task)), Waker::from(Arc::new(Task))];
    let event = ManualResetEvent::new();
    let mut wait = pin!(event.wait());
    poll_pending(wait.as_mut(), &mut Context::from_waker(&wakers[0]));
    let mut next = 1;

    bencher.bench_local(|| {
        poll_pending(wait.as_mut(), &mut Context::from_waker(&wakers[next]));
        next ^= 1;
    });
}

#[divan::bench(args = [1, 16, 256], sample_size = 64)]
fn set_registered(bencher: Bencher, waiter_count: usize) {
    bencher
        .with_inputs(|| {
            let event = Arc::new(ManualResetEvent::new());
            let mut context = bench_context();
            let mut waiters = (0..waiter_count)
                .map(|_| Box::pin(event.clone().wait_owned()))
                .collect::<Vec<_>>();
            for waiter in &mut waiters {
                poll_pending(waiter.as_mut(), &mut context);
            }
            (event, waiters)
        })
        .bench_local_values(|(event, waiters)| {
            // Time cohort detachment and wake callbacks, excluding registration and cleanup.
            event.set();
            defer_input_drop((event, waiters), ())
        });
}
