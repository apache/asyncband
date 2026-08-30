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

use std::pin::Pin;
use std::pin::pin;

use asyncband::event::ManualResetEvent;
use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;

const WAITER_COUNTS: &[usize] = &[1, 8, 32];
const THREAD_COUNTS: &[usize] = &[1, 2, 8, 32];
const CONTENDED_SAMPLE_SIZE: u32 = 256;

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

#[divan::bench(threads = THREAD_COUNTS, sample_size = CONTENDED_SAMPLE_SIZE)]
fn is_set_contended(bencher: Bencher) {
    let event = ManualResetEvent::with_state(true);

    bencher.bench(|| black_box(event.is_set()));
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
fn wake_waiter(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let event = ManualResetEvent::new();
        {
            let mut wait = pin!(event.wait());
            poll_pending(wait.as_mut(), &mut context);

            event.set();
            poll_pinned_ready(wait.as_mut(), &mut context);
        }
        black_box(event)
    });
}

#[divan::bench]
fn wake_waiter_reused(bencher: Bencher) {
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
            let mut waiters = (0..waiter_count).map(|_| event.wait()).collect::<Vec<_>>();
            for waiter in &mut waiters {
                poll_pending(Pin::new(waiter), &mut context);
            }

            event.set();
            for mut waiter in waiters {
                poll_pinned_ready(Pin::new(&mut waiter), &mut context);
            }
            black_box(event)
        });
}
