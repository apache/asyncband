// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cell::Cell;
use std::pin::pin;

use asyncband::once::Once;
use asyncband::once::OnceCell;
use divan::Bencher;
use divan::black_box;

use super::support::bench_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;
use super::support::poll_ready;
use super::support::wait_until_open;

const WAITER_COUNTS: &[usize] = &[1, 8, 32];

#[divan::bench]
fn cancel_pending_wait(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let once = Once::new();
        {
            let mut wait = pin!(once.wait());
            poll_pending(wait.as_mut(), &mut context);
        }
        black_box(once)
    });
}

#[divan::bench]
fn complete_waiter(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let once = Once::new();
        let mut wait = pin!(once.wait());
        poll_pending(wait.as_mut(), &mut context);

        poll_ready(once.call_once(async || {}), &mut context);
        poll_pinned_ready(wait.as_mut(), &mut context);
        black_box(once.is_completed())
    });
}

#[divan::bench(args = WAITER_COUNTS)]
fn complete_waiter_batch(bencher: Bencher, waiter_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let once = Once::new();
        let mut waiters = (0..waiter_count)
            .map(|_| Box::pin(once.wait()))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }

        poll_ready(once.call_once(async || {}), &mut context);
        for mut waiter in waiters {
            poll_pinned_ready(waiter.as_mut(), &mut context);
        }
        black_box(once.is_completed())
    });
}

#[divan::bench]
fn initialize_cell(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let cell = OnceCell::new();
        let value = poll_ready(
            cell.get_or_init(|| async { black_box(1usize) }),
            &mut context,
        );
        black_box(*value)
    });
}

#[divan::bench(args = WAITER_COUNTS)]
fn initialize_cell_waiter_batch(bencher: Bencher, waiter_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let cell = OnceCell::new();
        let gate = Cell::new(false);
        let mut initializer = Box::pin(cell.get_or_init(|| async {
            wait_until_open(&gate).await;
            black_box(1usize)
        }));
        poll_pending(initializer.as_mut(), &mut context);

        let mut waiters = (0..waiter_count)
            .map(|_| Box::pin(cell.get_or_init(|| async { unreachable!() })))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }

        gate.set(true);
        let value = poll_pinned_ready(initializer.as_mut(), &mut context);
        black_box(*value);
        drop(initializer);
        for mut waiter in waiters {
            let value = poll_pinned_ready(waiter.as_mut(), &mut context);
            black_box(*value);
        }
    });
}
