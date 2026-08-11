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

use divan::Bencher;
use divan::black_box;
use mea::singleflight::Group;

use super::support::bench_context;
use super::support::defer_input_drop;
use super::support::poll_pending;
use super::support::poll_pinned_ready;
use super::support::poll_ready;
use super::support::wait_until_open;

const WAITER_COUNTS: &[usize] = &[1, 8, 32];

#[divan::bench]
fn work_ready(bencher: Bencher) {
    let mut context = bench_context();
    bencher
        .with_inputs(Group::<usize, usize>::new)
        .bench_local_values(|group| {
            let result = black_box(poll_ready(
                group.work(black_box(0), || async { black_box(1) }),
                &mut context,
            ));
            defer_input_drop(group, result)
        });
}

#[divan::bench]
fn try_work_error(bencher: Bencher) {
    let mut context = bench_context();
    bencher
        .with_inputs(Group::<usize, usize>::new)
        .bench_local_values(|group| {
            let result = black_box(poll_ready(
                group.try_work(black_box(0), || async { Err::<usize, ()>(()) }),
                &mut context,
            ));
            defer_input_drop(group, result)
        });
}

#[divan::bench(args = WAITER_COUNTS)]
fn coalesced_work_batch(bencher: Bencher, waiter_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let group = Group::<usize, usize>::new();
        let gate = Cell::new(false);
        let mut leader = Box::pin(group.work(0, || async {
            wait_until_open(&gate).await;
            black_box(1usize)
        }));
        poll_pending(leader.as_mut(), &mut context);

        let mut waiters = (0..waiter_count)
            .map(|_| Box::pin(group.work(0, || async { unreachable!() })))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }

        gate.set(true);
        black_box(poll_pinned_ready(leader.as_mut(), &mut context));
        drop(leader);
        for mut waiter in waiters {
            black_box(poll_pinned_ready(waiter.as_mut(), &mut context));
        }
    });
}
use std::cell::Cell;
