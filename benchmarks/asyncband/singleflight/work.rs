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

use std::cell::Cell;
use std::hash::BuildHasherDefault;
use std::hash::DefaultHasher;

use asyncband::singleflight::Group;
use divan::Bencher;
use divan::black_box;

use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;
use crate::support::poll_ready;
use crate::support::wait_until_open;

const WAITER_COUNTS: &[usize] = &[1, 8, 32];

type BenchGroup = Group<usize, usize, BuildHasherDefault<DefaultHasher>>;

#[divan::bench]
fn construct_default(bencher: Bencher) {
    bencher.bench_local(|| black_box(BenchGroup::default()));
}

#[divan::bench]
fn work_vacant(bencher: Bencher) {
    let group = BenchGroup::default();
    let mut context = bench_context();
    bencher.bench_local(|| {
        black_box(poll_ready(
            group.work(black_box(0), || async { black_box(1) }),
            &mut context,
        ))
    });
}

#[divan::bench]
fn try_work_error(bencher: Bencher) {
    let group = BenchGroup::default();
    let mut context = bench_context();
    bencher.bench_local(|| {
        black_box(poll_ready(
            group.try_work(black_box(0), || async { Err::<usize, ()>(()) }),
            &mut context,
        ))
    });
}

#[divan::bench(args = WAITER_COUNTS)]
fn coalesced_work_batch(bencher: Bencher, waiter_count: usize) {
    let group = BenchGroup::default();
    let mut context = bench_context();

    bencher.bench_local(|| {
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
