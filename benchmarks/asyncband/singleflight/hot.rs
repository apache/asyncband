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

use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::support::BATCH_SAMPLE_SIZE;
use super::support::BATCH_SIZES;
use super::support::BenchGroup;
use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;
use crate::support::wait_until_open;

// Isolates duplicate admission: the leader is already pending and future construction happens
// outside the timed section. A whole batch is needed because one admission is below the timer's
// useful resolution; sample_size=1 lets Divan drop the batch before the next sample instead of
// growing a synthetic waiter backlog across samples.
#[divan::bench(args = BATCH_SIZES, sample_count = 100, sample_size = 1)]
fn join_in_flight(bencher: Bencher, duplicate_count: usize) {
    let group = BenchGroup::default();
    let gate = Cell::new(false);
    let mut context = bench_context();
    let mut leader = Box::pin(group.work(0, || async {
        wait_until_open(&gate).await;
        black_box(1usize)
    }));
    poll_pending(leader.as_mut(), &mut context);

    bencher
        .with_inputs(|| {
            (0..duplicate_count)
                .map(|_| Box::pin(group.work(0, || async { unreachable!() })))
                .collect::<Vec<_>>()
        })
        .counter(ItemsCount::new(duplicate_count))
        .bench_local_values(|mut duplicates| {
            for duplicate in &mut duplicates {
                poll_pending(duplicate.as_mut(), &mut context);
            }
            duplicates
        });

    gate.set(true);
    black_box(poll_pinned_ready(leader.as_mut(), &mut context));
}

// Measures the complete successful fan-in: one leader is suspended, every duplicate joins it, and
// all callers receive the cloned result after the leader completes.
#[divan::bench(args = BATCH_SIZES, sample_size = BATCH_SAMPLE_SIZE)]
fn complete_coalesced(bencher: Bencher, caller_count: usize) {
    let group = BenchGroup::default();
    let mut context = bench_context();
    bencher
        .counter(ItemsCount::new(caller_count))
        .bench_local(|| {
            let gate = Cell::new(false);
            let mut leader = Box::pin(group.work(0, || async {
                wait_until_open(&gate).await;
                black_box(1usize)
            }));
            poll_pending(leader.as_mut(), &mut context);

            let mut duplicates = (1..caller_count)
                .map(|_| Box::pin(group.work(0, || async { unreachable!() })))
                .collect::<Vec<_>>();
            for duplicate in &mut duplicates {
                poll_pending(duplicate.as_mut(), &mut context);
            }

            gate.set(true);
            black_box(poll_pinned_ready(leader.as_mut(), &mut context));
            for mut duplicate in duplicates {
                black_box(poll_pinned_ready(duplicate.as_mut(), &mut context));
            }
        });
}

// SingleFlight errors are not broadcast as a cached result: after the failed leader, one waiting
// caller retries and the remaining callers coalesce on that successful retry.
#[divan::bench(args = BATCH_SIZES, sample_size = BATCH_SAMPLE_SIZE)]
fn retry_after_leader_error(bencher: Bencher, caller_count: usize) {
    let group = BenchGroup::default();
    let mut context = bench_context();
    bencher
        .counter(ItemsCount::new(caller_count))
        .bench_local(|| {
            let gate = Cell::new(false);
            let mut leader = Box::pin(group.try_work(0, || async {
                wait_until_open(&gate).await;
                Err::<usize, ()>(())
            }));
            poll_pending(leader.as_mut(), &mut context);

            let mut retries = (1..caller_count)
                .map(|_| Box::pin(group.try_work(0, || async { Ok::<usize, ()>(1) })))
                .collect::<Vec<_>>();
            for retry in &mut retries {
                poll_pending(retry.as_mut(), &mut context);
            }

            gate.set(true);
            let _ = black_box(poll_pinned_ready(leader.as_mut(), &mut context));
            for mut retry in retries {
                let _ = black_box(poll_pinned_ready(retry.as_mut(), &mut context));
            }
        });
}
