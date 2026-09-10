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
use super::support::CONTENDED_SAMPLE_SIZE;
use super::support::FAST_SAMPLE_SIZE;
use super::support::THREAD_COUNTS;
use super::support::unique_thread_key;
use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;
use crate::support::poll_ready;
use crate::support::wait_until_open;

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn construct_default(bencher: Bencher) {
    bencher.bench_local(|| black_box(BenchGroup::default()));
}

// The group is intentionally long-lived. Every call starts with no in-flight entry, while the
// table can retain capacity just as a production Group does across completed calls.
#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn leader_success(bencher: Bencher) {
    let group = BenchGroup::default();
    let mut context = bench_context();
    bencher.bench_local(|| {
        black_box(poll_ready(
            group.work(black_box(0), || async { black_box(1) }),
            &mut context,
        ))
    });
}

#[divan::bench(sample_size = FAST_SAMPLE_SIZE)]
fn leader_error(bencher: Bencher) {
    let group = BenchGroup::default();
    let mut context = bench_context();
    bencher.bench_local(|| {
        black_box(poll_ready(
            group.try_work(black_box(0), || async { Err::<usize, ()>(()) }),
            &mut context,
        ))
    });
}

// Suspends every leader before completing it so all distinct-key entries coexist. This isolates
// the table bookkeeping from same-key duplicate suppression without depending on thread timing.
#[divan::bench(args = BATCH_SIZES, sample_size = BATCH_SAMPLE_SIZE)]
fn distinct_keys(bencher: Bencher, caller_count: usize) {
    let group = BenchGroup::default();
    let mut context = bench_context();
    bencher
        .counter(ItemsCount::new(caller_count))
        .bench_local(|| {
            let gate = Cell::new(false);
            let mut calls = (0..caller_count)
                .map(|key| {
                    let gate = &gate;
                    Box::pin(group.work(key, move || async move {
                        wait_until_open(gate).await;
                        black_box(key)
                    }))
                })
                .collect::<Vec<_>>();
            for call in &mut calls {
                poll_pending(call.as_mut(), &mut context);
            }

            gate.set(true);
            for mut call in calls {
                black_box(poll_pinned_ready(call.as_mut(), &mut context));
            }
        });
}

// Every call owns a unique key, so this measures cross-thread contention on the shared table while
// preserving SingleFlight's normal insert-work-remove lifecycle. It deliberately does not claim
// to measure same-key coalescing, which cannot be guaranteed by Divan's thread scheduling.
#[divan::bench(threads = THREAD_COUNTS, sample_size = CONTENDED_SAMPLE_SIZE)]
fn distributed_leaders(bencher: Bencher) {
    let group = BenchGroup::default();
    bencher.with_inputs(unique_thread_key).bench_values(|key| {
        let mut context = bench_context();
        black_box(poll_ready(
            group.work(black_box(key), || async move { black_box(key) }),
            &mut context,
        ))
    });
}
