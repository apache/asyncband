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

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Wake;
use std::task::Waker;

use asyncband::task_group::TaskGroup;
use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;
use crate::support::poll_ready;

const BATCH_SIZES: &[usize] = &[1, 8, 32, 128];
const THREAD_COUNTS: &[usize] = &[1, 2, 8, 32];
const CONTENDED_SAMPLE_SIZE: u32 = 256;

struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[divan::bench(threads = THREAD_COUNTS, sample_size = CONTENDED_SAMPLE_SIZE)]
fn is_closed_contended(bencher: Bencher) {
    let (group, _registrar) = TaskGroup::<()>::new();
    bencher.bench(|| black_box(group.is_closed()));
}

#[divan::bench(threads = THREAD_COUNTS, sample_size = CONTENDED_SAMPLE_SIZE)]
fn register_then_drop_contended(bencher: Bencher) {
    let (group, registrar) = TaskGroup::new();
    bencher.bench(|| drop(black_box(registrar.track(async {}).unwrap())));
    black_box(group);
}

// Every thread completes tasks into the same group. Returning `()` avoids allocating output data,
// so the result mainly shows the cost of updating one shared count and output queue.
#[divan::bench(threads = THREAD_COUNTS, sample_size = CONTENDED_SAMPLE_SIZE)]
fn complete_contended(bencher: Bencher) {
    let (group, registrar) = TaskGroup::new();
    bencher.bench(|| {
        let task = registrar.track(std::future::ready(())).unwrap();
        let mut task = pin!(task);
        let mut context = Context::from_waker(Waker::noop());
        black_box(task.as_mut().poll(&mut context))
    });
    black_box(group);
}

// Keep `wait()` pending while every thread completes tasks returning `()`. In this mode the group
// discards each output instead of adding it to the queue.
#[divan::bench(threads = THREAD_COUNTS, sample_size = CONTENDED_SAMPLE_SIZE)]
fn complete_discarded_contended(bencher: Bencher) {
    let (mut group, registrar) = TaskGroup::new();
    let mut context = bench_context();
    let wait = group.wait();
    let mut wait = pin!(wait);
    poll_pending(wait.as_mut(), &mut context);

    bencher.bench(|| {
        let task = registrar.track(std::future::ready(())).unwrap();
        let mut task = pin!(task);
        let mut context = Context::from_waker(Waker::noop());
        black_box(task.as_mut().poll(&mut context))
    });
}

#[divan::bench(threads = THREAD_COUNTS, sample_size = CONTENDED_SAMPLE_SIZE)]
fn reject_registration_contended(bencher: Bencher) {
    let (group, registrar) = TaskGroup::<()>::new();
    group.close();
    bencher.bench(|| black_box(registrar.track(async {}).is_err()));
}

#[divan::bench]
fn complete_then_join(bencher: Bencher) {
    let mut context = bench_context();
    let (mut group, registrar) = TaskGroup::new();

    bencher.bench_local(|| {
        let task = registrar.track(async { black_box(1usize) }).unwrap();
        poll_ready(task, &mut context);
        black_box(poll_ready(group.join_next(), &mut context).unwrap())
    });
}

#[divan::bench]
fn wake_pending_join(bencher: Bencher) {
    let mut context = bench_context();
    let (mut group, registrar) = TaskGroup::new();

    bencher.bench_local(|| {
        let task = registrar.track(async { black_box(1usize) }).unwrap();
        let mut join = pin!(group.join_next());
        poll_pending(join.as_mut(), &mut context);

        poll_ready(task, &mut context);
        black_box(poll_pinned_ready(join.as_mut(), &mut context).unwrap())
    });
}

#[divan::bench(args = BATCH_SIZES)]
fn complete_batch_then_join(bencher: Bencher, task_count: usize) {
    let mut context = bench_context();
    let (mut group, registrar) = TaskGroup::new();

    bencher
        .counter(ItemsCount::new(task_count))
        .bench_local(|| {
            for output in 0..task_count {
                let task = registrar.track(async move { black_box(output) }).unwrap();
                poll_ready(task, &mut context);
            }
            for _ in 0..task_count {
                black_box(poll_ready(group.join_next(), &mut context).unwrap());
            }
        });
}

// Prepare a closed group with every output already queued before timing starts. The measured work
// is `join()` moving those outputs into its result vector.
#[divan::bench(args = BATCH_SIZES)]
fn collect_completed_batch(bencher: Bencher, task_count: usize) {
    let mut context = bench_context();

    bencher
        .with_inputs(|| {
            let mut setup_context = bench_context();
            let (group, registrar) = TaskGroup::new();
            for output in 0..task_count {
                let task = registrar.track(async move { output }).unwrap();
                poll_ready(task, &mut setup_context);
            }
            group.close();
            group
        })
        .counter(ItemsCount::new(task_count))
        .bench_local_values(|mut group| black_box(poll_ready(group.join(), &mut context)));
}

#[divan::bench(args = BATCH_SIZES)]
fn complete_batch_then_collect(bencher: Bencher, task_count: usize) {
    let mut context = bench_context();

    bencher
        .with_inputs(|| {
            let (group, registrar) = TaskGroup::new();
            let tasks = (0..task_count)
                .map(|output| registrar.track(std::future::ready(output)).unwrap())
                .collect::<Vec<_>>();
            group.close();
            (group, tasks)
        })
        .counter(ItemsCount::new(task_count))
        .bench_local_values(|(mut group, tasks)| {
            let mut join = pin!(group.join());
            poll_pending(join.as_mut(), &mut context);
            for task in tasks {
                poll_ready(task, &mut context);
            }
            black_box(poll_pinned_ready(join.as_mut(), &mut context))
        });
}

#[divan::bench(args = BATCH_SIZES)]
fn complete_batch_then_wait(bencher: Bencher, task_count: usize) {
    let mut context = bench_context();

    bencher
        .with_inputs(|| {
            let (group, registrar) = TaskGroup::new();
            let tasks = (0..task_count)
                .map(|_| registrar.track(std::future::ready(())).unwrap())
                .collect::<Vec<_>>();
            group.close();
            (group, tasks)
        })
        .counter(ItemsCount::new(task_count))
        .bench_local_values(|(mut group, tasks)| {
            let mut wait = pin!(group.wait());
            poll_pending(wait.as_mut(), &mut context);
            for task in tasks {
                poll_ready(task, &mut context);
            }
            poll_pinned_ready(wait.as_mut(), &mut context);
        });
}

// Complete futures one at a time and poll `wait()` after every wake, as an executor normally would.
// `wait()` should wake only after the final future finishes.
#[divan::bench(args = BATCH_SIZES)]
fn complete_staggered_then_wait(bencher: Bencher, task_count: usize) {
    bencher
        .with_inputs(|| {
            let (group, registrar) = TaskGroup::new();
            let tasks = (0..task_count)
                .map(|_| registrar.track(std::future::ready(())).unwrap())
                .collect::<Vec<_>>();
            group.close();

            let wake_count = Arc::new(CountWake(AtomicUsize::new(0)));
            let waker = Waker::from(wake_count.clone());
            (group, tasks, wake_count, waker)
        })
        .counter(ItemsCount::new(task_count))
        .bench_local_values(|(mut group, tasks, wake_count, waker)| {
            let mut context = Context::from_waker(&waker);
            let mut wait = pin!(group.wait());
            poll_pending(wait.as_mut(), &mut context);

            let mut observed_wakes = 0;
            for (index, task) in tasks.into_iter().enumerate() {
                poll_ready(task, &mut context);
                let current_wakes = wake_count.0.load(Ordering::Relaxed);
                if current_wakes != observed_wakes && index + 1 < task_count {
                    observed_wakes = current_wakes;
                    poll_pending(wait.as_mut(), &mut context);
                }
            }

            poll_pinned_ready(wait.as_mut(), &mut context);
            black_box(wake_count.0.load(Ordering::Relaxed))
        });
}
