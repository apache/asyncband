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

// Successful try-acquire contention with a small set of distinct borrower keys.
// This measures implementation cost, not whether a core primitive is needed.

use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread;
use std::thread::JoinHandle;

use asyncband::capacity_limiter::CapacityLimiter;
use asyncband::semaphore::Semaphore;
use divan::Bencher;
use divan::black_box;

const WORKER_COUNTS: &[usize] = &[1, 2, 4, 8];

// Held constant across worker counts so a round always performs the same amount of work and the
// reported times compare directly. Every count in `WORKER_COUNTS` divides it exactly.
const OPS_PER_ROUND: usize = 1024;

/// A fixed set of worker threads that hammer a shared primitive one round at a time.
///
/// The threads are created once and parked on a barrier, so thread startup stays out of the
/// measured region and each timed iteration is exactly one round of `OPS_PER_ROUND` operations.
struct Contention {
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    workers: Vec<JoinHandle<()>>,
}

impl Contention {
    fn new<T>(worker_count: usize, shared: Arc<T>, op: fn(&T, usize)) -> Self
    where
        T: Send + Sync + 'static,
    {
        let start = Arc::new(Barrier::new(worker_count + 1));
        let done = Arc::new(Barrier::new(worker_count + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let ops_per_worker = OPS_PER_ROUND / worker_count;
        let mut workers = Vec::with_capacity(worker_count);

        for index in 0..worker_count {
            let shared = shared.clone();
            let start = start.clone();
            let done = done.clone();
            let stop = stop.clone();

            workers.push(thread::spawn(move || {
                loop {
                    start.wait();
                    if stop.load(Ordering::Acquire) {
                        return;
                    }
                    for _ in 0..ops_per_worker {
                        op(&shared, index);
                    }
                    done.wait();
                }
            }));
        }

        Self {
            start,
            done,
            stop,
            workers,
        }
    }

    fn round(&self) {
        self.start.wait();
        self.done.wait();
    }
}

impl Drop for Contention {
    fn drop(&mut self) {
        // The workers are parked on `start` between rounds, so one more release lets them observe
        // the stop flag and exit without reaching `done`.
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        for worker in self.workers.drain(..) {
            worker.join().expect("worker threads must not panic");
        }
    }
}

// Capacity equals the worker count so every attempt succeeds. The benchmark measures the cost of a
// successful acquire and release under contention, not the cost of being turned away.

#[divan::bench(args = WORKER_COUNTS)]
fn semaphore_try_acquire_release(bencher: Bencher, workers: usize) {
    let contention = Contention::new(
        workers,
        Arc::new(Semaphore::new(workers)),
        |semaphore, _| {
            drop(black_box(
                semaphore.try_acquire(1).expect("capacity is available"),
            ));
        },
    );

    bencher.bench_local(|| contention.round());
}

#[divan::bench(args = WORKER_COUNTS)]
fn limiter_try_acquire_release(bencher: Bencher, workers: usize) {
    let limiter: Arc<CapacityLimiter<usize>> = Arc::new(CapacityLimiter::new(workers));
    let contention = Contention::new(workers, limiter, |limiter, index| {
        // Each worker owns a distinct borrower, so nothing is rejected for identity reasons and the
        // registry stays at `workers` live entries.
        drop(black_box(
            limiter.try_acquire(index).expect("capacity is available"),
        ));
    });

    bencher.bench_local(|| contention.round());
}
