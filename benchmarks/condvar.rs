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

use asyncband::condvar::Condvar;
use asyncband::mutex::Mutex;
use divan::Bencher;
use divan::black_box;

use super::support::bench_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;
use super::support::poll_ready;

const WAITER_COUNTS: &[usize] = &[1, 8, 32];

#[divan::bench]
fn cancel_pending(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let mutex = Mutex::new(());
        let condvar = Condvar::new();
        let guard = poll_ready(mutex.lock(), &mut context);

        {
            let mut wait = pin!(condvar.wait(guard));
            poll_pending(wait.as_mut(), &mut context);
        }

        black_box(mutex.try_lock().is_some())
    });
}

#[divan::bench]
fn notify_waiter(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let mutex = Mutex::new(());
        let condvar = Condvar::new();
        let guard = poll_ready(mutex.lock(), &mut context);
        let mut wait = pin!(condvar.wait(guard));
        poll_pending(wait.as_mut(), &mut context);

        condvar.notify_one();
        let guard = poll_pinned_ready(wait.as_mut(), &mut context);
        black_box(&guard);
        drop(guard);
    });
}

#[divan::bench(args = WAITER_COUNTS)]
fn notify_waiter_batch(bencher: Bencher, waiter_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let mutex = Mutex::new(());
        let condvar = Condvar::new();
        let mut waiters = Vec::with_capacity(waiter_count);
        for _ in 0..waiter_count {
            let guard = poll_ready(mutex.lock(), &mut context);
            let mut wait = Box::pin(condvar.wait(guard));
            poll_pending(wait.as_mut(), &mut context);
            waiters.push(wait);
        }

        condvar.notify_all();
        for mut waiter in waiters {
            let guard = poll_pinned_ready(waiter.as_mut(), &mut context);
            black_box(&guard);
            drop(guard);
        }
    });
}
