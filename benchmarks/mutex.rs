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
use std::sync::Arc;

use asyncband::mutex::Mutex;
use divan::Bencher;
use divan::black_box;

use super::support::bench_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;
use super::support::poll_ready;

const QUEUE_DEPTHS: &[usize] = &[1, 8, 32];

#[divan::bench]
fn uncontended_reuse(bencher: Bencher) {
    let mutex = Mutex::new(0usize);
    let mut context = bench_context();

    bencher.bench_local(|| {
        let mut guard = poll_ready(mutex.lock(), &mut context);
        *guard = black_box(guard.wrapping_add(1));
        black_box(*guard);
    });
}

#[divan::bench(args = QUEUE_DEPTHS)]
fn queued_handoff(bencher: Bencher, queue_depth: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let mutex = Arc::new(Mutex::new(0usize));
        let held = poll_ready(mutex.clone().lock_owned(), &mut context);
        let mut waiters = (0..queue_depth)
            .map(|_| Box::pin(mutex.clone().lock_owned()))
            .collect::<Vec<Pin<Box<_>>>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }

        drop(held);
        for mut waiter in waiters {
            let mut guard = poll_pinned_ready(waiter.as_mut(), &mut context);
            *guard = guard.wrapping_add(1);
        }

        let value = *mutex.try_lock().expect("queued mutex handoff must finish");
        black_box(value)
    });
}
