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

use std::future::IntoFuture;

use asyncband::waitgroup::WaitGroup;
use divan::Bencher;
use divan::black_box;

use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;

const WAITER_COUNTS: &[usize] = &[1, 8, 32];

#[divan::bench(args = WAITER_COUNTS)]
fn complete_waiter_batch(bencher: Bencher, waiter_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let root = WaitGroup::new();
        let worker = root.clone();
        let wait = root.into_future();
        let mut waiters = (0..waiter_count)
            .map(|_| Box::pin(wait.clone()))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }

        drop(worker);
        for mut waiter in waiters {
            poll_pinned_ready(waiter.as_mut(), &mut context);
        }
        black_box(())
    });
}
