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

use asyncband::completion;
use divan::Bencher;
use divan::black_box;

use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;
use crate::support::poll_ready;

const OBSERVER_COUNTS: &[usize] = &[1, 2, 4, 8, 32];

#[divan::bench]
fn ready_wait(bencher: Bencher) {
    let mut context = bench_context();
    let (completer, completion) = completion::new();
    completer.complete(1usize).unwrap();

    bencher.bench_local(|| black_box(*poll_ready(completion.wait(), &mut context).unwrap()));
}

#[divan::bench]
fn cancel_pending(bencher: Bencher) {
    let mut context = bench_context();
    let (_completer, completion) = completion::new::<usize>();

    bencher.bench_local(|| {
        let mut wait = pin!(completion.wait());
        poll_pending(wait.as_mut(), &mut context);
    });
    black_box(completion);
}

#[divan::bench]
fn complete_then_wait(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (completer, completion) = black_box(completion::new());
        completer.complete(black_box(1usize)).unwrap();
        black_box(*poll_ready(completion.wait(), &mut context).unwrap())
    });
}

#[divan::bench]
fn notify_pending(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (completer, completion) = black_box(completion::new());
        let mut wait = pin!(completion.wait());
        poll_pending(wait.as_mut(), &mut context);

        completer.complete(black_box(1usize)).unwrap();
        black_box(*poll_pinned_ready(wait.as_mut(), &mut context).unwrap())
    });
}

#[divan::bench(args = OBSERVER_COUNTS)]
fn notify_pending_fanout(bencher: Bencher, observer_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (completer, first) = black_box(completion::new());
        let mut observers = Vec::with_capacity(observer_count);
        observers.push(first);
        for _ in 1..observer_count {
            observers.push(observers[0].clone());
        }
        let mut waiters = observers
            .iter()
            .map(|observer| Box::pin(observer.wait()))
            .collect::<Vec<_>>();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }

        completer.complete(black_box(1usize)).unwrap();
        for mut waiter in waiters {
            black_box(*poll_pinned_ready(waiter.as_mut(), &mut context).unwrap());
        }
    });
}
