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

use std::hint::black_box;
use std::pin::pin;

use asyncband::completion;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::poll_ready;

const OBSERVER_COUNTS: &[usize] = &[2, 4, 16];

fn ready_wait(bencher: Bencher) {
    let mut context = bench_context();
    let (completer, completion) = completion::new();
    completer.complete(1usize).unwrap();

    bencher.bench_local(|| black_box(*poll_ready(completion.wait(), &mut context).unwrap()));
}

fn complete_then_wait(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (completer, completion) = black_box(completion::new());
        completer.complete(black_box(1usize)).unwrap();
        black_box(*poll_ready(completion.wait(), &mut context).unwrap())
    });
}

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

fn notify_pending_fanout(bencher: Bencher, observer_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (completer, completion) = black_box(completion::new());
        let mut waiters = (0..observer_count)
            .map(|_| Box::pin(completion.wait()))
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

fn abandoned_wait(bencher: Bencher) {
    let mut context = bench_context();
    let (completer, completion) = completion::new::<usize>();
    drop(completer);

    bencher.bench_local(|| black_box(poll_ready(completion.wait(), &mut context).unwrap_err()));
}

fn repoll_pending(bencher: Bencher) {
    let mut context = bench_context();
    let (_completer, completion) = completion::new::<usize>();
    let mut wait = pin!(completion.wait());
    poll_pending(wait.as_mut(), &mut context);

    bencher.bench_local(|| poll_pending(wait.as_mut(), &mut context));
}

fn cancel_pending(bencher: Bencher) {
    let mut context = bench_context();
    let (_completer, completion) = completion::new::<usize>();

    bencher.bench_local(|| {
        let mut wait = pin!(completion.wait());
        poll_pending(wait.as_mut(), &mut context);
    });
    black_box(completion);
}

pub fn register(runner: &mut Runner) {
    runner.case(
        concat!(module_path!(), "::ready_wait"),
        100_000_000,
        1,
        ready_wait,
    );
    runner.case(
        concat!(module_path!(), "::complete_then_wait"),
        6_000_000,
        1,
        complete_then_wait,
    );
    runner.case(
        concat!(module_path!(), "::notify_pending"),
        3_000_000,
        1,
        notify_pending,
    );
    for &arg in OBSERVER_COUNTS {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::notify_pending_fanout")
            ),
            (5_000_000 / arg).max(1),
            1,
            |bencher| notify_pending_fanout(bencher, arg),
        );
    }
    runner.case(
        concat!(module_path!(), "::abandoned_wait"),
        96_000_000,
        1,
        abandoned_wait,
    );
    runner.case(
        concat!(module_path!(), "::repoll_pending"),
        50_000_000,
        1,
        repoll_pending,
    );
    runner.case(
        concat!(module_path!(), "::cancel_pending"),
        24_000_000,
        1,
        cancel_pending,
    );
}
