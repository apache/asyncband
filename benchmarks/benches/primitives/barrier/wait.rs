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

use asyncband::barrier::Barrier;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::poll_ready;

const PARTICIPANT_COUNTS: &[usize] = &[1, 4, 16];

fn reused_generation_fanout(bencher: Bencher, participant_count: usize) {
    let mut context = bench_context();
    let barrier = Barrier::new((participant_count + 1) as u32);

    bencher.bench_local(|| {
        let mut followers = (0..participant_count)
            .map(|_| Box::pin(barrier.wait()))
            .collect::<Vec<_>>();
        for follower in &mut followers {
            poll_pending(follower.as_mut(), &mut context);
        }

        let leader = poll_ready(barrier.wait(), &mut context);
        for mut follower in followers {
            let result = poll_pinned_ready(follower.as_mut(), &mut context);
            black_box(result.is_leader());
        }
        black_box(leader.is_leader())
    });
}

fn cancel_pending(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let barrier = Barrier::new(2);
        {
            let mut wait = pin!(barrier.wait());
            poll_pending(wait.as_mut(), &mut context);
        }
        black_box(barrier)
    });
}

pub fn register(runner: &mut Runner) {
    for &arg in PARTICIPANT_COUNTS {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::reused_generation_fanout")
            ),
            (5_000_000 / arg).max(1),
            1,
            |bencher| reused_generation_fanout(bencher, arg),
        );
    }
    runner.case(
        concat!(module_path!(), "::cancel_pending"),
        6_000_000,
        1,
        cancel_pending,
    );
}
