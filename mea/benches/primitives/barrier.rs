// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::pin::pin;

use divan::Bencher;
use divan::black_box;
use mea::barrier::Barrier;

use super::support::bench_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;
use super::support::poll_ready;

const PARTICIPANT_COUNTS: &[usize] = &[1, 8, 32];

#[divan::bench]
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

#[divan::bench]
fn complete_generation(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let barrier = Barrier::new(2);
        let mut first = pin!(barrier.wait());
        poll_pending(first.as_mut(), &mut context);

        let leader = poll_ready(barrier.wait(), &mut context);
        let follower = poll_pinned_ready(first.as_mut(), &mut context);

        black_box((leader.is_leader(), follower.is_leader()))
    });
}

#[divan::bench(args = PARTICIPANT_COUNTS)]
fn complete_generation_batch(bencher: Bencher, participant_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let barrier = Barrier::new((participant_count + 1) as u32);
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
