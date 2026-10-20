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

use asyncband::phaser::Phaser;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::poll_ready;

const PARTICIPANT_COUNTS: &[usize] = &[2, 4, 16];

fn ready_wait(bencher: Bencher) {
    let mut context = bench_context();
    let phaser = Phaser::new();
    let observed = phaser.phase();
    phaser.register_one().unwrap().arrive().unwrap();

    bencher.bench_local(|| black_box(poll_ready(phaser.wait(observed), &mut context).unwrap()));
}

fn arrive_then_wait(bencher: Bencher, parties: usize) {
    let mut context = bench_context();
    let phaser = Phaser::new();
    let mut participants: Vec<_> = phaser.register(parties).unwrap().collect();

    bencher.bench_local(|| {
        for participant in &mut participants {
            black_box(participant.arrive().unwrap());
        }
        for participant in &mut participants {
            black_box(poll_ready(participant.wait(), &mut context).unwrap());
        }
    });
}

fn reused_phase_handoff(bencher: Bencher, parties: usize) {
    let mut context = bench_context();
    let phaser = Phaser::new();
    let mut participants: Vec<_> = phaser.register(parties).unwrap().collect();
    let (last, others) = participants.split_last_mut().unwrap();

    bencher.bench_local(|| {
        let mut waiters: Vec<_> = others.iter_mut().map(|p| Box::pin(p.wait())).collect();
        for waiter in &mut waiters {
            poll_pending(waiter.as_mut(), &mut context);
        }
        black_box(poll_ready(last.wait(), &mut context).unwrap());
        for waiter in &mut waiters {
            black_box(poll_pinned_ready(waiter.as_mut(), &mut context).unwrap());
        }
    });
}

fn repoll_pending(bencher: Bencher) {
    let mut context = bench_context();
    let phaser = Phaser::new();
    let mut wait = pin!(phaser.wait(phaser.phase()));
    poll_pending(wait.as_mut(), &mut context);

    bencher.bench_local(|| poll_pending(wait.as_mut(), &mut context));
}

fn cancel_pending(bencher: Bencher) {
    let mut context = bench_context();
    let phaser = Phaser::new();
    let observed = phaser.phase();

    bencher.bench_local(|| {
        let mut wait = pin!(phaser.wait(observed));
        poll_pending(wait.as_mut(), &mut context);
    });
    black_box(phaser);
}

fn register_batch(bencher: Bencher, parties: usize) {
    let phaser = Phaser::new();

    bencher.bench_local(|| {
        let participants: Vec<_> = phaser.register(black_box(parties)).unwrap().collect();
        drop(black_box(participants));
    });
}

fn register_individually(bencher: Bencher, parties: usize) {
    let phaser = Phaser::new();

    bencher.bench_local(|| {
        let participants: Vec<_> = (0..black_box(parties))
            .map(|_| phaser.register_one().unwrap())
            .collect();
        drop(black_box(participants));
    });
}

pub fn register(runner: &mut Runner) {
    runner.case(
        concat!(module_path!(), "::ready_wait"),
        50_000_000,
        1,
        ready_wait,
    );
    for &arg in PARTICIPANT_COUNTS {
        runner.case(
            format!("{}/{arg}", concat!(module_path!(), "::arrive_then_wait")),
            (20_000_000 / arg).max(1),
            1,
            |bencher| arrive_then_wait(bencher, arg),
        );
    }
    for &arg in PARTICIPANT_COUNTS {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::reused_phase_handoff")
            ),
            (8_000_000 / arg).max(1),
            1,
            |bencher| reused_phase_handoff(bencher, arg),
        );
    }
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
    for &arg in PARTICIPANT_COUNTS {
        runner.case(
            format!("{}/{arg}", concat!(module_path!(), "::register_batch")),
            (20_000_000 / arg).max(1),
            1,
            |bencher| register_batch(bencher, arg),
        );
    }
    for &arg in PARTICIPANT_COUNTS {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::register_individually")
            ),
            (16_000_000 / arg).max(1),
            1,
            |bencher| register_individually(bencher, arg),
        );
    }
}
