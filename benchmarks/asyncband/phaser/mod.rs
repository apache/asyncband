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

use asyncband::phaser::Phaser;
use divan::Bencher;
use divan::black_box;

use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;
use crate::support::poll_ready;

const PARTICIPANT_COUNTS: &[usize] = &[2, 8, 32];

#[divan::bench]
fn ready_wait(bencher: Bencher) {
    let mut context = bench_context();
    let phaser = Phaser::new();
    let observed = phaser.phase();
    phaser.register_one().unwrap().arrive().unwrap();

    bencher.bench_local(|| black_box(poll_ready(phaser.wait(observed), &mut context).unwrap()));
}

#[divan::bench]
fn repoll_pending(bencher: Bencher) {
    let mut context = bench_context();
    let phaser = Phaser::new();
    let mut wait = pin!(phaser.wait(phaser.phase()));
    poll_pending(wait.as_mut(), &mut context);

    bencher.bench_local(|| poll_pending(wait.as_mut(), &mut context));
}

#[divan::bench]
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

#[divan::bench(args = PARTICIPANT_COUNTS)]
fn register_batch(bencher: Bencher, parties: usize) {
    let phaser = Phaser::new();

    bencher.bench_local(|| {
        let participants: Vec<_> = phaser.register(black_box(parties)).unwrap().collect();
        drop(black_box(participants));
    });
}

#[divan::bench(args = PARTICIPANT_COUNTS)]
fn register_individually(bencher: Bencher, parties: usize) {
    let phaser = Phaser::new();

    bencher.bench_local(|| {
        let participants: Vec<_> = (0..black_box(parties))
            .map(|_| phaser.register_one().unwrap())
            .collect();
        drop(black_box(participants));
    });
}

#[divan::bench(args = PARTICIPANT_COUNTS)]
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

#[divan::bench(args = PARTICIPANT_COUNTS)]
fn notify_pending_fanout(bencher: Bencher, parties: usize) {
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
