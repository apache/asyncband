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

// Both adapters return an owned `usize`. The Tokio adapter's `recv` combines `changed` and
// `borrow_and_update`.

use std::hint::black_box;
use std::pin::pin;

use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use benchmarks::support::poll_ready;

use crate::watch::adapters::Asyncband;
use crate::watch::adapters::Tokio;
use crate::watch::adapters::Watch;

const RECEIVER_COUNTS: &[usize] = &[2, 4, 8, 32];

fn get_current<C: Watch>(bencher: Bencher) {
    let (sender, mut receivers) = C::channel(1);
    let receiver = receivers.pop().unwrap();
    bencher.bench_local(|| black_box(C::get(&receiver)));
    black_box(sender);
}

fn publish_then_snapshot<C: Watch>(bencher: Bencher) {
    let (sender, mut receivers) = C::channel(1);
    let receiver = receivers.pop().unwrap();
    bencher.bench_local(|| {
        C::send(&sender, black_box(1));
        black_box(C::get(&receiver))
    });
}

fn publish_then_receive<C: Watch>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(1);
    let mut receiver = receivers.pop().unwrap();
    bencher.bench_local(|| {
        C::send(&sender, black_box(1));
        black_box(poll_ready(C::recv(&mut receiver), &mut context))
    });
}

fn publish_then_observe_version<C: Watch>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(1);
    let mut receiver = receivers.pop().unwrap();
    bencher.bench_local(|| {
        C::send(&sender, black_box(1));
        poll_ready(C::changed(&mut receiver), &mut context);
    });
}

fn register_publish_observe<C: Watch>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(1);
    let mut receiver = receivers.pop().unwrap();
    bencher.bench_local(|| {
        let mut changed = pin!(C::changed(&mut receiver));
        poll_pending(changed.as_mut(), &mut context);
        C::send(&sender, black_box(1));
        poll_pinned_ready(changed.as_mut(), &mut context);
    });
}

fn register_publish_observe_fanout<C: Watch>(bencher: Bencher, receiver_count: usize) {
    let mut context = bench_context();
    let (sender, mut receivers) = C::channel(receiver_count);

    bencher.bench_local(|| {
        let mut changed = receivers
            .iter_mut()
            .map(|receiver| Box::pin(C::changed(receiver)))
            .collect::<Vec<_>>();
        for future in &mut changed {
            poll_pending(future.as_mut(), &mut context);
        }

        C::send(&sender, black_box(1));
        for mut future in changed {
            poll_pinned_ready(future.as_mut(), &mut context);
        }
    });
}

pub fn register(runner: &mut Runner) {
    register_implementation::<Asyncband>(runner, "Asyncband");
    register_implementation::<Tokio>(runner, "Tokio");
}

fn register_implementation<C>(runner: &mut Runner, implementation: &str)
where
    C: Watch,
{
    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::get_current")
        ),
        80_000_000,
        1,
        get_current::<C>,
    );

    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::publish_then_snapshot")
        ),
        15_000_000,
        1,
        publish_then_snapshot::<C>,
    );

    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::publish_then_receive")
        ),
        9_000_000,
        1,
        publish_then_receive::<C>,
    );

    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::publish_then_observe_version")
        ),
        12_000_000,
        1,
        publish_then_observe_version::<C>,
    );

    runner.case(
        format!(
            "{}/{implementation}",
            concat!(module_path!(), "::register_publish_observe")
        ),
        6_000_000,
        1,
        register_publish_observe::<C>,
    );

    for &arg in RECEIVER_COUNTS {
        runner.case(
            format!(
                "{}/{implementation}/{arg}",
                concat!(module_path!(), "::register_publish_observe_fanout")
            ),
            (5_000_000 / arg).max(1),
            1,
            |bencher| register_publish_observe_fanout::<C>(bencher, arg),
        );
    }
}
