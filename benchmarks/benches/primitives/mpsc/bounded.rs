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

use asyncband::mpsc;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;

const SENDER_COUNTS: &[usize] = &[1, 4, 16];

fn drain_backpressured_senders(bencher: Bencher, sender_count: usize) {
    let mut context = bench_context();
    let (sender, mut receiver) = mpsc::bounded(1);
    let senders = (0..sender_count)
        .map(|_| sender.clone())
        .collect::<Vec<_>>();

    bencher.bench_local(|| {
        sender.try_send(black_box(usize::MAX)).unwrap();
        let mut sends = senders
            .iter()
            .enumerate()
            .map(|(index, sender)| Box::pin(sender.send(index)))
            .collect::<Vec<_>>();
        for send in &mut sends {
            poll_pending(send.as_mut(), &mut context);
        }

        for mut send in sends {
            black_box(receiver.try_recv().unwrap());
            poll_pinned_ready(send.as_mut(), &mut context).unwrap();
        }
        black_box(receiver.try_recv().unwrap())
    });
}

fn cancel_backpressured_senders(bencher: Bencher, sender_count: usize) {
    let mut context = bench_context();
    let (sender, mut receiver) = mpsc::bounded(1);
    let senders = (0..sender_count)
        .map(|_| sender.clone())
        .collect::<Vec<_>>();

    bencher.bench_local(|| {
        sender.try_send(black_box(usize::MAX)).unwrap();
        let mut sends = senders
            .iter()
            .enumerate()
            .map(|(index, sender)| Box::pin(sender.send(index)))
            .collect::<Vec<_>>();
        for send in &mut sends {
            poll_pending(send.as_mut(), &mut context);
        }

        drop(sends);
        black_box(receiver.try_recv().unwrap())
    });
}

pub fn register(runner: &mut Runner) {
    for &arg in SENDER_COUNTS {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::drain_backpressured_senders")
            ),
            (5_000_000 / arg).max(1),
            1,
            |bencher| drain_backpressured_senders(bencher, arg),
        );
    }
    for &arg in SENDER_COUNTS {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::cancel_backpressured_senders")
            ),
            (8_000_000 / arg).max(1),
            1,
            |bencher| cancel_backpressured_senders(bencher, arg),
        );
    }
}
