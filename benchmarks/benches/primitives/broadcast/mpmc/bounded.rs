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

// Restore backlog and permits each iteration so later rounds cannot stall on exhausted capacity.

use std::hint::black_box;
use std::pin::pin;

use asyncband::broadcast::mpmc;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::support::bench_context;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;

const RECEIVER_COUNTS: &[usize] = &[1, 4, 16];
const BLOCKED_SENDER_COUNTS: &[usize] = &[1, 4, 16];
const CAPACITY: usize = 64;

fn try_send_and_try_recv(bencher: Bencher) {
    let (tx, mut rx) = mpmc::bounded(CAPACITY);

    bencher.bench_local(|| {
        tx.try_send(black_box(1)).unwrap();
        black_box(rx.try_recv().unwrap())
    });
}

fn try_send_and_drain_fanout(bencher: Bencher, receiver_count: usize) {
    let (tx, rx) = mpmc::bounded(CAPACITY);
    let mut receivers = Vec::with_capacity(receiver_count);
    receivers.push(rx);
    for _ in 1..receiver_count {
        receivers.push(tx.subscribe());
    }

    // One message in, every receiver drains it out: the last one to read pays the head reclaim
    // and the capacity release, and the channel is empty again for the next iteration.
    bencher.bench_local(|| {
        tx.try_send(black_box(1)).unwrap();
        for receiver in &mut receivers {
            black_box(receiver.try_recv().unwrap());
        }
    });
}

fn full_channel_sender_handoff_cycle(bencher: Bencher, sender_count: usize) {
    let mut context = bench_context();
    let (tx, mut rx) = mpmc::bounded(1);
    tx.try_send(0).unwrap();

    // Register every producer on a full channel, complete one `send` future after reclaiming a
    // slot, cancel the remaining futures, and restore the original full backlog. Registration
    // and cancellation are timed as part of this cycle.
    bencher.bench_local(|| {
        let mut sends = (0..sender_count)
            .map(|value| Box::pin(tx.send(value)))
            .collect::<Vec<_>>();
        for send in &mut sends {
            poll_pending(send.as_mut(), &mut context);
        }

        // Poll pending `send` futures until one publishes into the freed slot.
        black_box(rx.try_recv().unwrap());
        for send in &mut sends {
            if send.as_mut().poll(&mut context).is_ready() {
                break;
            }
        }

        // Drain the republished message so the next iteration starts from the same state.
        black_box(rx.try_recv().unwrap());
        drop(sends);
        tx.try_send(0).unwrap();
    });
}

fn deliver_to_waiting_receiver(bencher: Bencher) {
    let mut context = bench_context();
    let (tx, mut rx) = mpmc::bounded(CAPACITY);

    bencher.bench_local(|| {
        let mut recv = pin!(rx.recv());
        poll_pending(recv.as_mut(), &mut context);
        tx.try_send(black_box(1)).unwrap();
        black_box(poll_pinned_ready(recv, &mut context).unwrap())
    });
}

fn send_without_receivers(bencher: Bencher) {
    // With no subscribers, this measures the discard path.
    let (tx, rx) = mpmc::bounded(CAPACITY);
    drop(rx);

    bencher.bench_local(|| tx.try_send(black_box(1)));
}

fn try_send_when_full(bencher: Bencher) {
    let (tx, _rx) = mpmc::bounded(1);
    tx.try_send(0).unwrap();

    bencher.bench_local(|| black_box(tx.try_send(black_box(1))).is_err());
}

fn cancel_blocked_send(bencher: Bencher) {
    let mut context = bench_context();
    let (tx, _rx) = mpmc::bounded(1);
    tx.try_send(0).unwrap();

    bencher.bench_local(|| {
        let send = pin!(tx.send(black_box(1)));
        poll_pending(send, &mut context);
    });
}

fn drop_lagging_receiver_wakes_senders(bencher: Bencher, backlog: usize) {
    bencher.bench_local(|| {
        let (slow, fast, sends) = {
            let (sender, mut fast) = mpmc::bounded(backlog);
            let slow = sender.subscribe();
            for value in 0..backlog {
                sender.try_send(value).unwrap();
                assert_eq!(fast.try_recv().unwrap(), value);
            }
            let mut context = bench_context();
            let mut sends = (0..backlog)
                .map(|value| {
                    let sender = sender.clone();
                    Box::pin(async move { sender.send(value).await })
                })
                .collect::<Vec<_>>();
            for send in &mut sends {
                poll_pending(send.as_mut(), &mut context);
            }
            (slow, fast, sends)
        };
        // The fast subscription stays alive so this measures reclaim, not last-receiver
        // exit. Each round also prepares the backlog, parks senders, and disposes of futures.
        drop(slow);
        drop((fast, sends));
    });
}

pub fn register(runner: &mut Runner) {
    runner.case(
        concat!(module_path!(), "::try_send_and_try_recv"),
        9_000_000,
        1,
        try_send_and_try_recv,
    );
    for &arg in RECEIVER_COUNTS {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::try_send_and_drain_fanout")
            ),
            (20_000_000 / arg).max(1),
            1,
            |bencher| try_send_and_drain_fanout(bencher, arg),
        );
    }
    for &arg in BLOCKED_SENDER_COUNTS {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::full_channel_sender_handoff_cycle")
            ),
            (2_000_000 / arg).max(1),
            1,
            |bencher| full_channel_sender_handoff_cycle(bencher, arg),
        );
    }
    runner.case(
        concat!(module_path!(), "::deliver_to_waiting_receiver"),
        6_000_000,
        1,
        deliver_to_waiting_receiver,
    );
    runner.case(
        concat!(module_path!(), "::send_without_receivers"),
        48_000_000,
        1,
        send_without_receivers,
    );
    runner.case(
        concat!(module_path!(), "::try_send_when_full"),
        60_000_000,
        1,
        try_send_when_full,
    );
    runner.case(
        concat!(module_path!(), "::cancel_blocked_send"),
        6_000_000,
        1,
        cancel_blocked_send,
    );
    for &arg in &[1, 2, 32, 256] {
        runner.case(
            format!(
                "{}/{arg}",
                concat!(module_path!(), "::drop_lagging_receiver_wakes_senders")
            ),
            (2_000_000 / arg).max(1),
            1,
            |bencher| drop_lagging_receiver_wakes_senders(bencher, arg),
        );
    }
}
