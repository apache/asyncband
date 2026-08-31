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

// Every benchmark here must return the channel to a steady state on each iteration: the retained
// backlog back where it started, no parked producer left behind, and no permit slack in the
// producer wait queue. Unlike the unbounded channel the hazard is not unbounded memory but a
// wedged timed loop — a send that never gets its capacity back would hang the bench, not slow it.

use std::pin::pin;

use asyncband::broadcast::mpmc;
use divan::Bencher;
use divan::black_box;

use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;

const RECEIVER_COUNTS: &[usize] = &[1, 8, 32];
const BLOCKED_SENDER_COUNTS: &[usize] = &[1, 8, 32];
const CAPACITY: usize = 64;

#[divan::bench]
fn send_without_receivers(bencher: Bencher) {
    // No subscription means nothing is retained, so this measures the discard path, which never
    // allocates and never waits.
    let (tx, rx) = mpmc::bounded(CAPACITY);
    drop(rx);

    bencher.bench_local(|| tx.try_send(black_box(1)));
}

#[divan::bench]
fn try_send_and_try_recv(bencher: Bencher) {
    let (tx, mut rx) = mpmc::bounded(CAPACITY);

    bencher.bench_local(|| {
        tx.try_send(black_box(1)).unwrap();
        black_box(rx.try_recv().unwrap())
    });
}

#[divan::bench]
fn try_send_when_full(bencher: Bencher) {
    let (tx, _rx) = mpmc::bounded(1);
    tx.try_send(0).unwrap();

    // The rejected value comes straight back, so the channel stays exactly as full as it started.
    bencher.bench_local(|| black_box(tx.try_send(black_box(1))).is_err());
}

#[divan::bench(args = RECEIVER_COUNTS)]
fn try_send_and_drain_fanout(bencher: Bencher, receiver_count: usize) {
    let (tx, rx) = mpmc::bounded(CAPACITY);
    let mut receivers = Vec::with_capacity(receiver_count);
    receivers.push(rx);
    for _ in 1..receiver_count {
        receivers.push(tx.subscribe());
    }

    // One message in, every receiver drains it out: the last one to read pays the reclaim scan and
    // the capacity release, and the channel is empty again for the next iteration.
    bencher.bench_local(|| {
        tx.try_send(black_box(1)).unwrap();
        for receiver in &mut receivers {
            black_box(receiver.try_recv().unwrap());
        }
    });
}

#[divan::bench(args = BLOCKED_SENDER_COUNTS)]
fn reclaim_wakes_blocked_senders(bencher: Bencher, sender_count: usize) {
    let mut context = bench_context();

    // Measures the whole backpressure cycle: park `sender_count` producers on a full channel, free
    // one slot, and let exactly one of them through. Each iteration ends with the same number of
    // producers parked and the same backlog, so the loop is stationary.
    bencher
        .with_inputs(|| {
            let (tx, rx) = mpmc::bounded(1);
            tx.try_send(0).unwrap();
            (tx, rx)
        })
        .bench_local_refs(|(tx, rx)| {
            let mut sends = (0..sender_count)
                .map(|value| Box::pin(tx.send(value)))
                .collect::<Vec<_>>();
            for send in &mut sends {
                poll_pending(send.as_mut(), &mut context);
            }

            // Releasing one slot wakes the queue; one producer republishes and the rest re-park.
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

#[divan::bench]
fn cancel_blocked_send(bencher: Bencher) {
    let mut context = bench_context();
    let (tx, _rx) = mpmc::bounded(1);
    tx.try_send(0).unwrap();

    // Park a producer and immediately cancel it: measures registering and unlinking one waiter.
    bencher.bench_local(|| {
        let send = pin!(tx.send(black_box(1)));
        poll_pending(send, &mut context);
    });
}

#[divan::bench]
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
