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
// backlog back where it started and no parked producer left behind. Unlike the unbounded channel
// the hazard is not unbounded memory but a wedged timed loop — a send that never gets its capacity
// back would hang the bench, not slow it.

use std::pin::pin;

use asyncband::broadcast::spmc;
use divan::Bencher;
use divan::black_box;

use crate::support::bench_context;
use crate::support::defer_input_drop;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;

const RECEIVER_COUNTS: &[usize] = &[1, 8, 32];
const CAPACITY: usize = 64;

#[divan::bench]
fn send_without_receivers(bencher: Bencher) {
    // No subscription means nothing is retained, so this measures the discard path, which never
    // allocates and never waits.
    let (mut tx, rx) = spmc::bounded(CAPACITY);
    drop(rx);

    bencher.bench_local(|| tx.try_send(black_box(1)));
}

#[divan::bench]
fn try_send_and_try_recv(bencher: Bencher) {
    let (mut tx, mut rx) = spmc::bounded(CAPACITY);

    bencher.bench_local(|| {
        tx.try_send(black_box(1)).unwrap();
        black_box(rx.try_recv().unwrap())
    });
}

#[divan::bench]
fn try_send_when_full(bencher: Bencher) {
    let (mut tx, _rx) = spmc::bounded(1);
    tx.try_send(0).unwrap();

    // The rejected value comes straight back, so the channel stays exactly as full as it started.
    bencher.bench_local(|| black_box(tx.try_send(black_box(1))).is_err());
}

#[divan::bench(args = RECEIVER_COUNTS)]
fn try_send_and_drain_fanout(bencher: Bencher, receiver_count: usize) {
    let (mut tx, rx) = spmc::bounded(CAPACITY);
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

#[divan::bench]
fn reclaim_wakes_the_blocked_producer(bencher: Bencher) {
    let mut context = bench_context();

    // Measures the whole backpressure cycle: park the single producer on a full channel, free one
    // slot, and let it through. Each iteration ends with the producer parked and the same backlog,
    // so the loop is stationary.
    bencher
        .with_inputs(|| {
            let (mut tx, rx) = spmc::bounded(1);
            tx.try_send(0).unwrap();
            (tx, rx)
        })
        .bench_local_refs(|(tx, rx)| {
            let mut send = Box::pin(tx.send(1));
            poll_pending(send.as_mut(), &mut context);

            black_box(rx.try_recv().unwrap());
            assert!(send.as_mut().poll(&mut context).is_ready());

            // Drain the republished message so the next iteration starts from the same state.
            black_box(rx.try_recv().unwrap());
            drop(send);
            tx.try_send(0).unwrap();
        });
}

#[divan::bench]
fn cancel_blocked_send(bencher: Bencher) {
    let mut context = bench_context();
    let (mut tx, _rx) = spmc::bounded(1);
    tx.try_send(0).unwrap();

    // Park the producer and immediately cancel it: measures registering and unlinking the waiter.
    bencher.bench_local(|| {
        let send = pin!(tx.send(black_box(1)));
        poll_pending(send, &mut context);
    });
}

#[divan::bench]
fn deliver_to_waiting_receiver(bencher: Bencher) {
    let mut context = bench_context();
    let (mut tx, mut rx) = spmc::bounded(CAPACITY);

    bencher.bench_local(|| {
        let mut recv = pin!(rx.recv());
        poll_pending(recv.as_mut(), &mut context);
        tx.try_send(black_box(1)).unwrap();
        black_box(poll_pinned_ready(recv, &mut context).unwrap())
    });
}

#[divan::bench(args = [1, 2, 32, 256], sample_size = 64)]
fn drop_lagging_receiver_wakes_producer(bencher: Bencher, backlog: usize) {
    bencher
        .with_inputs(|| {
            let (mut sender, mut fast) = spmc::bounded(backlog);
            let slow = sender.subscribe();
            for value in 0..backlog {
                sender.try_send(value).unwrap();
                assert_eq!(fast.try_recv().unwrap(), value);
            }
            let mut context = bench_context();
            let mut send = Box::pin(async move { sender.send(backlog).await });
            poll_pending(send.as_mut(), &mut context);
            (slow, fast, send)
        })
        .bench_local_values(|(slow, fast, send)| {
            // The fast subscription stays alive so this measures reclaim, not last-receiver exit.
            // Preparing the backlog, parking the producer, and disposing of futures are outside
            // timing.
            drop(slow);
            defer_input_drop((fast, send), ())
        });
}
