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
use mea::broadcast::overflow;

use super::support::bench_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;

const RECEIVER_COUNTS: &[usize] = &[1, 8, 32];

#[divan::bench]
fn cancel_pending(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (sender, mut receiver) = overflow::channel::<usize>(1);
        {
            let mut recv = pin!(receiver.recv());
            poll_pending(recv.as_mut(), &mut context);
        }
        black_box((sender, receiver))
    });
}

#[divan::bench]
fn deliver_to_waiter(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (sender, mut receiver) = overflow::channel(1);
        let mut recv = pin!(receiver.recv());
        poll_pending(recv.as_mut(), &mut context);

        sender.send(black_box(1usize));
        let value = poll_pinned_ready(recv.as_mut(), &mut context).unwrap();
        black_box(value)
    });
}

#[divan::bench(args = RECEIVER_COUNTS)]
fn deliver_to_receiver_batch(bencher: Bencher, receiver_count: usize) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (sender, receiver) = overflow::channel(1);
        let mut receivers = (0..receiver_count)
            .map(|_| receiver.resubscribe())
            .collect::<Vec<_>>();
        let mut recvs = receivers
            .iter_mut()
            .map(|receiver| Box::pin(receiver.recv()))
            .collect::<Vec<_>>();
        for recv in &mut recvs {
            poll_pending(recv.as_mut(), &mut context);
        }

        sender.send(black_box(1usize));
        for mut recv in recvs {
            let value = poll_pinned_ready(recv.as_mut(), &mut context).unwrap();
            black_box(value);
        }
    });
}
