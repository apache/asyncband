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

use asyncband::mpsc;
use divan::Bencher;
use divan::black_box;

use super::support::bench_context;
use super::support::poll_pending;
use super::support::poll_pinned_ready;

const SENDER_COUNTS: &[usize] = &[1, 8, 32];

#[divan::bench(args = SENDER_COUNTS)]
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

#[divan::bench(args = SENDER_COUNTS)]
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
