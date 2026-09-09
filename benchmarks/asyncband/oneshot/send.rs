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

use std::future::IntoFuture;
use std::pin::pin;

use asyncband::oneshot::channel;
use divan::Bencher;
use divan::black_box;

use crate::support::bench_context;
use crate::support::poll_pending;
use crate::support::poll_pinned_ready;

#[divan::bench]
fn send_before_poll(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (sender, receiver) = black_box(channel());
        let mut receiver = pin!(receiver.into_future());

        sender.send(black_box(1usize)).unwrap();

        black_box(poll_pinned_ready(receiver.as_mut(), &mut context).unwrap())
    });
}

#[divan::bench]
fn poll_before_send(bencher: Bencher) {
    let mut context = bench_context();

    bencher.bench_local(|| {
        let (sender, receiver) = black_box(channel());
        let mut receiver = pin!(receiver.into_future());

        poll_pending(receiver.as_mut(), &mut context);
        sender.send(black_box(1usize)).unwrap();

        black_box(poll_pinned_ready(receiver.as_mut(), &mut context).unwrap())
    });
}
