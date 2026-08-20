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

use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use asyncband::oneshot;
use divan::Bencher;
use divan::black_box;

#[divan::bench]
fn send_before_poll(bencher: Bencher) {
    let mut context = Context::from_waker(Waker::noop());

    bencher.bench_local(|| {
        let (sender, receiver) = black_box(oneshot::channel());
        let mut receiver = receiver.into_future();

        sender.send(black_box(1usize)).unwrap();

        match Pin::new(&mut receiver).poll(&mut context) {
            Poll::Ready(Ok(value)) => black_box(value),
            result => panic!("unexpected receive result: {result:?}"),
        }
    });
}

#[divan::bench]
fn poll_before_send(bencher: Bencher) {
    let mut context = Context::from_waker(Waker::noop());

    bencher.bench_local(|| {
        let (sender, receiver) = black_box(oneshot::channel());
        let mut receiver = receiver.into_future();

        assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Pending);
        sender.send(black_box(1usize)).unwrap();

        match Pin::new(&mut receiver).poll(&mut context) {
            Poll::Ready(Ok(value)) => black_box(value),
            result => panic!("unexpected receive result: {result:?}"),
        }
    });
}
