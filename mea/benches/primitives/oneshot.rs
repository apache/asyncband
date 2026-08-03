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

use std::future::Future;
use std::future::IntoFuture;
use std::hint::black_box;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use criterion::Criterion;
use mea::oneshot;

pub(crate) fn benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("oneshot");
    let mut context = Context::from_waker(Waker::noop());

    group.bench_function("send_before_poll", |b| {
        b.iter(|| {
            let (sender, receiver) = black_box(oneshot::channel());
            let mut receiver = receiver.into_future();

            sender.send(black_box(1usize)).unwrap();

            match Pin::new(&mut receiver).poll(&mut context) {
                Poll::Ready(Ok(value)) => black_box(value),
                result => panic!("unexpected receive result: {result:?}"),
            }
        });
    });

    group.bench_function("poll_before_send", |b| {
        b.iter(|| {
            let (sender, receiver) = black_box(oneshot::channel());
            let mut receiver = receiver.into_future();

            assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Pending);
            sender.send(black_box(1usize)).unwrap();

            match Pin::new(&mut receiver).poll(&mut context) {
                Poll::Ready(Ok(value)) => black_box(value),
                result => panic!("unexpected receive result: {result:?}"),
            }
        });
    });

    group.finish();
}
