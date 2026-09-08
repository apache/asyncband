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
use std::marker::PhantomData;

use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::adapters::Asyncband;
use super::adapters::BoundedMpsc;
use super::adapters::Tokio;
use super::support::BATCH_MESSAGES;
use super::support::ConcurrentMpsc;
use super::support::RepeatedTasks;
use crate::support::bench_context;
use crate::support::poll_ready;

trait Reservable: BoundedMpsc {
    type Permit<'a>: Send;
    fn try_reserve(sender: &Self::Sender) -> Self::Permit<'_>;
    fn reserve(sender: &Self::Sender) -> impl Future<Output = Self::Permit<'_>> + Send;
    fn publish(permit: Self::Permit<'_>, value: usize);
}

impl Reservable for Asyncband {
    type Permit<'a> = asyncband::mpsc::Permit<'a, usize>;
    fn try_reserve(sender: &Self::Sender) -> Self::Permit<'_> {
        sender.try_reserve().unwrap()
    }
    async fn reserve(sender: &Self::Sender) -> Self::Permit<'_> {
        sender.reserve().await.unwrap()
    }
    fn publish(permit: Self::Permit<'_>, value: usize) {
        permit.send(value).unwrap();
    }
}

impl Reservable for Tokio {
    type Permit<'a> = tokio::sync::mpsc::Permit<'a, usize>;
    fn try_reserve(sender: &Self::Sender) -> Self::Permit<'_> {
        sender.try_reserve().unwrap()
    }
    async fn reserve(sender: &Self::Sender) -> Self::Permit<'_> {
        sender.reserve().await.unwrap()
    }
    fn publish(permit: Self::Permit<'_>, value: usize) {
        permit.send(value);
    }
}

#[divan::bench(types = [Asyncband, Tokio])]
fn reserve_publish_receive<C: Reservable>(bencher: Bencher) {
    let (sender, mut receiver) = C::channel(64);
    let mut context = bench_context();
    bencher.bench_local(|| {
        let permit = poll_ready(C::reserve(&sender), &mut context);
        C::publish(permit, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver))
    });
}

#[divan::bench(types = [Asyncband, Tokio])]
fn cancel_reserved_capacity<C: Reservable>(bencher: Bencher) {
    let (sender, _receiver) = C::channel(64);
    bencher.bench_local(|| drop(black_box(C::try_reserve(&sender))));
}

struct Reserved<C, const CAPACITY: usize>(PhantomData<C>);

impl<C: Reservable, const CAPACITY: usize> ConcurrentMpsc for Reserved<C, CAPACITY> {
    type Sender = C::Sender;
    type Receiver = C::Receiver;
    fn channel() -> (Self::Sender, Self::Receiver) {
        C::channel(CAPACITY)
    }
    fn send(sender: &Self::Sender, value: usize) {
        C::publish(pollster::block_on(C::reserve(sender)), value);
    }
    fn recv(receiver: &mut Self::Receiver) -> usize {
        C::recv_blocking(receiver)
    }
    async fn send_async(sender: &Self::Sender, value: usize) {
        C::publish(C::reserve(sender).await, value);
    }
    async fn recv_async(receiver: &mut Self::Receiver) -> usize {
        C::recv_async(receiver).await
    }
}

#[divan::bench(
    types = [Asyncband, Tokio],
    consts = [64, 4096],
    args = [(1, 0), (8, 4)],
    sample_count = 50,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn scheduled<C: Reservable, const CAPACITY: usize>(
    bencher: Bencher,
    (producers, workers): (usize, usize),
) {
    let mut batch = RepeatedTasks::<Reserved<C, CAPACITY>>::new(producers, workers);
    batch.run();
    bencher.bench_local(|| batch.run());
}
