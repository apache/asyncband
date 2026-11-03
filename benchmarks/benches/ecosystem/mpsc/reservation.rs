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
use std::hint::black_box;
use std::marker::PhantomData;

use asyncband::blocking::FutureExt;
use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::implementations;
use benchmarks::support::bench_context;
use benchmarks::support::poll_ready;

use crate::mpsc::adapters::Asyncband;
use crate::mpsc::adapters::BoundedMpsc;
use crate::mpsc::adapters::Tokio;
use crate::mpsc::support::ConcurrentMpsc;
use crate::mpsc::support::RepeatedTasks;

trait Reservable: BoundedMpsc {
    type Permit<'a>: Send;
    fn reserve(sender: &Self::Sender) -> impl Future<Output = Self::Permit<'_>> + Send;
    fn publish(permit: Self::Permit<'_>, value: usize);
}

impl Reservable for Asyncband {
    type Permit<'a> = asyncband::mpsc::Permit<'a, usize>;
    async fn reserve(sender: &Self::Sender) -> Self::Permit<'_> {
        sender.reserve().await.unwrap()
    }
    fn publish(permit: Self::Permit<'_>, value: usize) {
        permit.send(value).unwrap();
    }
}

impl Reservable for Tokio {
    type Permit<'a> = tokio::sync::mpsc::Permit<'a, usize>;
    async fn reserve(sender: &Self::Sender) -> Self::Permit<'_> {
        sender.reserve().await.unwrap()
    }
    fn publish(permit: Self::Permit<'_>, value: usize) {
        permit.send(value);
    }
}

fn reserve_publish_receive<C: Reservable>(bencher: Bencher) {
    let (sender, mut receiver) = C::channel(64);
    let mut context = bench_context();
    bencher.bench_local(|| {
        let permit = poll_ready(C::reserve(&sender), &mut context);
        C::publish(permit, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver))
    });
}

struct Reserved<C, const CAPACITY: usize>(PhantomData<C>);

impl<C: Reservable, const CAPACITY: usize> ConcurrentMpsc for Reserved<C, CAPACITY> {
    type Message = usize;
    type Sender = C::Sender;
    type Receiver = C::Receiver;
    fn channel() -> (Self::Sender, Self::Receiver) {
        C::channel(CAPACITY)
    }
    fn send(sender: &Self::Sender, value: usize) {
        C::publish(FutureExt::block_on(C::reserve(sender)), value);
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

fn capacity_one_task_handoff<C: Reservable>(bencher: Bencher) {
    let mut batch = RepeatedTasks::<Reserved<C, 1>>::new(4, 4);
    batch.run();
    bencher.bench_local(|| batch.run());
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "mpsc/reservation/reserve-publish-receive",
        18_000_000,
        implementations!([Asyncband, Tokio], |C, b| reserve_publish_receive::<C>(b)),
    );
    runner.compare(
        "mpsc/reservation/tasks-4p1c-cap1-4w",
        170,
        implementations!([Asyncband, Tokio], |C, b| capacity_one_task_handoff::<C>(b)),
    );
}
