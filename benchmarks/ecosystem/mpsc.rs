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

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Barrier;
use std::task::Context;
use std::thread;
use std::thread::JoinHandle;

use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::support::bench_context;
use super::support::poll_ready;

const BOUNDED_CAPACITY: usize = 64;
const BATCH_MESSAGES: usize = 16_384;
const PRODUCER_COUNTS: &[usize] = &[1, 2, 4, 8];

struct Asyncband;
struct Tokio;
struct AsyncChannel;
struct Flume;

trait BoundedMpsc: Send + Sync + 'static {
    type Sender: Clone + Send + 'static;
    type Receiver: Send + 'static;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver);
    fn try_send(sender: &Self::Sender, value: usize);
    fn try_recv(receiver: &mut Self::Receiver) -> usize;
    fn send_ready(sender: &Self::Sender, value: usize, context: &mut Context<'_>);
    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize;
    fn send_blocking(sender: &Self::Sender, value: usize);
    fn recv_blocking(receiver: &mut Self::Receiver) -> usize;
}

trait UnboundedMpsc: Send + Sync + 'static {
    type Sender: Clone + Send + 'static;
    type Receiver: Send + 'static;

    fn channel() -> (Self::Sender, Self::Receiver);
    fn send(sender: &Self::Sender, value: usize);
    fn try_recv(receiver: &mut Self::Receiver) -> usize;
    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize;
    fn recv_blocking(receiver: &mut Self::Receiver) -> usize;
}

impl BoundedMpsc for Asyncband {
    type Receiver = asyncband::mpsc::BoundedReceiver<usize>;
    type Sender = asyncband::mpsc::BoundedSender<usize>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        asyncband::mpsc::bounded(capacity)
    }

    fn try_send(sender: &Self::Sender, value: usize) {
        sender.try_send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn send_ready(sender: &Self::Sender, value: usize, context: &mut Context<'_>) {
        poll_ready(sender.send(value), context).unwrap();
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv(), context).unwrap()
    }

    fn send_blocking(sender: &Self::Sender, value: usize) {
        pollster::block_on(sender.send(value)).unwrap();
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).unwrap()
    }
}

impl BoundedMpsc for Tokio {
    type Receiver = tokio::sync::mpsc::Receiver<usize>;
    type Sender = tokio::sync::mpsc::Sender<usize>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        tokio::sync::mpsc::channel(capacity)
    }

    fn try_send(sender: &Self::Sender, value: usize) {
        sender.try_send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn send_ready(sender: &Self::Sender, value: usize, context: &mut Context<'_>) {
        poll_ready(sender.send(value), context).unwrap();
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv(), context).unwrap()
    }

    fn send_blocking(sender: &Self::Sender, value: usize) {
        pollster::block_on(sender.send(value)).unwrap();
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).unwrap()
    }
}

impl BoundedMpsc for AsyncChannel {
    type Receiver = async_channel::Receiver<usize>;
    type Sender = async_channel::Sender<usize>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        async_channel::bounded(capacity)
    }

    fn try_send(sender: &Self::Sender, value: usize) {
        sender.try_send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn send_ready(sender: &Self::Sender, value: usize, context: &mut Context<'_>) {
        poll_ready(sender.send(value), context).unwrap();
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv(), context).unwrap()
    }

    fn send_blocking(sender: &Self::Sender, value: usize) {
        pollster::block_on(sender.send(value)).unwrap();
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).unwrap()
    }
}

impl BoundedMpsc for Flume {
    type Receiver = flume::Receiver<usize>;
    type Sender = flume::Sender<usize>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        flume::bounded(capacity)
    }

    fn try_send(sender: &Self::Sender, value: usize) {
        sender.try_send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn send_ready(sender: &Self::Sender, value: usize, context: &mut Context<'_>) {
        poll_ready(sender.send_async(value), context).unwrap();
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv_async(), context).unwrap()
    }

    fn send_blocking(sender: &Self::Sender, value: usize) {
        pollster::block_on(sender.send_async(value)).unwrap();
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv_async()).unwrap()
    }
}

impl UnboundedMpsc for Asyncband {
    type Receiver = asyncband::mpsc::UnboundedReceiver<usize>;
    type Sender = asyncband::mpsc::UnboundedSender<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        asyncband::mpsc::unbounded()
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv(), context).unwrap()
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).unwrap()
    }
}

impl UnboundedMpsc for Tokio {
    type Receiver = tokio::sync::mpsc::UnboundedReceiver<usize>;
    type Sender = tokio::sync::mpsc::UnboundedSender<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        tokio::sync::mpsc::unbounded_channel()
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv(), context).unwrap()
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).unwrap()
    }
}

impl UnboundedMpsc for AsyncChannel {
    type Receiver = async_channel::Receiver<usize>;
    type Sender = async_channel::Sender<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        async_channel::unbounded()
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.try_send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv(), context).unwrap()
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).unwrap()
    }
}

impl UnboundedMpsc for Flume {
    type Receiver = flume::Receiver<usize>;
    type Sender = flume::Sender<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        flume::unbounded()
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv_async(), context).unwrap()
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv_async()).unwrap()
    }
}

trait ConcurrentMpsc: Send + Sync + 'static {
    type Sender: Clone + Send + 'static;
    type Receiver: Send + 'static;

    fn channel() -> (Self::Sender, Self::Receiver);
    fn send(sender: &Self::Sender, value: usize);
    fn recv(receiver: &mut Self::Receiver) -> usize;
}

struct Bounded<C>(PhantomData<C>);

impl<C: BoundedMpsc> ConcurrentMpsc for Bounded<C> {
    type Receiver = C::Receiver;
    type Sender = C::Sender;

    fn channel() -> (Self::Sender, Self::Receiver) {
        C::channel(BOUNDED_CAPACITY)
    }

    fn send(sender: &Self::Sender, value: usize) {
        C::send_blocking(sender, value);
    }

    fn recv(receiver: &mut Self::Receiver) -> usize {
        C::recv_blocking(receiver)
    }
}

struct Unbounded<C>(PhantomData<C>);

impl<C: UnboundedMpsc> ConcurrentMpsc for Unbounded<C> {
    type Receiver = C::Receiver;
    type Sender = C::Sender;

    fn channel() -> (Self::Sender, Self::Receiver) {
        C::channel()
    }

    fn send(sender: &Self::Sender, value: usize) {
        C::send(sender, value);
    }

    fn recv(receiver: &mut Self::Receiver) -> usize {
        C::recv_blocking(receiver)
    }
}

struct ConcurrentBatch<C: ConcurrentMpsc> {
    receiver: C::Receiver,
    start: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
}

impl<C: ConcurrentMpsc> ConcurrentBatch<C> {
    fn new(producer_count: usize) -> Self {
        assert_eq!(BATCH_MESSAGES % producer_count, 0);

        let (sender, receiver) = C::channel();
        let start = Arc::new(Barrier::new(producer_count + 1));
        let messages_per_producer = BATCH_MESSAGES / producer_count;
        let workers = (0..producer_count)
            .map(|producer| {
                let sender = sender.clone();
                let start = start.clone();
                thread::spawn(move || {
                    start.wait();
                    let first = producer * messages_per_producer;
                    for offset in 0..messages_per_producer {
                        C::send(&sender, black_box(first + offset));
                    }
                })
            })
            .collect();
        drop(sender);

        Self {
            receiver,
            start,
            workers,
        }
    }

    fn run(&mut self) -> usize {
        self.start.wait();
        let mut checksum = 0usize;
        for _ in 0..BATCH_MESSAGES {
            checksum = checksum.wrapping_add(C::recv(&mut self.receiver));
        }
        black_box(checksum)
    }
}

impl<C: ConcurrentMpsc> Drop for ConcurrentBatch<C> {
    fn drop(&mut self) {
        let panicking = thread::panicking();
        for worker in self.workers.drain(..) {
            let result = worker.join();
            if !panicking {
                result.expect("benchmark producer panicked");
            }
        }
    }
}

#[divan::bench(types = [Asyncband, Tokio, AsyncChannel, Flume])]
fn bounded_try_round_trip<C: BoundedMpsc>(bencher: Bencher) {
    let (sender, mut receiver) = C::channel(BOUNDED_CAPACITY);

    bencher.bench_local(|| {
        C::try_send(&sender, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver))
    });
}

#[divan::bench(types = [Asyncband, Tokio, AsyncChannel, Flume])]
fn bounded_ready_round_trip<C: BoundedMpsc>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receiver) = C::channel(BOUNDED_CAPACITY);

    bencher.bench_local(|| {
        C::send_ready(&sender, black_box(usize::MAX), &mut context);
        black_box(C::recv_ready(&mut receiver, &mut context))
    });
}

#[divan::bench(types = [Asyncband, Tokio, AsyncChannel, Flume])]
fn unbounded_ready_round_trip<C: UnboundedMpsc>(bencher: Bencher) {
    let mut context = bench_context();
    let (sender, mut receiver) = C::channel();

    bencher.bench_local(|| {
        C::send(&sender, black_box(usize::MAX));
        black_box(C::recv_ready(&mut receiver, &mut context))
    });
}

#[divan::bench(types = [Asyncband, Tokio, AsyncChannel, Flume])]
fn unbounded_try_round_trip<C: UnboundedMpsc>(bencher: Bencher) {
    let (sender, mut receiver) = C::channel();

    bencher.bench_local(|| {
        C::send(&sender, black_box(usize::MAX));
        black_box(C::try_recv(&mut receiver))
    });
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Flume],
    args = PRODUCER_COUNTS,
    sample_count = 20,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn bounded_concurrent<C: BoundedMpsc>(bencher: Bencher, producer_count: usize) {
    bencher
        .with_inputs(|| ConcurrentBatch::<Bounded<C>>::new(producer_count))
        .bench_local_refs(|batch| batch.run());
}

#[divan::bench(
    types = [Asyncband, Tokio, AsyncChannel, Flume],
    args = PRODUCER_COUNTS,
    sample_count = 20,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn unbounded_concurrent<C: UnboundedMpsc>(bencher: Bencher, producer_count: usize) {
    bencher
        .with_inputs(|| ConcurrentBatch::<Unbounded<C>>::new(producer_count))
        .bench_local_refs(|batch| batch.run());
}
