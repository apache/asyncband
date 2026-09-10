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
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread;
use std::thread::JoinHandle;

use divan::black_box;

use super::adapters::AsyncUnboundedMpsc;
use super::adapters::BoundedMpsc;
use super::adapters::UnboundedMpsc;

pub const BOUNDED_CAPACITY: usize = 64;
pub const BATCH_MESSAGES: usize = 16_384;
pub const PRODUCER_COUNTS: &[usize] = &[1, 8];

pub trait Message: Send + 'static {
    fn new(sequence: usize) -> Self;
    fn sequence(self) -> usize;
}

impl Message for usize {
    fn new(sequence: usize) -> Self {
        sequence
    }

    fn sequence(self) -> usize {
        self
    }
}

impl Message for [u8; 1024] {
    fn new(sequence: usize) -> Self {
        let mut value = [1; 1024];
        value[..size_of::<usize>()].copy_from_slice(&sequence.to_le_bytes());
        value
    }

    fn sequence(self) -> usize {
        let value = black_box(self);
        assert_eq!(value[1023], 1);
        usize::from_le_bytes(value[..size_of::<usize>()].try_into().unwrap())
    }
}

pub trait ConcurrentMpsc: Send + Sync + 'static {
    type Message: Message;
    type Sender: Clone + Send + Sync + 'static;
    type Receiver: Send + 'static;

    fn channel() -> (Self::Sender, Self::Receiver);
    fn send(sender: &Self::Sender, value: Self::Message);
    fn recv(receiver: &mut Self::Receiver) -> Self::Message;
}

pub trait AsyncConcurrentMpsc: ConcurrentMpsc {
    fn send_async(sender: &Self::Sender, value: Self::Message) -> impl Future<Output = ()> + Send;
    fn recv_async(receiver: &mut Self::Receiver) -> impl Future<Output = Self::Message> + Send;
}

pub struct Bounded<C, const CAPACITY: usize = BOUNDED_CAPACITY, T = usize>(
    PhantomData<fn() -> (C, T)>,
);

impl<C: BoundedMpsc<T>, const CAPACITY: usize, T: Message> ConcurrentMpsc
    for Bounded<C, CAPACITY, T>
{
    type Message = T;
    type Receiver = C::Receiver;
    type Sender = C::Sender;

    fn channel() -> (Self::Sender, Self::Receiver) {
        C::channel(CAPACITY)
    }

    fn send(sender: &Self::Sender, value: T) {
        C::send_blocking(sender, value);
    }

    fn recv(receiver: &mut Self::Receiver) -> T {
        C::recv_blocking(receiver)
    }
}

impl<C: BoundedMpsc<T>, const CAPACITY: usize, T: Message> AsyncConcurrentMpsc
    for Bounded<C, CAPACITY, T>
{
    async fn send_async(sender: &Self::Sender, value: T) {
        C::send_async(sender, value).await;
    }

    async fn recv_async(receiver: &mut Self::Receiver) -> T {
        C::recv_async(receiver).await
    }
}

pub struct Unbounded<C>(PhantomData<C>);

impl<C: UnboundedMpsc> ConcurrentMpsc for Unbounded<C> {
    type Message = usize;
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

impl<C: AsyncUnboundedMpsc> AsyncConcurrentMpsc for Unbounded<C> {
    async fn send_async(sender: &Self::Sender, value: usize) {
        C::send(sender, value);
    }

    async fn recv_async(receiver: &mut Self::Receiver) -> usize {
        C::recv_async(receiver).await
    }
}

// Reuse worker threads and channel storage so steady-state samples exclude thread creation.
pub struct RepeatedBatch<C: ConcurrentMpsc<Message = usize>> {
    receiver: C::Receiver,
    start: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    workers: Vec<JoinHandle<()>>,
}

impl<C: ConcurrentMpsc<Message = usize>> RepeatedBatch<C> {
    pub fn new(producer_count: usize) -> Self {
        assert_eq!(BATCH_MESSAGES % producer_count, 0);
        let (sender, receiver) = C::channel();
        let start = Arc::new(Barrier::new(producer_count + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let messages_per_producer = BATCH_MESSAGES / producer_count;
        let workers = (0..producer_count)
            .map(|producer| {
                let sender = sender.clone();
                let start = start.clone();
                let stop = stop.clone();
                thread::spawn(move || {
                    loop {
                        start.wait();
                        if stop.load(Ordering::Acquire) {
                            break;
                        }
                        let first = producer * messages_per_producer;
                        for offset in 0..messages_per_producer {
                            C::send(&sender, black_box(first + offset));
                        }
                    }
                })
            })
            .collect();
        drop(sender);
        Self {
            receiver,
            start,
            stop,
            workers,
        }
    }

    pub fn run(&mut self) -> usize {
        self.start.wait();
        let mut checksum = 0usize;
        for _ in 0..BATCH_MESSAGES {
            checksum = checksum.wrapping_add(C::recv(&mut self.receiver));
        }
        black_box(checksum)
    }
}

impl<C: ConcurrentMpsc<Message = usize>> Drop for RepeatedBatch<C> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        for worker in self.workers.drain(..) {
            worker.join().expect("benchmark producer panicked");
        }
    }
}

// A spawned receiver shares the executor's scheduling with producers. Keeping the receiver in
// block_on instead measures worker-to-caller thread handoffs, which is a separate workload.
enum Receiver<C: ConcurrentMpsc> {
    Task {
        start: Arc<tokio::sync::Notify>,
        completed: tokio::sync::mpsc::UnboundedReceiver<usize>,
    },
    External(C::Receiver),
}

// Reuse every task and the channel. The small control exchange happens once per 16,384-message
// batch; it never forwards measured messages. Both payload sizes use this same start protocol.
pub struct RepeatedTasks<C: AsyncConcurrentMpsc> {
    runtime: tokio::runtime::Runtime,
    receiver: Receiver<C>,
    start: Arc<tokio::sync::Barrier>,
    stop: Arc<AtomicBool>,
    workers: Vec<tokio::task::JoinHandle<()>>,
}

impl<C: AsyncConcurrentMpsc> RepeatedTasks<C> {
    pub fn new(producer_count: usize, worker_threads: usize) -> Self {
        Self::with_receiver(producer_count, worker_threads, false)
    }

    pub fn external_receiver(producer_count: usize, worker_threads: usize) -> Self {
        Self::with_receiver(producer_count, worker_threads, true)
    }

    fn with_receiver(
        producer_count: usize,
        worker_threads: usize,
        external_receiver: bool,
    ) -> Self {
        assert_eq!(BATCH_MESSAGES % producer_count, 0);
        let runtime = if worker_threads == 0 {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
        } else {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .build()
                .unwrap()
        };
        let (sender, mut receiver) = C::channel();
        let start = Arc::new(tokio::sync::Barrier::new(producer_count + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let messages_per_producer = BATCH_MESSAGES / producer_count;
        let mut workers: Vec<_> = (0..producer_count)
            .map(|producer| {
                let sender = sender.clone();
                let start = start.clone();
                let stop = stop.clone();
                runtime.spawn(async move {
                    loop {
                        start.wait().await;
                        if stop.load(Ordering::Acquire) {
                            break;
                        }
                        let first = producer * messages_per_producer;
                        for offset in 0..messages_per_producer {
                            C::send_async(&sender, black_box(C::Message::new(first + offset)))
                                .await;
                        }
                    }
                })
            })
            .collect();
        drop(sender);
        let receiver = if external_receiver {
            Receiver::External(receiver)
        } else {
            let request = Arc::new(tokio::sync::Notify::new());
            let (completed_tx, completed) = tokio::sync::mpsc::unbounded_channel();
            let request_rx = request.clone();
            let start = start.clone();
            let stop = stop.clone();
            workers.push(runtime.spawn(async move {
                loop {
                    request_rx.notified().await;
                    start.wait().await;
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    let checksum = receive_batch::<C>(&mut receiver).await;
                    completed_tx.send(checksum).unwrap();
                }
            }));
            Receiver::Task {
                start: request,
                completed,
            }
        };
        Self {
            runtime,
            receiver,
            start,
            stop,
            workers,
        }
    }

    pub fn run(&mut self) -> usize {
        self.runtime.block_on(async {
            match &mut self.receiver {
                Receiver::Task { start, completed } => {
                    start.notify_one();
                    completed.recv().await.expect("benchmark receiver panicked")
                }
                Receiver::External(receiver) => {
                    self.start.wait().await;
                    receive_batch::<C>(receiver).await
                }
            }
        })
    }
}

impl<C: AsyncConcurrentMpsc> Drop for RepeatedTasks<C> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.runtime.block_on(async {
            match &self.receiver {
                Receiver::Task { start, .. } => start.notify_one(),
                Receiver::External(_) => {
                    self.start.wait().await;
                }
            }
            for worker in self.workers.drain(..) {
                worker.await.expect("benchmark producer panicked");
            }
        });
    }
}

async fn receive_batch<C: AsyncConcurrentMpsc>(receiver: &mut C::Receiver) -> usize {
    let mut checksum = 0usize;
    for _ in 0..BATCH_MESSAGES {
        checksum = checksum.wrapping_add(C::recv_async(receiver).await.sequence());
    }
    assert_eq!(checksum, BATCH_MESSAGES * (BATCH_MESSAGES - 1) / 2);
    black_box(checksum)
}
