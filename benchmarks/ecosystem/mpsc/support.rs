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

use super::adapters::BoundedMpsc;
use super::adapters::UnboundedMpsc;

pub const BOUNDED_CAPACITY: usize = 64;
pub const BATCH_MESSAGES: usize = 16_384;
pub const PRODUCER_COUNTS: &[usize] = &[1, 2, 4, 8];

pub trait ConcurrentMpsc: Send + Sync + 'static {
    type Sender: Clone + Send + Sync + 'static;
    type Receiver: Send + 'static;

    fn channel() -> (Self::Sender, Self::Receiver);
    fn send(sender: &Self::Sender, value: usize);
    fn recv(receiver: &mut Self::Receiver) -> usize;
    fn send_async(sender: &Self::Sender, value: usize) -> impl Future<Output = ()> + Send;
    fn recv_async(receiver: &mut Self::Receiver) -> impl Future<Output = usize> + Send;
}

pub struct Bounded<C, const CAPACITY: usize = BOUNDED_CAPACITY>(PhantomData<C>);

impl<C: BoundedMpsc, const CAPACITY: usize> ConcurrentMpsc for Bounded<C, CAPACITY> {
    type Receiver = C::Receiver;
    type Sender = C::Sender;

    fn channel() -> (Self::Sender, Self::Receiver) {
        C::channel(CAPACITY)
    }

    fn send(sender: &Self::Sender, value: usize) {
        C::send_blocking(sender, value);
    }

    async fn send_async(sender: &Self::Sender, value: usize) {
        C::send_async(sender, value).await;
    }

    async fn recv_async(receiver: &mut Self::Receiver) -> usize {
        C::recv_async(receiver).await
    }

    fn recv(receiver: &mut Self::Receiver) -> usize {
        C::recv_blocking(receiver)
    }
}

pub struct Unbounded<C>(PhantomData<C>);

impl<C: UnboundedMpsc> ConcurrentMpsc for Unbounded<C> {
    type Receiver = C::Receiver;
    type Sender = C::Sender;

    fn channel() -> (Self::Sender, Self::Receiver) {
        C::channel()
    }

    fn send(sender: &Self::Sender, value: usize) {
        C::send(sender, value);
    }

    async fn send_async(sender: &Self::Sender, value: usize) {
        C::send(sender, value);
    }

    async fn recv_async(receiver: &mut Self::Receiver) -> usize {
        C::recv_async(receiver).await
    }

    fn recv(receiver: &mut Self::Receiver) -> usize {
        C::recv_blocking(receiver)
    }
}

pub struct ConcurrentBatch<C: ConcurrentMpsc> {
    receiver: C::Receiver,
    start: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
}

impl<C: ConcurrentMpsc> ConcurrentBatch<C> {
    pub fn new(producer_count: usize) -> Self {
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

    pub fn run(&mut self) -> usize {
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

// Reuse worker threads and channel storage so steady-state samples exclude thread creation.
pub struct RepeatedBatch<C: ConcurrentMpsc> {
    receiver: C::Receiver,
    start: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    workers: Vec<JoinHandle<()>>,
}

impl<C: ConcurrentMpsc> RepeatedBatch<C> {
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

impl<C: ConcurrentMpsc> Drop for RepeatedBatch<C> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        for worker in self.workers.drain(..) {
            worker.join().expect("benchmark producer panicked");
        }
    }
}

// Exercise executor wakeups as well as channel traffic. Reuse tasks, threads, and channel storage
// across samples; a current-thread runtime also exposes polling that monopolizes the executor.
pub struct RepeatedTasks<C: ConcurrentMpsc> {
    runtime: tokio::runtime::Runtime,
    receiver: C::Receiver,
    start: Arc<tokio::sync::Barrier>,
    stop: Arc<AtomicBool>,
    workers: Vec<tokio::task::JoinHandle<()>>,
}

impl<C: ConcurrentMpsc> RepeatedTasks<C> {
    pub fn new(producer_count: usize, worker_threads: usize) -> Self {
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
        let (sender, receiver) = C::channel();
        let start = Arc::new(tokio::sync::Barrier::new(producer_count + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let messages_per_producer = BATCH_MESSAGES / producer_count;
        let workers = (0..producer_count)
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
                            C::send_async(&sender, black_box(first + offset)).await;
                        }
                    }
                })
            })
            .collect();
        drop(sender);
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
            self.start.wait().await;
            let mut checksum = 0usize;
            for _ in 0..BATCH_MESSAGES {
                checksum = checksum.wrapping_add(C::recv_async(&mut self.receiver).await);
            }
            assert_eq!(checksum, BATCH_MESSAGES * (BATCH_MESSAGES - 1) / 2);
            black_box(checksum)
        })
    }
}

impl<C: ConcurrentMpsc> Drop for RepeatedTasks<C> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.runtime.block_on(async {
            self.start.wait().await;
            for worker in self.workers.drain(..) {
                worker.await.expect("benchmark producer panicked");
            }
        });
    }
}
