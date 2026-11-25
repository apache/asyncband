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

use std::hint::black_box;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread;
use std::thread::JoinHandle;

use tokio::runtime::Runtime;
use tokio::task::JoinSet;

use crate::broadcast::mpmc::adapters::BoundedBroadcastMpmc;
use crate::broadcast::mpmc::adapters::BroadcastMpmc;
use crate::broadcast::mpmc::adapters::Payload;

pub const BATCH_MESSAGES: usize = 4096;
pub const EXPECTED_BATCH_CHECKSUM: usize = BATCH_MESSAGES * (BATCH_MESSAGES - 1) / 2;
pub const ROUND_TRIP_CAPACITY: usize = 64;

#[derive(Clone, Copy)]
pub struct BoundedShape {
    pub capacity: usize,
    pub producers: usize,
    pub receivers: usize,
}

pub const BACKPRESSURE_SHAPE: BoundedShape = BoundedShape {
    capacity: 1,
    producers: 4,
    receivers: 4,
};

pub fn recv<C: BroadcastMpmc<M>, M: Payload>(receiver: &mut C::Receiver) -> M::Output {
    C::try_recv(receiver).expect("the published benchmark batch must be ready")
}

pub struct ConcurrentSend<C: BroadcastMpmc> {
    receiver: C::Receiver,
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
    channel: PhantomData<C>,
}

impl<C: BroadcastMpmc> ConcurrentSend<C> {
    pub fn new(producer_count: usize) -> Self {
        assert_eq!(BATCH_MESSAGES % producer_count, 0);
        let (sender, mut receivers) = C::channel(BATCH_MESSAGES, 1);
        let receiver = receivers.pop().unwrap();
        let start = Arc::new(Barrier::new(producer_count + 1));
        let done = Arc::new(Barrier::new(producer_count + 1));
        let messages_per_producer = BATCH_MESSAGES / producer_count;
        let workers = (0..producer_count)
            .map(|producer| {
                let sender = sender.clone();
                let start = start.clone();
                let done = done.clone();
                thread::spawn(move || {
                    start.wait();
                    let first = producer * messages_per_producer;
                    for value in first..first + messages_per_producer {
                        C::send(&sender, black_box(value));
                    }
                    done.wait();
                })
            })
            .collect();
        drop(sender);

        Self {
            receiver,
            start,
            done,
            workers,
            channel: PhantomData,
        }
    }

    pub fn run(&mut self) -> usize {
        self.start.wait();
        self.done.wait();

        let mut checksum = 0usize;
        for _ in 0..BATCH_MESSAGES {
            checksum = checksum.wrapping_add(recv::<C, usize>(&mut self.receiver));
        }
        assert_eq!(checksum, EXPECTED_BATCH_CHECKSUM);
        black_box(checksum)
    }
}

impl<C: BroadcastMpmc> Drop for ConcurrentSend<C> {
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

pub struct Fanout<C: BroadcastMpmc> {
    sender: C::Sender,
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
}

impl<C: BroadcastMpmc> Fanout<C> {
    pub fn new(receiver_count: usize) -> Self {
        let (sender, receivers) = C::channel(BATCH_MESSAGES, receiver_count);
        let start = Arc::new(Barrier::new(receiver_count + 1));
        let done = Arc::new(Barrier::new(receiver_count + 1));
        let workers = receivers
            .into_iter()
            .map(|mut receiver| {
                let start = start.clone();
                let done = done.clone();
                thread::spawn(move || {
                    start.wait();
                    let mut checksum = 0usize;
                    for _ in 0..BATCH_MESSAGES {
                        checksum = checksum.wrapping_add(recv::<C, usize>(&mut receiver));
                    }
                    assert_eq!(checksum, EXPECTED_BATCH_CHECKSUM);
                    black_box(checksum);
                    done.wait();
                })
            })
            .collect();

        Self {
            sender,
            start,
            done,
            workers,
        }
    }

    pub fn run(&mut self) {
        for value in 0..BATCH_MESSAGES {
            C::send(&self.sender, black_box(value));
        }
        self.start.wait();
        self.done.wait();
    }
}

impl<C: BroadcastMpmc> Drop for Fanout<C> {
    fn drop(&mut self) {
        let panicking = thread::panicking();
        for worker in self.workers.drain(..) {
            let result = worker.join();
            if !panicking {
                result.expect("benchmark receiver panicked");
            }
        }
    }
}

/// Reuses one channel and persistent receiver threads across rounds so parallel drain timings
/// exclude per-round channel construction and thread creation.
pub struct ReusedFanout<C: BroadcastMpmc<M>, M: Payload = usize> {
    sender: C::Sender,
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    workers: Vec<JoinHandle<()>>,
    payload: PhantomData<fn() -> M>,
}

impl<C: BroadcastMpmc<M>, M: Payload> ReusedFanout<C, M> {
    pub fn new(receiver_count: usize) -> Self {
        let (sender, receivers) = C::channel(BATCH_MESSAGES, receiver_count);
        let start = Arc::new(Barrier::new(receiver_count + 1));
        let done = Arc::new(Barrier::new(receiver_count + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let workers = receivers
            .into_iter()
            .map(|mut receiver| {
                let start = start.clone();
                let done = done.clone();
                let stop = stop.clone();
                thread::spawn(move || {
                    loop {
                        start.wait();
                        if stop.load(Ordering::Acquire) {
                            break;
                        }
                        let mut checksum = 0usize;
                        for _ in 0..BATCH_MESSAGES {
                            checksum = checksum
                                .wrapping_add(M::output_sequence(recv::<C, M>(&mut receiver)));
                        }
                        assert_eq!(checksum, EXPECTED_BATCH_CHECKSUM);
                        black_box(checksum);
                        done.wait();
                    }
                })
            })
            .collect();

        Self {
            sender,
            start,
            done,
            stop,
            workers,
            payload: PhantomData,
        }
    }

    pub fn run(&mut self) {
        for value in 0..BATCH_MESSAGES {
            C::send(&self.sender, black_box(M::new(value)));
        }
        self.start.wait();
        self.done.wait();
    }
}

impl<C: BroadcastMpmc<M>, M: Payload> Drop for ReusedFanout<C, M> {
    fn drop(&mut self) {
        let panicking = thread::panicking();
        self.stop.store(true, Ordering::Release);
        if !panicking {
            self.start.wait();
        }
        for worker in self.workers.drain(..) {
            let result = worker.join();
            if !panicking {
                result.expect("benchmark receiver panicked");
            }
        }
    }
}

pub struct BoundedConcurrent {
    start: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
}

impl BoundedConcurrent {
    pub fn new<C: BoundedBroadcastMpmc>(shape: BoundedShape) -> Self {
        let BoundedShape {
            capacity,
            producers,
            receivers,
        } = shape;
        assert_eq!(BATCH_MESSAGES % producers, 0);
        let (sender, receivers) = C::channel(capacity, receivers);
        let start = Arc::new(Barrier::new(producers + receivers.len() + 1));
        let messages_per_producer = BATCH_MESSAGES / producers;
        let mut workers = Vec::with_capacity(producers + receivers.len());

        for mut receiver in receivers {
            let start = start.clone();
            workers.push(thread::spawn(move || {
                start.wait();
                let mut checksum = 0usize;
                for _ in 0..BATCH_MESSAGES {
                    checksum = checksum.wrapping_add(C::recv_blocking(&mut receiver));
                }
                assert_eq!(checksum, EXPECTED_BATCH_CHECKSUM);
            }));
        }
        for producer in 0..producers {
            let sender = sender.clone();
            let start = start.clone();
            workers.push(thread::spawn(move || {
                start.wait();
                let first = producer * messages_per_producer;
                for value in first..first + messages_per_producer {
                    C::send_blocking(&sender, black_box(value));
                }
            }));
        }

        Self { start, workers }
    }

    pub fn run(&mut self) {
        self.start.wait();
        for worker in self.workers.drain(..) {
            worker.join().expect("bounded benchmark worker panicked");
        }
    }
}

pub struct BoundedTasks {
    start: Arc<tokio::sync::Barrier>,
    tasks: JoinSet<()>,
}

impl BoundedTasks {
    pub fn new<C: BoundedBroadcastMpmc>(runtime: &Runtime, shape: BoundedShape) -> Self {
        let BoundedShape {
            capacity,
            producers,
            receivers,
        } = shape;
        assert_eq!(BATCH_MESSAGES % producers, 0);
        let (sender, receivers) = C::channel(capacity, receivers);
        let start = Arc::new(tokio::sync::Barrier::new(producers + receivers.len() + 1));
        let mut tasks = JoinSet::new();

        for mut receiver in receivers {
            let start = start.clone();
            tasks.spawn_on(
                async move {
                    start.wait().await;
                    let mut checksum = 0usize;
                    for _ in 0..BATCH_MESSAGES {
                        checksum = checksum.wrapping_add(C::recv_async(&mut receiver).await);
                    }
                    assert_eq!(checksum, EXPECTED_BATCH_CHECKSUM);
                },
                runtime.handle(),
            );
        }
        for producer in 0..producers {
            let sender = sender.clone();
            let start = start.clone();
            tasks.spawn_on(
                async move {
                    start.wait().await;
                    let first = producer * (BATCH_MESSAGES / producers);
                    for value in first..first + BATCH_MESSAGES / producers {
                        C::send_async(&sender, black_box(value)).await;
                    }
                },
                runtime.handle(),
            );
        }

        Self { start, tasks }
    }

    pub fn run(&mut self, runtime: &Runtime) {
        runtime.block_on(async {
            self.start.wait().await;
            while let Some(result) = self.tasks.join_next().await {
                result.expect("bounded benchmark task panicked");
            }
        });
    }
}
