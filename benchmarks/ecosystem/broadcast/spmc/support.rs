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

use std::fmt;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::thread::JoinHandle;

use divan::black_box;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

use super::adapters::BoundedBroadcastSpmc;
use super::adapters::BroadcastSpmc;

pub const BATCH_MESSAGES: usize = 4096;
pub const RECEIVER_COUNTS: &[usize] = &[1, 2, 4, 8, 32];
pub const ROUND_TRIP_CAPACITY: usize = 64;

/// One-producer bounded workload. Capacity is the shared backlog; receivers are subscriptions.
#[derive(Clone, Copy)]
pub struct BoundedShape {
    pub capacity: usize,
    pub receivers: usize,
}

impl BoundedShape {
    const fn new(capacity: usize, receivers: usize) -> Self {
        Self {
            capacity,
            receivers,
        }
    }
}

impl fmt::Display for BoundedShape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cap {} 1 producer {} receivers",
            self.capacity, self.receivers
        )
    }
}

/// Primary 1-producer shapes from the SPMC comparison matrix.
pub const BOUNDED_SHAPES: &[BoundedShape] = &[
    BoundedShape::new(1, 1),
    BoundedShape::new(1, 8),
    BoundedShape::new(1, 32),
    BoundedShape::new(64, 1),
    BoundedShape::new(64, 8),
    BoundedShape::new(64, 32),
];

fn recv<C: BroadcastSpmc>(receiver: &mut C::Receiver) -> usize {
    C::try_recv(receiver).expect("the published benchmark batch must be ready")
}

/// One producer publishes the batch, then every subscription drains it.
pub struct Fanout<C: BroadcastSpmc> {
    sender: C::Sender,
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
}

impl<C: BroadcastSpmc> Fanout<C> {
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
                        checksum = checksum.wrapping_add(recv::<C>(&mut receiver));
                    }
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
            C::send(&mut self.sender, black_box(value));
        }
        self.start.wait();
        self.done.wait();
    }
}

impl<C: BroadcastSpmc> Drop for Fanout<C> {
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

/// One producer and `receivers` subscriptions on native threads. All wait, then transfer.
pub struct BoundedConcurrent {
    start: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
}

impl BoundedConcurrent {
    pub fn new<C: BoundedBroadcastSpmc>(shape: BoundedShape) -> Self {
        let BoundedShape {
            capacity,
            receivers,
        } = shape;
        let (mut sender, receivers) = C::channel(capacity, receivers);
        let start = Arc::new(Barrier::new(receivers.len() + 2));
        let mut workers = Vec::with_capacity(receivers.len() + 1);

        for mut receiver in receivers {
            let start = start.clone();
            workers.push(thread::spawn(move || {
                start.wait();
                let mut checksum = 0usize;
                for _ in 0..BATCH_MESSAGES {
                    checksum = checksum.wrapping_add(C::recv_blocking(&mut receiver));
                }
                assert_eq!(checksum, BATCH_MESSAGES * (BATCH_MESSAGES - 1) / 2);
            }));
        }
        let producer_start = start.clone();
        workers.push(thread::spawn(move || {
            producer_start.wait();
            for value in 0..BATCH_MESSAGES {
                C::send_blocking(&mut sender, black_box(value));
            }
        }));

        Self { start, workers }
    }

    pub fn run(&mut self) {
        self.start.wait();
        for worker in self.workers.drain(..) {
            worker.join().expect("bounded benchmark worker panicked");
        }
    }
}

/// The same 1-producer bounded workload on async tasks.
pub struct BoundedTasks {
    start: Arc<tokio::sync::Barrier>,
    tasks: JoinSet<()>,
}

impl BoundedTasks {
    pub fn new<C: BoundedBroadcastSpmc>(runtime: &Runtime, shape: BoundedShape) -> Self {
        let BoundedShape {
            capacity,
            receivers,
        } = shape;
        let (mut sender, receivers) = C::channel(capacity, receivers);
        let start = Arc::new(tokio::sync::Barrier::new(receivers.len() + 2));
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
                    assert_eq!(checksum, BATCH_MESSAGES * (BATCH_MESSAGES - 1) / 2);
                },
                runtime.handle(),
            );
        }
        let start_producer = start.clone();
        tasks.spawn_on(
            async move {
                start_producer.wait().await;
                for value in 0..BATCH_MESSAGES {
                    C::send_async(&mut sender, black_box(value)).await;
                }
            },
            runtime.handle(),
        );

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
