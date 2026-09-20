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

use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::thread::JoinHandle;

use divan::black_box;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

use super::BATCH_MESSAGES;
use super::adapters::BoundedMpmc;
use super::adapters::Channel;
use super::adapters::UnboundedMpmc;

#[derive(Clone, Copy, Debug)]
pub struct Topology {
    pub producers: usize,
    pub consumers: usize,
}

pub const TOPOLOGIES: &[Topology] = &[
    Topology {
        producers: 1,
        consumers: 1,
    },
    Topology {
        producers: 1,
        consumers: 8,
    },
    Topology {
        producers: 8,
        consumers: 1,
    },
    Topology {
        producers: 8,
        consumers: 8,
    },
];

// The caller only coordinates the batch. All measured sends and receives run in spawned tasks,
// including on the current-thread runtime; no data is received by Runtime::block_on itself.
pub struct TaskBatch {
    start: Arc<tokio::sync::Barrier>,
    workers: JoinSet<(usize, usize)>,
}

impl TaskBatch {
    pub fn new<C: Channel>(runtime: &Runtime, topology: Topology) -> Self
    where
        C::Sender: Clone,
    {
        assert_eq!(BATCH_MESSAGES % topology.producers, 0);
        let (sender, receiver) = C::channel();
        let start = Arc::new(tokio::sync::Barrier::new(
            topology.producers + topology.consumers + 1,
        ));
        let messages_per_producer = BATCH_MESSAGES / topology.producers;
        let mut workers = JoinSet::new();
        for producer in 0..topology.producers {
            let mut sender = sender.clone();
            let start = start.clone();
            workers.spawn_on(
                async move {
                    start.wait().await;
                    let first = producer * messages_per_producer;
                    for value in first..first + messages_per_producer {
                        C::send(&mut sender, black_box(value)).await;
                    }
                    // Completion drops this sender so receivers can observe the end of input.
                    (0, 0)
                },
                runtime.handle(),
            );
        }
        for _ in 0..topology.consumers {
            let receiver = receiver.clone();
            let start = start.clone();
            workers.spawn_on(
                async move {
                    start.wait().await;
                    let mut count = 0;
                    let mut checksum = 0usize;
                    // Competing consumers drain freely, rather than stopping at equal quotas.
                    while let Some(value) = C::recv(&receiver).await {
                        count += 1;
                        checksum = checksum.wrapping_add(value);
                    }
                    (count, checksum)
                },
                runtime.handle(),
            );
        }
        drop(sender);
        drop(receiver);
        Self { start, workers }
    }

    pub async fn run(&mut self) -> (usize, usize) {
        self.start.wait().await;
        let mut count = 0;
        let mut checksum = 0usize;
        while let Some(result) = self.workers.join_next().await {
            let (received, sum) = result.expect("benchmark task panicked");
            count += received;
            checksum = checksum.wrapping_add(sum);
        }
        assert_eq!(count, BATCH_MESSAGES);
        assert_eq!(checksum, BATCH_MESSAGES * (BATCH_MESSAGES - 1) / 2);
        black_box((count, checksum))
    }
}

pub struct ThreadBatch {
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
}

impl ThreadBatch {
    pub fn new_bounded<C: BoundedMpmc>(capacity: usize, topology: Topology) -> Self {
        let (sender, receiver) = C::channel(capacity);
        Self::new(sender, receiver, topology, C::send, C::recv)
    }

    pub fn new_unbounded<C: UnboundedMpmc>(topology: Topology) -> Self {
        let (sender, receiver) = C::channel();
        Self::new(sender, receiver, topology, C::send, C::recv)
    }

    fn new<S, R>(
        sender: S,
        receiver: R,
        topology: Topology,
        send: fn(&S, usize),
        recv: fn(&R) -> usize,
    ) -> Self
    where
        S: Clone + Send + 'static,
        R: Clone + Send + 'static,
    {
        assert_eq!(BATCH_MESSAGES % topology.producers, 0);
        assert_eq!(BATCH_MESSAGES % topology.consumers, 0);

        let participants = topology.producers + topology.consumers;
        let start = Arc::new(Barrier::new(participants + 1));
        let done = Arc::new(Barrier::new(participants + 1));
        let messages_per_producer = BATCH_MESSAGES / topology.producers;
        let messages_per_consumer = BATCH_MESSAGES / topology.consumers;
        let mut workers = Vec::with_capacity(participants);

        for producer in 0..topology.producers {
            let sender = sender.clone();
            let start = start.clone();
            let done = done.clone();
            workers.push(thread::spawn(move || {
                start.wait();
                let first = producer * messages_per_producer;
                for offset in 0..messages_per_producer {
                    send(&sender, black_box(first + offset));
                }
                // Close the channel when production ends so pending receivers can finish
                // draining it before all workers rendezvous at the completion barrier.
                drop(sender);
                done.wait();
            }));
        }

        for _ in 0..topology.consumers {
            let receiver = receiver.clone();
            let start = start.clone();
            let done = done.clone();
            workers.push(thread::spawn(move || {
                start.wait();
                let mut checksum = 0usize;
                for _ in 0..messages_per_consumer {
                    checksum = checksum.wrapping_add(recv(&receiver));
                }
                black_box(checksum);
                done.wait();
            }));
        }

        drop(sender);
        drop(receiver);

        Self {
            start,
            done,
            workers,
        }
    }

    pub fn run(&self) {
        self.start.wait();
        self.done.wait();
    }
}

impl Drop for ThreadBatch {
    fn drop(&mut self) {
        let panicking = thread::panicking();
        for worker in self.workers.drain(..) {
            let result = worker.join();
            if !panicking {
                result.expect("benchmark worker panicked");
            }
        }
    }
}
