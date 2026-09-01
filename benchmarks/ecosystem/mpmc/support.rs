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

use super::adapters::BoundedMpmc;
use super::adapters::UnboundedMpmc;

pub const BATCH_MESSAGES: usize = 16_384;
pub const BOUNDED_CAPACITY: usize = 64;

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

pub struct ConcurrentBatch {
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
}

impl ConcurrentBatch {
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

impl Drop for ConcurrentBatch {
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
