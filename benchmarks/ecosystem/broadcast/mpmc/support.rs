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
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::thread::JoinHandle;

use divan::black_box;

use super::adapters::BoundedBroadcastMpmc;
use super::adapters::BroadcastMpmc;

pub const BATCH_MESSAGES: usize = 4096;
pub const PRODUCER_COUNTS: &[usize] = &[1, 2, 4, 8];
pub const RECEIVER_COUNTS: &[usize] = &[1, 2, 4, 8, 32];
/// Capacity for the round-trip benches, which pair every send with a receive and so never fill
/// the channel.
pub const ROUND_TRIP_CAPACITY: usize = 64;

/// One bounded workload: the channel capacity, how many producers publish, and how many
/// subscriptions read.
///
/// Capacity is a dimension rather than a constant because it is the parameter that decides how a
/// bounded broadcast behaves. At `TIGHT` the backlog is a fraction of the fanout, so the run
/// degenerates into a per-message round trip: the producer publishes one value, every subscription
/// is woken to read it, and only then does a slot come back. At `ROOMY` the producer runs ahead
/// and both sides batch. Reporting only one of the two would describe half the channel.
#[derive(Clone, Copy)]
pub struct BoundedShape {
    pub capacity: usize,
    pub producers: usize,
    pub receivers: usize,
}

impl BoundedShape {
    const TIGHT: usize = 64;
    const ROOMY: usize = 1024;

    const fn new(capacity: usize, producers: usize, receivers: usize) -> Self {
        Self {
            capacity,
            producers,
            receivers,
        }
    }
}

impl fmt::Display for BoundedShape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cap {} {} producers {} receivers",
            self.capacity, self.producers, self.receivers
        )
    }
}

pub const BOUNDED_SHAPES: &[BoundedShape] = &[
    BoundedShape::new(BoundedShape::TIGHT, 1, 1),
    BoundedShape::new(BoundedShape::TIGHT, 1, 8),
    BoundedShape::new(BoundedShape::TIGHT, 1, 32),
    BoundedShape::new(BoundedShape::TIGHT, 8, 1),
    BoundedShape::new(BoundedShape::TIGHT, 8, 8),
    BoundedShape::new(BoundedShape::TIGHT, 8, 32),
    BoundedShape::new(BoundedShape::ROOMY, 1, 1),
    BoundedShape::new(BoundedShape::ROOMY, 1, 8),
    BoundedShape::new(BoundedShape::ROOMY, 1, 32),
    BoundedShape::new(BoundedShape::ROOMY, 8, 1),
    BoundedShape::new(BoundedShape::ROOMY, 8, 8),
    BoundedShape::new(BoundedShape::ROOMY, 8, 32),
];

fn recv<C: BroadcastMpmc>(receiver: &mut C::Receiver) -> usize {
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
            checksum = checksum.wrapping_add(recv::<C>(&mut self.receiver));
        }
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

/// Producers and receivers running concurrently against a channel far smaller than the batch.
///
/// The unbounded fixtures above publish the whole batch before anyone drains it, which is only
/// safe because every peer is given room for the entire batch. That shape deadlocks a genuinely
/// bounded channel, so this one interleaves: every thread blocks, and the drain runs while the
/// producers are still publishing.
///
/// This terminates. The run could only wedge if every producer and every receiver waited at the
/// same time, but a producer waits only while at least one message is retained, and a retained
/// message is by definition unread by the slowest receiver — so that receiver is runnable. The
/// counts balance exactly: the producers publish `BATCH_MESSAGES` between them and each receiver
/// consumes `BATCH_MESSAGES`, so no thread over- or under-runs. Every receiver is subscribed
/// before the first send, so every receiver sees every message.
///
/// Benches using this must set `sample_size = 1`: the worker threads are spawned in `new` and exit
/// after one pass, so a second `run` on the same value would block forever.
pub struct BoundedConcurrent<C: BoundedBroadcastMpmc> {
    start: Arc<Barrier>,
    done: Arc<Barrier>,
    workers: Vec<JoinHandle<()>>,
    channel: PhantomData<C>,
}

impl<C: BoundedBroadcastMpmc> BoundedConcurrent<C> {
    pub fn new(shape: BoundedShape) -> Self {
        let BoundedShape {
            capacity,
            producers,
            receivers,
        } = shape;
        assert_eq!(BATCH_MESSAGES % producers, 0);

        let (sender, receivers) = C::channel(capacity, receivers);
        let start = Arc::new(Barrier::new(producers + receivers.len() + 1));
        let done = Arc::new(Barrier::new(producers + receivers.len() + 1));
        let messages_per_producer = BATCH_MESSAGES / producers;
        let mut workers = Vec::with_capacity(producers + receivers.len());

        for mut receiver in receivers {
            let start = start.clone();
            let done = done.clone();
            workers.push(thread::spawn(move || {
                start.wait();
                let mut checksum = 0usize;
                for _ in 0..BATCH_MESSAGES {
                    checksum = checksum.wrapping_add(C::recv_blocking(&mut receiver));
                }
                black_box(checksum);
                done.wait();
            }));
        }

        for producer in 0..producers {
            let sender = sender.clone();
            let start = start.clone();
            let done = done.clone();
            workers.push(thread::spawn(move || {
                start.wait();
                let first = producer * messages_per_producer;
                for value in first..first + messages_per_producer {
                    C::send_blocking(&sender, black_box(value));
                }
                done.wait();
            }));
        }
        drop(sender);

        Self {
            start,
            done,
            workers,
            channel: PhantomData,
        }
    }

    pub fn run(&mut self) {
        self.start.wait();
        self.done.wait();
    }
}

impl<C: BoundedBroadcastMpmc> Drop for BoundedConcurrent<C> {
    fn drop(&mut self) {
        let panicking = thread::panicking();
        for worker in self.workers.drain(..) {
            let result = worker.join();
            if !panicking {
                result.expect("bounded benchmark worker panicked");
            }
        }
    }
}
