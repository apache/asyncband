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
use std::thread;
use std::thread::JoinHandle;

use divan::black_box;

use super::adapters::BroadcastMpmc;

pub const BATCH_MESSAGES: usize = 4096;
pub const PRODUCER_COUNTS: &[usize] = &[1, 2, 4, 8];
pub const RECEIVER_COUNTS: &[usize] = &[1, 2, 4, 8, 32];
pub const ROUND_TRIP_CAPACITY: usize = 64;

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
