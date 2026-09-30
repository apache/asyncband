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

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use benchmarks::channels;
use channels::adapters;
use channels::mpmc;
use channels::spmc;

// Deliberately drain only after the last producer drops its sender. This makes
// retaining senders while waiting for consumers deadlock deterministically.
struct DrainAfterClose;

impl adapters::UnboundedMpmc for DrainAfterClose {
    type Receiver = flume::Receiver<usize>;
    type Sender = flume::Sender<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        flume::unbounded()
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.send(value).unwrap();
    }

    async fn recv_async(receiver: &Self::Receiver) -> Option<usize> {
        while !receiver.is_disconnected() {
            tokio::task::yield_now().await;
        }
        receiver.try_recv().ok()
    }

    fn recv(receiver: &Self::Receiver) -> Option<usize> {
        while !receiver.is_disconnected() {
            thread::yield_now();
        }
        receiver.try_recv().ok()
    }
}

fn assert_completes(run: impl FnOnce() + Send + 'static) {
    let (done, completion) = mpsc::channel();
    let worker = thread::spawn(move || {
        run();
        done.send(()).unwrap();
    });
    completion
        .recv_timeout(Duration::from_secs(30))
        .expect("benchmark batch did not finish after production ended");
    worker.join().unwrap();
}

#[test]
fn thread_batch_closes_before_waiting_for_consumers() {
    assert_completes(|| {
        let topology = mpmc::Topology {
            producers: 2,
            consumers: 3,
        };
        let mut batch = mpmc::ThreadBatch::new_unbounded::<DrainAfterClose>(topology);
        batch.run();
    });
}

#[test]
fn task_batches_close_before_draining_uneven_consumers() {
    assert_completes(|| {
        for workers in [0, 4] {
            let runtime = channels::runtime(workers);
            let mut batch = mpmc::TaskBatch::new::<adapters::Unbounded<DrainAfterClose>>(
                &runtime,
                mpmc::Topology {
                    producers: 2,
                    consumers: 3,
                },
            );
            runtime.block_on(batch.run());
            let mut batch =
                spmc::TaskBatch::new::<adapters::Unbounded<DrainAfterClose>>(&runtime, 3);
            runtime.block_on(batch.run());
        }
    });
}

#[test]
fn bounded_batches_drain_without_per_consumer_quotas() {
    assert_completes(|| {
        for workers in [0, 4] {
            let runtime = channels::runtime(workers);
            let mut batch = mpmc::TaskBatch::new::<adapters::Bounded<adapters::Mpmc, 1>>(
                &runtime,
                mpmc::Topology {
                    producers: 2,
                    consumers: 3,
                },
            );
            runtime.block_on(batch.run());
            let mut batch =
                spmc::TaskBatch::new::<adapters::Bounded<adapters::Spmc, 1>>(&runtime, 3);
            runtime.block_on(batch.run());
        }
    });
}
