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

#[allow(dead_code)]
#[path = "../mpmc/mod.rs"]
mod mpmc_support;

use mpmc_support::adapters;
use mpmc_support::support;

// Deliberately drain only after the last producer drops its sender. This makes
// retaining senders across the completion barrier deadlock deterministically.
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

    fn recv(receiver: &Self::Receiver) -> usize {
        while !receiver.is_disconnected() {
            thread::sleep(Duration::from_millis(1));
        }
        receiver.try_recv().unwrap()
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
        for &topology in support::TOPOLOGIES {
            let batch = support::ThreadBatch::new_unbounded::<DrainAfterClose>(topology);
            batch.run();
        }
    });
}

#[test]
fn flume_batches_complete_with_competing_consumers() {
    assert_completes(|| {
        for _ in 0..100 {
            let batch = support::ThreadBatch::new_unbounded::<adapters::Flume>(support::Topology {
                producers: 1,
                consumers: 8,
            });
            batch.run();
        }
    });
}

#[test]
fn tokio_batches_drain_all_messages_before_completion() {
    fn check<C: support::ConcurrentMpmc>(runtime: &tokio::runtime::Runtime) {
        for topology in support::TOPOLOGIES
            .iter()
            .copied()
            .chain([support::Topology {
                producers: 1,
                consumers: 3,
            }])
        {
            // Three consumers cannot receive equal quotas from a 16,384-message batch.
            let mut batch = support::TaskBatch::new::<C>(runtime, topology);
            runtime.block_on(batch.run());
        }
    }

    assert_completes(|| {
        for workers in [0, 4] {
            let runtime = support::runtime(workers);
            check::<support::Bounded<adapters::Asyncband, 1>>(&runtime);
            check::<support::Bounded<adapters::AsyncChannel, 1>>(&runtime);
            check::<support::Bounded<adapters::Flume, 1>>(&runtime);
            check::<support::Unbounded<adapters::Asyncband>>(&runtime);
            check::<support::Unbounded<adapters::AsyncChannel>>(&runtime);
            check::<support::Unbounded<adapters::Flume>>(&runtime);
            check::<support::Unbounded<DrainAfterClose>>(&runtime);
        }
    });
}
