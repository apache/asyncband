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
#[path = "../ecosystem/mpmc/adapters.rs"]
mod adapters;
#[allow(dead_code)]
#[path = "../asyncband/mpmc/support.rs"]
mod asyncband_support;
#[allow(dead_code)]
#[path = "../ecosystem/mpmc/support.rs"]
mod ecosystem_support;

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
fn ecosystem_batch_closes_before_waiting_for_consumers() {
    assert_completes(|| {
        for &topology in ecosystem_support::TOPOLOGIES {
            let batch =
                ecosystem_support::ConcurrentBatch::new_unbounded::<DrainAfterClose>(topology);
            batch.run();
        }
    });
}

#[test]
fn asyncband_batch_closes_before_waiting_for_consumers() {
    use adapters::UnboundedMpmc;

    assert_completes(|| {
        for &topology in asyncband_support::TOPOLOGIES {
            let (sender, receiver) = DrainAfterClose::channel();
            let batch = asyncband_support::ConcurrentBatch::new(
                sender,
                receiver,
                topology,
                DrainAfterClose::send,
                DrainAfterClose::recv,
            );
            batch.run();
        }
    });
}

#[test]
fn flume_batches_complete_with_competing_consumers() {
    assert_completes(|| {
        for _ in 0..100 {
            let batch = ecosystem_support::ConcurrentBatch::new_unbounded::<adapters::Flume>(
                ecosystem_support::Topology {
                    producers: 1,
                    consumers: 8,
                },
            );
            batch.run();
        }
    });
}
