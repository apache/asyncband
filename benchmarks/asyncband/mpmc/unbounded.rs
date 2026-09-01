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

use asyncband::mpmc;
use divan::Bencher;
use divan::counter::ItemsCount;

use super::support::BATCH_MESSAGES;
use super::support::ConcurrentBatch;
use super::support::TOPOLOGIES;
use super::support::Topology;

fn send(sender: &mpmc::UnboundedSender<usize>, value: usize) {
    sender.send(value).expect("benchmark sender disconnected");
}

fn recv(receiver: &mpmc::UnboundedReceiver<usize>) -> usize {
    pollster::block_on(receiver.recv()).expect("benchmark receiver disconnected")
}

#[divan::bench(
    args = TOPOLOGIES,
    sample_count = 20,
    sample_size = 1,
    counter = ItemsCount::new(BATCH_MESSAGES),
)]
fn concurrent(bencher: Bencher, topology: Topology) {
    bencher
        .with_inputs(|| {
            let (sender, receiver) = mpmc::unbounded();
            ConcurrentBatch::new(sender, receiver, topology, send, recv)
        })
        .bench_local_refs(|batch| batch.run());
}
