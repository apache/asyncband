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

use std::ops::Range;
use std::time::Duration;

use asyncband::mpmc;
use tokio::task::JoinHandle;

use super::Receiver;

const PRODUCERS: usize = 8;
const CONSUMERS: usize = 8;
const VALUES_PER_PRODUCER: usize = 512;
const TOTAL: usize = PRODUCERS * VALUES_PER_PRODUCER;

fn values_of(producer: usize) -> Range<usize> {
    let first = producer * VALUES_PER_PRODUCER;
    first..first + VALUES_PER_PRODUCER
}

/// Consumes until disconnection and asserts that every produced value arrived exactly once.
async fn assert_delivered_exactly_once<R>(receiver: R, producers: Vec<JoinHandle<()>>)
where
    R: Receiver<usize> + Send + 'static,
{
    let consumers = (0..CONSUMERS)
        .map(|_| {
            let receiver = receiver.clone();
            tokio::spawn(async move {
                let mut values = Vec::new();
                while let Ok(value) = receiver.recv().await {
                    values.push(value);
                }
                values
            })
        })
        .collect::<Vec<_>>();
    drop(receiver);

    let mut received = tokio::time::timeout(Duration::from_secs(10), async {
        for producer in producers {
            producer.await.unwrap();
        }
        let mut received = Vec::with_capacity(TOTAL);
        for consumer in consumers {
            received.extend(consumer.await.unwrap());
        }
        received
    })
    .await
    .expect("producers and consumers must make progress");
    received.sort_unstable();
    assert_eq!(received, (0..TOTAL).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn bounded_values_are_delivered_exactly_once_under_contention() {
    let (sender, receiver) = mpmc::bounded(32);
    let producers = (0..PRODUCERS)
        .map(|producer| {
            let sender = sender.clone();
            tokio::spawn(async move {
                for value in values_of(producer) {
                    sender.send(value).await.unwrap();
                }
            })
        })
        .collect();
    drop(sender);

    assert_delivered_exactly_once(receiver, producers).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn unbounded_values_are_delivered_exactly_once_under_contention() {
    let (sender, receiver) = mpmc::unbounded();
    let producers = (0..PRODUCERS)
        .map(|producer| {
            let sender = sender.clone();
            tokio::spawn(async move {
                for value in values_of(producer) {
                    sender.send(value).unwrap();
                }
            })
        })
        .collect();
    drop(sender);

    assert_delivered_exactly_once(receiver, producers).await;
}
