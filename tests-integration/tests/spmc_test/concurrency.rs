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
use std::task::Poll;
use std::time::Duration;

use asyncband::spmc;
use tests_integration::WakeCounter;
use tests_integration::poll_with;
use tokio::sync::Barrier;
use tokio::task::JoinHandle;

const CONSUMERS: usize = 8;
const TOTAL: usize = 2_048;

#[test]
fn freeing_capacity_during_sender_registration_cannot_lose_the_notification() {
    let (mut sender, receiver) = spmc::bounded(1);
    for value in 0..32 {
        sender.try_send(value).unwrap();
        let start = std::sync::Barrier::new(2);
        let (waker, wakes) = WakeCounter::new();
        let mut pending = Box::pin(sender.send(value + 1));
        let first_poll = std::thread::scope(|scope| {
            scope.spawn(|| {
                start.wait();
                assert_eq!(receiver.try_recv(), Ok(value));
            });
            start.wait();
            poll_with(pending.as_mut(), &waker)
        });

        if first_poll.is_pending() {
            assert_eq!(wakes.count(), 1);
            assert_eq!(poll_with(pending.as_mut(), &waker), Poll::Ready(Ok(())));
        } else {
            assert_eq!(first_poll, Poll::Ready(Ok(())));
        }
        drop(pending);
        assert_eq!(receiver.try_recv(), Ok(value + 1));
    }
}

async fn assert_delivered_exactly_once(
    producer: JoinHandle<()>,
    consumers: Vec<JoinHandle<Vec<usize>>>,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        producer.await.unwrap();
        let mut received = Vec::with_capacity(TOTAL);
        for consumer in consumers {
            let values = consumer.await.unwrap();
            assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
            received.extend(values);
        }
        received.sort_unstable();
        assert_eq!(received, (0..TOTAL).collect::<Vec<_>>());
    })
    .await
    .expect("SPMC sender and all consumers must make progress");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn bounded_values_are_delivered_exactly_once_to_eight_consumers() {
    for capacity in [1, 64] {
        let (mut sender, receiver) = spmc::bounded(capacity);
        let start = Arc::new(Barrier::new(CONSUMERS + 1));
        let consumers = (0..CONSUMERS)
            .map(|_| {
                let receiver = receiver.clone();
                let start = start.clone();
                tokio::spawn(async move {
                    start.wait().await;
                    let mut values = vec![];
                    while let Ok(value) = receiver.recv().await {
                        values.push(value);
                    }
                    values
                })
            })
            .collect();
        drop(receiver);
        let producer = tokio::spawn(async move {
            start.wait().await;
            for value in 0..TOTAL {
                sender.send(value).await.unwrap();
            }
        });
        assert_delivered_exactly_once(producer, consumers).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn unbounded_values_are_delivered_exactly_once_to_eight_consumers() {
    let (mut sender, receiver) = spmc::unbounded();
    let start = Arc::new(Barrier::new(CONSUMERS + 1));
    let consumers = (0..CONSUMERS)
        .map(|_| {
            let receiver = receiver.clone();
            let start = start.clone();
            tokio::spawn(async move {
                start.wait().await;
                let mut values = vec![];
                while let Ok(value) = receiver.recv().await {
                    values.push(value);
                }
                values
            })
        })
        .collect();
    drop(receiver);
    let producer = tokio::spawn(async move {
        start.wait().await;
        for value in 0..TOTAL {
            sender.send(value).unwrap();
        }
    });
    assert_delivered_exactly_once(producer, consumers).await;
}
