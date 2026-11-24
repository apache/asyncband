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

use std::future::Future;
use std::ops::Range;
#[cfg(not(miri))]
use std::time::Duration;

use asyncband::mpmc;
use asyncband::mpmc::TrySendError;
use tokio::task::JoinHandle;

use crate::Receiver;

const PRODUCERS: usize = 8;
const CONSUMERS: usize = 8;
const VALUES_PER_PRODUCER: usize = 512;
const TOTAL: usize = PRODUCERS * VALUES_PER_PRODUCER;

async fn with_timeout<F: Future>(future: F) -> F::Output {
    #[cfg(not(miri))]
    {
        tokio::time::timeout(Duration::from_secs(10), future)
            .await
            .expect("producers and consumers must make progress")
    }
    #[cfg(miri)]
    {
        future.await
    }
}

fn values_of(producer: usize) -> Range<usize> {
    let first = producer * VALUES_PER_PRODUCER;
    first..first + VALUES_PER_PRODUCER
}

/// Consumes until disconnection and asserts that every produced value arrived exactly once.
async fn assert_delivered_exactly_once<R>(receiver: R, producers: Vec<JoinHandle<()>>)
where
    R: Receiver<usize> + Send + 'static,
{
    assert_delivered(receiver, producers, (0..TOTAL).collect()).await;
}

/// Consumes until disconnection and asserts that `expected` arrived exactly once and nothing else.
async fn assert_delivered<R>(receiver: R, producers: Vec<JoinHandle<()>>, expected: Vec<usize>)
where
    R: Receiver<usize> + Send + 'static,
{
    let consumers = (0..CONSUMERS)
        .map(|_| {
            let receiver = receiver.clone();
            tokio::spawn(async move {
                let mut values = vec![];
                while let Ok(value) = receiver.recv().await {
                    values.push(value);
                }
                values
            })
        })
        .collect::<Vec<_>>();
    drop(receiver);

    let mut received = with_timeout(async {
        for producer in producers {
            producer.await.unwrap();
        }
        let mut received = Vec::with_capacity(expected.len());
        for consumer in consumers {
            received.extend(consumer.await.unwrap());
        }
        received
    })
    .await;
    received.sort_unstable();
    assert_eq!(received, expected);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn bounded_owned_reservations_deliver_values_exactly_once_under_contention() {
    let (sender, receiver) = mpmc::bounded(32);
    let producers = (0..PRODUCERS)
        .map(|producer| {
            let mut sender = sender.clone();
            tokio::spawn(async move {
                for value in values_of(producer) {
                    let permit = sender.reserve_owned().await.unwrap();
                    sender = permit.send(value).unwrap();
                }
            })
        })
        .collect();
    drop(sender);

    assert_delivered_exactly_once(receiver, producers).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn bounded_owned_reservations_fall_back_to_waiting_when_full() {
    let (sender, receiver) = mpmc::bounded(2);
    let producers = (0..PRODUCERS)
        .map(|producer| {
            let mut sender = sender.clone();
            tokio::spawn(async move {
                for value in values_of(producer) {
                    let permit = match sender.try_reserve_owned() {
                        Ok(permit) => permit,
                        Err(TrySendError::Full(sender)) => sender.reserve_owned().await.unwrap(),
                        Err(TrySendError::Disconnected(_)) => panic!("receivers remain"),
                    };
                    sender = permit.send(value).unwrap();
                }
            })
        })
        .collect();
    drop(sender);

    assert_delivered_exactly_once(receiver, producers).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn bounded_released_owned_reservations_are_never_delivered() {
    let (sender, receiver) = mpmc::bounded(8);
    let producers = (0..PRODUCERS)
        .map(|producer| {
            let mut sender = sender.clone();
            tokio::spawn(async move {
                for value in values_of(producer) {
                    let permit = sender.reserve_owned().await.unwrap();
                    // Release every third reservation instead of sending its value.
                    sender = if value % 3 == 0 {
                        permit.release()
                    } else {
                        permit.send(value).unwrap()
                    };
                }
            })
        })
        .collect();
    drop(sender);

    let sent = (0..TOTAL).filter(|value| value % 3 != 0).collect();
    assert_delivered(receiver, producers, sent).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn bounded_owned_reservations_keep_one_sender_alive_across_tasks() {
    const VALUES: usize = 64;

    let (sender, receiver) = mpmc::bounded::<usize>(1);
    let consumer = tokio::spawn(async move {
        let mut received = 0;
        while receiver.recv().await.is_ok() {
            received += 1;
        }
        received
    });

    // The only sender always lives inside a reservation owned by one task at a time.
    let received = with_timeout(async {
        let mut sender = sender;
        for value in 0..VALUES {
            sender = tokio::spawn(async move {
                let permit = sender.reserve_owned().await.unwrap();
                tokio::task::yield_now().await;
                permit.send(value).unwrap()
            })
            .await
            .unwrap();
        }
        drop(sender);
        consumer.await.unwrap()
    })
    .await;
    assert_eq!(received, VALUES);
}

#[tokio::test]
async fn awaited_owned_reservations_deliver_values_in_order() {
    let (sender, receiver) = mpmc::bounded::<usize>(1);
    let consumer = tokio::spawn(async move {
        let mut received = vec![];
        while let Ok(value) = receiver.recv().await {
            received.push(value);
        }
        received
    });

    let received = with_timeout(async {
        let mut sender = sender;
        for value in 0..64 {
            let permit = sender.reserve_owned().await.unwrap();
            sender = permit.send(value).unwrap();
        }
        drop(sender);
        consumer.await.unwrap()
    })
    .await;
    assert_eq!(received, (0..64).collect::<Vec<_>>());
}
