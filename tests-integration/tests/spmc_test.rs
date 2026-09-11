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

use std::cell::Cell;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use asyncband::spmc;
use asyncband::spmc::RecvError;
use asyncband::spmc::TryRecvError;
use asyncband::spmc::TrySendError;
use tests_integration::WakeCounter;
use tests_integration::poll_once;

#[derive(Debug)]
struct DropSpy(Arc<AtomicUsize>);

impl Drop for DropSpy {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

macro_rules! receiver_contract {
    ($name:ident, $channel:expr, $send:ident) => {
        mod $name {
            use super::*;

            #[test]
            fn receivers_compete_in_fifo_order_and_drain_after_sender_drop() {
                let (mut sender, receiver) = $channel;
                let competing = receiver.clone();
                sender.$send(10).unwrap();
                sender.$send(20).unwrap();
                assert_eq!(receiver.try_recv(), Ok(10));
                assert_eq!(competing.try_recv(), Ok(20));
                assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
                sender.$send(30).unwrap();
                sender.$send(40).unwrap();
                drop(sender);
                assert_eq!(poll_once(pin!(receiver.recv())), Poll::Ready(Ok(30)));
                assert_eq!(poll_once(pin!(competing.recv())), Poll::Ready(Ok(40)));
                assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
                assert_eq!(
                    poll_once(pin!(competing.recv())),
                    Poll::Ready(Err(RecvError::Disconnected))
                );
            }

            #[test]
            fn only_last_receiver_disconnects_and_returns_unsent_value() {
                let (mut sender, receiver) = $channel;
                let competing = receiver.clone();
                drop(receiver);
                sender.$send(10).unwrap();
                assert_eq!(competing.try_recv(), Ok(10));
                drop(competing);
                let error = sender.$send(20).unwrap_err();
                assert_eq!(error.as_inner(), &20);
                assert_eq!(error.into_inner(), 20);
            }

            #[test]
            fn one_send_wakes_one_of_eight_receivers_and_cancellation_hands_off() {
                let (mut sender, receiver) = $channel;
                let receivers: Vec<_> = (0..8).map(|_| receiver.clone()).collect();
                let counts: Vec<_> = (0..8).map(|_| Arc::new(WakeCounter::default())).collect();
                let wakers: Vec<_> = counts.iter().cloned().map(Waker::from).collect();
                let mut pending: Vec<_> = receivers.iter().map(|rx| Box::pin(rx.recv())).collect();
                for (future, waker) in pending.iter_mut().zip(&wakers) {
                    assert!(
                        future
                            .as_mut()
                            .poll(&mut Context::from_waker(waker))
                            .is_pending()
                    );
                }
                sender.$send(7).unwrap();
                assert_eq!(counts[0].count(), 1);
                assert!(counts[1..].iter().all(|count| count.count() == 0));

                // Every selected receiver cancels before consuming. The message and notification
                // must survive the entire chain without waking every remaining receiver at once.
                for next in 1..8 {
                    drop(pending.remove(0));
                    assert_eq!(counts[next].count(), 1);
                    assert!(counts[next + 1..].iter().all(|count| count.count() == 0));
                }
                assert_eq!(poll_once(pending[0].as_mut()), Poll::Ready(Ok(7)));
                assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
            }

            #[test]
            fn cancelling_before_notification_removes_the_waiter() {
                let (mut sender, receiver) = $channel;
                let competing = receiver.clone();
                let cancelled_count = Arc::new(WakeCounter::default());
                let waiting_count = Arc::new(WakeCounter::default());
                let cancelled_waker = Waker::from(cancelled_count.clone());
                let waiting_waker = Waker::from(waiting_count.clone());
                let mut cancelled = Box::pin(receiver.recv());
                let mut waiting = Box::pin(competing.recv());
                assert!(
                    cancelled
                        .as_mut()
                        .poll(&mut Context::from_waker(&cancelled_waker))
                        .is_pending()
                );
                assert!(
                    waiting
                        .as_mut()
                        .poll(&mut Context::from_waker(&waiting_waker))
                        .is_pending()
                );
                drop(cancelled);
                sender.$send(5).unwrap();
                assert_eq!(cancelled_count.count(), 0);
                assert_eq!(waiting_count.count(), 1);
                assert_eq!(poll_once(waiting.as_mut()), Poll::Ready(Ok(5)));
            }

            #[test]
            fn sender_disconnection_wakes_all_receivers() {
                let (sender, receiver) = $channel;
                let receivers: Vec<_> = (0..8).map(|_| receiver.clone()).collect();
                let counts: Vec<_> = (0..8).map(|_| Arc::new(WakeCounter::default())).collect();
                let wakers: Vec<_> = counts.iter().cloned().map(Waker::from).collect();
                let mut pending: Vec<_> = receivers.iter().map(|rx| Box::pin(rx.recv())).collect();
                for (future, waker) in pending.iter_mut().zip(&wakers) {
                    assert!(
                        future
                            .as_mut()
                            .poll(&mut Context::from_waker(waker))
                            .is_pending()
                    );
                }
                drop(sender);
                assert!(counts.iter().all(|count| count.count() == 1));
                for future in &mut pending {
                    assert_eq!(
                        poll_once(future.as_mut()),
                        Poll::Ready(Err(RecvError::Disconnected))
                    );
                }
                // Fix the payload type without sending into a disconnected queue.
                let _: Result<usize, _> = receiver.try_recv();
            }

            #[test]
            fn buffered_received_and_rejected_values_are_each_dropped_once() {
                let (mut sender, receiver) = $channel;
                let competing = receiver.clone();
                let drops: Vec<_> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
                sender.$send(DropSpy(drops[0].clone())).unwrap();
                sender.$send(DropSpy(drops[1].clone())).unwrap();
                let received = receiver.try_recv().unwrap();
                sender.$send(DropSpy(drops[2].clone())).unwrap();
                drop(receiver);
                assert!(drops.iter().all(|count| count.load(Ordering::SeqCst) == 0));
                drop(competing);
                assert_eq!(drops[1].load(Ordering::SeqCst), 1);
                assert_eq!(drops[2].load(Ordering::SeqCst), 1);
                let rejected = sender.$send(DropSpy(drops[3].clone())).unwrap_err();
                assert_eq!(drops[3].load(Ordering::SeqCst), 0);
                drop(rejected.into_inner());
                drop(received);
                drop(sender);
                assert!(drops.iter().all(|count| count.load(Ordering::SeqCst) == 1));
            }
        }
    };
}

receiver_contract!(bounded, spmc::bounded(2), try_send);
receiver_contract!(unbounded, spmc::unbounded(), send);

#[test]
#[should_panic(expected = "spmc bounded queue requires capacity > 0")]
fn bounded_rejects_zero_capacity() {
    let _ = spmc::bounded::<()>(0);
}

#[test]
fn bounded_capacity_and_pending_send_progress() {
    for capacity in [1, 2, 3, 8] {
        let (mut sender, receiver) = spmc::bounded(capacity);
        for value in 0..capacity {
            sender.try_send(value).unwrap();
        }
        assert_eq!(sender.try_send(capacity), Err(TrySendError::Full(capacity)));
        let mut waiting = Box::pin(sender.send(capacity));
        let count = Arc::new(WakeCounter::default());
        let waker = Waker::from(count.clone());
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(receiver.try_recv(), Ok(0));
        assert_eq!(count.count(), 1);
        assert_eq!(poll_once(waiting.as_mut()), Poll::Ready(Ok(())));
        drop(waiting);
        drop(sender);
        for value in 1..=capacity {
            assert_eq!(receiver.try_recv(), Ok(value));
        }
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }
}

#[test]
fn cancelling_a_send_before_or_after_notification_preserves_capacity() {
    for notified in [false, true] {
        let (mut sender, receiver) = spmc::bounded(1);
        let drops = Arc::new(AtomicUsize::new(0));
        sender.try_send(DropSpy(drops.clone())).unwrap();
        let mut cancelled = Box::pin(sender.send(DropSpy(drops.clone())));
        assert!(poll_once(cancelled.as_mut()).is_pending());
        if notified {
            drop(receiver.try_recv().unwrap());
        }
        drop(cancelled);
        assert_eq!(drops.load(Ordering::SeqCst), 1 + usize::from(notified));
        if !notified {
            drop(receiver.try_recv().unwrap());
        }
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert!(matches!(
            poll_once(pin!(sender.send(DropSpy(drops.clone())))),
            Poll::Ready(Ok(()))
        ));
        drop(receiver);
        drop(sender);
        assert_eq!(drops.load(Ordering::SeqCst), 3);
    }
}

#[test]
fn last_receiver_wakes_pending_sender_and_returns_its_value() {
    let (mut sender, receiver) = spmc::bounded(1);
    let competing = receiver.clone();
    sender.try_send(0).unwrap();
    let count = Arc::new(WakeCounter::default());
    let waker = Waker::from(count.clone());
    let mut pending = Box::pin(sender.send(1));
    assert!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(receiver);
    assert_eq!(count.count(), 0);
    drop(competing);
    assert_eq!(count.count(), 1);
    let Poll::Ready(Err(error)) = poll_once(pending.as_mut()) else {
        panic!("disconnected sender must return the unsent value");
    };
    assert_eq!(error.into_inner(), 1);
}

#[test]
fn endpoint_and_future_traits_allow_send_but_not_sync_payloads() {
    fn assert_traits<T: Send + Sync + Unpin>() {}
    fn assert_send<T: Send>(_: T) {}
    assert_traits::<spmc::BoundedSender<Cell<u8>>>();
    assert_traits::<spmc::BoundedReceiver<Cell<u8>>>();
    assert_traits::<spmc::UnboundedSender<Cell<u8>>>();
    assert_traits::<spmc::UnboundedReceiver<Cell<u8>>>();
    let (mut sender, receiver) = spmc::bounded::<Cell<u8>>(1);
    assert_send(sender.send(Cell::new(1)));
    assert_send(receiver.recv());
    let (_sender, receiver) = spmc::unbounded::<Cell<u8>>();
    assert_send(receiver.recv());
}

// Exercise the single producer on a different worker from eight competing consumers. Keep Miri
// focused on the deterministic notification, cancellation, and destruction contracts above.
#[cfg(not(miri))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_producer_delivers_every_value_once_to_eight_competing_consumers() {
    use std::time::Duration;

    macro_rules! run {
        ($channel:expr, $send:expr) => {{
            const TOTAL: usize = 2_048;
            let (mut sender, receiver) = $channel;
            let start = Arc::new(tokio::sync::Barrier::new(9));
            let consumers: Vec<_> = (0..8)
                .map(|_| {
                    let receiver = receiver.clone();
                    let start = start.clone();
                    tokio::spawn(async move {
                        start.wait().await;
                        let mut values = Vec::new();
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
                    $send(&mut sender, value).await;
                }
            });
            tokio::time::timeout(Duration::from_secs(10), async {
                producer.await.unwrap();
                let mut received = Vec::new();
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
        }};
    }

    async fn bounded_send(sender: &mut spmc::BoundedSender<usize>, value: usize) {
        sender.send(value).await.unwrap();
    }
    async fn unbounded_send(sender: &mut spmc::UnboundedSender<usize>, value: usize) {
        sender.send(value).unwrap();
    }
    run!(spmc::bounded(1), bounded_send);
    run!(spmc::bounded(64), bounded_send);
    run!(spmc::unbounded(), unbounded_send);
}
