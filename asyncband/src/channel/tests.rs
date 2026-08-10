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
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use super::FullBehavior;
use super::SendOutcome;
use super::TryRecvError;
use super::TrySendError;
use super::broadcast;
use super::disruptor;
use super::mpmc;
use super::mpsc;
use super::oneshot;
use super::spmc;
use super::spsc;
use super::watch;
use crate::test_support::poll_once;

fn capacity(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn ring_capacity(value: usize) -> disruptor::Capacity {
    disruptor::Capacity::new(value).unwrap()
}

fn poll_with_waker<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

#[derive(Debug)]
struct WakeProbe {
    wakes: AtomicUsize,
}

impl Wake for WakeProbe {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn topology_endpoints_have_expected_positive_auto_traits() {
    use std::any::TypeId;

    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send::<spsc::Sender<i32>>();
    assert_send::<spsc::Receiver<i32>>();
    assert_send_sync::<mpsc::Sender<i32>>();
    assert_send::<mpsc::Receiver<i32>>();
    assert_send::<spmc::Sender<i32>>();
    assert_send_sync::<spmc::Receiver<i32>>();
    assert_send_sync::<mpmc::Sender<i32>>();
    assert_send_sync::<mpmc::Receiver<i32>>();
    assert_send::<disruptor::single_producer::Publisher<i32>>();
    assert_send_sync::<disruptor::multi_producer::Publisher<i32>>();
    assert_send_sync::<disruptor::multi_producer::Subscriber<i32>>();
    assert_send_sync::<broadcast::overflow::Sender<i32>>();
    assert_send_sync::<broadcast::backpressure::Sender<i32>>();
    assert_send_sync::<broadcast::unbounded::Sender<i32>>();

    assert_ne!(
        TypeId::of::<mpsc::Sender<i32>>(),
        TypeId::of::<mpmc::Sender<i32>>()
    );
    assert_ne!(
        TypeId::of::<spsc::Receiver<i32>>(),
        TypeId::of::<spmc::Receiver<i32>>()
    );
    assert_ne!(
        TypeId::of::<broadcast::overflow::Sender<i32>>(),
        TypeId::of::<broadcast::backpressure::Sender<i32>>()
    );
    assert_ne!(
        TypeId::of::<broadcast::backpressure::Receiver<i32>>(),
        TypeId::of::<broadcast::unbounded::Receiver<i32>>()
    );
}

#[test]
fn oneshot_transfers_or_returns_the_value() {
    let (tx, mut rx) = oneshot::channel();
    tx.send(42).unwrap();
    assert_eq!(rx.try_recv(), Ok(42));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));

    let (tx, rx) = oneshot::channel();
    drop(rx);
    assert_eq!(tx.send(7).unwrap_err().into_inner(), 7);

    let (tx, rx) = oneshot::channel::<i32>();
    drop(tx);
    assert!(rx.is_disconnected());
    assert_eq!(
        pollster::block_on(rx.recv()),
        Err(super::RecvError::Disconnected)
    );
}

#[test]
fn bounded_queue_is_fifo_and_loss_is_explicit() {
    let (mut tx, mut rx) = spsc::bounded(capacity(2));
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert_eq!(tx.try_send(3), Err(TrySendError::Full(3)));
    assert_eq!(
        tx.force_send(3, FullBehavior::DropOldest),
        Ok(SendOutcome::Replaced(1))
    );
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Ok(3));

    tx.try_send(4).unwrap();
    tx.try_send(5).unwrap();
    assert_eq!(
        tx.force_send(6, FullBehavior::DropNewest),
        Ok(SendOutcome::Replaced(5))
    );
    assert_eq!(rx.try_recv(), Ok(4));
    assert_eq!(rx.try_recv(), Ok(6));
}

#[test]
fn rendezvous_send_completes_after_receive() {
    let (mut tx, mut rx) = spsc::rendezvous();
    assert_eq!(tx.try_send(1), Err(TrySendError::Full(1)));

    let mut send = Box::pin(tx.send(2));
    assert_eq!(poll_once(send.as_mut()), Poll::Pending);
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(poll_once(send.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn accepted_rendezvous_send_stays_successful_after_receiver_drop() {
    let (mut tx, mut rx) = spsc::rendezvous();
    let mut send = tokio_test::task::spawn(tx.send(1));
    assert_eq!(send.poll(), Poll::Pending);
    assert_eq!(rx.try_recv(), Ok(1));
    drop(rx);
    assert!(send.is_woken());
    assert_eq!(send.poll(), Poll::Ready(Ok(())));
}

#[test]
fn dropping_receiver_returns_pending_rendezvous_value() {
    let (mut tx, rx) = spsc::rendezvous();
    let mut send = tokio_test::task::spawn(tx.send(1));
    assert_eq!(send.poll(), Poll::Pending);
    drop(rx);
    assert!(send.is_woken());
    let Poll::Ready(Err(error)) = send.poll() else {
        panic!("pending rendezvous send should observe disconnection");
    };
    assert_eq!(error.into_inner(), 1);
}

#[test]
fn cancelled_rendezvous_send_does_not_leave_a_value() {
    let (mut tx, mut rx) = spsc::rendezvous();
    let mut send = Box::pin(tx.send(1));
    assert_eq!(poll_once(send.as_mut()), Poll::Pending);
    drop(send);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    let mut recv = Box::pin(rx.recv());
    assert_eq!(poll_once(recv.as_mut()), Poll::Pending);
    assert_eq!(tx.try_send(2), Ok(()));
    assert_eq!(poll_once(recv.as_mut()), Poll::Ready(Ok(2)));
}

#[test]
fn cloned_queue_endpoints_follow_topology() {
    let (tx, mut rx) = mpsc::unbounded();
    let tx2 = tx.clone();
    tx.try_send(1).unwrap();
    tx2.try_send(2).unwrap();
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));

    let (mut tx, rx) = spmc::unbounded();
    let rx2 = rx.clone();
    tx.try_send(3).unwrap();
    tx.try_send(4).unwrap();
    assert_eq!(rx.try_recv(), Ok(3));
    assert_eq!(rx2.try_recv(), Ok(4));
}

#[test]
fn freeing_capacity_wakes_all_senders_for_cancellation_transfer() {
    let (tx, rx) = mpmc::bounded(capacity(1));
    tx.try_send(0).unwrap();
    let mut first = tokio_test::task::spawn(tx.send(1));
    let mut second = tokio_test::task::spawn(tx.send(2));
    assert_eq!(first.poll(), Poll::Pending);
    assert_eq!(second.poll(), Poll::Pending);

    assert_eq!(rx.try_recv(), Ok(0));
    assert!(first.is_woken());
    assert!(second.is_woken());
    drop(first);
    assert_eq!(second.poll(), Poll::Ready(Ok(())));
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn sending_wakes_all_receivers_for_cancellation_transfer() {
    let (tx, rx) = mpmc::unbounded();
    let rx2 = rx.clone();
    let mut first = tokio_test::task::spawn(rx.recv());
    let mut second = tokio_test::task::spawn(rx2.recv());
    assert_eq!(first.poll(), Poll::Pending);
    assert_eq!(second.poll(), Poll::Pending);

    tx.try_send(1).unwrap();
    assert!(first.is_woken());
    assert!(second.is_woken());
    drop(first);
    assert_eq!(second.poll(), Poll::Ready(Ok(1)));
}

#[test]
fn cancelled_receive_deregisters_its_waker() {
    let (_tx, rx) = mpmc::unbounded::<i32>();
    let probe = Arc::new(WakeProbe {
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(probe.clone());
    let mut recv = Box::pin(rx.recv());
    assert_eq!(poll_with_waker(recv.as_mut(), &waker), Poll::Pending);
    assert_eq!(Arc::strong_count(&probe), 3);
    drop(recv);
    assert_eq!(Arc::strong_count(&probe), 2);
}

#[test]
fn mpmc_supports_concurrent_producers_and_consumers() {
    let (tx, rx) = mpmc::unbounded();
    let values = Arc::new(std::sync::Mutex::new(Vec::new()));

    std::thread::scope(|scope| {
        let mut producers = Vec::new();
        for producer in 0..4 {
            let tx = tx.clone();
            producers.push(scope.spawn(move || {
                for value in 0..100 {
                    tx.try_send(producer * 100 + value).unwrap();
                }
            }));
        }

        let mut consumers = Vec::new();
        for _ in 0..3 {
            let rx = rx.clone();
            let values = values.clone();
            consumers.push(scope.spawn(move || {
                while let Ok(value) = pollster::block_on(rx.recv()) {
                    values.lock().unwrap().push(value);
                }
            }));
        }
        drop(rx);

        for producer in producers {
            producer.join().unwrap();
        }
        drop(tx);
        for consumer in consumers {
            consumer.join().unwrap();
        }
    });

    let mut values = Arc::into_inner(values).unwrap().into_inner().unwrap();
    values.sort_unstable();
    assert_eq!(values, (0..400).collect::<Vec<_>>());
}

#[test]
fn overflow_broadcast_reports_exact_lag() {
    let (tx, mut rx) = broadcast::overflow::channel(capacity(2));
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    tx.send(3).unwrap();
    assert_eq!(
        rx.try_recv(),
        Err(broadcast::overflow::TryRecvError::Lagged(1))
    );
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Ok(3));
}

#[test]
fn concurrent_broadcast_senders_publish_one_shared_order() {
    let (tx, mut rx1) = broadcast::overflow::channel(capacity(512));
    let mut rx2 = rx1.clone();
    std::thread::scope(|scope| {
        let mut senders = Vec::new();
        for producer in 0..4 {
            let tx = tx.clone();
            senders.push(scope.spawn(move || {
                for value in 0..100 {
                    tx.try_send(producer * 100 + value).unwrap();
                }
            }));
        }
        for sender in senders {
            sender.join().unwrap();
        }
    });

    let first = (0..400)
        .map(|_| rx1.try_recv().unwrap())
        .collect::<Vec<_>>();
    let second = (0..400)
        .map(|_| rx2.try_recv().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(first, second);
    let mut values = first;
    values.sort_unstable();
    assert_eq!(values, (0..400).collect::<Vec<_>>());
}

#[test]
fn concurrent_overflow_broadcast_keeps_one_committed_suffix() {
    let (tx, mut rx1) = broadcast::overflow::channel(capacity(8));
    let mut rx2 = rx1.clone();
    std::thread::scope(|scope| {
        for producer in 0..4 {
            let tx = tx.clone();
            scope.spawn(move || {
                for value in 0..100 {
                    tx.send(producer * 100 + value).unwrap();
                }
            });
        }
    });

    assert_eq!(
        rx1.try_recv(),
        Err(broadcast::overflow::TryRecvError::Lagged(392))
    );
    assert_eq!(
        rx2.try_recv(),
        Err(broadcast::overflow::TryRecvError::Lagged(392))
    );
    let first = (0..8).map(|_| rx1.try_recv().unwrap()).collect::<Vec<_>>();
    let second = (0..8).map(|_| rx2.try_recv().unwrap()).collect::<Vec<_>>();
    assert_eq!(first, second);
    let mut values = first;
    values.sort_unstable();
    values.dedup();
    assert_eq!(values.len(), 8);
}

#[test]
fn cancelled_broadcast_receive_deregisters_its_waker() {
    let (_tx, mut rx) = broadcast::overflow::channel::<i32>(capacity(1));
    let probe = Arc::new(WakeProbe {
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(probe.clone());
    let mut recv = Box::pin(rx.recv());
    assert_eq!(poll_with_waker(recv.as_mut(), &waker), Poll::Pending);
    assert_eq!(Arc::strong_count(&probe), 3);
    drop(recv);
    assert_eq!(Arc::strong_count(&probe), 2);
}

#[test]
fn backpressure_broadcast_is_gated_by_slowest_receiver() {
    let (tx, mut rx1) = broadcast::backpressure::channel(capacity(2));
    let mut rx2 = rx1.clone();
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();

    let mut send = Box::pin(tx.send(3));
    assert_eq!(poll_once(send.as_mut()), Poll::Pending);
    assert_eq!(rx1.try_recv(), Ok(1));
    assert_eq!(poll_once(send.as_mut()), Poll::Pending);
    assert_eq!(rx2.try_recv(), Ok(1));
    assert_eq!(poll_once(send.as_mut()), Poll::Ready(Ok(2)));

    assert_eq!(rx1.try_recv(), Ok(2));
    assert_eq!(rx1.try_recv(), Ok(3));
    assert_eq!(rx2.try_recv(), Ok(2));
    assert_eq!(rx2.try_recv(), Ok(3));
}

#[test]
fn cancelled_backpressure_broadcast_send_deregisters_its_waker() {
    let (tx, mut rx) = broadcast::backpressure::channel(capacity(1));
    tx.try_send(1).unwrap();
    let probe = Arc::new(WakeProbe {
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(probe.clone());
    let mut send = Box::pin(tx.send(2));
    assert_eq!(poll_with_waker(send.as_mut(), &waker), Poll::Pending);
    assert_eq!(Arc::strong_count(&probe), 3);
    drop(send);
    assert_eq!(Arc::strong_count(&probe), 2);
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(tx.try_send(3), Ok(1));
}

#[test]
fn unbounded_broadcast_reclaims_after_all_receivers_advance() {
    let (tx, mut rx1) = broadcast::unbounded::channel();
    let mut rx2 = rx1.clone();
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(tx.len(), 2);

    assert_eq!(rx1.try_recv(), Ok(1));
    assert_eq!(tx.len(), 2);
    assert_eq!(rx2.try_recv(), Ok(1));
    assert_eq!(tx.len(), 1);
    assert_eq!(rx1.try_recv(), Ok(2));
    assert_eq!(rx2.try_recv(), Ok(2));
    assert_eq!(tx.len(), 0);
}

#[test]
fn watch_coalesces_versions() {
    let (tx, mut rx) = watch::channel(0);
    assert_eq!(*rx.borrow(), 0);
    assert_eq!(rx.has_changed(), Ok(false));
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(rx.has_changed(), Ok(true));
    assert_eq!(*pollster::block_on(rx.changed()).unwrap(), 2);
    assert_eq!(rx.has_changed(), Ok(false));
}

#[test]
fn disruptor_multicasts_and_gates_wraparound() {
    let (mut publisher, mut subscriber1) = disruptor::single_producer::channel(ring_capacity(2));
    let mut subscriber2 = subscriber1.clone();
    assert_eq!(publisher.try_publish(10), Ok(0));
    assert_eq!(publisher.try_publish(20), Ok(1));
    assert_eq!(publisher.try_publish(30), Err(TrySendError::Full(30)));

    assert_eq!(subscriber1.try_recv(), Ok((0, 10)));
    assert_eq!(publisher.try_publish(30), Err(TrySendError::Full(30)));
    assert_eq!(subscriber2.try_recv(), Ok((0, 10)));
    assert_eq!(publisher.try_publish(30), Ok(2));

    assert_eq!(subscriber1.try_recv(), Ok((1, 20)));
    assert_eq!(subscriber1.try_recv(), Ok((2, 30)));
    assert_eq!(subscriber2.try_recv(), Ok((1, 20)));
    assert_eq!(subscriber2.try_recv(), Ok((2, 30)));
}

#[test]
fn multi_producer_disruptor_assigns_one_sequence_per_publication() {
    let (publisher, mut subscriber) = disruptor::multi_producer::channel(ring_capacity(4));
    let publisher2 = publisher.clone();
    assert_eq!(publisher.try_publish("a"), Ok(0));
    assert_eq!(publisher2.try_publish("b"), Ok(1));
    assert_eq!(subscriber.try_recv(), Ok((0, "a")));
    assert_eq!(subscriber.try_recv(), Ok((1, "b")));
}

#[test]
fn concurrent_disruptor_publishers_form_a_contiguous_sequence() {
    let (publisher, mut subscriber) = disruptor::multi_producer::channel(ring_capacity(512));
    std::thread::scope(|scope| {
        let mut publishers = Vec::new();
        for producer in 0..4 {
            let publisher = publisher.clone();
            publishers.push(scope.spawn(move || {
                for value in 0..100 {
                    publisher.try_publish(producer * 100 + value).unwrap();
                }
            }));
        }
        for publisher in publishers {
            publisher.join().unwrap();
        }
    });

    let mut values = Vec::new();
    for sequence in 0..400 {
        let (actual, value) = subscriber.try_recv().unwrap();
        assert_eq!(actual, sequence);
        values.push(value);
    }
    values.sort_unstable();
    assert_eq!(values, (0..400).collect::<Vec<_>>());
}

#[test]
fn disruptor_validates_power_of_two_capacity() {
    assert_eq!(disruptor::Capacity::new(0), None);
    assert_eq!(disruptor::Capacity::new(3), None);
    assert_eq!(disruptor::Capacity::new(4).unwrap().get(), 4);
}
