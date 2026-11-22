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

use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;

use asyncband::spmc;
use asyncband::spmc::RecvError;
use asyncband::spmc::TryRecvError;
use tests_integration::poll_once;

// Public queue contracts. The other suites cover notifications, cancellation, and concurrency.
mod concurrency;
mod notification;

#[derive(Debug)]
struct DropSpy(Arc<AtomicUsize>);

impl Drop for DropSpy {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn bounded_receivers_compete_in_fifo_order_and_drain_after_sender_drop() {
    let (mut sender, receiver) = spmc::bounded(2);
    let competing = receiver.clone();
    assert!(receiver.is_empty());
    assert!(!receiver.is_disconnected());
    sender.try_send(10).unwrap();
    sender.try_send(20).unwrap();
    assert!(!receiver.is_empty());
    {
        let mut blocked = pin!(sender.send(99));
        assert!(poll_once(blocked.as_mut()).is_pending());
        assert_eq!(receiver.try_recv(), Ok(10));
        assert_eq!(competing.try_recv(), Ok(20));
        // A pending send waiting to be polled again is not a queued value.
        assert!(receiver.is_empty());
    }
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

    sender.try_send(30).unwrap();
    sender.try_send(40).unwrap();
    drop(sender);
    assert!(receiver.is_disconnected());
    assert!(competing.is_disconnected());
    assert!(!receiver.is_empty());
    assert_eq!(poll_once(pin!(receiver.recv())), Poll::Ready(Ok(30)));
    assert_eq!(poll_once(pin!(competing.recv())), Poll::Ready(Ok(40)));
    assert!(receiver.is_empty());
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    assert_eq!(
        poll_once(pin!(competing.recv())),
        Poll::Ready(Err(RecvError::Disconnected))
    );
}

#[test]
fn bounded_only_last_receiver_disconnects_and_returns_unsent_value() {
    let (mut sender, receiver) = spmc::bounded(2);
    let competing = receiver.clone();
    drop(receiver);
    assert!(!sender.is_disconnected());
    sender.try_send(10).unwrap();
    assert_eq!(competing.try_recv(), Ok(10));
    drop(competing);
    assert!(sender.is_disconnected());
    let error = sender.try_send(20).unwrap_err();
    assert_eq!(error.as_inner(), &20);
    assert_eq!(error.into_inner(), 20);
}

#[test]
fn bounded_buffered_received_and_rejected_values_are_each_dropped_once() {
    let (mut sender, receiver) = spmc::bounded(2);
    let competing = receiver.clone();
    let drops: Vec<_> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    sender.try_send(DropSpy(drops[0].clone())).unwrap();
    sender.try_send(DropSpy(drops[1].clone())).unwrap();
    let received = receiver.try_recv().unwrap();
    sender.try_send(DropSpy(drops[2].clone())).unwrap();

    drop(receiver);
    assert!(drops.iter().all(|count| count.load(Ordering::SeqCst) == 0));
    drop(competing);
    assert_eq!(drops[1].load(Ordering::SeqCst), 1);
    assert_eq!(drops[2].load(Ordering::SeqCst), 1);

    let rejected = sender.try_send(DropSpy(drops[3].clone())).unwrap_err();
    assert_eq!(drops[3].load(Ordering::SeqCst), 0);
    drop(rejected.into_inner());
    drop(received);
    drop(sender);
    assert!(drops.iter().all(|count| count.load(Ordering::SeqCst) == 1));
}

#[test]
fn unbounded_receivers_compete_in_fifo_order_and_drain_after_sender_drop() {
    let (mut sender, receiver) = spmc::unbounded();
    let competing = receiver.clone();
    assert!(receiver.is_empty());
    assert!(!receiver.is_disconnected());
    sender.send(10).unwrap();
    sender.send(20).unwrap();
    assert!(!receiver.is_empty());
    assert_eq!(receiver.try_recv(), Ok(10));
    assert_eq!(competing.try_recv(), Ok(20));
    assert!(receiver.is_empty());
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

    sender.send(30).unwrap();
    sender.send(40).unwrap();
    drop(sender);
    assert!(receiver.is_disconnected());
    assert!(competing.is_disconnected());
    assert!(!receiver.is_empty());
    assert_eq!(poll_once(pin!(receiver.recv())), Poll::Ready(Ok(30)));
    assert_eq!(poll_once(pin!(competing.recv())), Poll::Ready(Ok(40)));
    assert!(receiver.is_empty());
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    assert_eq!(
        poll_once(pin!(competing.recv())),
        Poll::Ready(Err(RecvError::Disconnected))
    );
}

#[test]
fn unbounded_only_last_receiver_disconnects_and_returns_unsent_value() {
    let (mut sender, receiver) = spmc::unbounded();
    let competing = receiver.clone();
    drop(receiver);
    assert!(!sender.is_disconnected());
    sender.send(10).unwrap();
    assert_eq!(competing.try_recv(), Ok(10));
    drop(competing);
    assert!(sender.is_disconnected());
    let error = sender.send(20).unwrap_err();
    assert_eq!(error.as_inner(), &20);
    assert_eq!(error.into_inner(), 20);
}

#[test]
fn unbounded_buffered_received_and_rejected_values_are_each_dropped_once() {
    let (mut sender, receiver) = spmc::unbounded();
    let competing = receiver.clone();
    let drops: Vec<_> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    sender.send(DropSpy(drops[0].clone())).unwrap();
    sender.send(DropSpy(drops[1].clone())).unwrap();
    let received = receiver.try_recv().unwrap();
    sender.send(DropSpy(drops[2].clone())).unwrap();

    drop(receiver);
    assert!(drops.iter().all(|count| count.load(Ordering::SeqCst) == 0));
    drop(competing);
    assert_eq!(drops[1].load(Ordering::SeqCst), 1);
    assert_eq!(drops[2].load(Ordering::SeqCst), 1);

    let rejected = sender.send(DropSpy(drops[3].clone())).unwrap_err();
    assert_eq!(drops[3].load(Ordering::SeqCst), 0);
    drop(rejected.into_inner());
    drop(received);
    drop(sender);
    assert!(drops.iter().all(|count| count.load(Ordering::SeqCst) == 1));
}

#[test]
#[should_panic(expected = "spmc bounded queue requires capacity > 0")]
fn bounded_rejects_zero_capacity() {
    let _ = spmc::bounded::<()>(0);
}
