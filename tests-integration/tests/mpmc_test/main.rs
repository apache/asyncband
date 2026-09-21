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
use std::pin::pin;

use asyncband::mpmc;
use asyncband::mpmc::RecvError;
use asyncband::mpmc::TryRecvError;
use asyncband::mpmc::TrySendError;
use tests_integration::expect_ready;
use tests_integration::poll_once;

// Public queue contracts. The other suites cover notifications, callbacks, and concurrency.
mod callbacks;
mod concurrency;
mod notification;
mod traits;

/// Either receiver flavor, so one case can cover both queues.
trait Receiver<T>: Clone {
    fn recv(&self) -> impl Future<Output = Result<T, RecvError>> + Send;
    fn try_recv(&self) -> Result<T, TryRecvError>;
}

impl<T: Send> Receiver<T> for mpmc::BoundedReceiver<T> {
    fn recv(&self) -> impl Future<Output = Result<T, RecvError>> + Send {
        self.recv()
    }

    fn try_recv(&self) -> Result<T, TryRecvError> {
        self.try_recv()
    }
}

impl<T: Send> Receiver<T> for mpmc::UnboundedReceiver<T> {
    fn recv(&self) -> impl Future<Output = Result<T, RecvError>> + Send {
        self.recv()
    }

    fn try_recv(&self) -> Result<T, TryRecvError> {
        self.try_recv()
    }
}

#[test]
fn bounded_enforces_exact_capacity_and_fifo_order() {
    let (sender, receiver) = mpmc::bounded(2);
    let competing = receiver.clone();

    sender.try_send(0).unwrap();
    sender.try_send(1).unwrap();
    assert_eq!(sender.try_send(2), Err(TrySendError::Full(2)));

    assert_eq!(receiver.try_recv(), Ok(0));
    assert_eq!(competing.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
}

#[test]
#[should_panic(expected = "mpmc bounded queue requires capacity > 0")]
fn bounded_rejects_zero_capacity() {
    let _ = mpmc::bounded::<()>(0);
}

#[test]
fn receiver_and_sender_clone_counts_control_disconnection() {
    let (sender, receiver) = mpmc::unbounded();
    let sender_clone = sender.clone();
    let receiver_clone = receiver.clone();

    drop(receiver);
    sender.send(1).unwrap();
    assert_eq!(receiver_clone.try_recv(), Ok(1));

    drop(sender);
    assert_eq!(receiver_clone.try_recv(), Err(TryRecvError::Empty));
    drop(sender_clone);
    assert_eq!(receiver_clone.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn sends_after_the_last_receiver_return_the_value() {
    let (bounded_sender, bounded_receiver) = mpmc::bounded(1);
    let bounded_receiver_clone = bounded_receiver.clone();
    drop(bounded_receiver);
    drop(bounded_receiver_clone);
    assert_eq!(
        bounded_sender.try_send(1),
        Err(TrySendError::Disconnected(1))
    );
    assert_eq!(
        bounded_sender.try_send(2),
        Err(TrySendError::Disconnected(2))
    );

    let (unbounded_sender, unbounded_receiver) = mpmc::unbounded();
    drop(unbounded_receiver);
    assert_eq!(unbounded_sender.send(3).unwrap_err().into_inner(), 3);
}

fn buffered_values_drain_in_order_before_disconnection<S>(
    sender: S,
    send: impl Fn(&S, usize),
    receiver: impl Receiver<usize>,
) {
    for value in 0..3 {
        send(&sender, value);
    }
    drop(sender);

    for expected in 0..3 {
        assert_eq!(expect_ready(poll_once(pin!(receiver.recv()))), Ok(expected));
    }
    assert_eq!(
        expect_ready(poll_once(pin!(receiver.recv()))),
        Err(RecvError::Disconnected)
    );
}

#[test]
fn bounded_buffered_values_drain_in_order_before_disconnection() {
    let (sender, receiver) = mpmc::bounded(3);
    buffered_values_drain_in_order_before_disconnection(
        sender,
        |sender, value| sender.try_send(value).unwrap(),
        receiver,
    );
}

#[test]
fn unbounded_buffered_values_drain_in_order_before_disconnection() {
    let (sender, receiver) = mpmc::unbounded();
    buffered_values_drain_in_order_before_disconnection(
        sender,
        |sender, value| sender.send(value).unwrap(),
        receiver,
    );
}
