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

use std::task::Context;

use crate::support::poll_ready;

pub struct Asyncband;
pub struct Tokio;
pub struct AsyncBroadcast;

pub trait BroadcastMpmc: Send + Sync + 'static {
    type Sender: Clone + Send + 'static;
    type Receiver: Send + 'static;

    fn channel(capacity: usize, receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>);
    fn send(sender: &Self::Sender, value: usize);
    fn try_recv(receiver: &mut Self::Receiver) -> Option<usize>;
    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize;
}

impl BroadcastMpmc for Asyncband {
    type Receiver = asyncband::broadcast::mpmc::UnboundedReceiver<usize>;
    type Sender = asyncband::broadcast::mpmc::UnboundedSender<usize>;

    fn channel(_capacity: usize, receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>) {
        let (sender, receiver) = asyncband::broadcast::mpmc::unbounded();
        let mut receivers = Vec::with_capacity(receiver_count);
        receivers.push(receiver);
        for _ in 1..receiver_count {
            receivers.push(sender.subscribe());
        }
        (sender, receivers)
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.send(value);
    }

    fn try_recv(receiver: &mut Self::Receiver) -> Option<usize> {
        match receiver.try_recv() {
            Ok(value) => Some(value),
            Err(asyncband::broadcast::mpmc::TryRecvError::Empty) => None,
            Err(asyncband::broadcast::mpmc::TryRecvError::Disconnected) => {
                panic!("asyncband channel closed during benchmark")
            }
        }
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv(), context).unwrap()
    }
}

impl BroadcastMpmc for Tokio {
    type Receiver = tokio::sync::broadcast::Receiver<usize>;
    type Sender = tokio::sync::broadcast::Sender<usize>;

    fn channel(capacity: usize, receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>) {
        let (sender, receiver) = tokio::sync::broadcast::channel(capacity);
        let mut receivers = Vec::with_capacity(receiver_count);
        receivers.push(receiver);
        for _ in 1..receiver_count {
            receivers.push(sender.subscribe());
        }
        (sender, receivers)
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> Option<usize> {
        match receiver.try_recv() {
            Ok(value) => Some(value),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => None,
            Err(error) => panic!("unexpected Tokio receive error: {error}"),
        }
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv(), context).unwrap()
    }
}

impl BroadcastMpmc for AsyncBroadcast {
    type Receiver = async_broadcast::Receiver<usize>;
    type Sender = async_broadcast::Sender<usize>;

    fn channel(capacity: usize, receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>) {
        let (sender, receiver) = async_broadcast::broadcast(capacity);
        let mut receivers = Vec::with_capacity(receiver_count);
        receivers.push(receiver);
        for _ in 1..receiver_count {
            let receiver = receivers[0].clone();
            receivers.push(receiver);
        }
        (sender, receivers)
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.try_broadcast(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> Option<usize> {
        match receiver.try_recv() {
            Ok(value) => Some(value),
            Err(async_broadcast::TryRecvError::Empty) => None,
            Err(error) => panic!("unexpected async-broadcast receive error: {error}"),
        }
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv_direct(), context).unwrap()
    }
}

/// A lossless bounded broadcast channel: every accepted value reaches every active subscription,
/// and a full channel makes producers wait rather than displacing anything.
///
/// `tokio::sync::broadcast` deliberately has no implementation here — see the note in `bounded.rs`.
pub trait BoundedBroadcastMpmc: Send + Sync + 'static {
    type Sender: Clone + Send + 'static;
    type Receiver: Send + 'static;

    fn channel(capacity: usize, receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>);
    fn try_send(sender: &Self::Sender, value: usize);
    fn send_ready(sender: &Self::Sender, value: usize, context: &mut Context<'_>);
    fn send_blocking(sender: &Self::Sender, value: usize);
    fn try_recv(receiver: &mut Self::Receiver) -> Option<usize>;
    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize;
    fn recv_blocking(receiver: &mut Self::Receiver) -> usize;
}

impl BoundedBroadcastMpmc for Asyncband {
    type Receiver = asyncband::broadcast::mpmc::BoundedReceiver<usize>;
    type Sender = asyncband::broadcast::mpmc::BoundedSender<usize>;

    fn channel(capacity: usize, receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>) {
        let (sender, receiver) = asyncband::broadcast::mpmc::bounded(capacity);
        let mut receivers = Vec::with_capacity(receiver_count);
        receivers.push(receiver);
        for _ in 1..receiver_count {
            receivers.push(sender.subscribe());
        }
        (sender, receivers)
    }

    fn try_send(sender: &Self::Sender, value: usize) {
        sender.try_send(value).unwrap();
    }

    fn send_ready(sender: &Self::Sender, value: usize, context: &mut Context<'_>) {
        poll_ready(sender.send(value), context);
    }

    fn send_blocking(sender: &Self::Sender, value: usize) {
        pollster::block_on(sender.send(value));
    }

    fn try_recv(receiver: &mut Self::Receiver) -> Option<usize> {
        match receiver.try_recv() {
            Ok(value) => Some(value),
            Err(asyncband::broadcast::mpmc::TryRecvError::Empty) => None,
            Err(asyncband::broadcast::mpmc::TryRecvError::Disconnected) => {
                panic!("asyncband channel closed during benchmark")
            }
        }
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv(), context).unwrap()
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).unwrap()
    }
}

impl BoundedBroadcastMpmc for AsyncBroadcast {
    type Receiver = async_broadcast::Receiver<usize>;
    type Sender = async_broadcast::Sender<usize>;

    fn channel(capacity: usize, receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>) {
        let (sender, receiver) = async_broadcast::broadcast(capacity);
        let mut receivers = Vec::with_capacity(receiver_count);
        receivers.push(receiver);
        for _ in 1..receiver_count {
            let receiver = receivers[0].clone();
            receivers.push(receiver);
        }
        (sender, receivers)
    }

    fn try_send(sender: &Self::Sender, value: usize) {
        sender.try_broadcast(value).unwrap();
    }

    fn send_ready(sender: &Self::Sender, value: usize, context: &mut Context<'_>) {
        poll_ready(sender.broadcast_direct(value), context)
            .expect("async-broadcast lost every receiver during benchmark");
    }

    fn send_blocking(sender: &Self::Sender, value: usize) {
        pollster::block_on(sender.broadcast_direct(value))
            .expect("async-broadcast lost every receiver during benchmark");
    }

    fn try_recv(receiver: &mut Self::Receiver) -> Option<usize> {
        match receiver.try_recv() {
            Ok(value) => Some(value),
            Err(async_broadcast::TryRecvError::Empty) => None,
            Err(error) => panic!("unexpected async-broadcast receive error: {error}"),
        }
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv_direct(), context).unwrap()
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv_direct()).unwrap()
    }
}
