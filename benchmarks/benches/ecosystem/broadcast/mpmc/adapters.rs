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
use std::hint::black_box;
use std::marker::PhantomData;
use std::sync::Arc;
use std::task::Context;

use asyncband::blocking::FutureExt;
use benchmarks::support::poll_ready;

pub struct Asyncband;
pub struct Tokio;
pub struct AsyncBroadcast;

/// Adapts a broadcast peer so every published message is delivered as a shared `Arc<usize>`.
pub struct Shared<C>(PhantomData<C>);

pub trait BroadcastMpmc<T = usize>: Send + Sync + 'static {
    type Sender: Clone + Send + 'static;
    type Receiver: Send + 'static;

    fn channel(capacity: usize, receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>);
    fn send(sender: &Self::Sender, value: T);
    fn try_recv(receiver: &mut Self::Receiver) -> Option<T>;
    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> T;
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
            Ok(value) => Some(*value),
            Err(asyncband::broadcast::mpmc::TryRecvError::Empty) => None,
            Err(asyncband::broadcast::mpmc::TryRecvError::Disconnected) => {
                panic!("asyncband channel closed during benchmark")
            }
        }
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        *poll_ready(receiver.recv(), context).unwrap()
    }
}

impl<T: Clone + Send + Sync + 'static> BroadcastMpmc<T> for Tokio {
    type Receiver = tokio::sync::broadcast::Receiver<T>;
    type Sender = tokio::sync::broadcast::Sender<T>;

    fn channel(capacity: usize, receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>) {
        let (sender, receiver) = tokio::sync::broadcast::channel(capacity);
        let mut receivers = Vec::with_capacity(receiver_count);
        receivers.push(receiver);
        for _ in 1..receiver_count {
            receivers.push(sender.subscribe());
        }
        (sender, receivers)
    }

    fn send(sender: &Self::Sender, value: T) {
        assert!(
            sender.send(value).is_ok(),
            "Tokio broadcast channel closed during benchmark"
        );
    }

    fn try_recv(receiver: &mut Self::Receiver) -> Option<T> {
        match receiver.try_recv() {
            Ok(value) => Some(value),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => None,
            Err(error) => panic!("unexpected Tokio receive error: {error}"),
        }
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> T {
        poll_ready(receiver.recv(), context).unwrap()
    }
}

impl<T: Clone + Send + Sync + 'static> BroadcastMpmc<T> for AsyncBroadcast {
    type Receiver = async_broadcast::Receiver<T>;
    type Sender = async_broadcast::Sender<T>;

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

    fn send(sender: &Self::Sender, value: T) {
        assert!(
            sender.try_broadcast(value).is_ok(),
            "async-broadcast channel rejected benchmark send"
        );
    }

    fn try_recv(receiver: &mut Self::Receiver) -> Option<T> {
        match receiver.try_recv() {
            Ok(value) => Some(value),
            Err(async_broadcast::TryRecvError::Empty) => None,
            Err(error) => panic!("unexpected async-broadcast receive error: {error}"),
        }
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> T {
        poll_ready(receiver.recv_direct(), context).unwrap()
    }
}

impl BroadcastMpmc for Shared<Asyncband> {
    type Receiver = <Asyncband as BroadcastMpmc>::Receiver;
    type Sender = <Asyncband as BroadcastMpmc>::Sender;

    fn channel(capacity: usize, receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>) {
        <Asyncband as BroadcastMpmc>::channel(capacity, receiver_count)
    }

    fn send(sender: &Self::Sender, value: usize) {
        <Asyncband as BroadcastMpmc>::send(sender, value);
    }

    fn try_recv(receiver: &mut Self::Receiver) -> Option<usize> {
        <Asyncband as BroadcastMpmc>::try_recv(receiver)
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        <Asyncband as BroadcastMpmc>::recv_ready(receiver, context)
    }
}

macro_rules! impl_shared_arc {
    ($peer:ty) => {
        impl BroadcastMpmc for Shared<$peer> {
            type Receiver = <$peer as BroadcastMpmc<Arc<usize>>>::Receiver;
            type Sender = <$peer as BroadcastMpmc<Arc<usize>>>::Sender;

            fn channel(
                capacity: usize,
                receiver_count: usize,
            ) -> (Self::Sender, Vec<Self::Receiver>) {
                <$peer as BroadcastMpmc<Arc<usize>>>::channel(capacity, receiver_count)
            }

            fn send(sender: &Self::Sender, value: usize) {
                <$peer as BroadcastMpmc<Arc<usize>>>::send(sender, Arc::new(value));
            }

            fn try_recv(receiver: &mut Self::Receiver) -> Option<usize> {
                <$peer as BroadcastMpmc<Arc<usize>>>::try_recv(receiver).map(|v| *black_box(v))
            }

            fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
                *black_box(<$peer as BroadcastMpmc<Arc<usize>>>::recv_ready(
                    receiver, context,
                ))
            }
        }
    };
}

impl_shared_arc!(Tokio);
impl_shared_arc!(AsyncBroadcast);

/// Every subscription receives each accepted value; producers wait when capacity is exhausted.
pub trait BoundedBroadcastMpmc: Send + Sync + 'static {
    type Sender: Clone + Send + Sync + 'static;
    type Receiver: Send + 'static;

    fn channel(capacity: usize, receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>);
    fn try_send(sender: &Self::Sender, value: usize);
    fn send_async(sender: &Self::Sender, value: usize) -> impl Future<Output = ()> + Send;
    fn recv_async(receiver: &mut Self::Receiver) -> impl Future<Output = usize> + Send;

    fn send_ready(sender: &Self::Sender, value: usize, context: &mut Context<'_>) {
        poll_ready(Self::send_async(sender, value), context);
    }

    fn send_blocking(sender: &Self::Sender, value: usize) {
        FutureExt::block_on(Self::send_async(sender, value));
    }

    fn try_recv(receiver: &mut Self::Receiver) -> Option<usize>;

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(Self::recv_async(receiver), context)
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        FutureExt::block_on(Self::recv_async(receiver))
    }
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

    async fn send_async(sender: &Self::Sender, value: usize) {
        sender.send(value).await;
    }

    async fn recv_async(receiver: &mut Self::Receiver) -> usize {
        *receiver.recv().await.unwrap()
    }

    fn try_recv(receiver: &mut Self::Receiver) -> Option<usize> {
        match receiver.try_recv() {
            Ok(value) => Some(*value),
            Err(asyncband::broadcast::mpmc::TryRecvError::Empty) => None,
            Err(asyncband::broadcast::mpmc::TryRecvError::Disconnected) => {
                panic!("asyncband channel closed during benchmark")
            }
        }
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

    async fn send_async(sender: &Self::Sender, value: usize) {
        sender
            .broadcast_direct(value)
            .await
            .expect("async-broadcast lost every receiver during benchmark");
    }

    async fn recv_async(receiver: &mut Self::Receiver) -> usize {
        receiver.recv_direct().await.unwrap()
    }

    fn try_recv(receiver: &mut Self::Receiver) -> Option<usize> {
        match receiver.try_recv() {
            Ok(value) => Some(value),
            Err(async_broadcast::TryRecvError::Empty) => None,
            Err(error) => panic!("unexpected async-broadcast receive error: {error}"),
        }
    }
}
