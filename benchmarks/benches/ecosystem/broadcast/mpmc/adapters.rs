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
use std::sync::Arc;
use std::task::Context;

use asyncband::blocking::FutureExt;
use benchmarks::support::poll_ready;

pub struct Asyncband;
pub struct Tokio;
pub struct AsyncBroadcast;

/// Selects the shared `Arc<T>` delivery contract across all broadcast adapters.
///
/// Asyncband returns its own shared `Arc<T>` directly from `recv` and `try_recv`; Tokio and
/// `async-broadcast` wrap each published `T` in `Arc::new` so every subscriber receives `Arc<T>`.
pub struct Shared<T = usize>(pub T);

pub trait Payload: Send + Sync + 'static {
    type Output: Send;

    fn new(sequence: usize) -> Self;
    fn sequence(&self) -> usize;
    fn output_sequence(output: Self::Output) -> usize;
}

impl Payload for usize {
    type Output = usize;

    #[inline]
    fn new(sequence: usize) -> Self {
        sequence
    }

    #[inline]
    fn sequence(&self) -> usize {
        *self
    }

    #[inline]
    fn output_sequence(output: usize) -> usize {
        output
    }
}

impl Payload for [u8; 1024] {
    type Output = [u8; 1024];

    #[inline]
    fn new(sequence: usize) -> Self {
        let mut value = [1; 1024];
        value[..size_of::<usize>()].copy_from_slice(&sequence.to_le_bytes());
        value
    }

    #[inline]
    fn sequence(&self) -> usize {
        assert_eq!(self[1023], 1);
        usize::from_le_bytes(self[..size_of::<usize>()].try_into().unwrap())
    }

    #[inline]
    fn output_sequence(output: [u8; 1024]) -> usize {
        black_box(output).sequence()
    }
}

impl<T: Payload> Payload for Shared<T> {
    type Output = Arc<T>;

    #[inline]
    fn new(sequence: usize) -> Self {
        Shared(T::new(sequence))
    }

    #[inline]
    fn sequence(&self) -> usize {
        self.0.sequence()
    }

    #[inline]
    fn output_sequence(output: Arc<T>) -> usize {
        black_box(output).sequence()
    }
}

pub trait BroadcastMpmc<M: Payload = usize>: Send + Sync + 'static {
    type Sender: Clone + Send + 'static;
    type Receiver: Send + 'static;

    fn channel(capacity: usize, receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>);
    fn send(sender: &Self::Sender, value: M);
    fn try_recv(receiver: &mut Self::Receiver) -> Option<M::Output>;
    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> M::Output;
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

impl BroadcastMpmc<[u8; 1024]> for Asyncband {
    type Receiver = asyncband::broadcast::mpmc::UnboundedReceiver<[u8; 1024]>;
    type Sender = asyncband::broadcast::mpmc::UnboundedSender<[u8; 1024]>;

    fn channel(_capacity: usize, receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>) {
        let (sender, receiver) = asyncband::broadcast::mpmc::unbounded();
        let mut receivers = Vec::with_capacity(receiver_count);
        receivers.push(receiver);
        for _ in 1..receiver_count {
            receivers.push(sender.subscribe());
        }
        (sender, receivers)
    }

    fn send(sender: &Self::Sender, value: [u8; 1024]) {
        sender.send(value);
    }

    fn try_recv(receiver: &mut Self::Receiver) -> Option<[u8; 1024]> {
        match receiver.try_recv_cloned() {
            Ok(value) => Some(value),
            Err(asyncband::broadcast::mpmc::TryRecvError::Empty) => None,
            Err(asyncband::broadcast::mpmc::TryRecvError::Disconnected) => {
                panic!("asyncband channel closed during benchmark")
            }
        }
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> [u8; 1024] {
        poll_ready(receiver.recv_cloned(), context).unwrap()
    }
}

impl<T: Payload> BroadcastMpmc<Shared<T>> for Asyncband {
    type Receiver = asyncband::broadcast::mpmc::UnboundedReceiver<T>;
    type Sender = asyncband::broadcast::mpmc::UnboundedSender<T>;

    fn channel(_capacity: usize, receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>) {
        let (sender, receiver) = asyncband::broadcast::mpmc::unbounded();
        let mut receivers = Vec::with_capacity(receiver_count);
        receivers.push(receiver);
        for _ in 1..receiver_count {
            receivers.push(sender.subscribe());
        }
        (sender, receivers)
    }

    fn send(sender: &Self::Sender, Shared(value): Shared<T>) {
        sender.send(value);
    }

    fn try_recv(receiver: &mut Self::Receiver) -> Option<Arc<T>> {
        match receiver.try_recv() {
            Ok(value) => Some(value),
            Err(asyncband::broadcast::mpmc::TryRecvError::Empty) => None,
            Err(asyncband::broadcast::mpmc::TryRecvError::Disconnected) => {
                panic!("asyncband channel closed during benchmark")
            }
        }
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> Arc<T> {
        poll_ready(receiver.recv(), context).unwrap()
    }
}

impl<T: Payload<Output = T> + Clone> BroadcastMpmc<T> for Tokio {
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

impl<T: Payload> BroadcastMpmc<Shared<T>> for Tokio {
    type Receiver = tokio::sync::broadcast::Receiver<Arc<T>>;
    type Sender = tokio::sync::broadcast::Sender<Arc<T>>;

    fn channel(capacity: usize, receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>) {
        let (sender, receiver) = tokio::sync::broadcast::channel(capacity);
        let mut receivers = Vec::with_capacity(receiver_count);
        receivers.push(receiver);
        for _ in 1..receiver_count {
            receivers.push(sender.subscribe());
        }
        (sender, receivers)
    }

    fn send(sender: &Self::Sender, Shared(value): Shared<T>) {
        assert!(
            sender.send(Arc::new(value)).is_ok(),
            "Tokio broadcast channel closed during benchmark"
        );
    }

    fn try_recv(receiver: &mut Self::Receiver) -> Option<Arc<T>> {
        match receiver.try_recv() {
            Ok(value) => Some(value),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => None,
            Err(error) => panic!("unexpected Tokio receive error: {error}"),
        }
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> Arc<T> {
        poll_ready(receiver.recv(), context).unwrap()
    }
}

impl<T: Payload<Output = T> + Clone> BroadcastMpmc<T> for AsyncBroadcast {
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

impl<T: Payload> BroadcastMpmc<Shared<T>> for AsyncBroadcast {
    type Receiver = async_broadcast::Receiver<Arc<T>>;
    type Sender = async_broadcast::Sender<Arc<T>>;

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

    fn send(sender: &Self::Sender, Shared(value): Shared<T>) {
        assert!(
            sender.try_broadcast(Arc::new(value)).is_ok(),
            "async-broadcast channel rejected benchmark send"
        );
    }

    fn try_recv(receiver: &mut Self::Receiver) -> Option<Arc<T>> {
        match receiver.try_recv() {
            Ok(value) => Some(value),
            Err(async_broadcast::TryRecvError::Empty) => None,
            Err(error) => panic!("unexpected async-broadcast receive error: {error}"),
        }
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> Arc<T> {
        poll_ready(receiver.recv_direct(), context).unwrap()
    }
}

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
