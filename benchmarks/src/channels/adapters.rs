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
use std::marker::PhantomData;

use asyncband::blocking::FutureExt;
use asyncband::mpmc;
use asyncband::spmc;

use crate::channels::BOUNDED_CAPACITY;

pub struct Mpmc;
pub struct Spmc;
pub struct AsyncChannel;
pub struct Flume;

pub struct Bounded<C, const CAPACITY: usize = BOUNDED_CAPACITY>(PhantomData<C>);
pub struct Unbounded<C>(PhantomData<C>);

// Each task owns its sender. Only workloads with multiple producers require Sender: Clone.
pub trait Channel: Send + Sync + 'static {
    type Sender: Send + 'static;
    type Receiver: Clone + Send + Sync + 'static;

    fn channel() -> (Self::Sender, Self::Receiver);
    fn send(sender: &mut Self::Sender, value: usize) -> impl Future<Output = ()> + Send;
    fn recv(receiver: &Self::Receiver) -> impl Future<Output = Option<usize>> + Send;
}

impl<const CAPACITY: usize> Channel for Bounded<Spmc, CAPACITY> {
    type Sender = spmc::BoundedSender<usize>;
    type Receiver = spmc::BoundedReceiver<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        spmc::bounded(CAPACITY)
    }

    async fn send(sender: &mut Self::Sender, value: usize) {
        sender.send(value).await.unwrap();
    }

    async fn recv(receiver: &Self::Receiver) -> Option<usize> {
        receiver.recv().await.ok()
    }
}

impl Channel for Unbounded<Spmc> {
    type Sender = spmc::UnboundedSender<usize>;
    type Receiver = spmc::UnboundedReceiver<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        spmc::unbounded()
    }

    async fn send(sender: &mut Self::Sender, value: usize) {
        sender.send(value).unwrap();
    }

    async fn recv(receiver: &Self::Receiver) -> Option<usize> {
        receiver.recv().await.ok()
    }
}

impl<C: BoundedMpmc, const CAPACITY: usize> Channel for Bounded<C, CAPACITY> {
    type Sender = C::Sender;
    type Receiver = C::Receiver;

    fn channel() -> (Self::Sender, Self::Receiver) {
        C::channel(CAPACITY)
    }

    async fn send(sender: &mut Self::Sender, value: usize) {
        C::send_async(sender, value).await;
    }

    async fn recv(receiver: &Self::Receiver) -> Option<usize> {
        C::recv_async(receiver).await
    }
}

impl<C: UnboundedMpmc> Channel for Unbounded<C> {
    type Sender = C::Sender;
    type Receiver = C::Receiver;

    fn channel() -> (Self::Sender, Self::Receiver) {
        C::channel()
    }

    async fn send(sender: &mut Self::Sender, value: usize) {
        C::send(sender, value);
    }

    async fn recv(receiver: &Self::Receiver) -> Option<usize> {
        C::recv_async(receiver).await
    }
}

pub trait BoundedMpmc: Send + Sync + 'static {
    type Sender: Clone + Send + Sync + 'static;
    type Receiver: Clone + Send + Sync + 'static;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver);
    fn send_async(sender: &Self::Sender, value: usize) -> impl Future<Output = ()> + Send;
    fn recv_async(receiver: &Self::Receiver) -> impl Future<Output = Option<usize>> + Send;

    fn send(sender: &Self::Sender, value: usize) {
        Self::send_async(sender, value).block_on();
    }

    fn recv(receiver: &Self::Receiver) -> Option<usize> {
        Self::recv_async(receiver).block_on()
    }
}

pub trait UnboundedMpmc: Send + Sync + 'static {
    type Sender: Clone + Send + Sync + 'static;
    type Receiver: Clone + Send + Sync + 'static;

    fn channel() -> (Self::Sender, Self::Receiver);
    fn send(sender: &Self::Sender, value: usize);
    fn recv_async(receiver: &Self::Receiver) -> impl Future<Output = Option<usize>> + Send;

    fn recv(receiver: &Self::Receiver) -> Option<usize> {
        Self::recv_async(receiver).block_on()
    }
}

impl BoundedMpmc for Mpmc {
    type Receiver = mpmc::BoundedReceiver<usize>;
    type Sender = mpmc::BoundedSender<usize>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        mpmc::bounded(capacity)
    }

    async fn send_async(sender: &Self::Sender, value: usize) {
        sender
            .send(value)
            .await
            .expect("benchmark sender disconnected");
    }

    async fn recv_async(receiver: &Self::Receiver) -> Option<usize> {
        receiver.recv().await.ok()
    }
}

impl UnboundedMpmc for Mpmc {
    type Receiver = mpmc::UnboundedReceiver<usize>;
    type Sender = mpmc::UnboundedSender<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        mpmc::unbounded()
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.send(value).expect("benchmark sender disconnected");
    }

    async fn recv_async(receiver: &Self::Receiver) -> Option<usize> {
        receiver.recv().await.ok()
    }
}

impl BoundedMpmc for AsyncChannel {
    type Receiver = async_channel::Receiver<usize>;
    type Sender = async_channel::Sender<usize>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        async_channel::bounded(capacity)
    }

    async fn send_async(sender: &Self::Sender, value: usize) {
        sender
            .send(value)
            .await
            .expect("benchmark sender disconnected");
    }

    async fn recv_async(receiver: &Self::Receiver) -> Option<usize> {
        receiver.recv().await.ok()
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender
            .send_blocking(value)
            .expect("benchmark sender disconnected");
    }

    fn recv(receiver: &Self::Receiver) -> Option<usize> {
        receiver.recv_blocking().ok()
    }
}

impl UnboundedMpmc for AsyncChannel {
    type Receiver = async_channel::Receiver<usize>;
    type Sender = async_channel::Sender<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        async_channel::unbounded()
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender
            .try_send(value)
            .expect("benchmark sender disconnected");
    }

    async fn recv_async(receiver: &Self::Receiver) -> Option<usize> {
        receiver.recv().await.ok()
    }

    fn recv(receiver: &Self::Receiver) -> Option<usize> {
        receiver.recv_blocking().ok()
    }
}

impl BoundedMpmc for Flume {
    type Receiver = flume::Receiver<usize>;
    type Sender = flume::Sender<usize>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        flume::bounded(capacity)
    }

    async fn send_async(sender: &Self::Sender, value: usize) {
        sender
            .send_async(value)
            .await
            .expect("benchmark sender disconnected");
    }

    async fn recv_async(receiver: &Self::Receiver) -> Option<usize> {
        receiver.recv_async().await.ok()
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.send(value).expect("benchmark sender disconnected");
    }

    fn recv(receiver: &Self::Receiver) -> Option<usize> {
        receiver.recv().ok()
    }
}

impl UnboundedMpmc for Flume {
    type Receiver = flume::Receiver<usize>;
    type Sender = flume::Sender<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        flume::unbounded()
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.send(value).expect("benchmark sender disconnected");
    }

    async fn recv_async(receiver: &Self::Receiver) -> Option<usize> {
        receiver.recv_async().await.ok()
    }

    fn recv(receiver: &Self::Receiver) -> Option<usize> {
        receiver.recv().ok()
    }
}
