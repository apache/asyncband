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

use asyncband::blocking::FutureExt;

pub struct Asyncband;
pub struct AsyncChannel;
pub struct Flume;

pub trait BoundedMpmc: Send + Sync + 'static {
    type Sender: Clone + Send + Sync + 'static;
    type Receiver: Clone + Send + Sync + 'static;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver);
    fn send_async(sender: &Self::Sender, value: usize) -> impl Future<Output = ()> + Send;
    fn recv_async(receiver: &Self::Receiver) -> impl Future<Output = Option<usize>> + Send;

    fn send(sender: &Self::Sender, value: usize) {
        Self::send_async(sender, value).block_on();
    }

    fn recv(receiver: &Self::Receiver) -> usize {
        Self::recv_async(receiver)
            .block_on()
            .expect("benchmark receiver disconnected")
    }
}

pub trait UnboundedMpmc: Send + Sync + 'static {
    type Sender: Clone + Send + Sync + 'static;
    type Receiver: Clone + Send + Sync + 'static;

    fn channel() -> (Self::Sender, Self::Receiver);
    fn send(sender: &Self::Sender, value: usize);
    fn recv_async(receiver: &Self::Receiver) -> impl Future<Output = Option<usize>> + Send;

    fn recv(receiver: &Self::Receiver) -> usize {
        Self::recv_async(receiver)
            .block_on()
            .expect("benchmark receiver disconnected")
    }
}

impl BoundedMpmc for Asyncband {
    type Receiver = asyncband::mpmc::BoundedReceiver<usize>;
    type Sender = asyncband::mpmc::BoundedSender<usize>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        asyncband::mpmc::bounded(capacity)
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

impl UnboundedMpmc for Asyncband {
    type Receiver = asyncband::mpmc::UnboundedReceiver<usize>;
    type Sender = asyncband::mpmc::UnboundedSender<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        asyncband::mpmc::unbounded()
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
}
