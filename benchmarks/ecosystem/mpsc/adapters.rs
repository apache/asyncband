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
use std::task::Context;

use crate::support::poll_ready;

pub struct Asyncband;
pub struct Tokio;
pub struct AsyncChannel;
pub struct Crossbeam;
pub struct Flume;

pub trait BoundedMpsc: Send + Sync + 'static {
    type Sender: Clone + Send + 'static;
    type Receiver: Send + 'static;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver);
    fn try_send(sender: &Self::Sender, value: usize);
    fn try_recv(receiver: &mut Self::Receiver) -> usize;
    fn send_ready(sender: &Self::Sender, value: usize, context: &mut Context<'_>);
    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize;
    fn send_blocking(sender: &Self::Sender, value: usize);
    fn recv_blocking(receiver: &mut Self::Receiver) -> usize;
}

pub trait UnboundedMpsc: Send + Sync + 'static {
    type Sender: Clone + Send + 'static;
    type Receiver: Send + 'static;

    fn channel() -> (Self::Sender, Self::Receiver);
    fn send(sender: &Self::Sender, value: usize);
    fn try_recv(receiver: &mut Self::Receiver) -> usize;
    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize;
    fn recv_blocking(receiver: &mut Self::Receiver) -> usize;
}

pub trait AsyncUnboundedMpsc: UnboundedMpsc {
    fn recv_async(receiver: &mut Self::Receiver) -> impl Future<Output = usize> + Send + '_;
}

impl BoundedMpsc for Asyncband {
    type Receiver = asyncband::mpsc::BoundedReceiver<usize>;
    type Sender = asyncband::mpsc::BoundedSender<usize>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        asyncband::mpsc::bounded(capacity)
    }

    fn try_send(sender: &Self::Sender, value: usize) {
        sender.try_send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn send_ready(sender: &Self::Sender, value: usize, context: &mut Context<'_>) {
        poll_ready(sender.send(value), context).unwrap();
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv(), context).unwrap()
    }

    fn send_blocking(sender: &Self::Sender, value: usize) {
        pollster::block_on(sender.send(value)).unwrap();
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).unwrap()
    }
}

impl BoundedMpsc for Tokio {
    type Receiver = tokio::sync::mpsc::Receiver<usize>;
    type Sender = tokio::sync::mpsc::Sender<usize>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        tokio::sync::mpsc::channel(capacity)
    }

    fn try_send(sender: &Self::Sender, value: usize) {
        sender.try_send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn send_ready(sender: &Self::Sender, value: usize, context: &mut Context<'_>) {
        poll_ready(sender.send(value), context).unwrap();
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv(), context).unwrap()
    }

    fn send_blocking(sender: &Self::Sender, value: usize) {
        pollster::block_on(sender.send(value)).unwrap();
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).unwrap()
    }
}

impl BoundedMpsc for AsyncChannel {
    type Receiver = async_channel::Receiver<usize>;
    type Sender = async_channel::Sender<usize>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        async_channel::bounded(capacity)
    }

    fn try_send(sender: &Self::Sender, value: usize) {
        sender.try_send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn send_ready(sender: &Self::Sender, value: usize, context: &mut Context<'_>) {
        poll_ready(sender.send(value), context).unwrap();
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv(), context).unwrap()
    }

    fn send_blocking(sender: &Self::Sender, value: usize) {
        pollster::block_on(sender.send(value)).unwrap();
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).unwrap()
    }
}

impl BoundedMpsc for Flume {
    type Receiver = flume::Receiver<usize>;
    type Sender = flume::Sender<usize>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        flume::bounded(capacity)
    }

    fn try_send(sender: &Self::Sender, value: usize) {
        sender.try_send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn send_ready(sender: &Self::Sender, value: usize, context: &mut Context<'_>) {
        poll_ready(sender.send_async(value), context).unwrap();
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv_async(), context).unwrap()
    }

    fn send_blocking(sender: &Self::Sender, value: usize) {
        pollster::block_on(sender.send_async(value)).unwrap();
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv_async()).unwrap()
    }
}

impl UnboundedMpsc for Asyncband {
    type Receiver = asyncband::mpsc::UnboundedReceiver<usize>;
    type Sender = asyncband::mpsc::UnboundedSender<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        asyncband::mpsc::unbounded()
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv(), context).unwrap()
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).unwrap()
    }
}

impl AsyncUnboundedMpsc for Asyncband {
    async fn recv_async(receiver: &mut Self::Receiver) -> usize {
        receiver.recv().await.unwrap()
    }
}

impl UnboundedMpsc for Tokio {
    type Receiver = tokio::sync::mpsc::UnboundedReceiver<usize>;
    type Sender = tokio::sync::mpsc::UnboundedSender<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        tokio::sync::mpsc::unbounded_channel()
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv(), context).unwrap()
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).unwrap()
    }
}

impl AsyncUnboundedMpsc for Tokio {
    async fn recv_async(receiver: &mut Self::Receiver) -> usize {
        receiver.recv().await.unwrap()
    }
}

impl UnboundedMpsc for AsyncChannel {
    type Receiver = async_channel::Receiver<usize>;
    type Sender = async_channel::Sender<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        async_channel::unbounded()
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.try_send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv(), context).unwrap()
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).unwrap()
    }
}

impl AsyncUnboundedMpsc for AsyncChannel {
    async fn recv_async(receiver: &mut Self::Receiver) -> usize {
        receiver.recv().await.unwrap()
    }
}

impl UnboundedMpsc for Crossbeam {
    type Receiver = crossbeam_channel::Receiver<usize>;
    type Sender = crossbeam_channel::Sender<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        crossbeam_channel::unbounded()
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn recv_ready(receiver: &mut Self::Receiver, _context: &mut Context<'_>) -> usize {
        receiver.try_recv().unwrap()
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        receiver.recv().unwrap()
    }
}

impl UnboundedMpsc for Flume {
    type Receiver = flume::Receiver<usize>;
    type Sender = flume::Sender<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        flume::unbounded()
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> usize {
        receiver.try_recv().unwrap()
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> usize {
        poll_ready(receiver.recv_async(), context).unwrap()
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> usize {
        pollster::block_on(receiver.recv_async()).unwrap()
    }
}

impl AsyncUnboundedMpsc for Flume {
    async fn recv_async(receiver: &mut Self::Receiver) -> usize {
        receiver.recv_async().await.unwrap()
    }
}
