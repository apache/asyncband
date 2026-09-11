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

use std::fmt::Debug;
use std::future::Future;
use std::task::Context;

use asyncband::blocking::FutureExt;

use crate::support::poll_ready;

pub struct Asyncband;
pub struct Tokio;
pub struct AsyncChannel;
pub struct Flume;

pub trait BoundedMpsc<T = usize>: Send + Sync + 'static {
    type Sender: Clone + Send + Sync + 'static;
    type Receiver: Send + 'static;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver);
    fn try_send(sender: &Self::Sender, value: T);
    fn try_recv(receiver: &mut Self::Receiver) -> T;
    fn send_ready(sender: &Self::Sender, value: T, context: &mut Context<'_>);
    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> T;
    fn send_async(sender: &Self::Sender, value: T) -> impl Future<Output = ()> + Send;
    fn recv_async(receiver: &mut Self::Receiver) -> impl Future<Output = T> + Send;
    fn send_blocking(sender: &Self::Sender, value: T);
    fn recv_blocking(receiver: &mut Self::Receiver) -> T;
}

pub trait UnboundedMpsc<T = usize>: Send + Sync + 'static {
    type Sender: Clone + Send + Sync + 'static;
    type Receiver: Send + 'static;

    fn channel() -> (Self::Sender, Self::Receiver);
    fn send(sender: &Self::Sender, value: T);
    fn try_recv(receiver: &mut Self::Receiver) -> T;
    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> T;
    fn recv_async(receiver: &mut Self::Receiver) -> impl Future<Output = T> + Send;
    fn recv_blocking(receiver: &mut Self::Receiver) -> T;
}

impl<T: Debug + Send + 'static> BoundedMpsc<T> for Asyncband {
    type Receiver = asyncband::mpsc::BoundedReceiver<T>;
    type Sender = asyncband::mpsc::BoundedSender<T>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        asyncband::mpsc::bounded(capacity)
    }

    fn try_send(sender: &Self::Sender, value: T) {
        sender.try_send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> T {
        receiver.try_recv().unwrap()
    }

    fn send_ready(sender: &Self::Sender, value: T, context: &mut Context<'_>) {
        poll_ready(sender.send(value), context).unwrap();
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> T {
        poll_ready(receiver.recv(), context).unwrap()
    }

    async fn send_async(sender: &Self::Sender, value: T) {
        sender.send(value).await.unwrap();
    }

    async fn recv_async(receiver: &mut Self::Receiver) -> T {
        receiver.recv().await.unwrap()
    }

    fn send_blocking(sender: &Self::Sender, value: T) {
        FutureExt::block_on(sender.send(value)).unwrap();
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> T {
        FutureExt::block_on(receiver.recv()).unwrap()
    }
}

impl<T: Debug + Send + 'static> BoundedMpsc<T> for Tokio {
    type Receiver = tokio::sync::mpsc::Receiver<T>;
    type Sender = tokio::sync::mpsc::Sender<T>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        tokio::sync::mpsc::channel(capacity)
    }

    fn try_send(sender: &Self::Sender, value: T) {
        sender.try_send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> T {
        receiver.try_recv().unwrap()
    }

    fn send_ready(sender: &Self::Sender, value: T, context: &mut Context<'_>) {
        poll_ready(sender.send(value), context).unwrap();
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> T {
        poll_ready(receiver.recv(), context).unwrap()
    }

    async fn send_async(sender: &Self::Sender, value: T) {
        sender.send(value).await.unwrap();
    }

    async fn recv_async(receiver: &mut Self::Receiver) -> T {
        receiver.recv().await.unwrap()
    }

    fn send_blocking(sender: &Self::Sender, value: T) {
        FutureExt::block_on(sender.send(value)).unwrap();
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> T {
        FutureExt::block_on(receiver.recv()).unwrap()
    }
}

impl<T: Debug + Send + 'static> BoundedMpsc<T> for AsyncChannel {
    type Receiver = async_channel::Receiver<T>;
    type Sender = async_channel::Sender<T>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        async_channel::bounded(capacity)
    }

    fn try_send(sender: &Self::Sender, value: T) {
        sender.try_send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> T {
        receiver.try_recv().unwrap()
    }

    fn send_ready(sender: &Self::Sender, value: T, context: &mut Context<'_>) {
        poll_ready(sender.send(value), context).unwrap();
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> T {
        poll_ready(receiver.recv(), context).unwrap()
    }

    async fn send_async(sender: &Self::Sender, value: T) {
        sender.send(value).await.unwrap();
    }

    async fn recv_async(receiver: &mut Self::Receiver) -> T {
        receiver.recv().await.unwrap()
    }

    fn send_blocking(sender: &Self::Sender, value: T) {
        FutureExt::block_on(sender.send(value)).unwrap();
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> T {
        FutureExt::block_on(receiver.recv()).unwrap()
    }
}

impl<T: Debug + Send + 'static> BoundedMpsc<T> for Flume {
    type Receiver = flume::Receiver<T>;
    type Sender = flume::Sender<T>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        flume::bounded(capacity)
    }

    fn try_send(sender: &Self::Sender, value: T) {
        sender.try_send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> T {
        receiver.try_recv().unwrap()
    }

    fn send_ready(sender: &Self::Sender, value: T, context: &mut Context<'_>) {
        poll_ready(sender.send_async(value), context).unwrap();
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> T {
        poll_ready(receiver.recv_async(), context).unwrap()
    }

    async fn send_async(sender: &Self::Sender, value: T) {
        sender.send_async(value).await.unwrap();
    }

    async fn recv_async(receiver: &mut Self::Receiver) -> T {
        receiver.recv_async().await.unwrap()
    }

    fn send_blocking(sender: &Self::Sender, value: T) {
        FutureExt::block_on(sender.send_async(value)).unwrap();
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> T {
        FutureExt::block_on(receiver.recv_async()).unwrap()
    }
}

impl<T: Debug + Send + 'static> UnboundedMpsc<T> for Asyncband {
    type Receiver = asyncband::mpsc::UnboundedReceiver<T>;
    type Sender = asyncband::mpsc::UnboundedSender<T>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        asyncband::mpsc::unbounded()
    }

    fn send(sender: &Self::Sender, value: T) {
        sender.send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> T {
        receiver.try_recv().unwrap()
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> T {
        poll_ready(receiver.recv(), context).unwrap()
    }

    async fn recv_async(receiver: &mut Self::Receiver) -> T {
        receiver.recv().await.unwrap()
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> T {
        FutureExt::block_on(receiver.recv()).unwrap()
    }
}

impl<T: Debug + Send + 'static> UnboundedMpsc<T> for Tokio {
    type Receiver = tokio::sync::mpsc::UnboundedReceiver<T>;
    type Sender = tokio::sync::mpsc::UnboundedSender<T>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        tokio::sync::mpsc::unbounded_channel()
    }

    fn send(sender: &Self::Sender, value: T) {
        sender.send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> T {
        receiver.try_recv().unwrap()
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> T {
        poll_ready(receiver.recv(), context).unwrap()
    }

    async fn recv_async(receiver: &mut Self::Receiver) -> T {
        receiver.recv().await.unwrap()
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> T {
        FutureExt::block_on(receiver.recv()).unwrap()
    }
}

impl<T: Debug + Send + 'static> UnboundedMpsc<T> for AsyncChannel {
    type Receiver = async_channel::Receiver<T>;
    type Sender = async_channel::Sender<T>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        async_channel::unbounded()
    }

    fn send(sender: &Self::Sender, value: T) {
        sender.try_send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> T {
        receiver.try_recv().unwrap()
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> T {
        poll_ready(receiver.recv(), context).unwrap()
    }

    async fn recv_async(receiver: &mut Self::Receiver) -> T {
        receiver.recv().await.unwrap()
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> T {
        FutureExt::block_on(receiver.recv()).unwrap()
    }
}

impl<T: Debug + Send + 'static> UnboundedMpsc<T> for Flume {
    type Receiver = flume::Receiver<T>;
    type Sender = flume::Sender<T>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        flume::unbounded()
    }

    fn send(sender: &Self::Sender, value: T) {
        sender.send(value).unwrap();
    }

    fn try_recv(receiver: &mut Self::Receiver) -> T {
        receiver.try_recv().unwrap()
    }

    fn recv_ready(receiver: &mut Self::Receiver, context: &mut Context<'_>) -> T {
        poll_ready(receiver.recv_async(), context).unwrap()
    }

    async fn recv_async(receiver: &mut Self::Receiver) -> T {
        receiver.recv_async().await.unwrap()
    }

    fn recv_blocking(receiver: &mut Self::Receiver) -> T {
        FutureExt::block_on(receiver.recv_async()).unwrap()
    }
}
