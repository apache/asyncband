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

pub use crate::mpmc_support::adapters::AsyncChannel;
pub use crate::mpmc_support::adapters::Asyncband as Mpmc;
use crate::mpmc_support::adapters::BoundedMpmc;
pub use crate::mpmc_support::adapters::Flume;
use crate::mpmc_support::adapters::UnboundedMpmc;
use crate::mpmc_support::support::BOUNDED_CAPACITY;

pub struct Spmc;
pub struct Bounded<C>(PhantomData<C>);
pub struct Unbounded<C>(PhantomData<C>);

// The harness moves the only sender into one task, without adding Clone or Sync requirements.
pub trait Channel: Send + Sync + 'static {
    type Sender: Send + 'static;
    type Receiver: Clone + Send + Sync + 'static;

    fn channel() -> (Self::Sender, Self::Receiver);
    fn send(sender: &mut Self::Sender, value: usize) -> impl Future<Output = ()> + Send;
    fn recv(receiver: &Self::Receiver) -> impl Future<Output = Option<usize>> + Send;
}

impl Channel for Bounded<Spmc> {
    type Sender = asyncband::spmc::BoundedSender<usize>;
    type Receiver = asyncband::spmc::BoundedReceiver<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        asyncband::spmc::bounded(BOUNDED_CAPACITY)
    }

    async fn send(sender: &mut Self::Sender, value: usize) {
        sender.send(value).await.unwrap();
    }

    async fn recv(receiver: &Self::Receiver) -> Option<usize> {
        receiver.recv().await.ok()
    }
}

impl Channel for Unbounded<Spmc> {
    type Sender = asyncband::spmc::UnboundedSender<usize>;
    type Receiver = asyncband::spmc::UnboundedReceiver<usize>;

    fn channel() -> (Self::Sender, Self::Receiver) {
        asyncband::spmc::unbounded()
    }

    async fn send(sender: &mut Self::Sender, value: usize) {
        sender.send(value).unwrap();
    }

    async fn recv(receiver: &Self::Receiver) -> Option<usize> {
        receiver.recv().await.ok()
    }
}

impl<C: BoundedMpmc> Channel for Bounded<C> {
    type Sender = C::Sender;
    type Receiver = C::Receiver;

    fn channel() -> (Self::Sender, Self::Receiver) {
        C::channel(BOUNDED_CAPACITY)
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
