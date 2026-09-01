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
use std::pin::Pin;

pub struct Asyncband;
pub struct Tokio;

pub trait Watch: Send + Sync + 'static {
    type Sender: Send + 'static;
    type Receiver: Send + 'static;

    fn channel(receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>);
    fn send(sender: &Self::Sender, value: usize);
    fn get(receiver: &Self::Receiver) -> usize;
    fn recv(receiver: &mut Self::Receiver) -> Pin<Box<dyn Future<Output = usize> + '_>>;
    fn changed(receiver: &mut Self::Receiver) -> Pin<Box<dyn Future<Output = ()> + '_>>;
}

impl Watch for Asyncband {
    type Sender = asyncband::watch::Sender<usize>;
    type Receiver = asyncband::watch::Receiver<usize>;

    fn channel(receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>) {
        let (sender, first) = asyncband::watch::channel(0);
        let mut receivers = Vec::with_capacity(receiver_count);
        receivers.push(first);
        receivers.extend((1..receiver_count).map(|_| sender.subscribe()));
        (sender, receivers)
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.send(value).unwrap();
    }

    fn get(receiver: &Self::Receiver) -> usize {
        receiver.get()
    }

    fn recv(receiver: &mut Self::Receiver) -> Pin<Box<dyn Future<Output = usize> + '_>> {
        Box::pin(async move { receiver.recv().await.unwrap() })
    }

    fn changed(receiver: &mut Self::Receiver) -> Pin<Box<dyn Future<Output = ()> + '_>> {
        Box::pin(async move { receiver.changed().await.unwrap() })
    }
}

impl Watch for Tokio {
    type Sender = tokio::sync::watch::Sender<usize>;
    type Receiver = tokio::sync::watch::Receiver<usize>;

    fn channel(receiver_count: usize) -> (Self::Sender, Vec<Self::Receiver>) {
        let (sender, first) = tokio::sync::watch::channel(0);
        let mut receivers = Vec::with_capacity(receiver_count);
        receivers.push(first);
        receivers.extend((1..receiver_count).map(|_| sender.subscribe()));
        (sender, receivers)
    }

    fn send(sender: &Self::Sender, value: usize) {
        sender.send(value).unwrap();
    }

    fn get(receiver: &Self::Receiver) -> usize {
        *receiver.borrow()
    }

    fn recv(receiver: &mut Self::Receiver) -> Pin<Box<dyn Future<Output = usize> + '_>> {
        Box::pin(async move {
            receiver.changed().await.unwrap();
            *receiver.borrow_and_update()
        })
    }

    fn changed(receiver: &mut Self::Receiver) -> Pin<Box<dyn Future<Output = ()> + '_>> {
        Box::pin(async move { receiver.changed().await.unwrap() })
    }
}
