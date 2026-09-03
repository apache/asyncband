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

pub struct Asyncband;
pub struct AsyncChannel;
pub struct Flume;

pub trait BoundedMpmc: Send + Sync + 'static {
    type Sender: Clone + Send + 'static;
    type Receiver: Clone + Send + 'static;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver);
    fn send(sender: &Self::Sender, value: usize);
    fn recv(receiver: &Self::Receiver) -> usize;
}

pub trait UnboundedMpmc: Send + Sync + 'static {
    type Sender: Clone + Send + 'static;
    type Receiver: Clone + Send + 'static;

    fn channel() -> (Self::Sender, Self::Receiver);
    fn send(sender: &Self::Sender, value: usize);
    fn recv(receiver: &Self::Receiver) -> usize;
}

impl BoundedMpmc for Asyncband {
    type Receiver = asyncband::mpmc::BoundedReceiver<usize>;
    type Sender = asyncband::mpmc::BoundedSender<usize>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        asyncband::mpmc::bounded(capacity)
    }

    fn send(sender: &Self::Sender, value: usize) {
        pollster::block_on(sender.send(value)).expect("benchmark sender disconnected");
    }

    fn recv(receiver: &Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).expect("benchmark receiver disconnected")
    }
}

impl BoundedMpmc for AsyncChannel {
    type Receiver = async_channel::Receiver<usize>;
    type Sender = async_channel::Sender<usize>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        async_channel::bounded(capacity)
    }

    fn send(sender: &Self::Sender, value: usize) {
        pollster::block_on(sender.send(value)).expect("benchmark sender disconnected");
    }

    fn recv(receiver: &Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).expect("benchmark receiver disconnected")
    }
}

impl BoundedMpmc for Flume {
    type Receiver = flume::Receiver<usize>;
    type Sender = flume::Sender<usize>;

    fn channel(capacity: usize) -> (Self::Sender, Self::Receiver) {
        flume::bounded(capacity)
    }

    fn send(sender: &Self::Sender, value: usize) {
        pollster::block_on(sender.send_async(value)).expect("benchmark sender disconnected");
    }

    fn recv(receiver: &Self::Receiver) -> usize {
        pollster::block_on(receiver.recv_async()).expect("benchmark receiver disconnected")
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

    fn recv(receiver: &Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).expect("benchmark receiver disconnected")
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

    fn recv(receiver: &Self::Receiver) -> usize {
        pollster::block_on(receiver.recv()).expect("benchmark receiver disconnected")
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

    fn recv(receiver: &Self::Receiver) -> usize {
        pollster::block_on(receiver.recv_async()).expect("benchmark receiver disconnected")
    }
}
