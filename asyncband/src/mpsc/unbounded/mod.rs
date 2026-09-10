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

//! An unbounded multi-producer, single-consumer queue for sending values between asynchronous
//! tasks.

use std::sync::Arc;
use std::task::Waker;

use self::buffer::Buffer;
use crate::internal::mutex::Mutex;

mod buffer;
mod receiver;
mod sender;

pub use self::receiver::UnboundedReceiver;
pub use self::sender::UnboundedSender;

/// Creates an unbounded mpsc channel whose send operation never waits for capacity.
///
/// Pending messages can grow with producer demand and are limited only by successful memory
/// allocation. Use a [`bounded`](crate::mpsc::bounded) channel or external admission control when
/// producers may outpace the receiver.
///
/// Messages are received in the order they were sent. After the last sender is dropped, the
/// receiver drains queued messages before reporting disconnection.
///
/// Storage is reclaimed incrementally as messages are received. A bounded amount of empty
/// storage may be retained for reuse, independently of the channel's previous peak occupancy.
///
/// Operations briefly acquire an internal mutex; no lock is held across an await point or while
/// invoking waker callbacks or message destructors. Sending and trying to receive may wait to
/// acquire this mutex, but never wait for capacity or new messages.
pub fn unbounded<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let shared = Arc::new(Mutex::new(State {
        buffer: Buffer::new(),
        senders: 1,
        receiver: true,
        recv_waker: None,
    }));
    (
        UnboundedSender::new(shared.clone()),
        UnboundedReceiver::new(shared),
    )
}

// Queue contents, endpoint liveness, and wake registration share one lock. Only the receiver
// accesses its current batch; refilling that batch preserves the order of concurrent sends.
struct State<T> {
    buffer: Buffer<T>,
    senders: usize,
    // True while the receiving endpoint is alive.
    receiver: bool,
    recv_waker: Option<Waker>,
}
