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

//! Single-producer, multi-consumer queues.
//!
//! The sender is non-cloneable, non-Sync, and requires mutable access. Receivers are cloneable and
//! compete for values, so each value is delivered to exactly one receiver.

use std::num::NonZeroUsize;

pub use super::FullBehavior;
pub use super::RecvError;
pub use super::SendError;
pub use super::SendOutcome;
pub use super::TryRecvError;
pub use super::TrySendError;
use super::queue;

/// The sending endpoint of an SPMC queue.
pub type Sender<T> = queue::Sender<T, queue::Single, queue::Multiple>;

/// A receiving endpoint of an SPMC queue.
pub type Receiver<T> = queue::Receiver<T, queue::Single, queue::Multiple>;

/// Creates an unbuffered SPMC rendezvous channel.
pub fn rendezvous<T>() -> (Sender<T>, Receiver<T>) {
    queue::channel(queue::QueueKind::Rendezvous)
}

/// Creates a bounded SPMC queue.
pub fn bounded<T>(capacity: NonZeroUsize) -> (Sender<T>, Receiver<T>) {
    queue::channel(queue::QueueKind::Bounded(capacity.get()))
}

/// Creates an unbounded SPMC queue.
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    queue::channel(queue::QueueKind::Unbounded)
}
