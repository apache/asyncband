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

//! A bounded multi-producer, single-consumer queue for sending values between asynchronous
//! tasks with backpressure control.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use self::buffer::Buffer;
use self::semaphore::Semaphore;
use super::RecvError;
use super::SendError;
use super::TryRecvError;
use super::TrySendError;
use crate::internal::atomic_waker::AtomicWaker;
use crate::internal::cache_padded::CachePadded;

mod buffer;
mod receiver;
mod semaphore;
mod sender;

/// Creates a bounded mpsc channel with room for `buffer` queued messages.
///
/// [`BoundedSender::send`] waits for capacity when the buffer is full. Receiving a message releases
/// one slot for a waiting sender. Capacity is granted in the order that pending sends and
/// reservations enter the wait queue; new senders cannot take an already granted slot.
///
/// Storage for nonzero-sized messages is preallocated and rounded up to a power of two; the
/// channel's capacity remains exactly `buffer`. Zero-sized messages need no per-slot storage.
///
/// # Panics
///
/// Panics if `buffer` is zero or exceeds the maximum capacity of `usize::MAX >> 1`.
#[track_caller]
pub fn bounded<T>(buffer: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    /// The largest capacity accepted by [`bounded`].
    ///
    /// The shared permit counter reserves two sentinel values above the usable range, and the
    /// zero-sized queue length packs a closed flag into its top bit. This bound also keeps the
    /// rounded-up slot storage from overflowing a power of two.
    const MAX_CAPACITY: usize = usize::MAX >> 1;

    assert!(
        buffer > 0,
        "mpsc bounded channel capacity {buffer} must be nonzero",
    );
    assert!(
        buffer <= MAX_CAPACITY,
        "mpsc bounded channel capacity {buffer} exceeds the maximum of {MAX_CAPACITY}",
    );

    let shared = Arc::new(Shared {
        senders: AtomicUsize::new(1),
        tx_permits: CachePadded::new(Semaphore::new(buffer)),
        rx_waker: AtomicWaker::new(),
        buffer: Buffer::new(buffer),
    });
    let sender = BoundedSender {
        shared: shared.clone(),
    };
    let receiver = BoundedReceiver { shared, head: 0 };
    (sender, receiver)
}

/// The sending endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`] function.
pub struct BoundedSender<T> {
    shared: Arc<Shared<T>>,
}

/// The receiving endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`] function. Dropping the receiver discards queued values.
/// The backing allocation remains alive until all endpoints are dropped, so a concurrent sender
/// can safely finish returning an unsent value.
pub struct BoundedReceiver<T> {
    shared: Arc<Shared<T>>,
    head: usize,
}

/// Capacity reserved for one message on a bounded channel.
///
/// Created by [`BoundedSender::reserve`] or [`BoundedSender::try_reserve`]. Holding a permit
/// reduces available capacity but does not prevent other messages from being received. Dropping
/// it without sending releases capacity and notifies a waiting sender.
#[must_use = "dropping the permit releases its reserved capacity"]
pub struct Permit<'a, T> {
    sender: Option<&'a BoundedSender<T>>,
}

struct Shared<T> {
    senders: AtomicUsize,
    tx_permits: CachePadded<Semaphore>,
    rx_waker: AtomicWaker,
    buffer: Buffer<T>,
}
