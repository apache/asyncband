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
use crate::internal::atomic_waker::AtomicWaker;
use crate::internal::cache_padded::CachePadded;

mod buffer;
mod receiver;
mod semaphore;
mod sender;

pub use self::receiver::BoundedReceiver;
pub use self::sender::BoundedSender;
pub use self::sender::Permit;

/// Creates a bounded mpsc channel with room for `buffer` queued messages.
///
/// [`BoundedSender::send`] waits for capacity when the buffer is full. Receiving a message releases
/// one slot for a waiting sender. Capacity is granted in the order that pending sends and
/// reservations enter the wait queue; new senders cannot take an already granted slot.
///
/// Message slots are preallocated and rounded up to a power of two; the channel's capacity
/// remains exactly `buffer`. Every slot needs state metadata, including for zero-sized messages.
///
/// # Panics
///
/// Panics if `buffer` is zero or exceeds the maximum capacity of `usize::MAX >> 1`.
#[track_caller]
pub fn bounded<T>(buffer: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    /// The largest capacity accepted by [`bounded`].
    ///
    /// The shared permit counter reserves two sentinel values above the usable range. This
    /// bound also keeps the rounded-up slot storage from overflowing a power of two.
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
    (
        BoundedSender::new(shared.clone()),
        BoundedReceiver::new(shared),
    )
}

struct Shared<T> {
    senders: AtomicUsize,
    tx_permits: CachePadded<Semaphore>,
    rx_waker: AtomicWaker,
    buffer: Buffer<T>,
}
