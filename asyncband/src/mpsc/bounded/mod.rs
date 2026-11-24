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

//! Bounded multi-producer, single-consumer channel with capacity backpressure.

use std::num::NonZeroUsize;

use crate::mpsc::bounded::queue::Shared;

mod queue;
mod receiver;
mod sender;

pub use self::receiver::BoundedReceiver;
pub use self::sender::BoundedSender;
pub use self::sender::Permit;

/// Creates a bounded mpsc channel with room for `buffer` queued messages.
///
/// [`BoundedSender::send`] waits for capacity when the buffer is full. Receiving a message releases
/// one slot for a waiting sender. Capacity is granted in the order that pending `send` and
/// `reserve` operations enter the wait queue; new senders cannot take an already granted slot.
///
/// Message storage is preallocated for `buffer` values. Queued messages and outstanding
/// reservations together occupy at most `buffer` capacity units.
///
/// The `try_*` methods do not wait for capacity or messages, but may briefly synchronize with a
/// concurrent operation.
///
/// # Panics
///
/// Panics if `buffer` is zero or the preallocated message storage exceeds the allocation size
/// limit.
#[track_caller]
pub fn bounded<T>(buffer: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    let Some(capacity) = NonZeroUsize::new(buffer) else {
        panic!("mpsc bounded channel capacity {buffer} must be nonzero");
    };
    let shared = Shared::new(capacity);
    (
        BoundedSender::new(shared.clone()),
        BoundedReceiver::new(shared),
    )
}

#[cfg(doctest)]
mod compile_fail_tests {
    /// ```compile_fail
    /// use asyncband::mpsc;
    ///
    /// fn assert_unpin<T: Unpin>(_: T) {}
    ///
    /// let (tx, _rx) = mpsc::bounded(1);
    /// assert_unpin(tx.send(1));
    /// ```
    struct SendFutureIsNotUnpin;

    /// ```compile_fail
    /// use asyncband::mpsc;
    ///
    /// fn assert_unpin<T: Unpin>(_: T) {}
    ///
    /// let (tx, _rx) = mpsc::bounded::<usize>(1);
    /// assert_unpin(tx.reserve());
    /// ```
    struct ReserveFutureIsNotUnpin;

    /// ```compile_fail
    /// use asyncband::mpsc;
    ///
    /// fn assert_unpin<T: Unpin>(_: T) {}
    ///
    /// let (_tx, mut rx) = mpsc::bounded::<usize>(1);
    /// assert_unpin(rx.recv());
    /// ```
    struct RecvFutureIsNotUnpin;
}
