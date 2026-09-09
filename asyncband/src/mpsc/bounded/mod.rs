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

use std::collections::VecDeque;
use std::sync::Arc;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::waitlist::WaitList;
use crate::mpsc::TryRecvError;
use crate::mpsc::TrySendError;

mod receiver;
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
/// Message storage is preallocated for `buffer` values. Queued messages and outstanding
/// reservations together occupy at most `buffer` capacity units.
///
/// Operations briefly acquire an internal mutex; no lock is held across an await point or while
/// invoking waker callbacks or message destructors. The `try_*` methods do not wait for capacity
/// or messages, but may wait to acquire this mutex.
///
/// # Panics
///
/// Panics if `buffer` is zero or exceeds the maximum capacity of `usize::MAX >> 1`.
#[track_caller]
pub fn bounded<T>(buffer: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    const MAX_CAPACITY: usize = usize::MAX >> 1;

    assert!(
        buffer > 0,
        "mpsc bounded channel capacity {buffer} must be nonzero",
    );
    assert!(
        buffer <= MAX_CAPACITY,
        "mpsc bounded channel capacity {buffer} exceeds the maximum of {MAX_CAPACITY}",
    );

    let shared = Arc::new(Mutex::new(State {
        queue: VecDeque::with_capacity(buffer),
        available: buffer,
        senders: 1,
        receiver_open: true,
        receiver_waker: None,
        waiters: WaitList::new(),
    }));
    (
        BoundedSender::new(shared.clone()),
        BoundedReceiver::new(shared),
    )
}

// While open, capacity belongs to available, a queued message, a Permit, or a granted waiter.
// All transitions hold one mutex. Waker callbacks and message destruction run after unlocking.
struct State<T> {
    queue: VecDeque<T>,
    available: usize,
    senders: usize,
    receiver_open: bool,
    receiver_waker: Option<Waker>,
    waiters: WaitList<Waiter>,
}

impl<T> State<T> {
    fn acquire(&mut self) -> Result<(), TrySendError<()>> {
        if !self.receiver_open {
            Err(TrySendError::Disconnected(()))
        } else if self.available == 0 {
            Err(TrySendError::Full(()))
        } else {
            self.available -= 1;
            Ok(())
        }
    }

    fn release(&mut self) -> Option<Waker> {
        if !self.receiver_open {
            return None;
        }
        if let Some((_, waiter)) = self.waiters.unlink_first_waiter(|_| true) {
            // The detached node owns capacity until its future claims or cancels the grant.
            waiter.granted = true;
            return waiter.waker.take();
        }
        self.available += 1;
        None
    }

    fn pop(&mut self) -> Result<(T, Option<Waker>), TryRecvError> {
        if let Some(value) = self.queue.pop_front() {
            Ok((value, self.release()))
        } else if self.senders == 0 {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }
}

struct Waiter {
    granted: bool,
    waker: Option<Waker>,
}
