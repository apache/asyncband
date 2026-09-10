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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Waker;

use self::queue::Queue;
use crate::internal::mutex::Mutex;

mod queue;
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
/// Sending and receiving may briefly wait for an in-progress producer or an internal mutex, but
/// never wait for capacity or new messages in `send` or `try_recv`. No lock is held across an
/// await point or while invoking waker callbacks or message destructors.
pub fn unbounded<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let (queue, consumer) = Queue::new();
    let shared = Arc::new(State {
        queue,
        senders: CachePadded(AtomicUsize::new(1)),
        recv: ReceiverWake {
            waiting: CachePadded(AtomicBool::new(false)),
            waker: Mutex::new(None),
        },
    });
    (
        UnboundedSender::new(shared.clone()),
        UnboundedReceiver::new(shared, consumer),
    )
}

// Separate producer reservations, sender counts, and the receiver's mostly-read waiting flag.
#[cfg_attr(
    any(
        target_arch = "aarch64",
        target_arch = "arm64ec",
        target_arch = "x86_64",
        target_arch = "powerpc64"
    ),
    repr(align(128))
)]
#[cfg_attr(target_arch = "s390x", repr(align(256)))]
#[cfg_attr(
    not(any(
        target_arch = "aarch64",
        target_arch = "arm64ec",
        target_arch = "x86_64",
        target_arch = "powerpc64",
        target_arch = "s390x"
    )),
    repr(align(64))
)]
struct CachePadded<T>(T);

struct State<T> {
    queue: Queue<T>,
    senders: CachePadded<AtomicUsize>,
    recv: ReceiverWake,
}

struct ReceiverWake {
    waiting: CachePadded<AtomicBool>,
    waker: Mutex<Option<Waker>>,
}

impl ReceiverWake {
    fn register(&self, waker: &Waker) {
        // Clone, replacement destruction, and wake may reenter this channel.
        let waker = waker.clone();
        let mut slot = self.waker.lock();
        let old = slot.replace(waker);
        // Publication and this flag share an SC order: after register -> recheck, either the
        // receiver sees the value or its producer observes this registration and wakes it.
        self.waiting.0.store(true, Ordering::SeqCst);
        drop(slot);
        drop(old);
    }

    fn take(&self) -> Option<Waker> {
        let mut slot = self.waker.lock();
        let waker = slot.take();
        // Clear under the registration lock so a delayed notifier cannot erase a newer wait.
        self.waiting.0.store(false, Ordering::SeqCst);
        waker
    }

    fn wake(&self) {
        if !self.waiting.0.load(Ordering::SeqCst)
            || self
                .waiting
                .0
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return;
        }
        if let Some(waker) = self.take() {
            waker.wake();
        }
    }
}
