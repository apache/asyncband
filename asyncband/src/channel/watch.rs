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

//! A multi-producer, multi-consumer channel that retains only the latest value.
//!
//! Watch is a coalescing state channel rather than a queue. A slow receiver observes the latest
//! version and does not receive an error for skipped intermediate versions.
//!
//! ~~~
//! use asyncband::channel::watch;
//!
//! let (tx, mut rx) = watch::channel(0);
//! tx.send(1).unwrap();
//! assert_eq!(*pollster::block_on(rx.changed()).unwrap(), 1);
//! ~~~

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

pub use crate::channel::RecvError;
pub use crate::channel::SendError;
use crate::channel::wait::WaitQueue;
use crate::channel::wait::wake_all;
use crate::internal::mutex::Mutex;

/// Creates a watch channel with an initial value.
pub fn channel<T>(initial: T) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            value: Arc::new(initial),
            version: 0,
            senders: 1,
            receivers: 1,
            waiters: WaitQueue::default(),
        }),
    });
    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared, seen: 0 },
    )
}

struct Shared<T> {
    state: Mutex<State<T>>,
}

struct State<T> {
    value: Arc<T>,
    version: u64,
    senders: usize,
    receivers: usize,
    waiters: WaitQueue,
}

/// A sending endpoint of a watch channel.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let mut state = self.shared.state.lock();
        state.senders = state
            .senders
            .checked_add(1)
            .expect("watch sender count overflow");
        drop(state);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("disconnected", &self.is_disconnected())
            .finish()
    }
}

impl<T> Sender<T> {
    /// Publishes a new current value and returns the number of receivers.
    pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
        let (receivers, wakers, replaced) = {
            let mut state = self.shared.state.lock();
            if state.receivers == 0 {
                return Err(SendError::new(value));
            }
            let next_version = state
                .version
                .checked_add(1)
                .expect("watch version overflow");
            let replaced = std::mem::replace(&mut state.value, Arc::new(value));
            state.version = next_version;
            (state.receivers, state.waiters.take_all(), replaced)
        };
        wake_all(wakers);
        drop(replaced);
        Ok(receivers)
    }

    /// Creates a receiver that considers the current value already observed.
    pub fn subscribe(&self) -> Receiver<T> {
        let mut state = self.shared.state.lock();
        state.receivers = state
            .receivers
            .checked_add(1)
            .expect("watch receiver count overflow");
        let seen = state.version;
        drop(state);
        Receiver {
            shared: self.shared.clone(),
            seen,
        }
    }

    /// Returns true if no receivers remain.
    pub fn is_disconnected(&self) -> bool {
        self.shared.state.lock().receivers == 0
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let wakers = {
            let mut state = self.shared.state.lock();
            state.senders -= 1;
            if state.senders == 0 {
                state.waiters.take_all()
            } else {
                Vec::new()
            }
        };
        wake_all(wakers);
    }
}

/// A receiving endpoint of a watch channel.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    seen: u64,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        let mut state = self.shared.state.lock();
        state.receivers = state
            .receivers
            .checked_add(1)
            .expect("watch receiver count overflow");
        drop(state);
        Self {
            shared: self.shared.clone(),
            seen: self.seen,
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("seen", &self.seen)
            .field("disconnected", &self.is_disconnected())
            .finish()
    }
}

impl<T> Receiver<T> {
    /// Returns the current value without marking it observed.
    pub fn borrow(&self) -> Arc<T> {
        self.shared.state.lock().value.clone()
    }

    /// Returns the current value and marks its version observed.
    pub fn borrow_and_update(&mut self) -> Arc<T> {
        let state = self.shared.state.lock();
        self.seen = state.version;
        state.value.clone()
    }

    /// Returns whether a version newer than the last observed version exists.
    pub fn has_changed(&self) -> Result<bool, RecvError> {
        let state = self.shared.state.lock();
        if state.version != self.seen {
            Ok(true)
        } else if state.senders == 0 {
            Err(RecvError::Disconnected)
        } else {
            Ok(false)
        }
    }

    /// Waits for a new version and returns the latest value.
    pub async fn changed(&mut self) -> Result<Arc<T>, RecvError> {
        Changed {
            receiver: self,
            waiter: None,
            completed: false,
        }
        .await
    }

    /// Returns true if no senders remain.
    pub fn is_disconnected(&self) -> bool {
        self.shared.state.lock().senders == 0
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock();
        state.receivers -= 1;
    }
}

struct Changed<'a, T> {
    receiver: &'a mut Receiver<T>,
    waiter: Option<u64>,
    completed: bool,
}

impl<T> Future for Changed<'_, T> {
    type Output = Result<Arc<T>, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let mut state = this.receiver.shared.state.lock();
        let poll = if state.version != this.receiver.seen {
            let retired_waker = state.waiters.remove(&mut this.waiter);
            this.receiver.seen = state.version;
            let value = state.value.clone();
            drop(state);
            drop(retired_waker);
            Poll::Ready(Ok(value))
        } else if state.senders == 0 {
            let retired_waker = state.waiters.remove(&mut this.waiter);
            drop(state);
            drop(retired_waker);
            Poll::Ready(Err(RecvError::Disconnected))
        } else {
            let retired_waker = state.waiters.register(&mut this.waiter, cx.waker());
            drop(state);
            drop(retired_waker);
            Poll::Pending
        };
        if poll.is_ready() {
            this.completed = true;
        }
        poll
    }
}

impl<T> Drop for Changed<'_, T> {
    fn drop(&mut self) {
        if !self.completed {
            let retired_waker = {
                let mut state = self.receiver.shared.state.lock();
                state.waiters.remove(&mut self.waiter)
            };
            drop(retired_waker);
        }
    }
}
