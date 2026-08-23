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

//! A channel that retains and distributes the latest state.
//!
//! Every receiver tracks the last version it observed. Intermediate updates are coalesced, so a
//! slow receiver sees the newest value rather than a backlog of every update.
//!
//! The receiver returned by [`channel`] considers the initial value observed. [`Sender::subscribe`]
//! likewise starts at the current version, while cloning a receiver preserves that receiver's
//! observed version. An unseen final update remains available after every sender disconnects.
//! Dropping a pending [`Receiver::changed`] future does not mark an update observed.
//!
//! ```
//! use asyncband::watch;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let (tx, mut rx) = watch::channel(0);
//! tx.send(1).unwrap();
//! tx.send(2).unwrap();
//!
//! assert_eq!(*rx.changed().await.unwrap(), 2);
//! assert_eq!(rx.has_changed(), Ok(false));
//! # }
//! ```

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

pub use super::error::RecvError;
pub use super::error::SendError;
use crate::internal::mutex::Mutex;
use crate::internal::waitset::WaitSet;
use crate::internal::waitset::WakerToken;

/// Creates a watch channel with an initial value.
pub fn channel<T>(initial: T) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            value: Arc::new(initial),
            version: 0,
            senders: 1,
            receivers: 1,
            waiters: WaitSet::new(),
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
    waiters: WaitSet,
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
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let wakers = {
            let mut state = self.shared.state.lock();
            state.senders -= 1;
            (state.senders == 0).then(|| state.waiters.take_wakers())
        };
        if let Some(wakers) = wakers {
            for waker in wakers {
                waker.wake();
            }
        }
    }
}

impl<T> Sender<T> {
    /// Publishes a new current value.
    ///
    /// Returns the value if no receivers remain.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let (wakers, replaced) = {
            let mut state = self.shared.state.lock();
            if state.receivers == 0 {
                return Err(SendError::new(value));
            }
            let version = state
                .version
                .checked_add(1)
                .expect("watch version overflow");
            let replaced = std::mem::replace(&mut state.value, Arc::new(value));
            state.version = version;
            let wakers = (!state.waiters.is_empty()).then(|| state.waiters.take_wakers());
            (wakers, replaced)
        };
        if let Some(wakers) = wakers {
            for waker in wakers {
                waker.wake();
            }
        }
        drop(replaced);
        Ok(())
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

    /// Returns the number of active receivers.
    pub fn receiver_count(&self) -> usize {
        self.shared.state.lock().receivers
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
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.state.lock().receivers -= 1;
    }
}

impl<T> Receiver<T> {
    /// Returns the current value without marking its version observed.
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

    /// Waits for a newer version and returns the latest value.
    pub async fn changed(&mut self) -> Result<Arc<T>, RecvError> {
        Changed {
            receiver: self,
            registration: None,
        }
        .await
    }

    /// Returns whether every sender has been dropped.
    pub fn is_disconnected(&self) -> bool {
        self.shared.state.lock().senders == 0
    }
}

struct Changed<'a, T> {
    receiver: &'a mut Receiver<T>,
    registration: Option<WakerToken>,
}

impl<T> Future for Changed<'_, T> {
    type Output = Result<Arc<T>, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let (poll, retired_waker) = {
            let mut state = this.receiver.shared.state.lock();
            if state.version != this.receiver.seen {
                let retired_waker = state.waiters.unregister_waker(&mut this.registration);
                this.receiver.seen = state.version;
                (Poll::Ready(Ok(state.value.clone())), retired_waker)
            } else if state.senders == 0 {
                let retired_waker = state.waiters.unregister_waker(&mut this.registration);
                (Poll::Ready(Err(RecvError::Disconnected)), retired_waker)
            } else {
                let retired_waker = state.waiters.register_waker(&mut this.registration, cx);
                (Poll::Pending, retired_waker)
            }
        };
        drop(retired_waker);
        poll
    }
}

impl<T> Drop for Changed<'_, T> {
    fn drop(&mut self) {
        if self.registration.is_none() {
            return;
        }
        let waker = {
            let mut state = self.receiver.shared.state.lock();
            state.waiters.unregister_waker(&mut self.registration)
        };
        drop(waker);
    }
}
