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
//! Every receiver independently tracks whether it has observed the current value. Intermediate
//! updates may be coalesced, so a slow receiver observes the latest state rather than every update.
//! The receiver returned by [`channel`] considers the initial value observed, as does a receiver
//! created by [`Sender::subscribe`]. Cloning a receiver preserves the source receiver's observed
//! version and then tracks future observations independently.
//!
//! [`Receiver::borrow`] returns an owning [`Arc`] snapshot without marking the current version
//! observed. [`Receiver::borrow_and_update`] and [`Receiver::changed`] explicitly mark the returned
//! version observed. Because snapshots do not retain the channel's internal lock, they may be kept
//! or moved independently while senders continue publishing newer values.
//!
//! If all sender handles are dropped after publishing a final unseen value, each receiver can still
//! observe that value once before [`RecvError::Disconnected`] is reported.
//!
//! # Examples
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

mod error;

#[cfg(test)]
mod tests;

use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

pub use self::error::RecvError;
pub use self::error::SendError;
use crate::internal::mutex::Mutex;
use crate::internal::waitset::WaitSet;
use crate::internal::waitset::WakerToken;
use crate::internal::waitset::wake_all;

/// Creates a watch channel with an initial value.
///
/// The receiver returned by this function considers the initial value already observed.
///
/// # Examples
///
/// ```
/// use asyncband::watch;
///
/// let (_tx, rx) = watch::channel("ready");
/// assert_eq!(&*rx.borrow(), &"ready");
/// ```
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
    let sender = Sender {
        shared: shared.clone(),
    };
    let receiver = Receiver { shared, seen: 0 };
    (sender, receiver)
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
            .expect("watch sender count overflowed");
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
            (state.senders == 0 && !state.waiters.is_empty()).then(|| state.waiters.take_wakers())
        };
        if let Some(wakers) = wakers {
            wake_all(wakers);
        }
    }
}

impl<T> Sender<T> {
    /// Publishes a new current value.
    ///
    /// If no receivers remain, the value is returned and the retained value is left unchanged.
    ///
    /// # Panics
    ///
    /// Panics if the internal version counter overflows.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let (wakers, replaced) = {
            let mut state = self.shared.state.lock();
            if state.receivers == 0 {
                return Err(SendError::new(value));
            }
            let version = state
                .version
                .checked_add(1)
                .expect("watch channel version counter overflowed");
            let replaced = mem::replace(&mut state.value, Arc::new(value));
            state.version = version;
            let wakers = (!state.waiters.is_empty()).then(|| state.waiters.take_wakers());
            (wakers, replaced)
        };
        if let Some(wakers) = wakers {
            wake_all(wakers);
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
            .expect("watch receiver count overflowed");
        let seen = state.version;
        drop(state);
        Receiver {
            shared: self.shared.clone(),
            seen,
        }
    }
}

/// A receiving endpoint of a watch channel.
///
/// Each receiver independently tracks the latest version it has observed.
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
            .expect("watch receiver count overflowed");
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
        let mut state = self.shared.state.lock();
        state.receivers -= 1;
    }
}

impl<T> Receiver<T> {
    /// Returns a snapshot of the current value without marking it observed.
    pub fn borrow(&self) -> Arc<T> {
        self.shared.state.lock().value.clone()
    }

    /// Returns a snapshot of the current value and marks its version observed.
    pub fn borrow_and_update(&mut self) -> Arc<T> {
        let state = self.shared.state.lock();
        self.seen = state.version;
        state.value.clone()
    }

    /// Returns whether a version newer than the last observed version exists.
    ///
    /// An unseen final version is reported before disconnection, even if all sender handles have
    /// been dropped.
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

    /// Waits for a newer version and returns its latest snapshot.
    ///
    /// Intermediate updates may be coalesced. This method is cancel safe: until it returns, no
    /// version is marked observed by the call.
    pub async fn changed(&mut self) -> Result<Arc<T>, RecvError> {
        Changed {
            receiver: self,
            registration: None,
        }
        .await
    }

    /// Returns whether all sender handles have been dropped.
    ///
    /// This does not mark the current version observed, so it may return `true` while a final
    /// unseen value is still available through [`Receiver::changed`].
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
                let retired = state.waiters.unregister_waker(&mut this.registration);
                this.receiver.seen = state.version;
                (Poll::Ready(Ok(state.value.clone())), retired)
            } else if state.senders == 0 {
                let retired = state.waiters.unregister_waker(&mut this.registration);
                (Poll::Ready(Err(RecvError::Disconnected)), retired)
            } else {
                let retired = state.waiters.register_waker(&mut this.registration, cx);
                (Poll::Pending, retired)
            }
        };
        drop(retired_waker);
        poll
    }
}

impl<T> Drop for Changed<'_, T> {
    fn drop(&mut self) {
        let waker = {
            let mut state = self.receiver.shared.state.lock();
            state.waiters.unregister_waker(&mut self.registration)
        };
        drop(waker);
    }
}
