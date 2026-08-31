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
//! [`Receiver::get`] returns a clone of the current value without marking its version observed.
//! [`Receiver::recv`] waits for an unseen version, then returns an owning clone and marks that
//! version observed. Callers can use an [`Arc`] as the watched value when cloning the underlying
//! state would be expensive.
//!
//! # Publication model
//!
//! Both [`Sender`] and [`Receiver`] are cloneable. Successful publications from every sender are
//! serialized into one channel-wide order, and the last publication in that order becomes the
//! current state. The order of concurrent calls is unspecified, so callers that need semantic
//! writer priority or ordering should coordinate before publishing.
//!
//! This makes a watch channel a last-publication-wins state register, not an event log. A receiver
//! may coalesce any number of intermediate publications and only observe the latest state. Use a
//! queue or broadcast channel when every update must be processed.
//!
//! If all senders are dropped after publishing a final unseen value, each receiver can still
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
//! assert_eq!(rx.recv().await.unwrap(), 2);
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
/// assert_eq!(rx.get(), "ready");
/// ```
pub fn channel<T: Clone>(initial: T) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            value: initial,
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
    value: T,
    version: u64,
    senders: usize,
    receivers: usize,
    waiters: WaitSet,
}

/// The sending side of a watch channel.
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
        // Only the final sender detaches the parked receivers; their wake callbacks run unlocked.
        let wakers = {
            let mut state = self.shared.state.lock();
            state.senders -= 1;
            if state.senders != 0 {
                return;
            }
            state.waiters.drain()
        };
        wake_all(wakers);
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
            let replaced = mem::replace(&mut state.value, value);
            state.version = version;
            let wakers = state.waiters.drain();
            (wakers, replaced)
        };
        // Waker callbacks and the replaced value's destructor may reenter this channel.
        wake_all(wakers);
        drop(replaced);
        Ok(())
    }

    /// Publishes a new current value and returns the previous value.
    ///
    /// Unlike [`Sender::send`], this method updates the retained value even when no receivers
    /// exist. Every call creates a new version and notifies all current receivers.
    ///
    /// # Panics
    ///
    /// Panics if the internal version counter overflows.
    pub fn send_replace(&self, value: T) -> T {
        let (wakers, replaced) = {
            let mut state = self.shared.state.lock();
            let version = state
                .version
                .checked_add(1)
                .expect("watch channel version counter overflowed");
            let replaced = mem::replace(&mut state.value, value);
            state.version = version;
            let wakers = state.waiters.drain();
            (wakers, replaced)
        };
        wake_all(wakers);
        replaced
    }

    /// Creates a receiver that considers the current value already observed.
    #[must_use = "the receiver is dropped immediately if it is not retained"]
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

/// A receiver for a watch channel.
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
    /// Returns a clone of the current value without marking its version observed.
    ///
    /// The clone is created while publication is locked. Keep `T::clone` inexpensive and avoid
    /// reentering this channel from the clone implementation. Use `T = Arc<U>` when the underlying
    /// state is expensive to clone.
    pub fn get(&self) -> T
    where
        T: Clone,
    {
        self.shared.state.lock().value.clone()
    }

    /// Returns whether a version newer than the last observed version exists.
    ///
    /// An unseen final version is reported before disconnection, even if all senders have been
    /// dropped.
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

    /// Waits for a newer version and marks the latest version observed.
    ///
    /// Intermediate updates may be coalesced. This method is cancel safe: until it returns, no
    /// version is marked observed by the call. Use [`Receiver::recv`] instead when the value is
    /// needed.
    pub async fn changed(&mut self) -> Result<(), RecvError> {
        Changed {
            receiver: self,
            token: None,
        }
        .await
    }

    /// Waits for a newer version, returns a clone of its latest value, and marks it observed.
    ///
    /// Intermediate updates may be coalesced. The returned value and the receiver's observed
    /// version are updated together, so a concurrent publication cannot cause the returned value
    /// to be received twice. This method is cancel safe: until it returns, no version is marked
    /// observed by the call.
    ///
    /// The clone is created while publication is locked. Keep `T::clone` inexpensive and avoid
    /// reentering this channel from the clone implementation. Use `T = Arc<U>` when the underlying
    /// state is expensive to clone.
    pub async fn recv(&mut self) -> Result<T, RecvError>
    where
        T: Clone,
    {
        self.changed().await?;
        let state = self.shared.state.lock();
        let value = state.value.clone();
        self.seen = state.version;
        Ok(value)
    }

    /// Returns whether all senders have been dropped.
    ///
    /// This does not mark the current version observed, so it may return `true` while a final
    /// unseen value is still available through [`Receiver::changed`].
    pub fn is_disconnected(&self) -> bool {
        self.shared.state.lock().senders == 0
    }
}

struct Changed<'a, T> {
    receiver: &'a mut Receiver<T>,
    token: Option<WakerToken>,
}

impl<T> Future for Changed<'_, T> {
    type Output = Result<(), RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let (poll, retired_waker) = {
            let mut state = this.receiver.shared.state.lock();
            if state.version != this.receiver.seen {
                let retired = state.waiters.unregister(&mut this.token);
                this.receiver.seen = state.version;
                (Poll::Ready(Ok(())), retired)
            } else if state.senders == 0 {
                let retired = state.waiters.unregister(&mut this.token);
                (Poll::Ready(Err(RecvError::Disconnected)), retired)
            } else {
                let retired = state
                    .waiters
                    .register_waker(&mut this.token, cx.waker());
                (Poll::Pending, retired)
            }
        };
        drop(retired_waker);
        poll
    }
}

impl<T> Drop for Changed<'_, T> {
    fn drop(&mut self) {
        if self.token.is_none() {
            return;
        }

        let waker = {
            let mut state = self.receiver.shared.state.lock();
            state.waiters.unregister(&mut self.token)
        };
        drop(waker);
    }
}
