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

//! A shared one-shot completion primitive.
//!
//! A [`Completer`] publishes one value, while any number of cloned [`Completion`] observers wait
//! for that same value. Observers created after completion see it immediately. The stored value is
//! returned by reference, so callers decide whether to borrow it, clone it, or use an [`Arc`]-
//! wrapped value when they need independently owned shared results.
//!
//! Unlike `oneshot`, which transfers one value to one receiver, completion can fan one result out
//! to many current and future observers without creating and managing one channel per observer.
//! Unlike `OnceCell`, initialization is controlled only by the distinct completer capability;
//! observers can only wait.
//!
//! # Examples
//!
//! ```
//! use asyncband::completion;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let (completer, completion) = completion::channel();
//! let first = completion.clone();
//! let second = completion.clone();
//!
//! completer.complete(String::from("ready")).unwrap();
//!
//! assert_eq!(first.wait().await.unwrap(), "ready");
//! assert_eq!(second.wait().await.unwrap(), "ready");
//! let late = completion.clone();
//! assert_eq!(late.wait().await.unwrap(), "ready");
//! # }
//! ```

mod error;

#[cfg(test)]
mod tests;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;
use std::task::Context;
use std::task::Poll;

pub use self::error::CompleteError;
pub use self::error::WaitError;
use crate::internal::mutex::Mutex;
use crate::internal::waitset::WaitSet;
use crate::internal::waitset::WakerToken;
use crate::internal::waitset::wake_all;

/// Creates a shared one-shot completion primitive.
pub fn channel<T>() -> (Completer<T>, Completion<T>) {
    let shared = Arc::new(Shared {
        value: OnceLock::new(),
        state: Mutex::new(State {
            status: Status::Pending,
            observers: 1,
            waiters: WaitSet::new(),
        }),
    });
    let completer = Completer {
        shared: Arc::downgrade(&shared),
    };
    let completion = Completion { shared };
    (completer, completion)
}

struct Shared<T> {
    value: OnceLock<T>,
    state: Mutex<State>,
}

struct State {
    status: Status,
    observers: usize,
    waiters: WaitSet,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Status {
    Pending,
    Completed,
    Closed,
}

/// The capability that completes a [`Completion`] with one value.
///
/// This type deliberately does not implement [`Clone`]. Dropping it before completion closes the
/// primitive and wakes all pending observers.
pub struct Completer<T> {
    shared: Weak<Shared<T>>,
}

impl<T> fmt::Debug for Completer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Completer").finish_non_exhaustive()
    }
}

impl<T> Completer<T> {
    /// Completes the primitive with `value` and wakes all pending observers.
    ///
    /// The value is rejected and returned if another value has already completed the primitive or
    /// if no observers remain.
    ///
    /// # Panics
    ///
    /// Panics if a registered waker panics while being notified. The value is committed before
    /// notification begins, so subsequent completion attempts are rejected. Before resuming the
    /// panic, `complete` still attempts to wake every remaining registered waker.
    pub fn complete(&self, value: T) -> Result<(), CompleteError<T>> {
        let Some(shared) = self.shared.upgrade() else {
            return Err(CompleteError::new(value));
        };
        let wakers = {
            let mut state = shared.state.lock();
            if state.status != Status::Pending || state.observers == 0 {
                return Err(CompleteError::new(value));
            }

            if let Err(value) = shared.value.set(value) {
                drop(state);
                drop(value);
                panic!("pending completion value must be unset");
            }
            state.status = Status::Completed;
            (!state.waiters.is_empty()).then(|| state.waiters.take_wakers())
        };
        if let Some(wakers) = wakers {
            wake_all(wakers);
        }
        Ok(())
    }
}

impl<T> Drop for Completer<T> {
    fn drop(&mut self) {
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        let wakers = {
            let mut state = shared.state.lock();
            if state.status != Status::Pending {
                return;
            }
            state.status = Status::Closed;
            (!state.waiters.is_empty()).then(|| state.waiters.take_wakers())
        };
        if let Some(wakers) = wakers {
            wake_all(wakers);
        }
    }
}

/// An observer of a shared one-shot completion.
///
/// Cloning this type creates another observer of the same eventual value. Each call to [`wait`]
/// registers independently and can be cancelled without affecting other observers.
///
/// [`wait`]: Completion::wait
pub struct Completion<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Completion<T> {
    fn clone(&self) -> Self {
        let mut state = self.shared.state.lock();
        state.observers = state
            .observers
            .checked_add(1)
            .expect("completion observer count overflowed");
        drop(state);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> fmt::Debug for Completion<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Completion").finish_non_exhaustive()
    }
}

impl<T> Drop for Completion<T> {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock();
        state.observers -= 1;
    }
}

impl<T> Completion<T> {
    /// Waits for the shared value and returns a reference to it.
    ///
    /// Returns [`WaitError::Closed`] if the completer is dropped before providing a value. This
    /// transport-level closure remains distinct from any error stored inside `T`.
    ///
    /// This method is cancel safe. Dropping one pending wait unregisters only that call and does
    /// not affect this observer, another wait, or the eventual result.
    pub async fn wait(&self) -> Result<&T, WaitError> {
        Wait {
            completion: self,
            registration: None,
        }
        .await
    }
}

struct Wait<'a, T> {
    completion: &'a Completion<T>,
    registration: Option<WakerToken>,
}

impl<'a, T> Future for Wait<'a, T> {
    type Output = Result<&'a T, WaitError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let (poll, retired_waker) = {
            let mut state = this.completion.shared.state.lock();
            match state.status {
                Status::Pending => {
                    let retired = state.waiters.register_waker(&mut this.registration, cx);
                    (Poll::Pending, retired)
                }
                Status::Completed => {
                    let retired = state.waiters.unregister_waker(&mut this.registration);
                    let completion: &'a Completion<T> = this.completion;
                    let value = completion
                        .shared
                        .value
                        .get()
                        .expect("completed value must be initialized");
                    (Poll::Ready(Ok(value)), retired)
                }
                Status::Closed => {
                    let retired = state.waiters.unregister_waker(&mut this.registration);
                    (Poll::Ready(Err(WaitError::Closed)), retired)
                }
            }
        };
        drop(retired_waker);
        poll
    }
}

impl<T> Drop for Wait<'_, T> {
    fn drop(&mut self) {
        let waker = {
            let mut state = self.completion.shared.state.lock();
            state.waiters.unregister_waker(&mut self.registration)
        };
        drop(waker);
    }
}
