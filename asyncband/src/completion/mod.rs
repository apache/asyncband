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
//! A single-use [`Completer`] publishes one value, while any number of cloned [`Completion`]
//! observers wait for that same value. Observers created after completion see it immediately.
//! If the completer is dropped without publishing a value, every observer returns [`Abandoned`].
//! The stored value is returned by reference, so callers decide whether to borrow it, clone it, or
//! use an [`Arc`]-wrapped value when they need independently owned shared results.
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
//! let (completer, completion) = completion::new();
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

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;
use std::task::Context;
use std::task::Poll;

use crate::internal::mutex::Mutex;
use crate::internal::waitset::WaitSet;
use crate::internal::waitset::WakerToken;
use crate::internal::waitset::wake_all;

/// Creates a single-use [`Completer`] and a cloneable [`Completion`] observer.
pub fn new<T>() -> (Completer<T>, Completion<T>) {
    let shared = Arc::new(Shared {
        value: OnceLock::new(),
        state: Mutex::new(State {
            status: Status::Pending,
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
    waiters: WaitSet,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Status {
    Pending,
    Completed,
    Abandoned,
}

/// The error returned by [`Completion::wait`] when the completer was dropped without a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Abandoned;

impl fmt::Display for Abandoned {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("completion was abandoned before a value was provided")
    }
}

impl std::error::Error for Abandoned {}

/// The capability that completes a [`Completion`] with one value.
///
/// This type deliberately does not implement [`Clone`], and [`complete`](Self::complete) consumes
/// it. Dropping it before completion abandons the primitive and wakes all pending observers.
#[must_use = "dropping the completer abandons the completion"]
pub struct Completer<T> {
    shared: Weak<Shared<T>>,
}

// SAFETY: The completer can only move an owned `T` into the shared `OnceLock` while holding the
// state mutex; it never exposes or accesses the stored value afterward. `Completion<T>` retains its
// ordinary auto traits, so observers cannot cross threads unless `T` can be shared. `T: Send` also
// permits the shared allocation and its value to be destroyed by the completing thread if its
// temporary strong reference is the last one.
unsafe impl<T: Send> Send for Completer<T> {}
unsafe impl<T: Send> Sync for Completer<T> {}

impl<T> fmt::Debug for Completer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Completer").finish_non_exhaustive()
    }
}

impl<T> Completer<T> {
    /// Completes the primitive with `value` and wakes all pending observers.
    ///
    /// Returns `value` if all observers were already dropped. A successful completion does not
    /// guarantee that an observer will remain alive long enough to read the value.
    ///
    /// # Panics
    ///
    /// Panics if a registered waker panics while being notified. The value is committed before
    /// notification begins. Before resuming the panic, `complete` still attempts to wake every
    /// remaining registered waker.
    pub fn complete(mut self, value: T) -> Result<(), T> {
        let Some(shared) = self.shared.upgrade() else {
            return Err(value);
        };
        let wakers = {
            let mut state = shared.state.lock();
            assert_eq!(
                state.status,
                Status::Pending,
                "a live completer must refer to a pending completion"
            );

            if let Err(value) = shared.value.set(value) {
                drop(state);
                drop(value);
                panic!("pending completion value must be unset");
            }
            state.status = Status::Completed;
            state.waiters.drain()
        };
        // `complete` consumes the only completer. Disarm its destructor before invoking arbitrary
        // wake callbacks; the completed state no longer needs abandonment handling.
        self.shared = Weak::new();
        wake_all(wakers);
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
            state.status = Status::Abandoned;
            state.waiters.drain()
        };
        wake_all(wakers);
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

impl<T> Completion<T> {
    /// Waits for the shared value and returns a reference to it.
    ///
    /// Returns [`Abandoned`] if the completer is dropped before providing a value. Abandonment
    /// remains distinct from any error stored inside `T`.
    ///
    /// This method is cancel safe. Dropping one pending wait unregisters only that call and does
    /// not affect this observer, another wait, or the eventual result.
    pub async fn wait(&self) -> Result<&T, Abandoned> {
        Wait {
            completion: self,
            token: None,
        }
        .await
    }
}

struct Wait<'a, T> {
    completion: &'a Completion<T>,
    token: Option<WakerToken>,
}

impl<'a, T> Future for Wait<'a, T> {
    type Output = Result<&'a T, Abandoned>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut prepared_waker = None;
        loop {
            let (poll, retired_waker) = {
                let mut state = this.completion.shared.state.lock();
                match state.status {
                    Status::Pending => {
                        if prepared_waker.is_none()
                            && state.waiters.will_wake(&this.token, cx.waker())
                        {
                            return Poll::Pending;
                        }
                        let Some(waker) = prepared_waker.take() else {
                            drop(state);
                            prepared_waker = Some(cx.waker().clone());
                            continue;
                        };
                        let retired = state.waiters.register(&mut this.token, waker);
                        (Poll::Pending, retired)
                    }
                    Status::Completed => {
                        let retired = state.waiters.unregister(&mut this.token);
                        let completion: &'a Completion<T> = this.completion;
                        let value = completion
                            .shared
                            .value
                            .get()
                            .expect("completed value must be initialized");
                        (Poll::Ready(Ok(value)), retired)
                    }
                    Status::Abandoned => {
                        let retired = state.waiters.unregister(&mut this.token);
                        (Poll::Ready(Err(Abandoned)), retired)
                    }
                }
            };
            drop(retired_waker);
            drop(prepared_waker);
            return poll;
        }
    }
}

impl<T> Drop for Wait<'_, T> {
    fn drop(&mut self) {
        if self.token.is_none() {
            return;
        }

        let waker = {
            let mut state = self.completion.shared.state.lock();
            state.waiters.unregister(&mut self.token)
        };
        drop(waker);
    }
}
