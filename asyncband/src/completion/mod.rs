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
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use crate::internal::mutex::Mutex;
use crate::internal::wake_all;
use crate::internal::wakerset::WakerSet;
use crate::internal::wakerset::WakerToken;

/// Creates a single-use [`Completer`] and a cloneable [`Completion`] observer.
pub fn new<T>() -> (Completer<T>, Completion<T>) {
    let shared = Arc::new(Shared {
        value: OnceLock::new(),
        status: AtomicU8::new(Status::Pending as u8),
        waiters: Mutex::new(WakerSet::new()),
    });
    let completer = Completer {
        shared: Arc::downgrade(&shared),
    };
    let completion = Completion { shared };
    (completer, completion)
}

struct Shared<T> {
    value: OnceLock<T>,
    status: AtomicU8,
    waiters: Mutex<WakerSet>,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Status {
    Pending,
    Completed,
    Abandoned,
}

impl Status {
    fn load(status: &AtomicU8) -> Self {
        match status.load(Ordering::Acquire) {
            value if value == Self::Pending as u8 => Self::Pending,
            value if value == Self::Completed as u8 => Self::Completed,
            value if value == Self::Abandoned as u8 => Self::Abandoned,
            _ => unreachable!("completion status must be valid"),
        }
    }
}

/// The error returned by [`Completion::wait`] when the completer was dropped without a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Abandoned(());

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
    /// Panics if notifying a waiting observer panics. The value remains committed, and notification
    /// is still attempted for every other pending observer before the panic resumes.
    pub fn complete(mut self, value: T) -> Result<(), T> {
        let Some(shared) = self.shared.upgrade() else {
            return Err(value);
        };
        let wakers = {
            let mut waiters = shared.waiters.lock();
            assert_eq!(
                Status::load(&shared.status),
                Status::Pending,
                "a live completer must refer to a pending completion"
            );

            if let Err(value) = shared.value.set(value) {
                drop(waiters);
                drop(value);
                panic!("pending completion value must be unset");
            }
            let wakers = waiters.take_all();
            // Release publishes both the value and the detached waiter cohort to lock-free polls.
            shared
                .status
                .store(Status::Completed as u8, Ordering::Release);
            wakers
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
            let mut waiters = shared.waiters.lock();
            if Status::load(&shared.status) != Status::Pending {
                return;
            }
            let wakers = waiters.take_all();
            shared
                .status
                .store(Status::Abandoned as u8, Ordering::Release);
            wakers
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
        match Status::load(&this.completion.shared.status) {
            Status::Completed => {
                this.token = None;
                return Poll::Ready(Ok(this
                    .completion
                    .shared
                    .value
                    .get()
                    .expect("completed value must be initialized")));
            }
            Status::Abandoned => {
                this.token = None;
                return Poll::Ready(Err(Abandoned(())));
            }
            Status::Pending => {}
        }

        // Cloning a RawWaker can execute arbitrary user code, so do it before taking the lock.
        let waker = cx.waker().clone();
        let mut waiters = this.completion.shared.waiters.lock();
        let (poll, retired_waker) = match Status::load(&this.completion.shared.status) {
            Status::Pending => {
                let retired = waiters.register_owned(&mut this.token, waker);
                (Poll::Pending, retired)
            }
            Status::Completed => {
                this.token = None;
                let completion: &'a Completion<T> = this.completion;
                let value = completion
                    .shared
                    .value
                    .get()
                    .expect("completed value must be initialized");
                (Poll::Ready(Ok(value)), Some(waker))
            }
            Status::Abandoned => {
                this.token = None;
                (Poll::Ready(Err(Abandoned(()))), Some(waker))
            }
        };
        drop(waiters);
        drop(retired_waker);
        poll
    }
}

impl<T> Drop for Wait<'_, T> {
    fn drop(&mut self) {
        if self.token.is_none() {
            return;
        }

        if Status::load(&self.completion.shared.status) != Status::Pending {
            self.token = None;
            return;
        }

        let mut waiters = self.completion.shared.waiters.lock();
        if Status::load(&self.completion.shared.status) != Status::Pending {
            self.token = None;
            return;
        }

        let waker = waiters.unregister(&mut self.token);
        drop(waiters);
        drop(waker);
    }
}
