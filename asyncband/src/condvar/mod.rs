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

//! A condition variable that allows tasks to wait for a notification.
//!
//! A condition variable is normally paired with a predicate protected by a [`Mutex`]. The predicate
//! records the state of the application; notifications only wake tasks that may need to check that
//! state again. Notifications are not buffered, so calling [`Condvar::notify_one`] or
//! [`Condvar::notify_all`] when no task is waiting has no effect.
//!
//! [`Mutex`]: mutex::Mutex
//!
//! Always check the predicate while holding the mutex and wait in a loop. [`Condvar::wait`]
//! registers the task before releasing the mutex, so a notifier that updates the predicate under
//! the same mutex cannot race with the transition into the wait state. [`Condvar::wait_while`]
//! expresses this pattern directly.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use std::sync::Arc;
//!
//! use asyncband::condvar::Condvar;
//! use asyncband::mutex::Mutex;
//!
//! let pair = Arc::new((Mutex::new(false), Condvar::new()));
//! let notifier_pair = pair.clone();
//!
//! let notifier = tokio::spawn(async move {
//!     let (lock, cvar) = &*notifier_pair;
//!     let mut ready = lock.lock().await;
//!     *ready = true;
//!     cvar.notify_one();
//! });
//!
//! let (lock, cvar) = &*pair;
//! let ready = cvar.wait_while(lock.lock().await, |ready| !*ready).await;
//! assert!(*ready);
//! drop(ready);
//! notifier.await.unwrap();
//! # }
//! ```

use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::wake_all;
use crate::internal::waker_batch::WakerBatch;
use crate::mutex;
use crate::mutex::MutexGuard;
use crate::mutex::OwnedMutexGuard;

/// A condition variable that allows tasks to wait for a notification.
///
/// See the [module level documentation](self) for more.
pub struct Condvar {
    waiters: Mutex<WaitList<WaitNode>>,
}

#[derive(Debug)]
struct WaitNode {
    state: WaitState,
}

#[derive(Debug)]
enum WaitState {
    Waiting(Waker),
    NotifiedOne,
    NotifiedAll,
}

fn notify_one_locked(waiters: &mut WaitList<WaitNode>) -> Option<Waker> {
    let mut waker = None;
    waiters.unlink_first_waiter(|node| {
        let WaitState::Waiting(waiting) = mem::replace(&mut node.state, WaitState::NotifiedOne)
        else {
            unreachable!("only waiting tasks remain linked")
        };
        waker = Some(waiting);
        true
    });
    waker
}

impl fmt::Debug for Condvar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Condvar").finish_non_exhaustive()
    }
}

impl Default for Condvar {
    fn default() -> Self {
        Self::new()
    }
}

impl Condvar {
    /// Creates a new condition variable
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::condvar::Condvar;
    ///
    /// let cvar = Condvar::new();
    /// ```
    pub const fn new() -> Condvar {
        Condvar {
            waiters: Mutex::new(WaitList::new()),
        }
    }

    /// Wakes up one task currently blocked on this condition variable.
    ///
    /// If no task is currently waiting, this call has no effect. Notifications are not buffered for
    /// future calls to [`wait`](Self::wait) or [`wait_owned`](Self::wait_owned).
    ///
    /// If the selected task is cancelled before its wait completes, the notification is passed to
    /// another task that is waiting at that point, if one exists.
    pub fn notify_one(&self) {
        let waker = {
            let mut waiters = self.waiters.lock();
            notify_one_locked(&mut waiters)
        };

        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Wakes up all tasks currently blocked on this condition variable.
    ///
    /// If no task is currently waiting, this call has no effect. Notifications are not buffered for
    /// future calls to [`wait`](Self::wait) or [`wait_owned`](Self::wait_owned).
    pub fn notify_all(&self) {
        let wakers = {
            let mut waiters = self.waiters.lock();
            let mut wakers = WakerBatch::new();

            while waiters
                .unlink_first_waiter(|node| {
                    let WaitState::Waiting(waker) =
                        mem::replace(&mut node.state, WaitState::NotifiedAll)
                    else {
                        unreachable!("only waiting tasks remain linked")
                    };
                    wakers.push(waker);
                    true
                })
                .is_some()
            {}

            wakers
        };

        wake_all(wakers.into_iter());
    }

    /// Waits for a notification, atomically releasing and then reacquiring the mutex.
    ///
    /// The task is registered with this condition variable before the mutex is released. When this
    /// function returns, the mutex has been reacquired. The associated predicate must be checked
    /// again after every return; prefer [`wait_while`](Self::wait_while) when possible.
    ///
    /// Unlike the standard library equivalent, this function does not check at runtime that the
    /// same mutex is always used with this condition variable.
    ///
    /// # Cancel safety
    ///
    /// Cancelling this wait removes the task from the wait queue. If the task was selected by
    /// [`notify_one`](Self::notify_one) but has not yet reacquired the mutex, the notification is
    /// passed to another task that is waiting at that point, if one exists. It is never buffered
    /// for a future waiter.
    pub async fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        let mutex = mutex::guard_lock(&guard);
        let notify_one_baton = Wait {
            condvar: self,
            guard: Some(guard),
            index: None,
        }
        .await;
        let guard = mutex.lock().await;
        if let Some(baton) = notify_one_baton {
            baton.complete();
        }
        guard
    }

    /// Waits for a notification, atomically releasing and then reacquiring the owned mutex.
    ///
    /// This has the same notification and cancellation semantics as [`wait`](Self::wait), but
    /// accepts and returns an owned guard.
    pub async fn wait_owned<T>(&self, guard: OwnedMutexGuard<T>) -> OwnedMutexGuard<T> {
        let mutex = mutex::owned_guard_lock(&guard);
        let notify_one_baton = Wait {
            condvar: self,
            guard: Some(guard),
            index: None,
        }
        .await;
        let guard = mutex.lock_owned().await;
        if let Some(baton) = notify_one_baton {
            baton.complete();
        }
        guard
    }

    /// Yields the current task until this condition variable receives a notification and the
    /// provided condition becomes false. Spurious wake-ups are ignored and this function will only
    /// return once the condition has been met.
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::condvar::Condvar;
    /// use asyncband::mutex::Mutex;
    ///
    /// let pair = Arc::new((Mutex::new(false), Condvar::new()));
    /// let pair_clone = pair.clone();
    ///
    /// let notifier = tokio::spawn(async move {
    ///     let (lock, cvar) = &*pair_clone;
    ///     let mut started = lock.lock().await;
    ///     *started = true;
    ///     cvar.notify_one();
    /// });
    ///
    /// let (lock, cvar) = &*pair;
    /// let guard = cvar
    ///     .wait_while(lock.lock().await, |started| !*started)
    ///     .await;
    /// assert!(*guard);
    /// drop(guard);
    /// notifier.await.unwrap();
    /// # }
    /// ```
    pub async fn wait_while<'a, T, F>(
        &self,
        mut guard: MutexGuard<'a, T>,
        mut condition: F,
    ) -> MutexGuard<'a, T>
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut *guard) {
            guard = self.wait(guard).await;
        }
        guard
    }

    /// Yields the current task until this condition variable receives a notification and the
    /// provided condition becomes false. Spurious wake-ups are ignored and this function will only
    /// return once the condition has been met.
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::condvar::Condvar;
    /// use asyncband::mutex::Mutex;
    ///
    /// let pair = (Arc::new(Mutex::new(false)), Arc::new(Condvar::new()));
    /// let pair_clone = pair.clone();
    ///
    /// let notifier = tokio::spawn(async move {
    ///     let (lock, cvar) = pair_clone;
    ///     let mut started = lock.lock_owned().await;
    ///     *started = true;
    ///     cvar.notify_one();
    /// });
    ///
    /// let (lock, cvar) = pair;
    /// let guard = cvar
    ///     .wait_while_owned(lock.lock_owned().await, |started| !*started)
    ///     .await;
    /// assert!(*guard);
    /// drop(guard);
    /// notifier.await.unwrap();
    /// # }
    /// ```
    pub async fn wait_while_owned<T, F>(
        &self,
        mut guard: OwnedMutexGuard<T>,
        mut condition: F,
    ) -> OwnedMutexGuard<T>
    where
        F: FnMut(&mut T) -> bool,
    {
        while condition(&mut *guard) {
            guard = self.wait_owned(guard).await;
        }
        guard
    }
}

struct Wait<'a, G> {
    condvar: &'a Condvar,
    guard: Option<G>,
    index: Option<WaiterId>,
}

impl<'a, G> Future for Wait<'a, G>
where
    G: Unpin,
{
    type Output = Option<NotifyOneBaton<'a>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if this.guard.is_some() {
            let mut waiters = this.condvar.waiters.lock();
            this.index = Some(waiters.push_back(WaitNode {
                state: WaitState::Waiting(cx.waker().clone()),
            }));
            let guard = this.guard.take().unwrap();

            // Registration must happen before unlocking the associated mutex. A notifier that
            // acquires the mutex after this point will therefore observe this waiter.
            drop(waiters);
            drop(guard);
            return Poll::Pending;
        }

        let index = this.index.expect("wait future polled after completion");
        let mut waiters = this.condvar.waiters.lock();
        let mut old_waker = None;
        let notify_one_baton = match &mut waiters.waiter_mut(index).state {
            WaitState::Waiting(waker) => {
                if !waker.will_wake(cx.waker()) {
                    old_waker = Some(mem::replace(waker, cx.waker().clone()));
                }
                drop(waiters);
                drop(old_waker);
                return Poll::Pending;
            }
            WaitState::NotifiedOne => Some(NotifyOneBaton::new(this.condvar)),
            WaitState::NotifiedAll => None,
        };

        let waiter = waiters.remove_unlinked_waiter(index);
        this.index = None;
        drop(waiters);
        drop(waiter);
        Poll::Ready(notify_one_baton)
    }
}

impl<G> Drop for Wait<'_, G> {
    fn drop(&mut self) {
        let Some(index) = self.index.take() else {
            return;
        };

        let (waiter, waker) = {
            let mut waiters = self.condvar.waiters.lock();
            let mut pass_notification = false;
            waiters.unlink_waiter(index, |node| match &node.state {
                WaitState::Waiting(_) => true,
                WaitState::NotifiedOne => {
                    pass_notification = true;
                    false
                }
                WaitState::NotifiedAll => false,
            });
            let waiter = waiters.remove_unlinked_waiter(index);

            let waker = if pass_notification {
                notify_one_locked(&mut waiters)
            } else {
                None
            };
            (waiter, waker)
        };

        drop(waiter);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Passes a selected notification onward if the wait is cancelled while reacquiring its mutex.
struct NotifyOneBaton<'a> {
    condvar: Option<&'a Condvar>,
}

impl<'a> NotifyOneBaton<'a> {
    fn new(condvar: &'a Condvar) -> Self {
        Self {
            condvar: Some(condvar),
        }
    }

    fn complete(mut self) {
        self.condvar = None;
    }
}

impl Drop for NotifyOneBaton<'_> {
    fn drop(&mut self) {
        if let Some(condvar) = self.condvar {
            condvar.notify_one();
        }
    }
}
