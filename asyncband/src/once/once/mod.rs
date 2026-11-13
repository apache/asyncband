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

use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use crate::internal::countdown::CountdownState;
use crate::internal::wakerset::WakerToken;
use crate::semaphore::Semaphore;

/// Coordinates a one-time asynchronous initialization step.
///
/// Unlike [`std::sync::Once`], this type waits asynchronously rather than blocking a thread. The
/// provided closure produces a future, and successful completion prevents later calls from running
/// another initializer.
///
/// This type also intentionally omits "poisoning" semantics. If an initialization future is
/// cancelled or panics, the attempt is abandoned and other tasks may retry the operation.
/// Retrying does not undo side effects from the abandoned attempt.
///
/// [`call_once`](Self::call_once) accepts only initializers returning `()`. For fallible
/// initialization, enable the `once-cell` feature and use `OnceCell::get_or_try_init`, which leaves
/// the cell empty on error. Use `OnceCell<()>` when no initialized value needs to be stored.
///
/// See the [module level documentation](super) for additional context.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use std::sync::atomic::AtomicUsize;
/// use std::sync::atomic::Ordering;
///
/// use asyncband::once::Once;
///
/// let once = Once::new();
/// let calls = AtomicUsize::new(0);
///
/// tokio::join!(
///     once.call_once(async || {
///         calls.fetch_add(1, Ordering::SeqCst);
///     }),
///     once.call_once(async || {
///         calls.fetch_add(1, Ordering::SeqCst);
///     }),
/// );
///
/// assert!(once.is_completed());
/// assert_eq!(calls.load(Ordering::SeqCst), 1);
/// # }
/// ```
pub struct Once {
    done: CountdownState,
    semaphore: Semaphore,
}

impl Default for Once {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Once {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let done = self.is_completed();
        f.debug_struct("Once").field("done", &done).finish()
    }
}

impl Once {
    /// Creates a new `Once` instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::once::Once;
    ///
    /// static ONCE: Once = Once::new();
    /// ```
    pub const fn new() -> Self {
        Self {
            done: CountdownState::new(1),
            semaphore: Semaphore::new(1),
        }
    }

    /// Reports whether an initializer passed to [`call_once`](Self::call_once) has completed.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::Once;
    ///
    /// static ONCE: Once = Once::new();
    ///
    /// assert!(!ONCE.is_completed());
    ///
    /// ONCE.call_once(async || {}).await;
    ///
    /// assert!(ONCE.is_completed());
    /// # }
    /// ```
    pub fn is_completed(&self) -> bool {
        self.done.try_wait().is_ok()
    }

    /// Runs `f` if initialization has not completed.
    ///
    /// One caller runs its initializer while others wait. After an initializer completes normally,
    /// all current and future calls return without running another initializer.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending call that is waiting does not interrupt the active initializer. Dropping
    /// the call running `f` cancels that attempt and allows a waiting or later caller to run its
    /// own initializer. Effects already performed by `f` are not rolled back.
    ///
    /// # Panics
    ///
    /// A panic in `f` propagates to its caller. Initialization remains incomplete and may be
    /// retried; this primitive is not poisoned.
    ///
    /// # Deadlocks
    ///
    /// An initializer must not await `call_once` or `wait` on the same `Once`, because completion
    /// depends on that initializer returning.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::atomic::AtomicUsize;
    /// use std::sync::atomic::Ordering;
    ///
    /// use asyncband::once::Once;
    ///
    /// let once = Once::new();
    /// let state = AtomicUsize::new(0);
    ///
    /// once.call_once(async || {
    ///     state.store(1, Ordering::Relaxed);
    /// })
    /// .await;
    /// once.call_once(async || {
    ///     state.store(2, Ordering::Relaxed);
    /// })
    /// .await;
    ///
    /// assert_eq!(state.load(Ordering::Relaxed), 1);
    /// # }
    /// ```
    pub async fn call_once<F>(&self, f: F)
    where
        F: AsyncFnOnce(),
    {
        if self.is_completed() {
            return;
        }

        let _permit = self.semaphore.acquire(1).await;

        if self.is_completed() {
            // double-checked: another task completed the initialization while we waited.
            return;
        }

        f().await;

        // `decrement` retries spurious failures from the weak CAS.
        if !self.done.decrement(1) {
            unreachable!("[BUG] Once completed more than once");
        }

        self.done.wake_all();
    }

    /// Waits until an initializer passed to [`call_once`](Self::call_once) completes successfully.
    ///
    /// This call does not start initialization.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending future unregisters only this wait. It does not cancel initialization,
    /// change the completed state, or affect other waiters.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::Once;
    ///
    /// static ONCE: Once = Once::new();
    ///
    /// let handle = tokio::spawn(async {
    ///     ONCE.wait().await;
    /// });
    ///
    /// ONCE.call_once(async || {
    ///     println!("initialized");
    /// })
    /// .await;
    ///
    /// handle.await.unwrap();
    /// # }
    /// ```
    pub async fn wait(&self) {
        if self.is_completed() {
            return;
        }

        OnceWait {
            token: None,
            once: self,
        }
        .await
    }
}

struct OnceWait<'a> {
    token: Option<WakerToken>,
    once: &'a Once,
}

impl fmt::Debug for OnceWait<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OnceWait").finish_non_exhaustive()
    }
}

impl Future for OnceWait<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self { token, once } = self.get_mut();
        once.done.poll_wait(token, cx)
    }
}

impl Drop for OnceWait<'_> {
    fn drop(&mut self) {
        self.once.done.unregister(&mut self.token);
    }
}
