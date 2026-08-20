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
use crate::internal::waitset::WaitRegistration;
use crate::semaphore::Semaphore;

/// A synchronization primitive which can be used to run a one-time async initialization.
///
/// Unlike [`std::sync::Once`], this type never blocks a thread. The provided closure must
/// produce a future and the future is awaited inside the primitive. Coordination happens
/// with asynchronous [`Semaphore`], which keeps the implementation runtime-agnostic.
///
/// This type also intentionally omits "poisoning" semantics. If an initialization future is
/// cancelled or panics, the attempt is abandoned and other tasks may retry the operation.
/// Encode partial-initialization detection in the future itself (e.g. return a `Result`)
/// when needed.
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
/// static ONCE: Once = Once::new();
/// static COUNTER: AtomicUsize = AtomicUsize::new(0);
///
/// let handle1 = tokio::spawn(async {
///     ONCE.call_once(async || {
///         COUNTER.fetch_add(1, Ordering::SeqCst);
///     })
///     .await;
/// });
///
/// let handle2 = tokio::spawn(async {
///     ONCE.call_once(async || {
///         COUNTER.fetch_add(1, Ordering::SeqCst);
///     })
///     .await;
/// });
///
/// handle1.await.unwrap();
/// handle2.await.unwrap();
///
/// // The counter is incremented only once, even though two tasks called `call_once`.
/// assert_eq!(COUNTER.load(Ordering::SeqCst), 1);
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

    /// Returns `true` if some `call_once` has completed successfully.
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
        self.done.spin_wait(0).is_ok()
    }

    /// Calls the given async closure if this is the first time `call_once` has been called
    /// on this `Once` instance.
    ///
    /// If another task is currently running the closure, this call will wait for that task
    /// to complete.
    ///
    /// If the provided operation is cancelled, the initialization attempt is cancelled. If there
    /// are other tasks waiting, one of them will start another attempt.
    ///
    /// Calling call_once recursively on the same Once from within the closure will deadlock,
    /// because the closure holds the semaphore permit while trying to acquire it again.
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
    /// ONCE.call_once(async || {
    ///     println!("Do some one-time async thing.");
    /// })
    /// .await;
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

    /// Waits asynchronously until some `call_once` has completed successfully.
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
            registration: None,
            once: self,
        }
        .await
    }
}

struct OnceWait<'a> {
    registration: Option<WaitRegistration>,
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
        let Self { registration, once } = self.get_mut();
        once.done.poll_wait(registration, cx)
    }
}

impl Drop for OnceWait<'_> {
    fn drop(&mut self) {
        if self.registration.is_some() {
            self.once.done.unregister_waker(&mut self.registration);
        }
    }
}
