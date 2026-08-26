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
use std::future::Future;
use std::panic::RefUnwindSafe;
use std::panic::UnwindSafe;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::internal::value_cell::ValueCell;
use crate::mutex::Mutex;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// A thread-safe value initialized by a stored asynchronous function on first access.
///
/// Initialization starts when [`force`](Self::force) is polled. Concurrent callers wait without
/// blocking their threads. If the forcing caller is cancelled, the initialization future remains
/// pinned in the cell and the next caller resumes that same future instead of starting over.
///
/// The initialization future must be `Send + 'static`: another task may resume it after the
/// original caller is gone, potentially on another thread. The initializer itself only needs to
/// be `Send`, not `Sync`, because the cell serializes access to it.
///
/// `LazyCell` represents one asynchronous initialization attempt. If initialization needs
/// access-time arguments or should retry after returning an error, use `OnceCell::get_or_try_init`
/// instead. A `Result` may still be the stored value when an error should be cached.
///
/// # Poisoning
///
/// A panic while creating or polling the initialization future permanently poisons the cell. The
/// panic is propagated to its caller, and future calls to [`force`](Self::force) or
/// [`force_mut`](Self::force_mut) panic.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use asyncband::once::LazyCell;
///
/// let lazy = LazyCell::<String, _>::new(async || "ready".to_owned());
///
/// assert_eq!(LazyCell::get(&lazy), None);
/// assert_eq!(LazyCell::force(&lazy).await, "ready");
/// assert_eq!(LazyCell::get(&lazy).map(String::as_str), Some("ready"));
/// # }
/// ```
pub struct LazyCell<T, F = fn() -> BoxFuture<T>> {
    value: ValueCell<T>,
    state: Mutex<State<T, F>>,
    poisoned: AtomicBool,
}

struct State<T, F> {
    initializer: Option<F>,
    attempt: Option<BoxFuture<T>>,
}

impl<T, F> State<T, F> {
    async fn drive_attempt<Fut>(&mut self, poisoned: &AtomicBool) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        if self.attempt.is_none() {
            let initializer = self
                .initializer
                .take()
                .expect("LazyCell initializer missing while uninitialized");
            let future = {
                let _poison = PoisonOnPanic(poisoned);
                initializer()
            };
            self.attempt = Some(Box::pin(future));
        }

        let value = std::future::poll_fn(|cx| {
            let _poison = PoisonOnPanic(poisoned);
            self.attempt
                .as_mut()
                .expect("LazyCell attempt missing while initializing")
                .as_mut()
                .poll(cx)
        })
        .await;

        // Treat panics from dropping a completed future as initializer panics as well.
        let _poison = PoisonOnPanic(poisoned);
        self.attempt = None;
        value
    }
}

impl<T, F> LazyCell<T, F> {
    /// Creates a new lazy value with the given asynchronous initializer.
    ///
    /// The initializer is not called until the first [`force`](Self::force) future is polled.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let lazy = LazyCell::<u32, _>::new(async || 92);
    /// assert_eq!(*LazyCell::force(&lazy).await, 92);
    /// # }
    /// ```
    pub const fn new(initializer: F) -> Self {
        Self {
            value: ValueCell::new(),
            state: Mutex::new(State {
                initializer: Some(initializer),
                attempt: None,
            }),
            poisoned: AtomicBool::new(false),
        }
    }

    /// Returns a reference to the value if initialized.
    ///
    /// This method never starts initialization or waits for an active attempt. It returns `None`
    /// when the cell is uninitialized, initializing, or poisoned.
    pub fn get(this: &Self) -> Option<&T> {
        this.value.get()
    }

    /// Returns a mutable reference to the value if initialized.
    ///
    /// This method never starts initialization. It returns `None` when the cell is uninitialized
    /// or poisoned.
    pub fn get_mut(this: &mut Self) -> Option<&mut T> {
        this.value.get_mut()
    }

    /// Initializes the value if needed and returns a reference to it.
    ///
    /// If another task is initializing the cell, this call waits for that attempt. If the task
    /// driving initialization is cancelled, a later caller resumes the same pinned future.
    ///
    /// # Panics
    ///
    /// Panics if the initializer panics or the cell was previously poisoned. Recursive
    /// initialization of the same cell deadlocks.
    pub async fn force<Fut>(this: &Self) -> &T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        if let Some(value) = this.value.get() {
            return value;
        }
        this.assert_unpoisoned();

        let mut state = this.state.lock().await;
        if let Some(value) = this.value.get() {
            return value;
        }
        this.assert_unpoisoned();

        let value = state.drive_attempt(&this.poisoned).await;

        // SAFETY: The state mutex serializes initialization, and the double check above verified
        // that the value was not initialized by another caller.
        unsafe { this.value.set(value) }
    }

    /// Initializes the value if needed and returns mutable access to it.
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`force`](Self::force).
    pub async fn force_mut<Fut>(this: &mut Self) -> &mut T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        if this.value.is_initialized_mut() {
            return this
                .value
                .get_mut()
                .expect("LazyCell value missing while initialized");
        }
        if *this.poisoned.get_mut() {
            panic_poisoned();
        }

        // Exclusive access makes locking and atomic value publication unnecessary.
        let value = this.state.get_mut().drive_attempt(&this.poisoned).await;
        this.value.set_mut(value)
    }

    fn assert_unpoisoned(&self) {
        if self.poisoned.load(Ordering::Acquire) {
            panic_poisoned();
        }
    }
}

impl<T> LazyCell<T> {
    /// Creates a new `LazyCell` from an already-created asynchronous future.
    ///
    /// The future is stored without being polled. Calling [`force`](Self::force) starts polling
    /// it, and cancellation preserves the in-flight future for a later caller.
    pub fn from_future<Fut>(future: Fut) -> Self
    where
        Fut: Future<Output = T> + Send + 'static,
    {
        Self {
            value: ValueCell::new(),
            state: Mutex::new(State {
                initializer: None,
                attempt: Some(Box::pin(future)),
            }),
            poisoned: AtomicBool::new(false),
        }
    }

    /// Creates a new `LazyCell` that already contains `value`.
    pub const fn from_value(value: T) -> Self {
        Self {
            value: ValueCell::from_value(value),
            state: Mutex::new(State {
                initializer: None,
                attempt: None,
            }),
            poisoned: AtomicBool::new(false),
        }
    }
}

impl<T> Default for LazyCell<T>
where
    T: Default,
{
    fn default() -> Self {
        fn initialize<T: Default>() -> BoxFuture<T> {
            Box::pin(async { T::default() })
        }

        Self::new(initialize::<T>)
    }
}

impl<T: fmt::Debug, F> fmt::Debug for LazyCell<T, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut tuple = f.debug_tuple("LazyCell");
        match Self::get(self) {
            Some(value) => tuple.field(value),
            None => tuple.field(&format_args!("<uninit>")),
        };
        tuple.finish()
    }
}

impl<T: UnwindSafe, F: UnwindSafe> UnwindSafe for LazyCell<T, F> {}

impl<T: RefUnwindSafe + UnwindSafe, F: UnwindSafe> RefUnwindSafe for LazyCell<T, F> {}

struct PoisonOnPanic<'a>(&'a AtomicBool);

impl Drop for PoisonOnPanic<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.store(true, Ordering::Release);
        }
    }
}

#[cold]
#[inline(never)]
fn panic_poisoned() -> ! {
    panic!("LazyCell instance has previously been poisoned")
}
