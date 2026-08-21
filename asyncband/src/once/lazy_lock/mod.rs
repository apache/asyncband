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

use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::panic::RefUnwindSafe;
use std::panic::UnwindSafe;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use super::OnceCell;
use crate::mutex::Mutex;

/// A boxed future suitable for the default [`LazyLock`] initializer type.
pub type LazyLockFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// A value initialized by an asynchronous function on first access.
///
/// Initialization starts when [`force`](Self::force) or
/// [`try_force`](Self::try_force) is polled. Concurrent callers wait without
/// blocking their threads.
///
/// If an attempt is cancelled, its future is dropped and the initializer is
/// retained. The next caller starts a new attempt. Initializers must therefore
/// be safe to invoke again after cancellation, therefore it is up to the user
/// to ensure idempotency.
///
/// # Poisoning
///
/// A panic from the initializer permanently poisons the lock. The panic is
/// propagated to its caller, and future calls to `force`, `try_force`,
/// `force_mut`, or `try_force_mut` panic. Errors returned through `Result` do
/// not poison the lock and are there to indicate initialization is possible.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use asyncband::once::LazyLock;
///
/// let lazy = LazyLock::<String, _>::new(async || "ready".to_owned());
///
/// assert_eq!(LazyLock::get(&lazy), None);
/// assert_eq!(LazyLock::force(&lazy).await, "ready");
/// assert_eq!(LazyLock::get(&lazy).map(String::as_str), Some("ready"));
/// # }
/// ```
pub struct LazyLock<T, F = fn() -> LazyLockFuture<T>> {
    value: OnceCell<T>,
    initializer: Mutex<Option<F>>,
    poisoned: AtomicBool,
}

impl<T, F> LazyLock<T, F> {
    /// Creates a new lazy value with the given asynchronous initializer.
    ///
    /// The initializer is not called until the first initialization future is
    /// polled.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyLock;
    ///
    /// let lazy = LazyLock::<u32, _>::new(async || 92);
    /// assert_eq!(*LazyLock::force(&lazy).await, 92);
    /// # }
    /// ```
    pub const fn new(initializer: F) -> Self {
        Self {
            value: OnceCell::new(),
            initializer: Mutex::new(Some(initializer)),
            poisoned: AtomicBool::new(false),
        }
    }

    /// Returns a reference to the value if initialized.
    ///
    /// This method never starts initialization or waits for an active attempt.
    /// It returns `None` when the lock is uninitialized, initializing, or
    /// poisoned.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyLock;
    ///
    /// let lazy = LazyLock::<u32, _>::new(async || 92);
    /// assert_eq!(LazyLock::get(&lazy), None);
    /// LazyLock::force(&lazy).await;
    /// assert_eq!(LazyLock::get(&lazy), Some(&92));
    /// # }
    /// ```
    pub fn get(this: &Self) -> Option<&T> {
        this.value.get()
    }

    /// Returns a mutable reference to the value if initialized.
    ///
    /// This method never starts initialization. It returns `None` when the lock
    /// is uninitialized or poisoned.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyLock;
    ///
    /// let mut lazy = LazyLock::<u32, _>::new(async || 92);
    /// assert_eq!(LazyLock::get_mut(&mut lazy), None);
    /// LazyLock::force(&lazy).await;
    /// *LazyLock::get_mut(&mut lazy).unwrap() = 44;
    /// assert_eq!(LazyLock::get(&lazy), Some(&44));
    /// # }
    /// ```
    pub fn get_mut(this: &mut Self) -> Option<&mut T> {
        this.value.get_mut()
    }

    /// Consumes the lock and returns its value or initializer.
    ///
    /// Returns `Ok(value)` when initialized and `Err(initializer)` otherwise.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyLock;
    ///
    /// let lazy = LazyLock::<u32, _>::new(async || 92);
    /// LazyLock::force(&lazy).await;
    /// assert_eq!(LazyLock::into_inner(lazy).ok(), Some(92));
    /// # }
    /// ```
    pub fn into_inner(this: Self) -> Result<T, F> {
        let Self {
            value,
            initializer,
            poisoned,
        } = this;

        if poisoned.into_inner() {
            panic_poisoned();
        }

        match value.into_inner() {
            Some(value) => Ok(value),
            None => Err(initializer
                .into_inner()
                .expect("LazyLock initializer missing while uninitialized")),
        }
    }

    /// Internal helper to initialize the value with a fallible initializer.
    /// Waiters are queued to restart on cancellation or error, winner removes
    /// the initializer and ensures those waiters see the value.
    async fn initialize<E, G>(&self, run: G) -> Result<&T, E>
    where
        G: AsyncFnOnce(&mut F) -> Result<T, E>,
    {
        if let Some(value) = self.value.get() {
            return Ok(value);
        }
        self.assert_unpoisoned();

        let mut initializer = self.initializer.lock().await;
        if let Some(value) = self.value.get() {
            return Ok(value);
        }
        self.assert_unpoisoned();

        // Running initializer under a lock ensures we queue waiters to restart
        // on cancellation or Err.
        let _poison = PoisonOnPanic(&self.poisoned);
        let value = run(initializer
            .as_mut()
            .expect("LazyLock initializer missing while uninitialized"))
        .await?;

        drop(initializer.take()); // Avoid reinitialization possibility
        let value = unsafe { self.value.set_value_unchecked(value) };

        Ok(value)
    }

    /// Panics if the lock is poisoned.
    fn assert_unpoisoned(&self) {
        if self.poisoned.load(Ordering::Acquire) {
            panic_poisoned();
        }
    }
}

impl<T, F> LazyLock<T, F>
where
    F: AsyncFnMut() -> T,
{
    /// Initializes the value if needed and returns a reference to it.
    ///
    /// If another task is initializing the lock, this call waits for that
    /// attempt. If the active attempt is cancelled, this or another waiting
    /// caller starts a new attempt.
    ///
    /// # Panics
    ///
    /// Panics if the initializer panics or the lock was previously poisoned.
    /// Recursive initialization of the same lock deadlocks.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyLock;
    ///
    /// let lazy = LazyLock::<u32, _>::new(async || 92);
    /// assert_eq!(LazyLock::force(&lazy).await, &92);
    /// # }
    /// ```
    pub async fn force(this: &Self) -> &T {
        // Templated to Infalliable for non-monadic initializers
        match this
            .initialize(async |initializer| Ok::<T, Infallible>(initializer().await))
            .await
        {
            Ok(value) => value,
            Err(error) => match error {},
        }
    }

    /// Initializes the value if needed and returns a mutable reference to it.
    ///
    /// # Panics
    ///
    /// Panics if the initializer panics or the lock was previously poisoned.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyLock;
    ///
    /// let mut lazy = LazyLock::<u32, _>::new(async || 92);
    /// *LazyLock::force_mut(&mut lazy).await = 44;
    /// assert_eq!(LazyLock::get(&lazy), Some(&44));
    /// # }
    /// ```
    pub async fn force_mut(this: &mut Self) -> &mut T {
        let _ = Self::force(this).await;
        this.value
            .get_mut()
            .expect("LazyLock value missing after success")
    }
}

impl<T, F> LazyLock<T, F> {
    /// Initializes the value with a fallible initializer.
    ///
    /// An error is returned only to the caller whose attempt produced it. The
    /// lock remains uninitialized, and the next waiting caller starts a new
    /// serialized attempt. Initializers must therefore be idempotent and safe
    /// to invoke again after cancellation or error.
    ///
    /// # Panics
    ///
    /// Panics if the initializer panics or the lock was previously poisoned.
    /// Recursive initialization of the same lock deadlocks.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyLock;
    ///
    /// let lazy = LazyLock::<u32, _>::new(async || Ok::<_, std::io::Error>(92));
    /// assert_eq!(LazyLock::try_force(&lazy).await.unwrap(), &92);
    /// # }
    /// ```
    pub async fn try_force<E>(this: &Self) -> Result<&T, E>
    where
        F: AsyncFnMut() -> Result<T, E>,
    {
        this.initialize(async |initializer| initializer().await)
            .await
    }

    /// Initializes the value with a fallible initializer and returns mutable access.
    ///
    /// An error leaves the lock uninitialized so a later caller can retry.
    ///
    /// # Panics
    ///
    /// Panics if the initializer panics or the lock was previously poisoned.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyLock;
    ///
    /// let mut lazy = LazyLock::<u32, _>::new(async || Ok::<_, ()>(92));
    /// *LazyLock::try_force_mut(&mut lazy).await.unwrap() = 44;
    /// assert_eq!(LazyLock::get(&lazy), Some(&44));
    /// # }
    /// ```
    pub async fn try_force_mut<E>(this: &mut Self) -> Result<&mut T, E>
    where
        F: AsyncFnMut() -> Result<T, E>,
    {
        let _ = Self::try_force(this).await?;
        Ok(this
            .value
            .get_mut()
            .expect("LazyLock value missing after success"))
    }
}

impl<T> Default for LazyLock<T>
where
    T: Default + Send + 'static,
{
    /// Creates a lazy value initialized with [`Default::default`].
    fn default() -> Self {
        fn initialize<T>() -> LazyLockFuture<T>
        where
            T: Default + Send + 'static,
        {
            Box::pin(async { T::default() })
        }

        Self::new(initialize::<T>)
    }
}

impl<T, F> From<T> for LazyLock<T, F> {
    /// Creates an already initialized lazy value.
    fn from(value: T) -> Self {
        Self {
            value: OnceCell::from_value(value),
            initializer: Mutex::new(None),
            poisoned: AtomicBool::new(false),
        }
    }
}

impl<T: fmt::Debug, F> fmt::Debug for LazyLock<T, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut tuple = f.debug_tuple("LazyLock");
        match Self::get(self) {
            Some(value) => tuple.field(value),
            None => tuple.field(&format_args!("<uninit>")),
        };
        tuple.finish()
    }
}

impl<T: UnwindSafe, F: UnwindSafe> UnwindSafe for LazyLock<T, F> {}

impl<T: RefUnwindSafe + UnwindSafe, F: UnwindSafe> RefUnwindSafe for LazyLock<T, F> {}

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
    panic!("LazyLock instance has previously been poisoned")
}
