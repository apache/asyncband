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

use std::any::Any;
use std::any::TypeId;
use std::fmt;
use std::future::Future;
use std::panic::RefUnwindSafe;
use std::panic::UnwindSafe;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use super::OnceCell;
use crate::mutex::Mutex;

/// A boxed future suitable for the default [`LazyCell`] initializer type.
pub type LazyCellFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// A thread-safe value initialized by an asynchronous function on first access.
///
/// Initialization starts when [`force`](Self::force) or
/// [`try_force`](Self::try_force) is polled. Concurrent callers wait without
/// blocking their threads.
///
/// If a caller is cancelled, the initialization future remains pinned in the
/// cell. The next caller resumes that same future. For fallible initialization,
/// an error ends the current attempt and the next caller starts a new attempt.
/// The stored future must be `Send + 'static` because a different task may
/// resume it. Call-time arguments may contain references only when the returned
/// future does not retain them.
///
/// Infallible initializers are called once and may move captured values directly
/// into their future. Fallible initializers are `FnMut` factories that must
/// remain callable after an error and return an owned future. Lending
/// `AsyncFnMut` closures that borrow captured state into the returned future are
/// not supported. State can instead be updated before creating the future or
/// cloned into each owned attempt:
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use std::sync::Arc;
///
/// use asyncband::once::LazyCell;
///
/// let client = Arc::new("client".to_owned());
/// let lazy = LazyCell::<usize, _>::new(move || {
///     let client = client.clone();
///     async move { Ok::<_, ()>(client.len()) }
/// });
///
/// assert_eq!(LazyCell::try_force(&lazy).await, Ok(&6));
/// # }
/// ```
///
/// # Poisoning
///
/// A panic from the initializer permanently poisons the cell. The panic is
/// propagated to its caller, and future calls to `force`, `try_force`,
/// `force_mut`, or `try_force_mut` panic. Errors returned through `Result` do
/// not poison the cell and allow a later caller to retry initialization.
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
pub struct LazyCell<T, F = fn() -> LazyCellFuture<T>> {
    value: OnceCell<T>,
    state: Mutex<State<T, F>>,
    poisoned: AtomicBool,
}

type Attempt<T> = Pin<Box<dyn Future<Output = AttemptOutput<T>> + Send + 'static>>;

enum AttemptOutput<T> {
    Value(T),
    Error(Box<dyn Any>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AttemptKind {
    Infallible,
    Fallible(TypeId),
}

struct State<T, F> {
    initializer: Option<F>,
    attempt: Option<(AttemptKind, Attempt<T>)>,
}

impl<T, F> LazyCell<T, F> {
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
    /// use asyncband::once::LazyCell;
    ///
    /// let lazy = LazyCell::<u32, _>::new(async || 92);
    /// assert_eq!(*LazyCell::force(&lazy).await, 92);
    /// # }
    /// ```
    pub const fn new(initializer: F) -> Self {
        Self {
            value: OnceCell::new(),
            state: Mutex::new(State {
                initializer: Some(initializer),
                attempt: None,
            }),
            poisoned: AtomicBool::new(false),
        }
    }

    /// Returns a reference to the value if initialized.
    ///
    /// This method never starts initialization or waits for an active attempt.
    /// It returns `None` when the cell is uninitialized, initializing, or
    /// poisoned.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let lazy = LazyCell::<u32, _>::new(async || 92);
    /// assert_eq!(LazyCell::get(&lazy), None);
    /// LazyCell::force(&lazy).await;
    /// assert_eq!(LazyCell::get(&lazy), Some(&92));
    /// # }
    /// ```
    pub fn get(this: &Self) -> Option<&T> {
        this.value.get()
    }

    /// Returns a mutable reference to the value if initialized.
    ///
    /// This method never starts initialization. It returns `None` when the cell
    /// is uninitialized or poisoned.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let mut lazy = LazyCell::<u32, _>::new(async || 92);
    /// assert_eq!(LazyCell::get_mut(&mut lazy), None);
    /// LazyCell::force(&lazy).await;
    /// *LazyCell::get_mut(&mut lazy).unwrap() = 44;
    /// assert_eq!(LazyCell::get(&lazy), Some(&44));
    /// # }
    /// ```
    pub fn get_mut(this: &mut Self) -> Option<&mut T> {
        this.value.get_mut()
    }

    /// Consumes the cell and returns its value or initializer.
    ///
    /// Returns `Ok(value)` when initialized and `Err(initializer)` otherwise.
    ///
    /// # Panics
    ///
    /// Panics if the cell is poisoned or a one-shot initializer was started but
    /// did not complete.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let lazy = LazyCell::<u32, _>::new(async || 92);
    /// LazyCell::force(&lazy).await;
    /// assert_eq!(LazyCell::into_inner(lazy).ok(), Some(92));
    /// # }
    /// ```
    pub fn into_inner(this: Self) -> Result<T, F> {
        let Self {
            value,
            state,
            poisoned,
        } = this;

        if poisoned.into_inner() {
            panic_poisoned();
        }

        let State {
            initializer,
            attempt,
        } = state.into_inner();
        drop(attempt);

        match value.into_inner() {
            Some(value) => Ok(value),
            None => Err(initializer.expect("LazyCell one-shot initializer has already started")),
        }
    }

    /// Initializes the value if needed and returns a reference to it.
    async fn initialize_once<G, Fut>(&self, start: G) -> &T
    where
        G: FnOnce(F) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        if let Some(value) = self.value.get() {
            return value;
        }
        self.assert_unpoisoned();

        let mut state = self.state.lock().await;
        if let Some(value) = self.value.get() {
            return value;
        }
        self.assert_unpoisoned();

        let mut start = Some(start);

        // Initialize the value if no other task has started an attempt.
        if state.attempt.is_none() {
            let initializer = state
                .initializer
                .take()
                .expect("LazyCell initializer missing while uninitialized");
            let future = {
                let _poison = PoisonOnPanic(&self.poisoned);
                start.take().expect("LazyCell initializer start missing")(initializer)
            };
            let attempt = Box::pin(async move { AttemptOutput::Value(future.await) });
            state.attempt = Some((AttemptKind::Infallible, attempt));
        }
        // Drop unused caller arguments before resuming the active attempt. Retaining a
        // guard could deadlock the attempt on a resource it needs.
        drop(start);

        let (kind, attempt) = state
            .attempt
            .as_mut()
            .expect("LazyCell attempt missing while initializing");
        assert!(
            *kind == AttemptKind::Infallible,
            "LazyCell force method does not match the active attempt"
        );

        // Avoid unrelated panics from the initializer poisoning the cell for all future callers.
        let output = std::future::poll_fn(|cx| {
            let _poison = PoisonOnPanic(&self.poisoned);
            attempt.as_mut().poll(cx)
        })
        .await;
        state.attempt = None;
        let AttemptOutput::Value(value) = output else {
            unreachable!("infallible LazyCell attempt returned an error")
        };
        unsafe { self.value.set_value_unchecked(value) }
    }

    /// Initializes the value with a fallible initializer and returns a reference to it.
    async fn initialize_retry<E, G, Fut>(&self, start: G) -> Result<&T, E>
    where
        G: FnOnce(&mut F) -> Fut,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        E: 'static,
    {
        if let Some(value) = self.value.get() {
            return Ok(value);
        }
        self.assert_unpoisoned();

        let mut state = self.state.lock().await;
        if let Some(value) = self.value.get() {
            return Ok(value);
        }
        self.assert_unpoisoned();

        let kind = AttemptKind::Fallible(TypeId::of::<E>());
        let mut start = Some(start);

        // Initialize the value if no other task has started an attempt.
        if state.attempt.is_none() {
            let initializer = state
                .initializer
                .as_mut()
                .expect("LazyCell initializer missing while uninitialized");
            let future = {
                let _poison = PoisonOnPanic(&self.poisoned);
                start.take().expect("LazyCell initializer start missing")(initializer)
            };
            let attempt = Box::pin(async move {
                match future.await {
                    Ok(value) => AttemptOutput::Value(value),
                    Err(error) => AttemptOutput::Error(Box::new(error)),
                }
            });
            state.attempt = Some((kind, attempt));
        }
        // Drop unused caller arguments before resuming the active attempt. Retaining a
        // guard could deadlock the attempt on a resource it needs.
        drop(start);

        let (active_kind, attempt) = state
            .attempt
            .as_mut()
            .expect("LazyCell attempt missing while initializing");
        assert!(
            *active_kind == kind,
            "LazyCell force method does not match the active attempt"
        );

        // Avoid unrelated panics from the initializer poisoning the cell for all future callers.
        let output = std::future::poll_fn(|cx| {
            let _poison = PoisonOnPanic(&self.poisoned);
            attempt.as_mut().poll(cx)
        })
        .await;
        state.attempt = None;
        match output {
            AttemptOutput::Value(value) => {
                state.initializer = None;
                Ok(unsafe { self.value.set_value_unchecked(value) })
            }
            AttemptOutput::Error(error) => {
                let error = error
                    .downcast::<E>()
                    .expect("LazyCell attempt error type changed");
                Err(*error)
            }
        }
    }

    /// Panics if the cell is poisoned.
    fn assert_unpoisoned(&self) {
        if self.poisoned.load(Ordering::Acquire) {
            panic_poisoned();
        }
    }
}

impl<T, F> LazyCell<T, F> {
    /// Initializes the value if needed and returns a reference to it.
    ///
    /// If another task is initializing the cell, this call waits for that
    /// attempt. If its caller is cancelled, a later caller resumes the same
    /// pinned future.
    ///
    /// # Panics
    ///
    /// Panics if the initializer panics or the cell was previously poisoned.
    /// Recursive initialization of the same cell deadlocks.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let lazy = LazyCell::<u32, _>::new(async || 92);
    /// assert_eq!(LazyCell::force(&lazy).await, &92);
    /// # }
    /// ```
    pub async fn force<Fut>(this: &Self) -> &T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        this.initialize_once(|initializer| initializer()).await
    }

    /// Initializes the value using call-time arguments.
    ///
    /// The arguments are passed to the initializer only when this call starts a
    /// new attempt. If an attempt is already active, this call resumes it and
    /// its arguments are unused.
    ///
    /// # Panics
    ///
    /// Panics if the initializer panics or the cell was previously poisoned.
    /// Recursive initialization of the same cell deadlocks.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let lazy = LazyCell::<String, _>::new(async |name: String| name.to_uppercase());
    /// assert_eq!(
    ///     LazyCell::force_with(&lazy, "asyncband".to_owned()).await,
    ///     "ASYNCBAND"
    /// );
    /// # }
    /// ```
    pub async fn force_with<A, Fut>(this: &Self, args: A) -> &T
    where
        F: FnOnce(A) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        this.initialize_once(|initializer| initializer(args)).await
    }

    /// Initializes the value if needed and returns a mutable reference to it.
    ///
    /// # Panics
    ///
    /// Panics if the initializer panics or the cell was previously poisoned.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let mut lazy = LazyCell::<u32, _>::new(async || 92);
    /// *LazyCell::force_mut(&mut lazy).await = 44;
    /// assert_eq!(LazyCell::get(&lazy), Some(&44));
    /// # }
    /// ```
    pub async fn force_mut<Fut>(this: &mut Self) -> &mut T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        let _ = Self::force(this).await;
        this.value
            .get_mut()
            .expect("LazyCell value missing after success")
    }

    /// Initializes the value with call-time arguments and returns mutable access.
    ///
    /// # Panics
    ///
    /// Panics if the initializer panics or the cell was previously poisoned.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let mut lazy = LazyCell::<u32, _>::new(async |value| value);
    /// *LazyCell::force_mut_with(&mut lazy, 42).await += 1;
    /// assert_eq!(LazyCell::get(&lazy), Some(&43));
    /// # }
    /// ```
    pub async fn force_mut_with<A, Fut>(this: &mut Self, args: A) -> &mut T
    where
        F: FnOnce(A) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        let _ = Self::force_with(this, args).await;
        this.value
            .get_mut()
            .expect("LazyCell value missing after success")
    }
}

impl<T, F> LazyCell<T, F> {
    /// Initializes the value with a fallible initializer.
    ///
    /// An error is returned only to the caller whose attempt produced it. The
    /// cell remains uninitialized, and the next waiting caller starts a new
    /// serialized attempt. Cancellation preserves the active future and does
    /// not invoke the initializer again.
    ///
    /// # Panics
    ///
    /// Panics if the initializer panics or the cell was previously poisoned.
    /// Recursive initialization of the same cell deadlocks.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let lazy = LazyCell::<u32, _>::new(async || Ok::<_, std::io::Error>(92));
    /// assert_eq!(LazyCell::try_force(&lazy).await.unwrap(), &92);
    /// # }
    /// ```
    pub async fn try_force<E, Fut>(this: &Self) -> Result<&T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        E: 'static,
    {
        this.initialize_retry(|initializer| initializer()).await
    }

    /// Initializes the value fallibly using call-time arguments.
    ///
    /// The arguments are passed to the initializer only when this call starts a
    /// new attempt. If an attempt is already active, this call resumes it and
    /// its arguments are unused. An error leaves the cell uninitialized so the
    /// next caller can start a new attempt with its own arguments.
    ///
    /// # Panics
    ///
    /// Panics if the initializer panics or the cell was previously poisoned.
    /// Recursive initialization of the same cell deadlocks.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let lazy = LazyCell::<u32, _>::new(async |(value, valid): (u32, bool)| {
    ///     valid.then_some(value).ok_or("invalid")
    /// });
    ///
    /// assert_eq!(
    ///     LazyCell::try_force_with(&lazy, (1, false)).await,
    ///     Err("invalid")
    /// );
    /// assert_eq!(LazyCell::try_force_with(&lazy, (42, true)).await, Ok(&42));
    /// # }
    /// ```
    pub async fn try_force_with<A, E, Fut>(this: &Self, args: A) -> Result<&T, E>
    where
        F: FnMut(A) -> Fut,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        E: 'static,
    {
        this.initialize_retry(|initializer| initializer(args)).await
    }

    /// Initializes the value with a fallible initializer and returns mutable access.
    ///
    /// An error leaves the cell uninitialized so a later caller can retry.
    ///
    /// # Panics
    ///
    /// Panics if the initializer panics or the cell was previously poisoned.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let mut lazy = LazyCell::<u32, _>::new(async || Ok::<_, ()>(92));
    /// *LazyCell::try_force_mut(&mut lazy).await.unwrap() = 44;
    /// assert_eq!(LazyCell::get(&lazy), Some(&44));
    /// # }
    /// ```
    pub async fn try_force_mut<E, Fut>(this: &mut Self) -> Result<&mut T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        E: 'static,
    {
        let _ = Self::try_force(this).await?;
        Ok(this
            .value
            .get_mut()
            .expect("LazyCell value missing after success"))
    }

    /// Initializes the value fallibly with call-time arguments and returns mutable access.
    ///
    /// An error leaves the cell uninitialized so a later caller can retry.
    ///
    /// # Panics
    ///
    /// Panics if the initializer panics or the cell was previously poisoned.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let mut lazy = LazyCell::<u32, _>::new(async |value| Ok::<_, ()>(value));
    /// *LazyCell::try_force_mut_with(&mut lazy, 42).await.unwrap() += 1;
    /// assert_eq!(LazyCell::get(&lazy), Some(&43));
    /// # }
    /// ```
    pub async fn try_force_mut_with<A, E, Fut>(this: &mut Self, args: A) -> Result<&mut T, E>
    where
        F: FnMut(A) -> Fut,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        E: 'static,
    {
        let _ = Self::try_force_with(this, args).await?;
        Ok(this
            .value
            .get_mut()
            .expect("LazyCell value missing after success"))
    }
}

impl<T> Default for LazyCell<T>
where
    T: Default + Send + 'static,
{
    /// Creates a lazy value initialized with [`Default::default`].
    fn default() -> Self {
        fn initialize<T>() -> LazyCellFuture<T>
        where
            T: Default + Send + 'static,
        {
            Box::pin(async { T::default() })
        }

        Self::new(initialize::<T>)
    }
}

impl<T, F> From<T> for LazyCell<T, F> {
    /// Creates an already initialized lazy value.
    fn from(value: T) -> Self {
        Self {
            value: OnceCell::from_value(value),
            state: Mutex::new(State {
                initializer: None,
                attempt: None,
            }),
            poisoned: AtomicBool::new(false),
        }
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
