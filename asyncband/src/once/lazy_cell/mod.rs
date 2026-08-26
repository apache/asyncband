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
use std::future::Pending;
use std::future::Ready;
use std::panic::RefUnwindSafe;
use std::panic::UnwindSafe;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::internal::value_cell::ValueCell;
use crate::mutex::Mutex;

/// A thread-safe value initialized by a stored asynchronous function on first access.
///
/// `LazyCell` stores the initializer's concrete future type without allocating or erasing it. The
/// caller chooses whether that future lives inline in the cell or behind a pointer such as
/// `Pin<Box<_>>`.
///
/// An `Unpin` future can be initialized with [`force`](Self::force). Otherwise, the cell itself
/// must be pinned and initialized with [`force_pin`](Self::force_pin). If the forcing caller is
/// cancelled, the initialization future remains in the cell and the next caller resumes that same
/// future instead of starting over.
///
/// The cell's `Send` and `Sync` implementations follow its value, initializer, and future. A local
/// future may therefore borrow local data or be non-`Send`, while a cell shared between threads
/// naturally requires those stored types to be `Send`.
///
/// `LazyCell` represents one asynchronous initialization attempt. If initialization needs
/// access-time arguments or should retry after returning an error, use `OnceCell::get_or_try_init`
/// instead. A `Result` may still be the stored value when an error should be cached.
///
/// # Poisoning
///
/// A panic while creating or polling the initialization future permanently poisons the cell. The
/// panic is propagated to its caller, and future calls to any forcing method panic.
///
/// # Examples
///
/// Pinning the future behind a box keeps the future in place while allowing its pointer to move.
/// The cell therefore remains movable and can be initialized with [`force`](Self::force):
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use asyncband::once::LazyCell;
///
/// let lazy = LazyCell::new(|| Box::pin(async { "ready".to_owned() }));
///
/// assert_eq!(LazyCell::get(&lazy), None);
/// assert_eq!(LazyCell::force(&lazy).await, "ready");
/// assert_eq!(LazyCell::get(&lazy).map(String::as_str), Some("ready"));
/// # }
/// ```
///
/// To store an `async` future inline, pin the cell itself and use
/// [`force_pin`](Self::force_pin). The cell cannot move once polling starts because its stored
/// future is not [`Unpin`]:
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use asyncband::once::LazyCell;
///
/// let lazy = LazyCell::new(|| async { "ready".to_owned() });
/// let lazy = Box::pin(lazy);
///
/// assert_eq!(LazyCell::force_pin(lazy.as_ref()).await, "ready");
/// # }
/// ```
///
/// [`Arc::pin`](std::sync::Arc::pin) provides shared ownership while keeping an inline future at a
/// stable address:
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use std::sync::Arc;
///
/// use asyncband::once::LazyCell;
///
/// let lazy = Arc::pin(LazyCell::new(|| async { 92 }));
/// let other = lazy.clone();
///
/// let (first, second) = tokio::join!(
///     LazyCell::force_pin(lazy.as_ref()),
///     LazyCell::force_pin(other.as_ref()),
/// );
/// assert_eq!((*first, *second), (92, 92));
/// # }
/// ```
pub struct LazyCell<T, Fut, F = fn() -> Fut> {
    value: ValueCell<T>,
    state: Mutex<State<F, Fut>>,
    poisoned: AtomicBool,
}

// Keep mutually exclusive phases in an enum so an inline future shares storage with its
// initializer instead of making the cell large enough to hold both at once.
enum State<F, Fut> {
    Initializer(Option<F>),
    Attempt(Fut),
    Complete,
}

impl<T, Fut, F> LazyCell<T, Fut, F> {
    /// Creates a new lazy value with the given asynchronous initializer.
    ///
    /// The initializer is not called until the first forcing future is polled. Its returned future
    /// is stored as-is; this method never boxes it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let lazy = LazyCell::new(async || 92);
    /// let lazy = std::pin::pin!(lazy);
    /// assert_eq!(*LazyCell::force_pin(lazy.as_ref()).await, 92);
    /// # }
    /// ```
    ///
    /// Or explicitly box the future so the cell remains movable:
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let lazy = LazyCell::new(|| Box::pin(async { 92 }));
    /// assert_eq!(*LazyCell::force(&lazy).await, 92);
    /// # }
    /// ```
    pub const fn new(initializer: F) -> Self
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        Self {
            value: ValueCell::new(),
            state: Mutex::new(State::Initializer(Some(initializer))),
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

    fn assert_unpoisoned(&self) {
        // This flag does not publish any accompanying data, so it only needs atomicity.
        if self.poisoned.load(Ordering::Relaxed) {
            panic_poisoned();
        }
    }
}

impl<T, Fut, F> LazyCell<T, Fut, F>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T> + Unpin,
{
    /// Initializes the value if needed and returns a reference to it.
    ///
    /// This method is available when moving the stored future is safe. A future behind
    /// `Pin<Box<_>>` is `Unpin` because moving the box does not move its pointee.
    ///
    /// If another task is initializing the cell, this call waits for that attempt. If the task
    /// driving initialization is cancelled, a later caller resumes the same future.
    ///
    /// # Examples
    ///
    /// Box the future when the cell itself must remain movable:
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let lazy = LazyCell::new(|| Box::pin(async { 92 }));
    /// assert_eq!(*LazyCell::force(&lazy).await, 92);
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the initializer panics or the cell was previously poisoned. Recursive
    /// initialization of the same cell deadlocks.
    pub async fn force(this: &Self) -> &T {
        Self::force_pin(Pin::new(this)).await
    }

    /// Initializes the value if needed and returns mutable access to it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let mut lazy = LazyCell::new(|| Box::pin(async { String::from("ready") }));
    /// LazyCell::force_mut(&mut lazy).await.push_str("!");
    /// assert_eq!(LazyCell::get(&lazy).map(String::as_str), Some("ready!"));
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`force`](Self::force).
    pub async fn force_mut(this: &mut Self) -> &mut T {
        Self::force_pin_mut(Pin::new(this)).await
    }
}

impl<T, Fut, F> LazyCell<T, Fut, F>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    /// Initializes a pinned cell and returns a reference to its value.
    ///
    /// Pinning the cell keeps an inline initialization future at a stable address. Cancellation
    /// leaves that future in the cell so a later caller can resume it.
    ///
    /// Use this method when the stored future is not [`Unpin`]. [`Box::pin`] is one way to pin the
    /// whole cell; [`Pin::as_ref`] then supplies the `Pin<&Self>` required here.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// // This async block produces a `!Unpin` future. Pin the cell that stores it inline.
    /// let lazy = Box::pin(LazyCell::new(|| async { 92 }));
    /// assert_eq!(*LazyCell::force_pin(lazy.as_ref()).await, 92);
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the initializer panics or the cell was previously poisoned. Recursive
    /// initialization of the same cell deadlocks.
    pub async fn force_pin<'a>(this: Pin<&'a Self>) -> &'a T {
        let this_ref: &'a Self = Pin::get_ref(this);
        if let Some(value) = Self::get(this_ref) {
            return value;
        }
        this_ref.assert_unpoisoned();

        let mut state = this_ref.state.lock().await;
        if let Some(value) = Self::get(this_ref) {
            return value;
        }
        this_ref.assert_unpoisoned();

        // SAFETY: `this` remains pinned for the duration of the returned future. The state is
        // stored in that cell, and a mutex guard never relocates its protected value.
        let state = unsafe { Pin::new_unchecked(&mut *state) };
        let value = drive_pinned_attempt(state, &this_ref.poisoned).await;

        // SAFETY: The state mutex serializes initialization, and the double check above verified
        // that the value was not initialized by another caller.
        unsafe { this_ref.value.set(value) }
    }

    /// Initializes a pinned cell and returns mutable access to its value.
    ///
    /// Exclusive access lets this method initialize the cell directly. Calling [`Pin::as_mut`] on
    /// a pinned box supplies the required `Pin<&mut Self>` without moving the cell.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::LazyCell;
    ///
    /// let mut lazy = Box::pin(LazyCell::new(|| async { String::from("ready") }));
    /// let value = LazyCell::force_pin_mut(lazy.as_mut()).await;
    /// value.push_str("!");
    /// assert_eq!(value, "ready!");
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`force_pin`](Self::force_pin).
    pub async fn force_pin_mut<'a>(this: Pin<&'a mut Self>) -> &'a mut T {
        // SAFETY: We do not move the structurally pinned future through this reference. The stored
        // value is not structurally pinned and may be accessed normally.
        let this: &'a mut Self = unsafe { Pin::get_unchecked_mut(this) };
        if this.value.is_initialized_mut() {
            return this
                .value
                .get_mut()
                .expect("LazyCell value missing while initialized");
        }
        if *this.poisoned.get_mut() {
            panic_poisoned();
        }

        // SAFETY: Exclusive access to the pinned cell pins its state in place for this call.
        let state = unsafe { Pin::new_unchecked(this.state.get_mut()) };
        let value = drive_pinned_attempt(state, &this.poisoned).await;
        this.value.set_mut(value)
    }
}

async fn drive_pinned_attempt<T, F, Fut>(
    mut state: Pin<&mut State<F, Fut>>,
    poisoned: &AtomicBool,
) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    // SAFETY: The initializer is not structurally pinned. We only move it from the initializer
    // variant, which cannot contain a pinned future.
    let initializer = match unsafe { state.as_mut().get_unchecked_mut() } {
        State::Initializer(initializer) => Some(
            initializer
                .take()
                .expect("LazyCell initializer missing while uninitialized"),
        ),
        State::Attempt(_) => None,
        State::Complete => panic!("LazyCell state complete while value is uninitialized"),
    };

    if let Some(initializer) = initializer {
        let future = {
            let _poison = PoisonOnPanic(poisoned);
            initializer()
        };

        // SAFETY: The current variant contains no structurally pinned future, and its initializer
        // slot is empty. The newly created future is written directly into its final pinned
        // location before it is polled.
        *unsafe { state.as_mut().get_unchecked_mut() } = State::Attempt(future);
    }

    let value = std::future::poll_fn(|cx| {
        let _poison = PoisonOnPanic(poisoned);
        // SAFETY: The state remains pinned while this future is polled, and the attempt is never
        // moved after it is installed.
        let attempt = match unsafe { state.as_mut().get_unchecked_mut() } {
            State::Attempt(attempt) => attempt,
            State::Initializer(_) => panic!("LazyCell attempt missing while initializing"),
            State::Complete => panic!("LazyCell state complete while value is uninitialized"),
        };
        unsafe { Pin::new_unchecked(attempt) }.poll(cx)
    })
    .await;

    // Assignment drops the completed future in place before changing the phase. Treat a panic
    // from that destructor as an initializer panic as well.
    let _poison = PoisonOnPanic(poisoned);
    // SAFETY: Dropping a pinned value in place is permitted, and no value is moved out.
    *unsafe { state.as_mut().get_unchecked_mut() } = State::Complete;
    value
}

// A default cell starts empty, so `Ready<T>` is a real attempt type: forcing the cell creates
// `T::default()` at access time and polls that allocation-free future to completion.
impl<T> Default for LazyCell<T, Ready<T>>
where
    T: Default,
{
    fn default() -> Self {
        fn initialize<T: Default>() -> Ready<T> {
            std::future::ready(T::default())
        }

        Self::new(initialize::<T>)
    }
}

// A future constructor starts in the attempt phase, so keep the caller's exact future type rather
// than replacing it with a marker or erasing it behind a pointer.
impl<T, Fut> LazyCell<T, Fut>
where
    Fut: Future<Output = T>,
{
    /// Creates a new `LazyCell` from an already-created asynchronous future.
    ///
    /// The future is stored as-is without being polled, boxed, or erased. Pin the returned cell
    /// before forcing it when `Fut` is not `Unpin`.
    pub fn from_future(future: Fut) -> Self {
        Self {
            value: ValueCell::new(),
            state: Mutex::new(State::Attempt(future)),
            poisoned: AtomicBool::new(false),
        }
    }
}

// A value constructor never creates or polls an attempt. `Pending<T>` supplies the required
// `Future<Output = T>` shape without reserving another `T` beside the initialized `ValueCell`.
impl<T> LazyCell<T, Pending<T>> {
    /// Creates a new `LazyCell` that already contains `value`.
    pub const fn from_value(value: T) -> Self {
        Self {
            value: ValueCell::from_value(value),
            state: Mutex::new(State::Complete),
            poisoned: AtomicBool::new(false),
        }
    }
}

impl<T: fmt::Debug, Fut, F> fmt::Debug for LazyCell<T, Fut, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut tuple = f.debug_tuple("LazyCell");
        match Self::get(self) {
            Some(value) => tuple.field(value),
            None => tuple.field(&format_args!("<uninit>")),
        };
        tuple.finish()
    }
}

impl<T, Fut: Unpin, F> Unpin for LazyCell<T, Fut, F> {}

impl<T: UnwindSafe, Fut: UnwindSafe, F: UnwindSafe> UnwindSafe for LazyCell<T, Fut, F> {}

impl<T: RefUnwindSafe + UnwindSafe, Fut: UnwindSafe, F: UnwindSafe> RefUnwindSafe
    for LazyCell<T, Fut, F>
{
}

struct PoisonOnPanic<'a>(&'a AtomicBool);

impl Drop for PoisonOnPanic<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.store(true, Ordering::Relaxed);
        }
    }
}

#[cold]
#[inline(never)]
fn panic_poisoned() -> ! {
    panic!("LazyCell instance has previously been poisoned")
}
