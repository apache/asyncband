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

// Portions of this file originated from Tokio 1.47.0's OnceCell implementation.
// Copyright (c) Tokio Contributors
// The Tokio-derived portions remain licensed under the MIT License.
// Asyncband substantially changed the synchronization model: initialization does not close the
// semaphore, the cell can be emptied and initialized again, and value storage is delegated to
// Asyncband's own ValueCell shared with LazyCell.
// Upstream source:
// https://github.com/tokio-rs/tokio/blob/3911cb8523f190142f61c64b66881c07c0d3e7be/tokio/src/sync/once_cell.rs

use std::convert::Infallible;
use std::fmt;

use crate::internal::value_cell::ValueCell;
use crate::semaphore::Semaphore;
use crate::semaphore::SemaphorePermit;

/// A thread-safe cell that stores one value from an initializer supplied at access time.
///
/// Callers provide an initializer when accessing an empty cell. An initializer that returns an
/// error, panics, or is cancelled leaves the cell empty so a later caller can retry. Use
/// `LazyCell` instead when the cell should own a one-shot initializer and preserve its in-flight
/// future across caller cancellation.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() {
/// use std::sync::Arc;
///
/// use asyncband::once::OnceCell;
///
/// static CELL: OnceCell<u8> = OnceCell::new();
///
/// let handle1 = tokio::spawn(async { CELL.get_or_init(move || async { 1 }).await });
/// let handle2 = tokio::spawn(async { CELL.get_or_init(move || async { 2 }).await });
/// let result1 = handle1.await.unwrap();
/// let result2 = handle2.await.unwrap();
/// assert_eq!(result1, result2);
/// assert!(*result1 == 1 || *result1 == 2);
/// # }
/// ```
pub struct OnceCell<T> {
    value: ValueCell<T>,
    // Each initializer takes two of three permits, so only one can run at a time. The spare
    // permit lets an uncontended release avoid locking the semaphore's wait queue.
    semaphore: Semaphore,
}

impl<T> Default for OnceCell<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> OnceCell<T> {
    /// Creates a new empty `OnceCell`.
    pub const fn new() -> Self {
        Self {
            value: ValueCell::empty(),
            semaphore: Semaphore::new(3),
        }
    }

    /// Creates a new `OnceCell` initialized with the provided value.
    pub const fn from_value(value: T) -> Self {
        Self {
            value: ValueCell::with_value(value),
            semaphore: Semaphore::new(3),
        }
    }

    /// Returns whether the cell contains a value.
    // `OnceMap` and `singleflight` inspect this state, while a standalone `OnceCell` build does
    // not need the internal helper.
    #[allow(dead_code)]
    pub(crate) fn initialized(&self) -> bool {
        self.value.is_published()
    }

    /// Returns a shared reference to the stored value without waiting.
    ///
    /// Yields [`None`] while the cell is empty, including when another task is in the middle of
    /// initializing it.
    pub fn get(&self) -> Option<&T> {
        self.value.get()
    }

    /// Returns an exclusive reference to the stored value without blocking or initializing.
    ///
    /// Yields [`None`] when the cell is empty. Because `&mut self` forbids concurrent access, no
    /// asynchronous initialization can be in flight during this call.
    pub fn get_mut(&mut self) -> Option<&mut T> {
        self.value.get_mut()
    }

    /// Returns a reference to the stored value, initializing the cell with `init` when needed.
    ///
    /// Concurrent callers wait for the active initializer. Once it publishes a value, all callers
    /// receive a reference to that value without running their own initializer.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending call while it waits leaves the active initializer running. If this call
    /// is running the initializer, cancellation drops that initializer and leaves the cell empty;
    /// a later caller can start a new attempt. Effects already performed by the initializer are
    /// not rolled back.
    ///
    /// # Panics
    ///
    /// An initializer panic propagates to its caller and leaves the cell empty for a later
    /// attempt. The cell is not poisoned.
    ///
    /// # Deadlocks
    ///
    /// An initializer must not wait for initialization of the same cell: that would wait for
    /// its own completion.
    pub async fn get_or_init<F>(&self, init: F) -> &T
    where
        F: AsyncFnOnce() -> T,
    {
        match self
            .get_or_try_init(async || Ok::<T, Infallible>(init().await))
            .await
        {
            Ok(val) => val,
        }
    }

    /// Returns a reference to the stored value, initializing the cell with a fallible `init` when
    /// needed.
    ///
    /// Concurrent callers wait for the active initializer. A published value is shared by all
    /// callers; an unsuccessful attempt allows another caller to run its own initializer.
    ///
    /// # Errors
    ///
    /// Returns the error from this call's initializer without storing a value. The error is not
    /// shared with waiting callers, which may make their own attempts.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending call while it waits leaves the active initializer running. If this call
    /// is running the initializer, cancellation drops that initializer and leaves the cell empty;
    /// a later caller can start a new attempt. Effects already performed by the initializer are
    /// not rolled back.
    ///
    /// # Panics
    ///
    /// An initializer panic propagates to its caller and leaves the cell empty for a later
    /// attempt. The cell is not poisoned.
    ///
    /// # Deadlocks
    ///
    /// An initializer must not wait for initialization of the same cell: that would wait for
    /// its own completion.
    pub async fn get_or_try_init<E, F>(&self, init: F) -> Result<&T, E>
    where
        F: AsyncFnOnce() -> Result<T, E>,
    {
        if let Some(v) = self.get() {
            return Ok(v);
        }

        let permit = self.semaphore.acquire(2).await;

        if let Some(v) = self.get() {
            // Another task initialized the cell while we waited for permits.
            return Ok(v);
        }

        let value = init().await?;
        Ok(self.set_value(value, permit))
    }

    /// Returns mutable access to the stored value, running `init` if the cell is empty.
    ///
    /// The exclusive borrow prevents concurrent initialization.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending call drops its initializer and leaves the cell empty. A later call can
    /// start a new attempt; effects already performed by the initializer are not rolled back.
    ///
    /// # Panics
    ///
    /// An initializer panic propagates to its caller and leaves the cell empty for a later
    /// attempt. The cell is not poisoned.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::OnceCell;
    ///
    /// let mut slot = OnceCell::new();
    /// let text = slot.get_mut_or_init(|| async { String::from("abc") }).await;
    /// text.push('d');
    /// assert_eq!(slot.get().map(String::as_str), Some("abcd"));
    /// # }
    /// ```
    pub async fn get_mut_or_init<F>(&mut self, init: F) -> &mut T
    where
        F: AsyncFnOnce() -> T,
    {
        match self
            .get_mut_or_try_init(async || Ok::<T, Infallible>(init().await))
            .await
        {
            Ok(val) => val,
        }
    }

    /// Returns mutable access to the stored value, running a fallible `init` if the cell is empty.
    ///
    /// The exclusive borrow prevents concurrent initialization.
    ///
    /// # Errors
    ///
    /// Returns the initializer's error and leaves the cell empty.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending call drops its initializer and leaves the cell empty. A later call can
    /// start a new attempt; effects already performed by the initializer are not rolled back.
    ///
    /// # Panics
    ///
    /// An initializer panic propagates to its caller and leaves the cell empty for a later
    /// attempt. The cell is not poisoned.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::OnceCell;
    ///
    /// let mut slot: OnceCell<usize> = OnceCell::new();
    /// assert!(
    ///     slot.get_mut_or_try_init(|| async { Err("unavailable") })
    ///         .await
    ///         .is_err()
    /// );
    ///
    /// let count = slot
    ///     .get_mut_or_try_init(|| async { Ok::<_, &str>(7) })
    ///     .await
    ///     .unwrap();
    /// *count *= 3;
    /// assert_eq!(slot.get(), Some(&21));
    /// # }
    /// ```
    pub async fn get_mut_or_try_init<E, F>(&mut self, init: F) -> Result<&mut T, E>
    where
        F: AsyncFnOnce() -> Result<T, E>,
    {
        // Check the flag before borrowing so the fallback branch can call `set_value_mut`.
        if self.value.has_value_mut() {
            return Ok(self
                .value
                .get_mut()
                .expect("OnceCell initialized value missing"));
        }

        let value = init().await?;
        Ok(self.set_value_mut(value))
    }

    /// Stores `value` if the cell is empty and returns a reference to it.
    ///
    /// Waits for an active initializer before checking whether a value has already been published.
    ///
    /// # Errors
    ///
    /// Returns `Err((existing, value))` if the cell is already initialized, including when another
    /// initializer publishes a value while this call waits. The error contains a reference to the
    /// stored value and the unused input. The stored value is unchanged.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending future drops `value` without storing it. It does not cancel another
    /// caller's initialization.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::OnceCell;
    ///
    /// let slot = OnceCell::new();
    /// assert_eq!(slot.try_insert("alpha").await, Ok(&"alpha"));
    /// assert_eq!(slot.try_insert("beta").await, Err((&"alpha", "beta")));
    /// # }
    /// ```
    pub async fn try_insert(&self, value: T) -> Result<&T, (&T, T)> {
        let mut value = Some(value);
        let res = self.get_or_init(async || value.take().unwrap()).await;
        match value {
            None => Ok(res),
            Some(value) => Err((res, value)),
        }
    }

    /// Stores `value` if the cell is empty.
    ///
    /// Waits for an active initializer before checking whether a value has already been published.
    ///
    /// # Errors
    ///
    /// Returns `Err(value)` if the cell is already initialized, including when another initializer
    /// publishes a value while this call waits. The stored value is unchanged.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending future drops `value` without storing it. It does not cancel another
    /// caller's initialization.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use std::sync::Arc;
    ///
    /// use asyncband::once::OnceCell;
    ///
    /// let shared = Arc::new(OnceCell::new());
    /// let first = shared.clone();
    ///
    /// tokio::spawn(async move {
    ///     assert_eq!(first.set("winner").await, Ok(()));
    /// })
    /// .await
    /// .unwrap();
    ///
    /// assert_eq!(shared.set("runner-up").await, Err("runner-up"));
    /// assert_eq!(shared.get(), Some(&"winner"));
    /// # }
    /// ```
    pub async fn set(&self, value: T) -> Result<(), T> {
        match self.try_insert(value).await {
            Ok(_) => Ok(()),
            Err((_, value)) => Err(value),
        }
    }

    /// Consumes the cell and extracts its stored value, or [`None`] if empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::OnceCell;
    ///
    /// let empty: OnceCell<u16> = OnceCell::new();
    /// assert_eq!(empty.into_inner(), None);
    ///
    /// let populated = OnceCell::from_value(2048);
    /// assert_eq!(populated.into_inner(), Some(2048));
    /// # }
    /// ```
    pub fn into_inner(self) -> Option<T> {
        self.value.into_inner()
    }

    /// Removes the stored value, leaving the cell empty so a future call can populate it again.
    ///
    /// Returns [`None`] if the cell held no value.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::once::OnceCell;
    ///
    /// let mut slot = OnceCell::from_value(String::from("cached"));
    /// assert_eq!(slot.take().as_deref(), Some("cached"));
    /// assert_eq!(slot.take(), None);
    ///
    /// slot.set(String::from("refreshed")).await.unwrap();
    /// assert_eq!(slot.get().map(String::as_str), Some("refreshed"));
    /// # }
    /// ```
    pub fn take(&mut self) -> Option<T> {
        self.value.take()
    }

    fn set_value(&self, value: T, _permit: SemaphorePermit<'_>) -> &T {
        // SAFETY: Holding two of the three semaphore permits serializes initialization.
        unsafe { self.value.publish(value) }
    }

    fn set_value_mut(&mut self, value: T) -> &mut T {
        self.value.write_mut(value)
    }
}

impl<T: fmt::Debug> fmt::Debug for OnceCell<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_tuple("OnceCell");
        match self.get() {
            Some(v) => d.field(v),
            None => d.field(&format_args!("<uninit>")),
        };
        d.finish()
    }
}

impl<T: Clone> Clone for OnceCell<T> {
    fn clone(&self) -> OnceCell<T> {
        match self.get() {
            Some(v) => OnceCell::from_value(v.clone()),
            None => OnceCell::new(),
        }
    }
}

impl<T> From<T> for OnceCell<T> {
    fn from(value: T) -> Self {
        OnceCell::from_value(value)
    }
}

impl<T: PartialEq> PartialEq for OnceCell<T> {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}

impl<T: Eq> Eq for OnceCell<T> {}
