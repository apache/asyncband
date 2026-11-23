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

//! Coalesce concurrent work that uses the same key.
//!
//! While work for a key is in flight, later callers wait and clone its successful value. A
//! completed value is not cached: the entry is removed when the work succeeds, so a non-overlapping
//! later call executes its own function.
//!
//! If the active call is cancelled or panics, a waiting caller may run its own function. `try_work`
//! also lets a waiter retry after the active function returns an error. Use `OnceMap` instead when
//! successful values should remain cached by key.

use std::borrow::Borrow;
use std::convert::Infallible;
use std::fmt;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;

use crate::internal::coalesce::EntryTable;

#[cfg(test)]
mod tests;

/// A namespace that coalesces concurrent work by key.
///
/// Different keys run independently. A group neither caches completed results nor limits overall
/// concurrency.
pub struct Group<K, V, S = RandomState> {
    table: EntryTable<K, V, S>,
}

impl<K, V, S> fmt::Debug for Group<K, V, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Group")
            .field("in_flight", &self.table.len())
            .finish()
    }
}

impl<K, V, S> Default for Group<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: BuildHasher + Default,
{
    fn default() -> Self {
        Self::with_hasher(S::default())
    }
}

impl<K, V> Group<K, V, RandomState>
where
    K: Eq + Hash,
    V: Clone,
{
    /// Creates a new Group with the default hasher.
    pub fn new() -> Self {
        Self::with_hasher(RandomState::new())
    }
}

impl<K, V, S> Group<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: BuildHasher,
{
    /// Creates a new Group with the given hasher.
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            table: EntryTable::with_hasher(hasher),
        }
    }

    /// Returns a clone of the shared computation's value for `key`.
    ///
    /// Callers that join the same in-flight entry share its successful result. Once `func`
    /// completes successfully, the entry is removed so later calls can start new work.
    /// [`forget`](Self::forget) lets new calls start independently of an existing entry.
    ///
    /// # Cancellation
    ///
    /// Dropping a caller that is waiting for a result does not cancel the active computation.
    /// Dropping the caller executing `func` drops that attempt; another caller on the entry may
    /// run its own `func`. Effects already performed by the cancelled attempt are not rolled back.
    ///
    /// # Panics
    ///
    /// A panic in `func` propagates to its caller without publishing a value. Another caller may
    /// retry the computation.
    ///
    /// # Deadlocks
    ///
    /// The computation must not await `work` or `try_work` for an equivalent key on
    /// the same current entry, because it would wait for its own result. Other keys are
    /// independent.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::AtomicUsize;
    /// use std::sync::atomic::Ordering;
    ///
    /// use asyncband::singleflight::Group;
    /// use tokio::sync::oneshot;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let group = Arc::new(Group::new());
    /// let runs = Arc::new(AtomicUsize::new(0));
    /// let (started_tx, started_rx) = oneshot::channel();
    /// let (release_tx, release_rx) = oneshot::channel();
    ///
    /// let leader = {
    ///     let group = group.clone();
    ///     let runs = runs.clone();
    ///     tokio::spawn(async move {
    ///         group
    ///             .work("manifest", || async move {
    ///                 runs.fetch_add(1, Ordering::SeqCst);
    ///                 started_tx.send(()).unwrap();
    ///                 release_rx.await.unwrap();
    ///                 "v1"
    ///             })
    ///             .await
    ///     })
    /// };
    ///
    /// started_rx.await.unwrap();
    ///
    /// let follower = {
    ///     let group = group.clone();
    ///     let runs = runs.clone();
    ///     tokio::spawn(async move {
    ///         group
    ///             .work("manifest", || async move {
    ///                 runs.fetch_add(1, Ordering::SeqCst);
    ///                 "unused"
    ///             })
    ///             .await
    ///     })
    /// };
    ///
    /// tokio::task::yield_now().await;
    /// release_tx.send(()).unwrap();
    ///
    /// assert_eq!(leader.await.unwrap(), "v1");
    /// assert_eq!(follower.await.unwrap(), "v1");
    /// assert_eq!(runs.load(Ordering::SeqCst), 1);
    ///
    /// assert_eq!(group.work("manifest", || async { "v2" }).await, "v2");
    /// # }
    /// ```
    pub async fn work<F>(&self, key: K, func: F) -> V
    where
        F: AsyncFnOnce() -> V,
    {
        match self
            .try_work(key, async || Ok::<V, Infallible>(func().await))
            .await
        {
            Ok(value) => value,
        }
    }

    /// Returns a clone of the shared computation's value for `key`.
    ///
    /// Callers that join the same in-flight entry share its successful result. Once `func`
    /// completes successfully, the entry is removed so later calls can start new work.
    /// [`forget`](Self::forget) lets new calls start independently of an existing entry.
    ///
    /// # Errors
    ///
    /// Returns an error from this call's `func` only to this caller. Errors are not stored or
    /// shared; another caller waiting on the entry may run its own `func`.
    ///
    /// # Cancellation
    ///
    /// Dropping a caller that is waiting for a result does not cancel the active computation.
    /// Dropping the caller executing `func` drops that attempt; another caller on the entry may
    /// run its own `func`. Effects already performed by the cancelled attempt are not rolled back.
    ///
    /// # Panics
    ///
    /// A panic in `func` propagates to its caller without publishing a value. Another caller may
    /// retry the computation.
    ///
    /// # Deadlocks
    ///
    /// The computation must not await `work` or `try_work` for an equivalent key on
    /// the same current entry, because it would wait for its own result. Other keys are
    /// independent.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use asyncband::singleflight::Group;
    /// use tokio::sync::oneshot;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let group = Arc::new(Group::new());
    /// let (started_tx, started_rx) = oneshot::channel();
    /// let (release_tx, release_rx) = oneshot::channel();
    ///
    /// let leader = {
    ///     let group = group.clone();
    ///     tokio::spawn(async move {
    ///         group
    ///             .try_work("manifest", || async move {
    ///                 started_tx.send(()).unwrap();
    ///                 release_rx.await.unwrap();
    ///                 Err::<&str, &str>("transient")
    ///             })
    ///             .await
    ///     })
    /// };
    ///
    /// started_rx.await.unwrap();
    ///
    /// let follower = {
    ///     let group = group.clone();
    ///     tokio::spawn(async move {
    ///         group
    ///             .try_work("manifest", || async { Ok::<&str, &str>("recovered") })
    ///             .await
    ///     })
    /// };
    ///
    /// tokio::task::yield_now().await;
    /// release_tx.send(()).unwrap();
    ///
    /// assert_eq!(leader.await.unwrap(), Err("transient"));
    /// assert_eq!(follower.await.unwrap(), Ok("recovered"));
    /// # }
    /// ```
    pub async fn try_work<E, F>(&self, key: K, func: F) -> Result<V, E>
    where
        F: AsyncFnOnce() -> Result<V, E>,
    {
        let guard = self.table.get_or_insert(key);
        let entry = guard.entry();
        Ok(entry
            .cell
            .get_or_try_init(async || {
                let result = func().await?;
                self.table.remove_if_current(entry);
                Ok(result)
            })
            .await?
            .clone())
    }

    /// Detaches any in-flight entry for `key` from the group.
    ///
    /// Callers that already joined the detached entry continue waiting for its outcome, while later
    /// calls to [`work`](Self::work) or [`try_work`](Self::try_work) for an equivalent key start a
    /// separate entry immediately. Has no effect if `key` has no active entry.
    pub fn forget<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        drop(self.table.remove(key));
    }
}
