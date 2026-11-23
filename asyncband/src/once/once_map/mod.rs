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

use std::borrow::Borrow;
use std::convert::Infallible;
use std::fmt;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;

use crate::internal::coalesce::EntryTable;

#[cfg(test)]
mod tests;

/// A hash map that runs computation only once for each key and stores the result.
///
/// Every successful lookup returns an owned clone of `V`. Choose a cheaply cloned value type, such
/// as `Arc<T>`, when entries are large or frequently shared.
pub struct OnceMap<K, V, S = RandomState> {
    table: EntryTable<K, V, S>,
}

impl<K, V, S> fmt::Debug for OnceMap<K, V, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (len, pending) = self.table.len_and_pending();
        f.debug_struct("OnceMap")
            .field("len", &len)
            .field("pending", &pending)
            .finish()
    }
}

impl<K, V, S> FromIterator<(K, V)> for OnceMap<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: BuildHasher + Default,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self {
            table: EntryTable::from_populated_iter(iter),
        }
    }
}

impl<K, V, S> Default for OnceMap<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: BuildHasher + Default,
{
    fn default() -> Self {
        Self::with_hasher(S::default())
    }
}

impl<K, V> OnceMap<K, V, RandomState>
where
    K: Eq + Hash,
    V: Clone,
{
    /// Creates a new OnceMap with the default hasher.
    pub fn new() -> Self {
        Self::with_hasher(RandomState::new())
    }
}

impl<K, V, S> OnceMap<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: BuildHasher,
{
    /// Creates a new OnceMap with the given hasher.
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            table: EntryTable::with_hasher(hasher),
        }
    }

    /// Returns a clone of the stored or newly computed value for `key`.
    ///
    /// A stored value is returned without invoking `func`. Otherwise, callers for an equivalent
    /// key share an active computation and receive clones of its successful result. Values are
    /// cached until removed. Removing an in-flight entry detaches it: existing callers still
    /// receive its result, but that result is not cached in the map.
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
    /// The computation must not await `compute` or `try_compute` for an equivalent key on
    /// the same current entry, because it would wait for its own result. Other keys are
    /// independent.
    pub async fn compute<F>(&self, key: K, func: F) -> V
    where
        F: AsyncFnOnce() -> V,
    {
        match self
            .try_compute(key, async || Ok::<V, Infallible>(func().await))
            .await
        {
            Ok(value) => value,
        }
    }

    /// Returns a clone of the stored or newly computed value for `key`.
    ///
    /// A stored value is returned without invoking `func`. Otherwise, callers for an equivalent
    /// key share an active computation and receive clones of its successful result. Values are
    /// cached until removed. Removing an in-flight entry detaches it: existing callers still
    /// receive its result, but that result is not cached in the map.
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
    /// The computation must not await `compute` or `try_compute` for an equivalent key on
    /// the same current entry, because it would wait for its own result. Other keys are
    /// independent.
    pub async fn try_compute<E, F>(&self, key: K, func: F) -> Result<V, E>
    where
        F: AsyncFnOnce() -> Result<V, E>,
    {
        let guard = match self.table.get_or_insert_cached(key) {
            Ok(value) => return Ok(value),
            Err(guard) => guard,
        };
        Ok(guard.entry().cell.get_or_try_init(func).await?.clone())
    }

    /// Returns a clone of the value for the given key without waiting.
    ///
    /// Returns `None` when the key is absent or its computation is still in flight.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let entry = self.table.find_ready(key)?;
        entry.cell.get().cloned()
    }

    /// Evicts `key` from the map without cloning any stored value.
    ///
    /// Detaching an in-flight entry leaves callers that already joined that attempt unaffected,
    /// while preventing its result from being stored in the map and allowing later calls to start
    /// fresh work. Use [`remove`](Self::remove) when a clone of a completed value is needed.
    pub fn discard<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        drop(self.table.remove(key));
    }

    /// Evicts `key` from the map, returning a clone of its value if computation had completed.
    ///
    /// Returns `None` when `key` is absent or its computation is still in flight. Detaching an
    /// in-flight entry leaves callers that already joined that attempt unaffected while preventing
    /// its result from being stored in the map. Use [`discard`](Self::discard) to evict an entry
    /// without cloning its value.
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let entry = self.table.remove(key)?;
        entry.cell.get().cloned()
    }
}
