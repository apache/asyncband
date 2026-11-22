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
use std::sync::Arc;

use hashbrown::HashTable;

use crate::internal::mutex::Mutex;
use crate::once::OnceCell;

#[cfg(test)]
mod tests;

struct Entry<K, V> {
    hash: u64,
    key: K,
    cell: OnceCell<V>,
}

enum Lookup<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    Ready(V),
    Pending(ComputeCleanupGuard<'a, K, V, S>),
}

/// A hash map that runs computation only once for each key and stores the result.
///
/// Every successful lookup returns an owned clone of `V`. Choose a cheaply cloned value type, such
/// as `Arc<T>`, when entries are large or frequently shared.
pub struct OnceMap<K, V, S = RandomState> {
    // Hashbrown allocates the table lazily, and computation always runs after releasing this lock.
    entries: Mutex<HashTable<Arc<Entry<K, V>>>>,
    hasher: S,
}

impl<K, V, S> fmt::Debug for OnceMap<K, V, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (len, pending) = {
            let entries = self.entries.lock();
            let pending = entries
                .iter()
                .filter(|entry| !entry.cell.initialized())
                .count();
            (entries.len(), pending)
        };
        f.debug_struct("OnceMap")
            .field("len", &len)
            .field("pending", &pending)
            .finish()
    }
}

impl<K, V, S> OnceMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn get_or_insert(&self, key: K) -> Lookup<'_, K, V, S>
    where
        V: Clone,
    {
        let hash = self.hasher.hash_one(&key);
        let mut entries = self.entries.lock();
        if let Some(entry) = entries.find(hash, |entry| entry.key.eq(&key)) {
            let entry = Arc::clone(entry);
            drop(entries);
            let lookup = self.classify(entry);
            // Own the value or cleanup guard before a duplicate key can unwind or reenter.
            drop(key);
            return lookup;
        }

        let entry = Arc::new(Entry {
            hash,
            key,
            cell: OnceCell::new(),
        });
        entries.insert_unique(hash, Arc::clone(&entry), |entry| entry.hash);
        drop(entries);
        self.classify(entry)
    }

    fn classify(&self, entry: Arc<Entry<K, V>>) -> Lookup<'_, K, V, S>
    where
        V: Clone,
    {
        match entry.cell.get().cloned() {
            Some(value) => Lookup::Ready(value),
            None => Lookup::Pending(ComputeCleanupGuard::new(self, entry)),
        }
    }

    fn find_ready_entry<Q>(&self, key: &Q) -> Option<Arc<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        // A reader holding a pending entry would stop an abandoned computation from reclaiming it.
        self.entries
            .lock()
            .find(hash, |entry| entry.key.borrow() == key)
            .filter(|entry| entry.cell.initialized())
            .cloned()
    }

    fn remove_entry<Q>(&self, key: &Q) -> Option<Arc<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let mut entries = self.entries.lock();
        let occupied = entries
            .find_entry(hash, |entry| entry.key.borrow() == key)
            .ok()?;
        Some(occupied.remove().0)
    }

    fn cleanup_abandoned_entry(&self, entry: Arc<Entry<K, V>>) {
        let removed = {
            let mut entries = self.entries.lock();
            let Ok(occupied) = entries.find_entry(entry.hash, |stored| Arc::ptr_eq(stored, &entry))
            else {
                drop(entries);
                drop(entry);
                return;
            };

            // With table ownership confirmed and new callers excluded, two owners means the table
            // and this cleanup guard are the only remaining references.
            if Arc::strong_count(&entry) == 2 && !entry.cell.initialized() {
                Some(occupied.remove().0)
            } else {
                // A waiting cleanup must observe this call's reference being released before it
                // can inspect the count while holding the table lock.
                drop(entry);
                None
            }
        };
        // Key and value destructors must not run while the table is locked.
        drop(removed);
    }
}

impl<K, V, S> FromIterator<(K, V)> for OnceMap<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: BuildHasher + Default,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let hasher = S::default();
        let mut entries = HashTable::<Arc<Entry<K, V>>>::with_capacity(iter.size_hint().0);
        for (key, value) in iter {
            let hash = hasher.hash_one(&key);
            if let Some(stored) = entries.find_mut(hash, |stored| stored.key.eq(&key)) {
                let stored = Arc::get_mut(stored).expect("building entry must be unique");
                stored.hash = hash;
                stored.key = key;
                stored.cell = OnceCell::from_value(value);
            } else {
                entries.insert_unique(
                    hash,
                    Arc::new(Entry {
                        hash,
                        key,
                        cell: OnceCell::from_value(value),
                    }),
                    |stored| stored.hash,
                );
            }
        }
        Self {
            entries: Mutex::new(entries),
            hasher,
        }
    }
}

// Holds one call's entry so Drop can clean it up if the computation is abandoned.
struct ComputeCleanupGuard<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    once_map: &'a OnceMap<K, V, S>,
    entry: Option<Arc<Entry<K, V>>>,
}

impl<'a, K, V, S> ComputeCleanupGuard<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(once_map: &'a OnceMap<K, V, S>, entry: Arc<Entry<K, V>>) -> Self {
        Self {
            once_map,
            entry: Some(entry),
        }
    }

    fn entry(&self) -> &Arc<Entry<K, V>> {
        self.entry.as_ref().unwrap()
    }

    fn dismiss(mut self) {
        drop(self.entry.take());
    }
}

impl<K, V, S> Drop for ComputeCleanupGuard<'_, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        let Some(entry) = self.entry.take() else {
            return;
        };
        self.once_map.cleanup_abandoned_entry(entry);
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
            entries: Mutex::new(HashTable::new()),
            hasher,
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
        let guard = match self.get_or_insert(key) {
            Lookup::Ready(value) => return Ok(value),
            Lookup::Pending(guard) => guard,
        };

        let result = guard.entry().cell.get_or_try_init(func).await?.clone();
        guard.dismiss();
        Ok(result)
    }

    /// Returns a clone of the value for the given key without waiting.
    ///
    /// Returns `None` when the key is absent or its computation is still in flight.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let entry = self.find_ready_entry(key)?;
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
        drop(self.remove_entry(key));
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
        let entry = self.remove_entry(key)?;
        entry.cell.get().cloned()
    }
}
