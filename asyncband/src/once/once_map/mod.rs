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
use std::fmt;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;
use std::sync::Arc;
use std::sync::PoisonError;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

use hashbrown::HashTable;

use crate::internal::cache_padded::CachePadded;
use crate::internal::once_map_shard_count;
use crate::once::OnceCell;

#[cfg(test)]
mod tests;

type Entries<K, V> = HashTable<Arc<Entry<K, V>>>;
type Shard<K, V> = CachePadded<RwLock<Entries<K, V>>>;

struct Entry<K, V> {
    hash: u64,
    key: K,
    cell: OnceCell<V>,
}

enum Lookup<K, V> {
    Ready(V),
    Pending(Arc<Entry<K, V>>),
}

/// A hash map that runs computation only once for each key and stores the result.
///
/// Note that this always clones the value out of the underlying map. Because of this, it's common
/// to wrap the `V` in an `Arc<V>` to make cloning cheap.
pub struct OnceMap<K, V, S = RandomState> {
    // Reads share a shard lock, while insertions and removals take it exclusively.
    shards: Box<[Shard<K, V>]>,
    hasher: S,
}

impl<K, V, S> fmt::Debug for OnceMap<K, V, S>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Write::write_str(f, "OnceMap ")?;
        let mut debug_map = f.debug_map();
        for shard in &self.shards {
            let entries = shard.read().unwrap_or_else(PoisonError::into_inner);
            debug_map.entries(entries.iter().map(|entry| (&entry.key, &entry.cell)));
        }
        debug_map.finish()
    }
}

impl<K, V, S> OnceMap<K, V, S> {
    fn read_shard(&self, hash: u64) -> RwLockReadGuard<'_, Entries<K, V>> {
        self.shards[(hash as usize) & (self.shards.len() - 1)]
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn write_shard(&self, hash: u64) -> RwLockWriteGuard<'_, Entries<K, V>> {
        self.shards[(hash as usize) & (self.shards.len() - 1)]
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.read().unwrap_or_else(PoisonError::into_inner).len())
            .sum()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.shards.iter().all(|shard| {
            shard
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        })
    }
}

impl<K, V, S> OnceMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn get_or_insert(&self, key: K) -> Lookup<K, V>
    where
        V: Clone,
    {
        let hash = self.hasher.hash_one(&key);
        let entry = {
            let shard = self.read_shard(hash);
            shard.find(hash, |entry| entry.key.eq(&key)).map(Arc::clone)
        };
        if let Some(entry) = entry {
            return Self::lookup(entry);
        }

        let entry = {
            let mut shard = self.write_shard(hash);
            shard
                .entry(hash, |entry| entry.key.eq(&key), |entry| entry.hash)
                .or_insert_with(|| {
                    Arc::new(Entry {
                        hash,
                        key,
                        cell: OnceCell::new(),
                    })
                })
                .into_mut()
                .clone()
        };

        Self::lookup(entry)
    }

    fn lookup(entry: Arc<Entry<K, V>>) -> Lookup<K, V>
    where
        V: Clone,
    {
        if let Some(value) = entry.cell.get().cloned() {
            Lookup::Ready(value)
        } else {
            Lookup::Pending(entry)
        }
    }

    fn get_value<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        V: Clone,
    {
        let hash = self.hasher.hash_one(key);
        let entry = {
            let shard = self.read_shard(hash);
            shard
                .find(hash, |entry| entry.key.borrow() == key)
                .map(Arc::clone)
        }?;
        entry.cell.get().cloned()
    }

    fn remove_entry<Q>(&self, key: &Q) -> Option<Arc<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let mut shard = self.write_shard(hash);
        let occupied = shard
            .find_entry(hash, |entry| entry.key.borrow() == key)
            .ok()?;
        let (entry, _) = occupied.remove();
        drop(shard);
        Some(entry)
    }

    fn cleanup_abandoned_entry(&self, entry: Arc<Entry<K, V>>) {
        let mut shard = self.write_shard(entry.hash);
        let Ok(occupied) = shard.find_entry(entry.hash, |stored| Arc::ptr_eq(stored, &entry))
        else {
            // A concurrent remove detached the entry. It may be the final owner, so release it
            // after unlocking rather than running user destructors under the shard lock.
            drop(shard);
            drop(entry);
            return;
        };

        // With map ownership confirmed and the shard locked against new callers, two owners means
        // the map and this cleanup guard are the only remaining references.
        if Arc::strong_count(&entry) == 2 && !entry.cell.initialized() {
            let (stored, _) = occupied.remove();
            drop(shard);
            drop(entry);
            drop(stored);
        } else {
            // A waiting cleanup must observe this call's reference being released while no new
            // caller can clone the map's reference.
            drop(entry);
        }
    }

    fn insert(&self, key: K, value: V) {
        let hash = self.hasher.hash_one(&key);
        let entry = Arc::new(Entry {
            hash,
            key,
            cell: OnceCell::from_value(value),
        });

        let mut shard = self.write_shard(hash);
        let replaced = shard
            .find_entry(hash, |stored| stored.key.eq(&entry.key))
            .ok()
            .map(|occupied| occupied.remove().0);
        shard.insert_unique(hash, entry, |entry| entry.hash);
        drop(shard);
        drop(replaced);
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
        let map = Self::with_capacity_and_hasher(iter.size_hint().0, S::default());
        for (key, value) in iter {
            map.insert(key, value);
        }

        map
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
        Self::with_capacity(0)
    }

    /// Creates a new OnceMap with the default hasher and the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, RandomState::new())
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
        Self::with_capacity_and_hasher(0, hasher)
    }

    /// Creates a new OnceMap with the specified capacity and hasher.
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        let shard_count = once_map_shard_count();
        let shard_capacity = capacity / shard_count;
        let extra_capacity = capacity % shard_count;
        let shards = (0..shard_count)
            .map(|shard_index| {
                let capacity = shard_capacity + usize::from(shard_index < extra_capacity);
                CachePadded::new(RwLock::new(HashTable::with_capacity(capacity)))
            })
            .collect();

        Self { shards, hasher }
    }

    /// Compute the value for the given key if absent.
    ///
    /// If the value for the key is already being computed by another task, this task will wait for
    /// the computation to finish and return the result.
    ///
    /// If the computation is cancelled or panics, another caller waiting for the same key may retry
    /// it.
    pub async fn compute<F>(&self, key: K, func: F) -> V
    where
        F: AsyncFnOnce() -> V,
    {
        let entry = match self.get_or_insert(key) {
            Lookup::Ready(value) => return value,
            Lookup::Pending(entry) => entry,
        };

        let guard = ComputeCleanupGuard::new(self, entry);
        let result = guard.entry().cell.get_or_init(func).await.clone();
        guard.dismiss();
        result
    }

    /// Compute the value for the given key if absent.
    ///
    /// If the value for the key is already being computed by another task, this task will wait for
    /// the computation to finish and return the result.
    ///
    /// If the computation returns an error, it is returned to that caller and the value is not
    /// stored. After an error, cancellation, or panic, another caller may retry the computation.
    pub async fn try_compute<E, F>(&self, key: K, func: F) -> Result<V, E>
    where
        F: AsyncFnOnce() -> Result<V, E>,
    {
        let entry = match self.get_or_insert(key) {
            Lookup::Ready(value) => return Ok(value),
            Lookup::Pending(entry) => entry,
        };

        let guard = ComputeCleanupGuard::new(self, entry);
        let result = guard.entry().cell.get_or_try_init(func).await?.clone();
        guard.dismiss();
        Ok(result)
    }

    /// Get a clone of the value for the given key if exists.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get_value(key)
    }

    /// Remove the given key from the map.
    ///
    /// If you need to get the value that has been removed, use the [`remove`] method instead.
    ///
    /// [`remove`]: Self::remove
    pub fn discard<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.remove_entry(key);
    }

    /// Remove the given key from the map and return a *clone* of the value if exists.
    ///
    /// If you do not need to get the value that has been removed, use the [`discard`] method
    /// instead.
    ///
    /// [`discard`]: Self::discard
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let entry = self.remove_entry(key)?;
        entry.cell.get().cloned()
    }
}
