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
use std::hash::BuildHasherDefault;
use std::hash::Hash;
use std::hash::Hasher;
use std::hash::RandomState;
use std::panic::UnwindSafe;
use std::sync::Arc;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use arc_swap::ArcSwap;
use hashbrown::HashTable;
use scc::HashIndex;
use scc::hash_index::Entry as IndexEntry;

use crate::internal::cache_padded::CachePadded;
use crate::internal::mutex::Mutex;
use crate::internal::once_map_shard_count;
use crate::once::OnceCell;

#[cfg(test)]
mod tests;

type Entries<K, V> = HashTable<Arc<Entry<K, V>>>;
type Shard<K, V> = CachePadded<Mutex<Entries<K, V>>>;

struct Entry<K, V> {
    hash: u64,
    key: K,
    cell: OnceCell<V>,
    // Accessed only while the corresponding mutation shard is locked. Atomic interior mutability
    // keeps `Entry` shareable with the ready index.
    indexed: AtomicBool,
}

enum Lookup<K, V> {
    Ready(V),
    Pending(Arc<Entry<K, V>>),
}

struct ReadyIndex<K, V> {
    slots: HashIndex<u64, ReadySlot<K, V>, BuildIdentityHasher>,
}

// Entries with the same user-provided hash share an immutable snapshot. Mutations are serialized by
// the corresponding primary shard, while ready readers never write shared state.
struct ReadySlot<K, V> {
    entries: ArcSwap<Vec<Arc<Entry<K, V>>>>,
}

impl<K: Eq, V> ReadySlot<K, V> {
    fn new(entry: &Arc<Entry<K, V>>) -> Self {
        Self {
            entries: ArcSwap::from_pointee(vec![Arc::clone(entry)]),
        }
    }

    fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
        V: Clone,
    {
        self.entries
            .load()
            .iter()
            .find(|entry| entry.key.borrow() == key)
            .and_then(|entry| entry.cell.get().cloned())
    }

    fn insert(&self, entry: &Arc<Entry<K, V>>) {
        let mut entries = (**self.entries.load()).clone();
        let replaced =
            if let Some(stored) = entries.iter_mut().find(|stored| stored.key == entry.key) {
                if Arc::ptr_eq(stored, entry) {
                    return;
                }
                Some(std::mem::replace(stored, Arc::clone(entry)))
            } else {
                entries.push(Arc::clone(entry));
                None
            };

        let previous = self.entries.swap(Arc::new(entries));
        drop(previous);
        drop(replaced);
    }

    fn remove(&self, entry: &Arc<Entry<K, V>>) -> bool {
        let mut entries = (**self.entries.load()).clone();
        let Some(index) = entries.iter().position(|stored| Arc::ptr_eq(stored, entry)) else {
            return false;
        };
        let removed = entries.swap_remove(index);
        let empty = entries.is_empty();

        let previous = self.entries.swap(Arc::new(entries));
        drop(previous);
        drop(removed);
        empty
    }

    fn is_empty(&self) -> bool {
        self.entries.load().is_empty()
    }
}

type BuildIdentityHasher = BuildHasherDefault<IdentityHasher>;

// The outer index is keyed by the hash already computed with the map's hasher.
#[derive(Default)]
struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(*byte);
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

impl<K: Eq, V> ReadyIndex<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            slots: HashIndex::with_capacity_and_hasher(capacity, BuildIdentityHasher::default()),
        }
    }

    fn get<Q>(&self, hash: u64, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
        V: Clone,
    {
        self.slots
            .peek_with(&hash, |_, slot| slot.get(key))
            .flatten()
    }

    fn insert(&self, entry: &Arc<Entry<K, V>>) {
        match self.slots.entry_sync(entry.hash) {
            IndexEntry::Occupied(occupied) => occupied.get().insert(entry),
            IndexEntry::Vacant(vacant) => {
                vacant.insert_entry(ReadySlot::new(entry));
            }
        }
    }

    fn remove(&self, entry: &Arc<Entry<K, V>>) {
        let became_empty = self
            .slots
            .peek_with(&entry.hash, |_, slot| slot.remove(entry))
            .unwrap_or(false);
        if became_empty {
            // HashIndex may reclaim its node later. Empty the snapshot first so deferred metadata
            // cannot keep the user's key or value alive after removal.
            self.slots.remove_if_sync(&entry.hash, ReadySlot::is_empty);
        }
    }
}

/// A hash map that runs computation only once for each key and stores the result.
///
/// Note that this always clones the value out of the underlying map. Because of this, it's common
/// to wrap the `V` in an `Arc<V>` to make cloning cheap.
pub struct OnceMap<K, V, S = RandomState> {
    // Pending computations and mutations are coordinated through these shards.
    shards: Box<[Shard<K, V>]>,
    // Initialized entries are mirrored here so hit-only reads avoid mutation-shard contention.
    ready: ReadyIndex<K, V>,
    hasher: S,
}

// HashIndex does not infer UnwindSafe for arbitrary entries. OnceMap recovers poisoned mutation
// shards and only publishes immutable initialized entries to the ready index.
impl<K, V, S: UnwindSafe> UnwindSafe for OnceMap<K, V, S> {}

impl<K, V, S> fmt::Debug for OnceMap<K, V, S>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Write::write_str(f, "OnceMap ")?;
        let mut debug_map = f.debug_map();
        for shard in &self.shards {
            let entries = shard.lock();
            debug_map.entries(entries.iter().map(|entry| (&entry.key, &entry.cell)));
        }
        debug_map.finish()
    }
}

impl<K, V, S> OnceMap<K, V, S> {
    fn lock_shard(&self, hash: u64) -> MutexGuard<'_, Entries<K, V>> {
        self.shards[(hash as usize) & (self.shards.len() - 1)].lock()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock().len()).sum()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.shards.iter().all(|shard| shard.lock().is_empty())
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
        if let Some(value) = self.ready.get(hash, &key) {
            return Lookup::Ready(value);
        }

        let entry = {
            let mut shard = self.lock_shard(hash);
            shard
                .entry(hash, |entry| entry.key.eq(&key), |entry| entry.hash)
                .or_insert_with(|| {
                    Arc::new(Entry {
                        hash,
                        key,
                        cell: OnceCell::new(),
                        indexed: AtomicBool::new(false),
                    })
                })
                .into_mut()
                .clone()
        };

        match self.index_if_ready(&entry) {
            Some(value) => Lookup::Ready(value),
            None => Lookup::Pending(entry),
        }
    }

    fn get_value<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        V: Clone,
    {
        let hash = self.hasher.hash_one(key);

        if let Some(value) = self.ready.get(hash, key) {
            Some(value)
        } else {
            let entry = self
                .lock_shard(hash)
                .find(hash, |entry| entry.key.borrow() == key)
                .map(Arc::clone)?;

            self.index_if_ready(&entry)
        }
    }

    fn index_if_ready(&self, entry: &Arc<Entry<K, V>>) -> Option<V>
    where
        V: Clone,
    {
        let value = entry.cell.get()?.clone();
        self.index_if_current(entry);
        Some(value)
    }

    fn index_if_current(&self, entry: &Arc<Entry<K, V>>) {
        let shard = self.lock_shard(entry.hash);
        if !entry.indexed.load(Ordering::Relaxed)
            && shard
                .find(entry.hash, |stored| Arc::ptr_eq(stored, entry))
                .is_some()
        {
            self.index(entry);
        }
    }

    fn remove_entry<Q>(&self, key: &Q) -> Option<Arc<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let mut shard = self.lock_shard(hash);

        let occupied = shard
            .find_entry(hash, |entry| entry.key.borrow() == key)
            .ok()?;
        let (entry, _) = occupied.remove();

        self.remove_from_index(&entry);
        Some(entry)
    }

    fn cleanup_abandoned_entry(&self, entry: Arc<Entry<K, V>>) {
        let mut shard = self.lock_shard(entry.hash);
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
            indexed: AtomicBool::new(false),
        });

        let mut shard = self.lock_shard(hash);
        let replaced = shard
            .find_entry(hash, |stored| stored.key.eq(&entry.key))
            .ok()
            .map(|occupied| occupied.remove().0);
        if let Some(replaced) = &replaced {
            self.remove_from_index(replaced);
        }
        shard.insert_unique(hash, Arc::clone(&entry), |entry| entry.hash);

        self.index(&entry);
        drop(shard);
        drop(replaced);
    }

    fn index(&self, entry: &Arc<Entry<K, V>>) {
        self.ready.insert(entry);
        entry.indexed.store(true, Ordering::Relaxed);
    }

    fn remove_from_index(&self, entry: &Arc<Entry<K, V>>) {
        if entry.indexed.load(Ordering::Relaxed) {
            self.ready.remove(entry);
        }
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
                CachePadded::new(Mutex::new(HashTable::with_capacity(capacity)))
            })
            .collect();

        Self {
            shards,
            ready: ReadyIndex::new(capacity),
            hasher,
        }
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
