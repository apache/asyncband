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
use std::mem;
use std::sync::Arc;
use std::sync::MutexGuard;
use std::sync::OnceLock;

use hashbrown::HashTable;

use crate::internal::mutex::Mutex;
use crate::once::OnceCell;

#[cfg(test)]
mod tests;

type Entries<K, V> = HashTable<Arc<Entry<K, V>>>;

const TRIE_BITS: u32 = 4;
const TRIE_FANOUT: usize = 1 << TRIE_BITS;
const TRIE_MASK: u64 = TRIE_FANOUT as u64 - 1;
// A full leaf can populate the next node under a well-distributed hash without allocating trie
// nodes for very small maps.
const MAX_LEAF_ENTRIES: usize = TRIE_FANOUT;

struct TrieTable<K, V> {
    root: OnceLock<Box<TrieNode<K, V>>>,
}

struct TrieNode<K, V> {
    slots: [TrieSlot<K, V>; TRIE_FANOUT],
}

struct TrieSlot<K, V> {
    child: OnceLock<Box<TrieNode<K, V>>>,
    entries: Mutex<Entries<K, V>>,
}

struct LockedLeaf<'a, K, V> {
    slot: &'a TrieSlot<K, V>,
    shift: u32,
    entries: MutexGuard<'a, Entries<K, V>>,
}

impl<K, V> TrieTable<K, V> {
    fn with_capacity(capacity: usize) -> Self {
        let root = OnceLock::new();
        if capacity != 0 {
            root.get_or_init(|| Box::new(TrieNode::with_capacity(capacity, 0)));
        }
        Self { root }
    }

    fn lock_leaf(&self, hash: u64) -> LockedLeaf<'_, K, V> {
        let root = self.root.get_or_init(|| Box::new(TrieNode::empty()));
        Self::lock_leaf_from(root, hash)
    }

    fn try_lock_leaf(&self, hash: u64) -> Option<LockedLeaf<'_, K, V>> {
        self.root.get().map(|root| Self::lock_leaf_from(root, hash))
    }

    fn lock_leaf_from(mut node: &TrieNode<K, V>, hash: u64) -> LockedLeaf<'_, K, V> {
        let mut shift = 0;
        loop {
            let slot = node.slot(hash, shift);
            if let Some(child) = slot.child.get() {
                node = child;
                shift += TRIE_BITS;
                continue;
            }

            let entries = slot.entries.lock();
            if let Some(child) = slot.child.get() {
                drop(entries);
                node = child;
                shift += TRIE_BITS;
                continue;
            }

            return LockedLeaf {
                slot,
                shift,
                entries,
            };
        }
    }

    fn split_if_needed(&self, leaf: LockedLeaf<'_, K, V>) {
        TrieNode::split_leaf(leaf.slot, leaf.shift, leaf.entries);
    }

    fn for_each(&self, mut visit: impl FnMut(&Arc<Entry<K, V>>)) {
        if let Some(root) = self.root.get() {
            root.for_each(&mut visit);
        }
    }
}

impl<K, V> TrieNode<K, V> {
    fn empty() -> Self {
        Self {
            slots: std::array::from_fn(|_| TrieSlot {
                child: OnceLock::new(),
                entries: Mutex::new(HashTable::new()),
            }),
        }
    }

    fn with_capacity(capacity: usize, shift: u32) -> Self {
        let slot_capacity = capacity / TRIE_FANOUT;
        let extra_capacity = capacity % TRIE_FANOUT;
        Self {
            slots: std::array::from_fn(|index| {
                let capacity = slot_capacity + usize::from(index < extra_capacity);
                let child = OnceLock::new();
                let entries = if capacity > MAX_LEAF_ENTRIES && shift + TRIE_BITS < u64::BITS {
                    child
                        .get_or_init(|| Box::new(Self::with_capacity(capacity, shift + TRIE_BITS)));
                    HashTable::new()
                } else {
                    HashTable::with_capacity(capacity)
                };
                TrieSlot {
                    child,
                    entries: Mutex::new(entries),
                }
            }),
        }
    }

    fn slot(&self, hash: u64, shift: u32) -> &TrieSlot<K, V> {
        &self.slots[((hash >> shift) & TRIE_MASK) as usize]
    }

    fn split_leaf(slot: &TrieSlot<K, V>, shift: u32, mut entries: MutexGuard<'_, Entries<K, V>>) {
        if entries.len() <= MAX_LEAF_ENTRIES || shift + TRIE_BITS >= u64::BITS {
            return;
        }

        let Some(first) = entries.iter().next() else {
            return;
        };
        if entries.iter().all(|entry| entry.hash == first.hash) {
            return;
        }

        let child_shift = shift + TRIE_BITS;
        let child = Box::new(Self::empty());
        for entry in mem::take(&mut *entries) {
            let child_slot = child.slot(entry.hash, child_shift);
            child_slot
                .entries
                .lock()
                .insert_unique(entry.hash, entry, |entry| entry.hash);
        }
        child.split_overfull_leaves(child_shift);

        // The leaf lock serializes publication. Readers that observed no child recheck after
        // acquiring that lock, so they cannot consult the now-empty leaf after this succeeds.
        assert!(
            slot.child.set(child).is_ok(),
            "the leaf lock must serialize trie splits"
        );
    }

    fn split_overfull_leaves(&self, shift: u32) {
        for slot in &self.slots {
            Self::split_leaf(slot, shift, slot.entries.lock());
        }
    }

    fn for_each(&self, visit: &mut impl FnMut(&Arc<Entry<K, V>>)) {
        for slot in &self.slots {
            if let Some(child) = slot.child.get() {
                child.for_each(visit);
                continue;
            }

            let entries = slot.entries.lock();
            if let Some(child) = slot.child.get() {
                drop(entries);
                child.for_each(visit);
                continue;
            }
            for entry in entries.iter() {
                visit(entry);
            }
        }
    }
}

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
    // The trie allocates its root on first insertion and splits only occupied hash prefixes.
    entries: TrieTable<K, V>,
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
        self.entries.for_each(|entry| {
            debug_map.entry(&entry.key, &entry.cell);
        });
        debug_map.finish()
    }
}

impl<K, V, S> OnceMap<K, V, S> {
    #[cfg(test)]
    fn len(&self) -> usize {
        let mut len = 0;
        self.entries.for_each(|_| len += 1);
        len
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.len() == 0
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
        let mut leaf = self.entries.lock_leaf(hash);
        if let Some(entry) = leaf.entries.find(hash, |entry| entry.key.eq(&key)) {
            return if let Some(value) = entry.cell.get().cloned() {
                Lookup::Ready(value)
            } else {
                Lookup::Pending(Arc::clone(entry))
            };
        }

        let entry = Arc::new(Entry {
            hash,
            key,
            cell: OnceCell::new(),
        });
        leaf.entries
            .insert_unique(hash, Arc::clone(&entry), |entry| entry.hash);
        self.entries.split_if_needed(leaf);
        Lookup::Pending(entry)
    }

    fn get_value<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        V: Clone,
    {
        let hash = self.hasher.hash_one(key);
        let leaf = self.entries.try_lock_leaf(hash)?;
        let entry = leaf.entries.find(hash, |entry| entry.key.borrow() == key)?;
        entry.cell.get().cloned()
    }

    fn remove_entry<Q>(&self, key: &Q) -> Option<Arc<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let mut leaf = self.entries.try_lock_leaf(hash)?;
        let occupied = leaf
            .entries
            .find_entry(hash, |entry| entry.key.borrow() == key)
            .ok()?;
        let (entry, _) = occupied.remove();
        drop(leaf);
        Some(entry)
    }

    fn cleanup_abandoned_entry(&self, entry: Arc<Entry<K, V>>) {
        let Some(mut leaf) = self.entries.try_lock_leaf(entry.hash) else {
            drop(entry);
            return;
        };
        let Ok(occupied) = leaf
            .entries
            .find_entry(entry.hash, |stored| Arc::ptr_eq(stored, &entry))
        else {
            // A concurrent remove detached the entry. It may be the final owner, so release it
            // after unlocking rather than running user destructors under the leaf lock.
            drop(leaf);
            drop(entry);
            return;
        };

        // With map ownership confirmed and the leaf locked against new callers, two owners means
        // the map and this cleanup guard are the only remaining references.
        if Arc::strong_count(&entry) == 2 && !entry.cell.initialized() {
            let (stored, _) = occupied.remove();
            drop(leaf);
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

        let mut leaf = self.entries.lock_leaf(hash);
        let replaced = leaf
            .entries
            .find_entry(hash, |stored| stored.key.eq(&entry.key))
            .ok()
            .map(|occupied| occupied.remove().0);
        leaf.entries.insert_unique(hash, entry, |entry| entry.hash);
        self.entries.split_if_needed(leaf);
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
        Self {
            entries: TrieTable::with_capacity(capacity),
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
