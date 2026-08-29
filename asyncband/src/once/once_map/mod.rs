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
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::internal::atomic_arc::AtomicArcOption;
use crate::internal::mutex::Mutex;
use crate::once::OnceCell;

#[cfg(test)]
mod tests;

const TRIE_BITS: u32 = 4;
const TRIE_FANOUT: usize = 1 << TRIE_BITS;
const TRIE_MASK: u64 = TRIE_FANOUT as u64 - 1;

struct TrieTable<K, V> {
    root: OnceLock<Box<TrieNode<K, V>>>,
}

struct TrieNode<K, V> {
    slots: [TrieSlot<K, V>; TRIE_FANOUT],
}

struct TrieSlot<K, V> {
    state: AtomicArcOption<TrieState<K, V>>,
    mutation: Mutex<()>,
}

enum TrieState<K, V> {
    Leaf(Leaf<K, V>),
    Branch(Box<TrieNode<K, V>>),
}

struct Leaf<K, V> {
    hash: u64,
    entries: Vec<Arc<Entry<K, V>>>,
}

enum SlotMutation<K, V> {
    Keep,
    Replace(Option<Arc<TrieState<K, V>>>),
}

impl<K, V> TrieTable<K, V> {
    fn with_capacity(capacity: usize) -> Self {
        let root = OnceLock::new();
        if capacity != 0 {
            root.get_or_init(|| Box::new(TrieNode::empty()));
        }
        Self { root }
    }

    fn read<R>(
        &self,
        hash: u64,
        matches: impl Fn(&Entry<K, V>) -> bool,
        found: impl Fn(&Entry<K, V>) -> R,
    ) -> Option<R> {
        self.root.get()?.read(hash, 0, &matches, &found)
    }

    fn mutate<R>(
        &self,
        hash: u64,
        update: impl FnOnce(Option<&Arc<TrieState<K, V>>>, u32) -> (SlotMutation<K, V>, R),
    ) -> R {
        let root = self.root.get_or_init(|| Box::new(TrieNode::empty()));
        root.mutate(hash, 0, update)
    }

    fn try_mutate<R>(
        &self,
        hash: u64,
        update: impl FnOnce(Option<&Arc<TrieState<K, V>>>, u32) -> (SlotMutation<K, V>, R),
    ) -> Option<R> {
        Some(self.root.get()?.mutate(hash, 0, update))
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
                state: AtomicArcOption::empty(),
                mutation: Mutex::new(()),
            }),
        }
    }

    fn slot(&self, hash: u64, shift: u32) -> &TrieSlot<K, V> {
        &self.slots[((hash >> shift) & TRIE_MASK) as usize]
    }

    fn read<R>(
        &self,
        hash: u64,
        shift: u32,
        matches: &impl Fn(&Entry<K, V>) -> bool,
        found: &impl Fn(&Entry<K, V>) -> R,
    ) -> Option<R> {
        self.slot(hash, shift).state.with(|state| {
            let state = state?;
            match &*state {
                TrieState::Leaf(leaf) if leaf.hash == hash => leaf
                    .entries
                    .iter()
                    .find(|entry| matches(entry))
                    .map(|entry| found(entry)),
                TrieState::Branch(child) => child.read(hash, shift + TRIE_BITS, matches, found),
                TrieState::Leaf(_) => None,
            }
        })
    }

    fn mutate<R>(
        &self,
        hash: u64,
        shift: u32,
        update: impl FnOnce(Option<&Arc<TrieState<K, V>>>, u32) -> (SlotMutation<K, V>, R),
    ) -> R {
        let slot = self.slot(hash, shift);
        let state = slot.state.load();
        if let Some(TrieState::Branch(child)) = state.as_ref().map(Arc::as_ref) {
            return child.mutate(hash, shift + TRIE_BITS, update);
        }
        drop(state);

        let mutation = slot.mutation.lock();
        let state = slot.state.load();
        if let Some(TrieState::Branch(child)) = state.as_deref() {
            drop(mutation);
            return child.mutate(hash, shift + TRIE_BITS, update);
        }

        let (update, result) = update(state.as_ref(), shift);
        let retired = match update {
            SlotMutation::Keep => None,
            SlotMutation::Replace(replacement) => slot.state.swap(replacement),
        };

        // A removed entry may own user data. Release old snapshots only after unlocking the slot.
        drop(mutation);
        drop(state);
        drop(retired);
        result
    }

    fn for_each(&self, visit: &mut impl FnMut(&Arc<Entry<K, V>>)) {
        for slot in &self.slots {
            let state = slot.state.load();
            match state.as_ref().map(Arc::as_ref) {
                Some(TrieState::Leaf(leaf)) => {
                    for entry in &leaf.entries {
                        visit(entry);
                    }
                }
                Some(TrieState::Branch(child)) => child.for_each(visit),
                None => {}
            }
        }
    }

    fn split_leaves(
        old: Arc<TrieState<K, V>>,
        new: Arc<TrieState<K, V>>,
        shift: u32,
    ) -> Arc<TrieState<K, V>> {
        let TrieState::Leaf(old_leaf) = old.as_ref() else {
            unreachable!("only leaves can be split")
        };
        let TrieState::Leaf(new_leaf) = new.as_ref() else {
            unreachable!("only leaves can be split")
        };
        debug_assert_ne!(old_leaf.hash, new_leaf.hash);
        debug_assert!(shift < u64::BITS);

        let branch = Self::empty();
        let old_index = ((old_leaf.hash >> shift) & TRIE_MASK) as usize;
        let new_index = ((new_leaf.hash >> shift) & TRIE_MASK) as usize;
        if old_index == new_index {
            let child = Self::split_leaves(old, new, shift + TRIE_BITS);
            branch.slots[old_index].state.store(Some(child));
        } else {
            branch.slots[old_index].state.store(Some(old));
            branch.slots[new_index].state.store(Some(new));
        }

        Arc::new(TrieState::Branch(Box::new(branch)))
    }
}

impl<K, V> TrieState<K, V> {
    fn leaf(hash: u64, entries: Vec<Arc<Entry<K, V>>>) -> Arc<Self> {
        Arc::new(Self::Leaf(Leaf { hash, entries }))
    }

    fn remove_from_leaf(leaf: &Leaf<K, V>, index: usize) -> (Option<Arc<Self>>, Arc<Entry<K, V>>) {
        let mut entries = leaf.entries.clone();
        let removed = entries.remove(index);
        let replacement = (!entries.is_empty()).then(|| Self::leaf(leaf.hash, entries));
        (replacement, removed)
    }
}

struct Entry<K, V> {
    hash: u64,
    key: K,
    cell: OnceCell<V>,
    callers: AtomicUsize,
}

impl<K, V> Entry<K, V> {
    fn add_caller(&self) {
        let previous = self.callers.fetch_add(1, Ordering::Relaxed);
        assert!(previous < usize::MAX, "OnceMap caller count overflowed");
    }

    fn release_caller(&self) -> usize {
        let previous = self.callers.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "OnceMap caller count underflowed");
        previous - 1
    }
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
        if let Some(value) = self
            .entries
            .read(
                hash,
                |entry| entry.key.eq(&key),
                |entry| entry.cell.get().cloned(),
            )
            .flatten()
        {
            return Lookup::Ready(value);
        }

        self.entries.mutate(hash, |state, shift| {
            let leaf = match state.map(Arc::as_ref) {
                Some(TrieState::Leaf(leaf)) => Some(leaf),
                Some(TrieState::Branch(_)) => unreachable!("mutations stop at leaves"),
                None => None,
            };
            if let Some(entry) = leaf
                .filter(|leaf| leaf.hash == hash)
                .and_then(|leaf| leaf.entries.iter().find(|entry| entry.key.eq(&key)))
            {
                let lookup = if let Some(value) = entry.cell.get().cloned() {
                    Lookup::Ready(value)
                } else {
                    entry.add_caller();
                    Lookup::Pending(Arc::clone(entry))
                };
                return (SlotMutation::Keep, lookup);
            }

            let entry = Arc::new(Entry {
                hash,
                key,
                cell: OnceCell::new(),
                callers: AtomicUsize::new(1),
            });
            let new_leaf = TrieState::leaf(hash, vec![Arc::clone(&entry)]);
            let replacement = match state {
                None => new_leaf,
                Some(old) => match old.as_ref() {
                    TrieState::Leaf(leaf) if leaf.hash == hash => {
                        let mut entries = leaf.entries.clone();
                        entries.push(Arc::clone(&entry));
                        TrieState::leaf(hash, entries)
                    }
                    TrieState::Leaf(_) => {
                        TrieNode::split_leaves(Arc::clone(old), new_leaf, shift + TRIE_BITS)
                    }
                    TrieState::Branch(_) => unreachable!("mutations stop at leaves"),
                },
            };

            (
                SlotMutation::Replace(Some(replacement)),
                Lookup::Pending(entry),
            )
        })
    }

    fn get_value<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        V: Clone,
    {
        let hash = self.hasher.hash_one(key);
        self.entries
            .read(
                hash,
                |entry| entry.key.borrow() == key,
                |entry| entry.cell.get().cloned(),
            )
            .flatten()
    }

    fn remove_entry<Q>(&self, key: &Q) -> Option<Arc<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        self.entries
            .try_mutate(hash, |state, _| {
                let Some(TrieState::Leaf(leaf)) = state.map(Arc::as_ref) else {
                    return (SlotMutation::Keep, None);
                };
                if leaf.hash != hash {
                    return (SlotMutation::Keep, None);
                }
                let Some(index) = leaf
                    .entries
                    .iter()
                    .position(|entry| entry.key.borrow() == key)
                else {
                    return (SlotMutation::Keep, None);
                };

                let (replacement, removed) = TrieState::remove_from_leaf(leaf, index);
                (SlotMutation::Replace(replacement), Some(removed))
            })
            .flatten()
    }

    fn cleanup_abandoned_entry(&self, entry: Arc<Entry<K, V>>) {
        let updated = self.entries.try_mutate(entry.hash, |state, _| {
            let remaining_callers = entry.release_caller();
            let Some(TrieState::Leaf(leaf)) = state.map(Arc::as_ref) else {
                return (SlotMutation::Keep, ());
            };
            let Some(index) = leaf
                .entries
                .iter()
                .position(|stored| Arc::ptr_eq(stored, &entry))
            else {
                return (SlotMutation::Keep, ());
            };
            if remaining_callers != 0 || entry.cell.initialized() {
                return (SlotMutation::Keep, ());
            }

            let (replacement, _) = TrieState::remove_from_leaf(leaf, index);
            (SlotMutation::Replace(replacement), ())
        });
        if updated.is_none() {
            entry.release_caller();
        }
    }

    fn insert(&self, key: K, value: V) {
        let hash = self.hasher.hash_one(&key);
        let entry = Arc::new(Entry {
            hash,
            key,
            cell: OnceCell::from_value(value),
            callers: AtomicUsize::new(0),
        });

        let replaced = self.entries.mutate(hash, |state, shift| {
            let new_leaf = TrieState::leaf(hash, vec![Arc::clone(&entry)]);
            let (replacement, replaced) = match state {
                None => (new_leaf, None),
                Some(old) => match old.as_ref() {
                    TrieState::Leaf(leaf) if leaf.hash == hash => {
                        let mut entries = leaf.entries.clone();
                        let replaced = entries
                            .iter()
                            .position(|stored| stored.key.eq(&entry.key))
                            .map(|index| {
                                std::mem::replace(&mut entries[index], Arc::clone(&entry))
                            });
                        if replaced.is_none() {
                            entries.push(Arc::clone(&entry));
                        }
                        (TrieState::leaf(hash, entries), replaced)
                    }
                    TrieState::Leaf(_) => (
                        TrieNode::split_leaves(Arc::clone(old), new_leaf, shift + TRIE_BITS),
                        None,
                    ),
                    TrieState::Branch(_) => unreachable!("mutations stop at leaves"),
                },
            };
            (SlotMutation::Replace(Some(replacement)), replaced)
        });
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
        let entry = self.entry.take().unwrap();
        debug_assert!(entry.cell.initialized());
        entry.release_caller();
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

    /// Creates a new OnceMap with the default hasher and the specified capacity hint.
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

    /// Creates a new OnceMap with the specified capacity hint and hasher.
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
