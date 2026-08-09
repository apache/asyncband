// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Shared storage for keyed once primitives.
//!
//! Keeping the key and cell in one `Arc` lets calls retain an entry's identity without requiring
//! `K: Clone`. `HashTable` provides hashed lookup with a custom equality check, preserving borrowed
//! key lookups and expected O(1) removal of an exact entry.

use std::borrow::Borrow;
use std::fmt;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::sync::Arc;

use hashbrown::HashTable;

use crate::once::OnceCell;

pub(crate) struct KeyedOnceEntry<K, V> {
    hash: u64,
    key: K,
    cell: OnceCell<V>,
}

impl<K, V> KeyedOnceEntry<K, V> {
    pub(crate) fn cell(&self) -> &OnceCell<V> {
        &self.cell
    }
}

pub(crate) struct KeyedOnceTable<K, V, S> {
    entries: HashTable<Arc<KeyedOnceEntry<K, V>>>,
    hash_builder: S,
}

impl<K, V, S> fmt::Debug for KeyedOnceTable<K, V, S>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.entries.iter().map(|entry| (&entry.key, &entry.cell)))
            .finish()
    }
}

impl<K, V, S> KeyedOnceTable<K, V, S> {
    pub(crate) fn with_hasher(hash_builder: S) -> Self {
        Self {
            entries: HashTable::new(),
            hash_builder,
        }
    }

    pub(crate) fn with_capacity_and_hasher(capacity: usize, hash_builder: S) -> Self {
        Self {
            entries: HashTable::with_capacity(capacity),
            hash_builder,
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl<K, V, S> KeyedOnceTable<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub(crate) fn get_or_insert(&mut self, key: K) -> &Arc<KeyedOnceEntry<K, V>> {
        let hash = self.hash_builder.hash_one(&key);
        self.entries
            .entry(hash, |entry| entry.key.eq(&key), |entry| entry.hash)
            .or_insert_with(|| {
                Arc::new(KeyedOnceEntry {
                    hash,
                    key,
                    cell: OnceCell::new(),
                })
            })
            .into_mut()
    }

    pub(crate) fn get<Q>(&self, key: &Q) -> Option<&Arc<KeyedOnceEntry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hash_builder.hash_one(key);
        self.entries.find(hash, |entry| entry.key.borrow() == key)
    }

    pub(crate) fn remove<Q>(&mut self, key: &Q) -> Option<Arc<KeyedOnceEntry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hash_builder.hash_one(key);
        let entry = self
            .entries
            .find_entry(hash, |entry| entry.key.borrow() == key)
            .ok()?;
        let (entry, _) = entry.remove();
        Some(entry)
    }

    /// Removes the entry only if the table still contains the same allocation.
    pub(crate) fn remove_exact(&mut self, expected: &Arc<KeyedOnceEntry<K, V>>) -> bool {
        let Ok(entry) = self
            .entries
            .find_entry(expected.hash, |entry| Arc::ptr_eq(entry, expected))
        else {
            return false;
        };

        drop(entry.remove());
        true
    }

    pub(crate) fn remove_abandoned(&mut self, entry: &Arc<KeyedOnceEntry<K, V>>) {
        // If the table still owns this entry, a count of two means the current call is its only
        // owner outside the table. remove_exact also rejects an entry that was detached or
        // replaced.
        if Arc::strong_count(entry) == 2 && entry.cell.get().is_none() {
            self.remove_exact(entry);
        }
    }

    pub(crate) fn insert_value(&mut self, key: K, value: V) {
        self.remove(&key);

        let hash = self.hash_builder.hash_one(&key);
        let entry = Arc::new(KeyedOnceEntry {
            hash,
            key,
            cell: OnceCell::from_value(value),
        });
        self.entries.insert_unique(hash, entry, |entry| entry.hash);
    }
}
