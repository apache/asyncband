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

use std::borrow::Borrow;
use std::fmt;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::sync::Arc;

use hashbrown::HashTable;
use hashbrown::hash_table::OccupiedEntry;

use crate::once::OnceCell;

pub struct OnceTableEntry<K, V> {
    hash: u64,
    key: K,
    cell: OnceCell<V>,
}

impl<K, V> OnceTableEntry<K, V> {
    pub fn cell(&self) -> &OnceCell<V> {
        &self.cell
    }
}

/// Shared keyed storage that lets once primitives clean up an exact entry without cloning its key.
pub struct OnceTable<K, V, S> {
    entries: HashTable<Arc<OnceTableEntry<K, V>>>,
    hasher: S,
}

impl<K, V, S> fmt::Debug for OnceTable<K, V, S>
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

impl<K, V, S> OnceTable<K, V, S> {
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            entries: HashTable::new(),
            hasher,
        }
    }

    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        Self {
            entries: HashTable::with_capacity(capacity),
            hasher,
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl<K, V, S> OnceTable<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub fn get_or_insert(&mut self, key: K) -> OccupiedEntry<'_, Arc<OnceTableEntry<K, V>>> {
        let hash = self.hasher.hash_one(&key);
        self.entries
            .entry(hash, |entry| entry.key.eq(&key), |entry| entry.hash)
            .or_insert_with(|| {
                Arc::new(OnceTableEntry {
                    hash,
                    key,
                    cell: OnceCell::new(),
                })
            })
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&Arc<OnceTableEntry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        self.entries.find(hash, |entry| entry.key.borrow() == key)
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<Arc<OnceTableEntry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let entry = self
            .entries
            .find_entry(hash, |entry| entry.key.borrow() == key)
            .ok()?;
        let (entry, _) = entry.remove();
        Some(entry)
    }

    /// Removes the entry only if the table still contains the same allocation.
    pub fn remove_exact(&mut self, expected: &Arc<OnceTableEntry<K, V>>) -> bool {
        let Ok(entry) = self
            .entries
            .find_entry(expected.hash, |entry| Arc::ptr_eq(entry, expected))
        else {
            return false;
        };

        drop(entry.remove());
        true
    }

    pub fn remove_abandoned(&mut self, entry: &Arc<OnceTableEntry<K, V>>) {
        // If the table still owns this entry, a count of two means the current call is its only
        // owner outside the table. remove_exact also rejects an entry that was detached or
        // replaced.
        if Arc::strong_count(entry) == 2 && !entry.cell.initialized() {
            self.remove_exact(entry);
        }
    }

    pub fn insert_value(&mut self, key: K, value: V) {
        self.remove(&key);

        let hash = self.hasher.hash_one(&key);
        let entry = Arc::new(OnceTableEntry {
            hash,
            key,
            cell: OnceCell::from_value(value),
        });
        self.entries.insert_unique(hash, entry, |entry| entry.hash);
    }
}
