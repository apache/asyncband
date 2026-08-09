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

use crate::once::OnceCell;

pub struct OnceTableEntry<K, V> {
    hash: u64,
    key: K,
    cell: OnceCell<V>,
}

impl<K, V> OnceTableEntry<K, V> {
    pub fn initialized(&self) -> bool {
        self.cell.initialized()
    }

    pub fn get(&self) -> Option<&V> {
        self.cell.get()
    }

    pub async fn get_or_init<F>(&self, init: F) -> &V
    where
        F: AsyncFnOnce() -> V,
    {
        self.cell.get_or_init(init).await
    }

    pub async fn get_or_try_init<E, F>(&self, init: F) -> Result<&V, E>
    where
        F: AsyncFnOnce() -> Result<V, E>,
    {
        self.cell.get_or_try_init(init).await
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
    pub fn get_or_insert(&mut self, key: K) -> &Arc<OnceTableEntry<K, V>> {
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
            .into_mut()
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

    /// Removes the entry if the table still contains the same allocation.
    pub fn remove_entry(&mut self, entry: &Arc<OnceTableEntry<K, V>>) {
        let Ok(occupied) = self
            .entries
            .find_entry(entry.hash, |existing| Arc::ptr_eq(existing, entry))
        else {
            return;
        };

        drop(occupied.remove());
    }

    pub fn insert(&mut self, key: K, value: V) {
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
