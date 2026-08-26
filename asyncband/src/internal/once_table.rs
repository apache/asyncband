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
use std::sync::Arc;
use std::sync::MutexGuard;

use hashbrown::HashTable;

use crate::internal::mutex::Mutex;
use crate::once::OnceCell;

const SHARD_COUNT: usize = 64;

type Entries<K, V> = HashTable<Arc<OnceTableEntry<K, V>>>;

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
    shards: Box<[Mutex<Entries<K, V>>]>,
    hasher: S,
}

impl<K, V, S> fmt::Debug for OnceTable<K, V, S>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_map = f.debug_map();
        for shard in &self.shards {
            let entries = shard.lock();
            debug_map.entries(entries.iter().map(|entry| (&entry.key, &entry.cell)));
        }
        debug_map.finish()
    }
}

impl<K, V, S> OnceTable<K, V, S> {
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(0, hasher)
    }

    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        let shard_capacity = capacity.div_ceil(SHARD_COUNT);
        let shards = (0..SHARD_COUNT)
            .map(|_| Mutex::new(HashTable::with_capacity(shard_capacity)))
            .collect();
        Self { shards, hasher }
    }

    fn shard(&self, hash: u64) -> MutexGuard<'_, Entries<K, V>> {
        self.shards[hash as usize & (SHARD_COUNT - 1)].lock()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock().len()).sum()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|shard| shard.lock().is_empty())
    }
}

impl<K, V, S> OnceTable<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub fn get_or_insert(&self, key: K) -> Arc<OnceTableEntry<K, V>> {
        let hash = self.hasher.hash_one(&key);
        let mut shard = self.shard(hash);
        Arc::clone(
            shard
                .entry(hash, |entry| entry.key.eq(&key), |entry| entry.hash)
                .or_insert_with(|| {
                    Arc::new(OnceTableEntry {
                        hash,
                        key,
                        cell: OnceCell::new(),
                    })
                })
                .into_mut(),
        )
    }

    pub fn get<Q>(&self, key: &Q) -> Option<Arc<OnceTableEntry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        self.shard(hash)
            .find(hash, |entry| entry.key.borrow() == key)
            .map(Arc::clone)
    }

    pub fn remove<Q>(&self, key: &Q) -> Option<Arc<OnceTableEntry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let mut shard = self.shard(hash);
        let entry = shard
            .find_entry(hash, |entry| entry.key.borrow() == key)
            .ok()?;
        let (entry, _) = entry.remove();
        Some(entry)
    }

    /// Removes the entry if the table still contains the same allocation.
    pub fn remove_entry(&self, entry: &Arc<OnceTableEntry<K, V>>) {
        let mut shard = self.shard(entry.hash);
        let Ok(occupied) = shard.find_entry(entry.hash, |stored| Arc::ptr_eq(stored, entry)) else {
            return;
        };

        drop(occupied.remove());
    }

    pub fn cleanup_abandoned_entry(&self, entry: Arc<OnceTableEntry<K, V>>) {
        let mut shard = self.shard(entry.hash);
        // If the table still owns this entry, a count of two means the current call is its only
        // owner outside the table. remove_entry rejects an entry that was detached or replaced.
        if Arc::strong_count(&entry) == 2 && !entry.initialized() {
            if let Ok(occupied) = shard.find_entry(entry.hash, |stored| Arc::ptr_eq(stored, &entry))
            {
                drop(occupied.remove());
            }
        }

        // Drop this call's reference before unlocking so a waiting cleanup observes the updated
        // reference count.
        drop(entry);
    }

    pub fn insert(&self, key: K, value: V) {
        self.remove(&key);

        let hash = self.hasher.hash_one(&key);
        let entry = Arc::new(OnceTableEntry {
            hash,
            key,
            cell: OnceCell::from_value(value),
        });
        self.shard(hash)
            .insert_unique(hash, entry, |entry| entry.hash);
    }
}
