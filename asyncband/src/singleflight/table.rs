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

use crate::internal::default_shard_count;
use crate::internal::mutex::CachePaddedMutex;
use crate::internal::mutex::Mutex;
use crate::once::OnceCell;

type Entries<K, V> = HashTable<Arc<Entry<K, V>>>;

pub struct Entry<K, V> {
    hash: u64,
    key: K,
    cell: OnceCell<V>,
}

impl<K, V> Entry<K, V> {
    fn initialized(&self) -> bool {
        self.cell.initialized()
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

/// Storage for one in-flight call per key.
pub struct Table<K, V, S> {
    shards: Box<[CachePaddedMutex<Entries<K, V>>]>,
    hasher: S,
}

impl<K, V, S> fmt::Debug for Table<K, V, S>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_map = f.debug_map();
        for shard in &self.shards {
            let entries = shard.0.lock();
            debug_map.entries(entries.iter().map(|entry| (&entry.key, &entry.cell)));
        }
        debug_map.finish()
    }
}

impl<K, V, S> Table<K, V, S> {
    pub fn with_hasher(hasher: S) -> Self {
        let shards = (0..default_shard_count())
            .map(|_| CachePaddedMutex(Mutex::new(HashTable::new())))
            .collect();
        Self { shards, hasher }
    }

    fn lock_shard(&self, hash: u64) -> MutexGuard<'_, Entries<K, V>> {
        self.shards[(hash as usize) & (self.shards.len() - 1)]
            .0
            .lock()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.0.lock().len()).sum()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|shard| shard.0.lock().is_empty())
    }
}

impl<K, V, S> Table<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub fn get_or_insert(&self, key: K) -> Arc<Entry<K, V>> {
        let hash = self.hasher.hash_one(&key);
        let mut shard = self.lock_shard(hash);
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
    }

    pub fn remove<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let mut shard = self.lock_shard(hash);
        if let Ok(occupied) = shard.find_entry(hash, |entry| entry.key.borrow() == key) {
            drop(occupied.remove());
        }
    }

    pub fn remove_if_current(&self, entry: &Arc<Entry<K, V>>) {
        let mut shard = self.lock_shard(entry.hash);
        if let Ok(occupied) = shard.find_entry(entry.hash, |stored| Arc::ptr_eq(stored, entry)) {
            drop(occupied.remove());
        }
    }

    pub fn cleanup_abandoned_entry(&self, entry: Arc<Entry<K, V>>) {
        let mut shard = self.lock_shard(entry.hash);
        // If the table still owns this entry, a count of two means the current call is its only
        // owner outside the table. The pointer comparison rejects a detached or replaced entry.
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
}
