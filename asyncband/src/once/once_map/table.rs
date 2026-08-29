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
use std::panic::UnwindSafe;
use std::sync::Arc;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use hashbrown::HashTable;

use crate::internal::cache_padded::CachePadded;
use crate::internal::default_shard_count;
use crate::internal::mutex::Mutex;
use crate::once::OnceCell;

mod ready_index;

use ready_index::ReadyIndex;

type Entries<K, V> = HashTable<Arc<Entry<K, V>>>;
type Shard<K, V> = CachePadded<Mutex<Entries<K, V>>>;

pub struct Entry<K, V> {
    hash: u64,
    key: K,
    cell: OnceCell<V>,
    was_indexed: AtomicBool,
}

pub enum Lookup<K, V> {
    Ready(V),
    Entry(Arc<Entry<K, V>>),
}

impl<K, V> Entry<K, V> {
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

pub struct Table<K, V, S> {
    shards: Box<[Shard<K, V>]>,
    index: ReadyIndex<K, V>,
    hasher: S,
}

/// Table operations recover poisoned ready-index locks before accessing their contents.
impl<K, V, S: UnwindSafe> UnwindSafe for Table<K, V, S> {}

impl<K, V, S> fmt::Debug for Table<K, V, S>
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

impl<K, V, S> Table<K, V, S> {
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(0, hasher)
    }

    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        Self::with_capacity_and_hasher_and_shard_amount(capacity, hasher, default_shard_count())
    }

    pub fn with_hasher_and_shard_amount(hasher: S, shard_amount: usize) -> Self {
        Self::with_capacity_and_hasher_and_shard_amount(0, hasher, shard_amount)
    }

    pub fn with_capacity_and_hasher_and_shard_amount(
        capacity: usize,
        hasher: S,
        shard_amount: usize,
    ) -> Self {
        assert!(
            shard_amount.is_power_of_two(),
            "shard amount must be greater than zero and a power of two"
        );

        let shard_capacity = capacity.div_ceil(shard_amount);
        let shards = (0..shard_amount)
            .map(|_| CachePadded::new(Mutex::new(HashTable::with_capacity(shard_capacity))))
            .collect();

        Self {
            shards,
            index: ReadyIndex::with_capacity_and_shard_amount(capacity, shard_amount),
            hasher,
        }
    }

    fn lock_shard(&self, hash: u64) -> MutexGuard<'_, Entries<K, V>> {
        self.shards[(hash as usize) & (self.shards.len() - 1)].lock()
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

impl<K, V, S> Table<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn lookup_index<Q>(&self, hash: u64, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
        V: Clone,
    {
        self.index.get(hash, key)
    }

    pub fn get_or_insert(&self, key: K) -> Lookup<K, V>
    where
        V: Clone,
    {
        let hash = self.hasher.hash_one(&key);
        if let Some(value) = self.lookup_index(hash, &key) {
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
                        was_indexed: AtomicBool::new(false),
                    })
                })
                .into_mut()
                .clone()
        };

        match self.index_if_ready(&entry) {
            Some(value) => Lookup::Ready(value),
            None => Lookup::Entry(entry),
        }
    }

    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        V: Clone,
    {
        let hash = self.hasher.hash_one(key);

        if let Some(value) = self.lookup_index(hash, key) {
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
        let value = entry.get()?.clone();

        {
            let shard = self.lock_shard(entry.hash);
            if shard
                .find(entry.hash, |stored| Arc::ptr_eq(stored, entry))
                .is_some()
            {
                self.index(entry);
            }
        }

        Some(value)
    }

    pub fn remove<Q>(&self, key: &Q) -> Option<Arc<Entry<K, V>>>
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

    fn insert(&self, key: K, value: V) {
        let hash = self.hasher.hash_one(&key);
        let entry = Arc::new(Entry {
            hash,
            key,
            cell: OnceCell::from_value(value),
            was_indexed: AtomicBool::new(false),
        });

        let mut shard = self.lock_shard(hash);
        if let Ok(occupied) = shard.find_entry(hash, |stored| stored.key.eq(&entry.key)) {
            let (replaced, _) = occupied.remove();
            self.remove_from_index(&replaced);
        }
        shard.insert_unique(hash, Arc::clone(&entry), |entry| entry.hash);

        self.index(&entry);
    }

    fn index(&self, entry: &Arc<Entry<K, V>>) {
        self.index.insert(entry);
        entry.was_indexed.store(true, Ordering::Relaxed);
    }

    fn remove_from_index(&self, entry: &Arc<Entry<K, V>>) {
        if entry.was_indexed.load(Ordering::Relaxed) {
            self.index.remove(entry);
        }
    }
}

impl<K, V, S> FromIterator<(K, V)> for Table<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher + Default,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let table = Self::with_capacity_and_hasher(iter.size_hint().0, S::default());
        for (key, value) in iter {
            table.insert(key, value);
        }

        table
    }
}
