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

//! Keyed entry table shared by `OnceMap` and `singleflight::Group`.

use std::borrow::Borrow;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::MutexGuard;

use hashbrown::HashTable;

use crate::internal::mutex::Mutex;
use crate::once::OnceCell;

pub struct Entry<K, V> {
    hash: u64,
    key: K,
    pub cell: OnceCell<V>,
}

/// Keyed table of coalesced `OnceCell` entries.
///
/// The mutex protects only table lookup, insertion, and detachment; caller computations and user
/// key/value destructors always run outside the lock.
pub struct EntryTable<K, V, S> {
    entries: Mutex<HashTable<Arc<Entry<K, V>>>>,
    hasher: S,
}

impl<K, V, S> EntryTable<K, V, S> {
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            entries: Mutex::new(HashTable::new()),
            hasher,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub fn len_and_pending(&self) -> (usize, usize) {
        let entries = self.entries.lock();
        let pending = entries
            .iter()
            .filter(|entry| !entry.cell.initialized())
            .count();
        (entries.len(), pending)
    }
}

impl<K, V, S> EntryTable<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub fn from_populated_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        S: Default,
    {
        let iter = iter.into_iter();
        let hasher = S::default();
        let mut entries = HashTable::<Arc<Entry<K, V>>>::with_capacity(iter.size_hint().0);
        for (key, value) in iter {
            let hash = hasher.hash_one(&key);
            if let Some(stored) = entries.find_mut(hash, |stored| stored.key.eq(&key)) {
                let stored = Arc::get_mut(stored).expect("building entry must be unique");
                stored.hash = hash;
                stored.key = key;
                stored.cell = OnceCell::from_value(value);
            } else {
                entries.insert_unique(
                    hash,
                    Arc::new(Entry {
                        hash,
                        key,
                        cell: OnceCell::from_value(value),
                    }),
                    |stored| stored.hash,
                );
            }
        }
        Self {
            entries: Mutex::new(entries),
            hasher,
        }
    }

    pub fn get_or_insert(&self, key: K) -> EntryGuard<'_, K, V, S> {
        let hash = self.hasher.hash_one(&key);
        let entries = self.entries.lock();
        if let Some(entry) = entries.find(hash, |entry| entry.key.eq(&key)) {
            let entry = Arc::clone(entry);
            drop(entries);
            let guard = EntryGuard {
                table: self,
                entry: Some(entry),
            };
            // Establish cleanup before a duplicate key can unwind or reenter the table.
            drop(key);
            return guard;
        }

        self.insert_fresh(hash, key, entries)
    }

    pub fn get_or_insert_cached(&self, key: K) -> Result<V, EntryGuard<'_, K, V, S>>
    where
        V: Clone,
    {
        let hash = self.hasher.hash_one(&key);
        let entries = self.entries.lock();
        if let Some(entry) = entries.find(hash, |entry| entry.key.eq(&key)) {
            let entry = Arc::clone(entry);
            drop(entries);
            if let Some(value) = entry.cell.get().cloned() {
                // Own the cloned value and release the entry before dropping the duplicate key.
                drop(entry);
                drop(key);
                return Ok(value);
            }
            let guard = EntryGuard {
                table: self,
                entry: Some(entry),
            };
            drop(key);
            return Err(guard);
        }

        Err(self.insert_fresh(hash, key, entries))
    }

    fn insert_fresh(
        &self,
        hash: u64,
        key: K,
        mut entries: MutexGuard<'_, HashTable<Arc<Entry<K, V>>>>,
    ) -> EntryGuard<'_, K, V, S> {
        let entry = Arc::new(Entry {
            hash,
            key,
            cell: OnceCell::new(),
        });
        entries.insert_unique(hash, Arc::clone(&entry), |entry| entry.hash);
        drop(entries);
        EntryGuard {
            table: self,
            entry: Some(entry),
        }
    }

    pub fn find_ready<Q>(&self, key: &Q) -> Option<Arc<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        // A reader holding a pending entry would stop an abandoned computation from reclaiming it.
        self.entries
            .lock()
            .find(hash, |entry| entry.key.borrow() == key)
            .filter(|entry| entry.cell.initialized())
            .cloned()
    }

    pub fn remove<Q>(&self, key: &Q) -> Option<Arc<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let mut entries = self.entries.lock();
        let occupied = entries
            .find_entry(hash, |entry| entry.key.borrow() == key)
            .ok()?;
        Some(occupied.remove().0)
    }

    pub fn remove_if_current(&self, entry: &Arc<Entry<K, V>>) {
        let removed = {
            let mut entries = self.entries.lock();
            let Ok(occupied) = entries.find_entry(entry.hash, |stored| Arc::ptr_eq(stored, entry))
            else {
                return;
            };
            occupied.remove().0
        };
        drop(removed);
    }

    fn cleanup_abandoned_entry(&self, entry: Arc<Entry<K, V>>) {
        let mut entries = self.entries.lock();
        let Ok(occupied) = entries.find_entry(entry.hash, |stored| Arc::ptr_eq(stored, &entry))
        else {
            // Detached by `remove`, `discard`, or `forget`; release outside the lock in case this
            // guard held the final reference.
            drop(entries);
            drop(entry);
            return;
        };

        // With table ownership confirmed and new callers excluded, two owners means the table and
        // this guard are the only remaining references.
        if Arc::strong_count(&entry) == 2 && !entry.cell.initialized() {
            let (stored, _) = occupied.remove();
            drop(entries);
            drop(entry);
            drop(stored);
        } else {
            // A concurrent cleanup must observe this reference being released while no new caller
            // can clone the table's reference.
            drop(entry);
        }
    }
}

/// Holds one caller's reference to an [`Entry`] and evicts uninitialized orphans on drop.
pub struct EntryGuard<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    table: &'a EntryTable<K, V, S>,
    entry: Option<Arc<Entry<K, V>>>,
}

impl<K, V, S> EntryGuard<'_, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub fn entry(&self) -> &Arc<Entry<K, V>> {
        self.entry.as_ref().expect("entry guard must hold an entry")
    }
}

impl<K, V, S> Drop for EntryGuard<'_, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        let Some(entry) = self.entry.take() else {
            return;
        };
        // Shared entries are never reset, so an initialized cell never needs orphan cleanup.
        if !entry.cell.initialized() {
            self.table.cleanup_abandoned_entry(entry);
        }
    }
}
