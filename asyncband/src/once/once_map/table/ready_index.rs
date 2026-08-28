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
use std::sync::Arc;

use hashbrown::HashTable;

use super::Entry;
use crate::internal::rwlock::RwLock;

const TARGET_ENTRIES_PER_BUCKET: usize = 2;

#[repr(align(64))]
struct Bucket<K, V> {
    entries: RwLock<HashTable<Arc<Entry<K, V>>>>,
}

impl<K, V> Bucket<K, V> {
    fn new() -> Self {
        Self {
            entries: RwLock::new(HashTable::new()),
        }
    }
}

pub struct ReadyIndex<K, V> {
    buckets: Box<[Bucket<K, V>]>,
}

impl<K, V> ReadyIndex<K, V> {
    pub fn with_capacity_and_shard_amount(capacity: usize, shard_amount: usize) -> Self {
        let bucket_count = capacity
            .div_ceil(TARGET_ENTRIES_PER_BUCKET)
            .max(shard_amount)
            .next_power_of_two();

        Self {
            buckets: (0..bucket_count).map(|_| Bucket::new()).collect(),
        }
    }

    fn bucket(&self, hash: u64) -> &Bucket<K, V> {
        &self.buckets[(hash as usize) & (self.buckets.len() - 1)]
    }

    pub fn get<Q>(&self, hash: u64, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
        V: Clone,
    {
        self.bucket(hash)
            .entries
            .read()
            .find(hash, |entry| entry.key.borrow() == key)
            .and_then(|entry| entry.get().cloned())
    }

    pub fn insert(&self, entry: &Arc<Entry<K, V>>)
    where
        K: Eq,
    {
        let mut entries = self.bucket(entry.hash).entries.write();

        if let Ok(occupied) = entries.find_entry(entry.hash, |stored| stored.key == entry.key) {
            if Arc::ptr_eq(occupied.get(), entry) {
                return;
            }
            drop(occupied.remove());
        }

        entries.insert_unique(entry.hash, Arc::clone(entry), |entry| entry.hash);
    }

    pub fn remove(&self, entry: &Arc<Entry<K, V>>) {
        let mut entries = self.bucket(entry.hash).entries.write();

        if let Ok(occupied) = entries.find_entry(entry.hash, |stored| Arc::ptr_eq(stored, entry)) {
            drop(occupied.remove());
        }
    }
}
