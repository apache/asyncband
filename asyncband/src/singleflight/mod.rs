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

//! Singleflight provides a duplicate function call suppression mechanism.

use std::borrow::Borrow;
use std::fmt;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;
use std::sync::Arc;
use std::sync::MutexGuard;

use hashbrown::HashTable;

use crate::internal::cache_padded::CachePadded;
use crate::internal::default_shard_count;
use crate::internal::mutex::Mutex;
use crate::once::OnceCell;

#[cfg(test)]
mod tests;

type Entries<K, V> = HashTable<Arc<Entry<K, V>>>;
type Shard<K, V> = CachePadded<Mutex<Entries<K, V>>>;

struct Entry<K, V> {
    hash: u64,
    key: K,
    cell: OnceCell<V>,
}

/// Group represents a class of work and forms a namespace in which
/// units of work can be executed with duplicate suppression.
pub struct Group<K, V, S = RandomState> {
    shards: Box<[Shard<K, V>]>,
    hasher: S,
}

impl<K, V, S> fmt::Debug for Group<K, V, S>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Write::write_str(f, "Group ")?;
        let mut debug_map = f.debug_map();
        for shard in &self.shards {
            let entries = shard.lock();
            debug_map.entries(entries.iter().map(|entry| (&entry.key, &entry.cell)));
        }
        debug_map.finish()
    }
}

impl<K, V, S> Group<K, V, S> {
    fn with_config(hasher: S, shard_amount: usize) -> Self {
        assert!(
            shard_amount.is_power_of_two(),
            "shard amount must be greater than zero and a power of two"
        );

        let shards = (0..shard_amount)
            .map(|_| CachePadded::new(Mutex::new(HashTable::new())))
            .collect();
        Self { shards, hasher }
    }

    fn lock_shard(&self, hash: u64) -> MutexGuard<'_, Entries<K, V>> {
        self.shards[(hash as usize) & (self.shards.len() - 1)].lock()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock().len()).sum()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.shards.iter().all(|shard| shard.lock().is_empty())
    }
}

impl<K, V, S> Group<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn get_or_insert(&self, key: K) -> Arc<Entry<K, V>> {
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

    fn remove<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let removed = {
            let mut shard = self.lock_shard(hash);
            let Ok(occupied) = shard.find_entry(hash, |entry| entry.key.borrow() == key) else {
                return;
            };
            occupied.remove().0
        };
        drop(removed);
    }

    fn remove_if_current(&self, entry: &Arc<Entry<K, V>>) {
        let removed = {
            let mut shard = self.lock_shard(entry.hash);
            let Ok(occupied) = shard.find_entry(entry.hash, |stored| Arc::ptr_eq(stored, entry))
            else {
                return;
            };
            occupied.remove().0
        };
        drop(removed);
    }

    fn cleanup_abandoned_entry(&self, entry: Arc<Entry<K, V>>) {
        let mut shard = self.lock_shard(entry.hash);
        let Ok(occupied) = shard.find_entry(entry.hash, |stored| Arc::ptr_eq(stored, &entry))
        else {
            // `forget` detached the entry. It may be the final owner, so release it after
            // unlocking rather than running user destructors under the shard lock.
            drop(shard);
            drop(entry);
            return;
        };

        // With group ownership confirmed and the shard locked against new callers, two owners
        // means the group and this cleanup guard are the only remaining references.
        if Arc::strong_count(&entry) == 2 && !entry.cell.initialized() {
            let (stored, _) = occupied.remove();
            drop(shard);
            drop(entry);
            drop(stored);
        } else {
            // A waiting cleanup must observe this call's reference being released while no new
            // caller can clone the group's reference.
            drop(entry);
        }
    }
}

// Holds one call's entry so Drop can clean it up if the work is abandoned.
struct WorkCleanupGuard<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    group: &'a Group<K, V, S>,
    entry: Option<Arc<Entry<K, V>>>,
}

impl<'a, K, V, S> WorkCleanupGuard<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(group: &'a Group<K, V, S>, key: K) -> Self {
        let entry = group.get_or_insert(key);

        Self {
            group,
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

impl<K, V, S> Drop for WorkCleanupGuard<'_, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        let Some(entry) = self.entry.take() else {
            return;
        };

        self.group.cleanup_abandoned_entry(entry);
    }
}

impl<K, V, S> Default for Group<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: BuildHasher + Default,
{
    fn default() -> Self {
        Self::with_hasher(S::default())
    }
}

impl<K, V> Group<K, V, RandomState>
where
    K: Eq + Hash,
    V: Clone,
{
    /// Creates a new Group with the default hasher.
    pub fn new() -> Self {
        Self::with_config(RandomState::new(), default_shard_count())
    }

    /// Creates a new Group with the default hasher and the specified shard amount.
    ///
    /// # Panics
    ///
    /// Panics if `shard_amount` is zero or is not a power of two.
    pub fn with_shard_amount(shard_amount: usize) -> Self {
        Self::with_config(RandomState::new(), shard_amount)
    }
}

impl<K, V, S> Group<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: BuildHasher,
{
    /// Creates a new Group with the given hasher.
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_config(hasher, default_shard_count())
    }

    /// Creates a new Group with the given hasher and the specified shard amount.
    ///
    /// # Panics
    ///
    /// Panics if `shard_amount` is zero or is not a power of two.
    pub fn with_hasher_and_shard_amount(hasher: S, shard_amount: usize) -> Self {
        Self::with_config(hasher, shard_amount)
    }

    /// Executes and returns the results of the given function, making sure that only one execution
    /// is in-flight for a given key at a time.
    ///
    /// If a duplicate comes in, the duplicate caller waits for the original to complete and
    /// receives the same results.
    ///
    /// If the computation is cancelled or panics, another caller waiting for the same key may retry
    /// it.
    ///
    /// Once the function completes, the key, if not [`forgotten`], is removed from the group,
    /// allowing future calls with the same key to execute the function again.
    ///
    /// [`forgotten`]: Self::forget
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::AtomicUsize;
    /// use std::sync::atomic::Ordering;
    /// use std::time::Duration;
    ///
    /// use asyncband::singleflight::Group;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let group = Group::new();
    /// let counter = Arc::new(AtomicUsize::new(0));
    ///
    /// let c1 = counter.clone();
    /// let fut1 = group.work("key", || async move {
    ///     c1.fetch_add(1, Ordering::SeqCst);
    ///     // simulate heavy work to avoid immediate completion
    ///     tokio::time::sleep(Duration::from_millis(100)).await;
    ///     "result"
    /// });
    ///
    /// let c2 = counter.clone();
    /// let fut2 = group.work("key", || async move {
    ///     c2.fetch_add(1, Ordering::SeqCst);
    ///     // simulate heavy work to avoid immediate completion
    ///     tokio::time::sleep(Duration::from_millis(100)).await;
    ///     "result"
    /// });
    ///
    /// let (r1, r2) = tokio::join!(fut1, fut2);
    ///
    /// assert_eq!(r1, "result");
    /// assert_eq!(r2, "result");
    /// assert_eq!(counter.load(Ordering::SeqCst), 1);
    /// # }
    /// ```
    pub async fn work<F>(&self, key: K, func: F) -> V
    where
        F: AsyncFnOnce() -> V,
    {
        let guard = WorkCleanupGuard::new(self, key);
        let entry = guard.entry();
        let result = entry
            .cell
            .get_or_init(async || {
                let result = func().await;
                self.remove_if_current(entry);
                result
            })
            .await
            .clone();
        guard.dismiss();
        result
    }

    /// Executes and returns the results of the given function, making sure that only one execution
    /// is in-flight for a given key at a time.
    ///
    /// If a duplicate comes in, the duplicate caller waits for the original to complete and
    /// receives the same results.
    ///
    /// If the computation returns an error, it is returned to that caller. After an error,
    /// cancellation, or panic, another caller may retry the computation.
    ///
    /// Once the function completes successfully, the key, if not [`forgotten`], is removed from
    /// the group, allowing future calls with the same key to execute the function again.
    ///
    /// [`forgotten`]: Self::forget
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::atomic::AtomicUsize;
    /// use std::sync::atomic::Ordering;
    /// use std::time::Duration;
    ///
    /// use asyncband::singleflight::Group;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let group = Group::new();
    ///
    /// let fut1 = group.try_work("key", || async move {
    ///     // simulate heavy work to avoid immediate completion
    ///     tokio::time::sleep(Duration::from_millis(100)).await;
    ///     Err::<_, &'static str>("fut1")
    /// });
    ///
    /// let fut2 = group.try_work("key", || async move {
    ///     // simulate heavy work to avoid immediate completion
    ///     tokio::time::sleep(Duration::from_millis(200)).await;
    ///     Ok::<_, &'static str>("fut2")
    /// });
    ///
    /// let (r1, r2) = tokio::join!(fut1, fut2);
    ///
    /// assert_eq!(r1, Err("fut1"));
    /// assert_eq!(r2, Ok("fut2"));
    /// # }
    /// ```
    pub async fn try_work<E, F>(&self, key: K, func: F) -> Result<V, E>
    where
        F: AsyncFnOnce() -> Result<V, E>,
    {
        let guard = WorkCleanupGuard::new(self, key);
        let entry = guard.entry();
        let result = entry
            .cell
            .get_or_try_init(async || {
                let result = func().await?;
                self.remove_if_current(entry);
                Ok(result)
            })
            .await?
            .clone();
        guard.dismiss();
        Ok(result)
    }

    /// Forgets about the given key.
    ///
    /// Future calls to `work` for this key will call the function rather than waiting for an
    /// earlier call to complete. Existing calls to `work` for this key are not affected.
    pub fn forget<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.remove(key);
    }
}
