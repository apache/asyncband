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

//! Coalesce concurrent work that uses the same key.
//!
//! While work for a key is in flight, later callers wait and clone its successful value. A
//! completed value is not cached: the entry is removed when the work succeeds, so a non-overlapping
//! later call executes its own function.
//!
//! If the active call is cancelled or panics, a waiting caller may run its own function. `try_work`
//! also lets a waiter retry after the active function returns an error. Use `OnceMap` instead when
//! successful values should remain cached by key.

use std::borrow::Borrow;
use std::fmt;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;
use std::sync::Arc;

use hashbrown::HashTable;

use crate::internal::mutex::Mutex;
use crate::once::OnceCell;

#[cfg(test)]
mod tests;

struct Entry<K, V> {
    hash: u64,
    key: K,
    cell: OnceCell<V>,
}

/// A namespace that coalesces concurrent work by key.
///
/// Different keys run independently. A group neither caches completed results nor limits overall
/// concurrency.
pub struct Group<K, V, S = RandomState> {
    // This lock protects only entry lookup, insertion, and removal.
    // User work is always run after releasing it.
    entries: Mutex<HashTable<Arc<Entry<K, V>>>>,
    hasher: S,
}

impl<K, V, S> fmt::Debug for Group<K, V, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let in_flight = self.entries.lock().len();
        f.debug_struct("Group")
            .field("in_flight", &in_flight)
            .finish()
    }
}

impl<K, V, S> Group<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn get_or_insert(&self, key: K) -> WorkCleanupGuard<'_, K, V, S> {
        let hash = self.hasher.hash_one(&key);
        let mut entries = self.entries.lock();
        if let Some(entry) = entries.find(hash, |entry| entry.key.eq(&key)) {
            let entry = Arc::clone(entry);
            drop(entries);
            let guard = WorkCleanupGuard::new(self, entry);
            // Establish cleanup before a duplicate key can unwind or reenter the group.
            drop(key);
            return guard;
        }

        let entry = Arc::new(Entry {
            hash,
            key,
            cell: OnceCell::new(),
        });
        entries.insert_unique(hash, entry.clone(), |entry| entry.hash);
        drop(entries);
        WorkCleanupGuard::new(self, entry)
    }

    fn remove<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let removed = {
            let mut entries = self.entries.lock();
            let Ok(occupied) = entries.find_entry(hash, |entry| entry.key.borrow() == key) else {
                return;
            };
            occupied.remove().0
        };
        drop(removed);
    }

    fn remove_if_current(&self, entry: &Arc<Entry<K, V>>) {
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
            // `forget` detached the entry. It may be the final owner, so release it after
            // unlocking rather than running user destructors under the table lock.
            drop(entries);
            drop(entry);
            return;
        };

        // With group ownership confirmed and the table locked against new callers, two owners
        // means the group and this cleanup guard are the only remaining references.
        if Arc::strong_count(&entry) == 2 && !entry.cell.initialized() {
            let (stored, _) = occupied.remove();
            drop(entries);
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
    fn new(group: &'a Group<K, V, S>, entry: Arc<Entry<K, V>>) -> Self {
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
        Self::with_hasher(RandomState::new())
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
        Self {
            entries: Mutex::new(HashTable::new()),
            hasher,
        }
    }

    /// Returns a clone of the shared computation's value for `key`.
    ///
    /// Callers that join the same in-flight entry share its successful result. Once `func`
    /// completes successfully, the entry is removed so later calls can start new work.
    /// [`forget`](Self::forget) lets new calls start independently of an existing entry.
    ///
    /// # Cancel safety
    ///
    /// Dropping a caller that is waiting for a result does not cancel the active computation.
    /// Dropping the caller executing `func` drops that attempt; another caller on the entry may
    /// run its own `func`. Effects already performed by the cancelled attempt are not rolled back.
    ///
    /// # Panics
    ///
    /// A panic in `func` propagates to its caller without publishing a value. Another caller may
    /// retry the computation.
    ///
    /// # Deadlocks
    ///
    /// The computation must not await `work` or `try_work` for an equivalent key on
    /// the same current entry, because it would wait for its own result. Other keys are
    /// independent.
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
    /// assert_eq!(r1, r2);
    /// assert_eq!(counter.load(Ordering::SeqCst), 1);
    /// # }
    /// ```
    pub async fn work<F>(&self, key: K, func: F) -> V
    where
        F: AsyncFnOnce() -> V,
    {
        let guard = self.get_or_insert(key);
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

    /// Returns a clone of the shared computation's value for `key`.
    ///
    /// Callers that join the same in-flight entry share its successful result. Once `func`
    /// completes successfully, the entry is removed so later calls can start new work.
    /// [`forget`](Self::forget) lets new calls start independently of an existing entry.
    ///
    /// # Errors
    ///
    /// Returns an error from this call's `func` only to this caller. Errors are not stored or
    /// shared; another caller waiting on the entry may run its own `func`.
    ///
    /// # Cancel safety
    ///
    /// Dropping a caller that is waiting for a result does not cancel the active computation.
    /// Dropping the caller executing `func` drops that attempt; another caller on the entry may
    /// run its own `func`. Effects already performed by the cancelled attempt are not rolled back.
    ///
    /// # Panics
    ///
    /// A panic in `func` propagates to its caller without publishing a value. Another caller may
    /// retry the computation.
    ///
    /// # Deadlocks
    ///
    /// The computation must not await `work` or `try_work` for an equivalent key on
    /// the same current entry, because it would wait for its own result. Other keys are
    /// independent.
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
        let guard = self.get_or_insert(key);
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
