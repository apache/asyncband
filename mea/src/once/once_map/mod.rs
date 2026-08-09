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
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;
use std::sync::Arc;

use crate::internal::KeyedOnceEntry;
use crate::internal::KeyedOnceTable;
use crate::internal::Mutex;
use crate::once::OnceCell;

#[cfg(test)]
mod tests;

/// A hash map that runs computation only once for each key and stores the result.
///
/// Note that this always clones the value out of the underlying map. Because of this, it's common
/// to wrap the `V` in an `Arc<V>` to make cloning cheap.
#[derive(Debug)]
pub struct OnceMap<K, V, S = RandomState> {
    map: Mutex<KeyedOnceTable<K, V, S>>,
}

enum ComputeState<K, V> {
    Cached(V),
    Uninitialized(Arc<KeyedOnceEntry<K, V>>),
}

// Holds one call's entry so Drop can clean it up if the computation is abandoned.
struct ComputeCleanupGuard<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    once_map: &'a OnceMap<K, V, S>,
    entry: Option<Arc<KeyedOnceEntry<K, V>>>,
}

impl<'a, K, V, S> ComputeCleanupGuard<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(once_map: &'a OnceMap<K, V, S>, entry: Arc<KeyedOnceEntry<K, V>>) -> Self {
        Self {
            once_map,
            entry: Some(entry),
        }
    }

    fn cell(&self) -> &OnceCell<V> {
        self.entry.as_deref().unwrap().cell()
    }

    fn disarm_cleanup(&mut self) {
        drop(self.entry.take());
    }
}

impl<K, V, S> Drop for ComputeCleanupGuard<'_, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        let Some(entry) = self.entry.take() else {
            return;
        };
        if entry.cell().get().is_some() {
            return;
        }

        let mut map = self.once_map.map.lock();
        map.remove_abandoned(&entry);
        // Let another cleanup waiting for the map observe the updated reference count.
        drop(entry);
    }
}

impl<K, V, S> Default for OnceMap<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: BuildHasher + Default,
{
    fn default() -> Self {
        Self::with_hasher(S::default())
    }
}

impl<K, V> OnceMap<K, V, RandomState>
where
    K: Eq + Hash,
    V: Clone,
{
    /// Creates a new OnceMap with the default hasher.
    pub fn new() -> Self {
        Self {
            map: Mutex::new(KeyedOnceTable::with_hasher(RandomState::new())),
        }
    }

    /// Creates a new OnceMap with the default hasher and the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: Mutex::new(KeyedOnceTable::with_capacity_and_hasher(
                capacity,
                RandomState::new(),
            )),
        }
    }
}

impl<K, V, S> OnceMap<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: BuildHasher,
{
    /// Creates a new OnceMap with the given hasher.
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            map: Mutex::new(KeyedOnceTable::with_hasher(hasher)),
        }
    }

    /// Create a OnceMap with the specified capacity and hasher.
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        Self {
            map: Mutex::new(KeyedOnceTable::with_capacity_and_hasher(capacity, hasher)),
        }
    }

    fn entry_state(&self, key: K) -> ComputeState<K, V> {
        let mut map = self.map.lock();
        let entry = map.get_or_insert(key);
        match entry.cell().get() {
            Some(value) => ComputeState::Cached(value.clone()),
            None => ComputeState::Uninitialized(Arc::clone(entry)),
        }
    }

    /// Compute the value for the given key if absent.
    ///
    /// If the value for the key is already being computed by another task, this task will wait for
    /// the computation to finish and return the result.
    ///
    /// If the computation is cancelled or panics, another caller waiting for the same key may retry
    /// it.
    pub async fn compute<F>(&self, key: K, func: F) -> V
    where
        F: AsyncFnOnce() -> V,
    {
        let entry = match self.entry_state(key) {
            ComputeState::Cached(value) => return value,
            ComputeState::Uninitialized(entry) => entry,
        };

        let mut guard = ComputeCleanupGuard::new(self, entry);
        let result = guard.cell().get_or_init(func).await.clone();
        guard.disarm_cleanup();
        result
    }

    /// Compute the value for the given key if absent.
    ///
    /// If the value for the key is already being computed by another task, this task will wait for
    /// the computation to finish and return the result.
    ///
    /// If the computation returns an error, it is returned to that caller and the value is not
    /// stored. After an error, cancellation, or panic, another caller may retry the computation.
    pub async fn try_compute<E, F>(&self, key: K, func: F) -> Result<V, E>
    where
        F: AsyncFnOnce() -> Result<V, E>,
    {
        let entry = match self.entry_state(key) {
            ComputeState::Cached(value) => return Ok(value),
            ComputeState::Uninitialized(entry) => entry,
        };

        let mut guard = ComputeCleanupGuard::new(self, entry);
        let result = guard.cell().get_or_try_init(func).await?.clone();
        guard.disarm_cleanup();
        Ok(result)
    }

    /// Get a clone of the value for the given key if exists.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let map = self.map.lock();
        let entry = map.get(key)?;
        entry.cell().get().cloned()
    }

    /// Remove the given key from the map.
    ///
    /// If you need to get the value that has been removed, use the [`remove`] method instead.
    ///
    /// [`remove`]: Self::remove
    pub fn discard<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut map = self.map.lock();
        map.remove(key);
    }

    /// Remove the given key from the map and return a *clone* of the value if exists.
    ///
    /// If you do not need to get the value that has been removed, use the [`discard`] method
    /// instead.
    ///
    /// [`discard`]: Self::discard
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let entry = self.map.lock().remove(key)?;
        entry.cell().get().cloned()
    }
}

impl<K, V, S> FromIterator<(K, V)> for OnceMap<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: Default + BuildHasher,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut map = KeyedOnceTable::with_hasher(S::default());
        for (key, value) in iter {
            map.insert_value(key, value);
        }

        Self {
            map: Mutex::new(map),
        }
    }
}
