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
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;
use std::sync::Arc;

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
    map: Mutex<HashMap<K, Arc<OnceCell<V>>, S>>,
}

// Holds one call's cell reference so Drop can clean up an abandoned entry.
struct ComputeCleanupGuard<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    once_map: &'a OnceMap<K, V, S>,
    cell: Option<Arc<OnceCell<V>>>,
}

impl<'a, K, V, S> ComputeCleanupGuard<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(once_map: &'a OnceMap<K, V, S>, key: K) -> Self {
        let cell = {
            let mut map = once_map.map.lock();
            map.entry(key)
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };

        Self {
            once_map,
            cell: Some(cell),
        }
    }

    fn cell(&self) -> &OnceCell<V> {
        self.cell.as_deref().unwrap()
    }

    fn disarm_cleanup(&mut self) {
        drop(self.cell.take());
    }
}

impl<K, V, S> Drop for ComputeCleanupGuard<'_, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        let Some(cell) = self.cell.take() else {
            return;
        };
        if cell.get().is_some() {
            return;
        }

        let mut map = self.once_map.map.lock();

        // The map and each current call own one strong reference. If the map still points to this
        // cell, a count of two means only the map and this last call own it. Drop this call's
        // reference before unlocking so a waiting cleanup observes the updated count.
        if Arc::strong_count(&cell) == 2 {
            // OnceMap intentionally does not require K: Clone, so locate the cell by allocation
            // identity. This scan only runs when the last call leaves a cell uninitialized.
            map.retain(|_, existing| !Arc::ptr_eq(existing, &cell) || existing.get().is_some());
        }
        drop(cell);
    }
}

impl<K, V, S> Default for OnceMap<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: BuildHasher + Clone + Default,
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
            map: Mutex::new(HashMap::new()),
        }
    }

    /// Creates a new OnceMap with the default hasher and the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: Mutex::new(HashMap::with_capacity(capacity)),
        }
    }
}

impl<K, V, S> OnceMap<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: BuildHasher + Clone,
{
    /// Creates a new OnceMap with the given hasher.
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            map: Mutex::new(HashMap::with_hasher(hasher)),
        }
    }

    /// Create a OnceMap with the specified capacity and hasher.
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        Self {
            map: Mutex::new(HashMap::with_capacity_and_hasher(capacity, hasher)),
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
        let mut guard = ComputeCleanupGuard::new(self, key);
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
        let mut guard = ComputeCleanupGuard::new(self, key);
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
        let cell = map.get(key)?;
        cell.get().cloned()
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
        let cell = self.map.lock().remove(key)?;
        cell.get().cloned()
    }
}

impl<K, V, S> FromIterator<(K, V)> for OnceMap<K, V, S>
where
    K: Eq + Hash + Clone,
    V: Clone,
    S: Default + BuildHasher + Clone,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self {
            map: Mutex::new(
                iter.into_iter()
                    .map(|(k, v)| (k, Arc::new(OnceCell::from_value(v))))
                    .collect(),
            ),
        }
    }
}
