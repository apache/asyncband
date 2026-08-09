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

//! Singleflight provides a duplicate function call suppression mechanism.

use std::borrow::Borrow;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;
use std::sync::Arc;

use crate::internal::Mutex;
use crate::internal::OnceTable;
use crate::internal::OnceTableEntry;

#[cfg(test)]
mod tests;

/// Group represents a class of work and forms a namespace in which
/// units of work can be executed with duplicate suppression.
#[derive(Debug)]
pub struct Group<K, V, S = RandomState> {
    map: Mutex<OnceTable<K, V, S>>,
}

// Holds one call's entry so Drop can clean it up if the work is abandoned.
struct WorkCleanupGuard<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    group: &'a Group<K, V, S>,
    entry: Option<Arc<OnceTableEntry<K, V>>>,
}

impl<'a, K, V, S> WorkCleanupGuard<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(group: &'a Group<K, V, S>, key: K) -> Self {
        let entry = {
            let mut map = group.map.lock();
            Arc::clone(map.get_or_insert(key))
        };

        Self {
            group,
            entry: Some(entry),
        }
    }

    fn entry(&self) -> &Arc<OnceTableEntry<K, V>> {
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

        let mut table = self.group.map.lock();
        // If the table still owns this entry, a count of two means the current call is its only
        // owner outside the table. remove_entry rejects an entry that was detached or replaced.
        if Arc::strong_count(&entry) == 2 && !entry.initialized() {
            table.remove_entry(&entry);
        }
        // Drop this call's reference before unlocking so a waiting cleanup observes the updated
        // reference count.
        drop(entry);
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
        Self {
            map: Mutex::new(OnceTable::with_hasher(RandomState::new())),
        }
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
            map: Mutex::new(OnceTable::with_hasher(hasher)),
        }
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
    /// use mea::singleflight::Group;
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
            .get_or_init(async || {
                let result = func().await;
                self.map.lock().remove_entry(entry);
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
    /// use mea::singleflight::Group;
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
            .get_or_try_init(async || {
                let result = func().await?;
                self.map.lock().remove_entry(entry);
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
        let mut map = self.map.lock();
        map.remove(key);
    }
}
