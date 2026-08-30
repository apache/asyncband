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
use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;
use std::mem;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use hashbrown::HashTable;

use crate::internal::mutex::Mutex;
use crate::internal::waitset::WaitSet;
use crate::internal::waitset::WakerToken;
use crate::internal::waitset::wake_all;

#[cfg(test)]
mod tests;

// The table is authoritative for each key. Initialization runs outside its lock, then changes the
// same entry from Pending to Ready; pointer identity prevents a detached generation from publishing
// over a replacement created by remove or discard.
struct Entry<K, V> {
    hash: u64,
    key: K,
    state: EntryState<V>,
}

enum EntryState<V> {
    Ready(Arc<V>),
    Pending(Arc<Initialization<V>>),
}

// Coordination exists only while a value is being initialized. A ready entry retains just its key
// and value; callers already joined to a detached initialization keep it alive through this Arc.
struct Initialization<V> {
    hash: u64,
    state: Mutex<InitializationState<V>>,
}

enum InitializationState<V> {
    Running(WaitSet),
    Complete(Completion<V>),
}

enum Completion<V> {
    Value(Arc<V>),
    Retry,
}

enum Lookup<K, V> {
    Ready(Arc<V>),
    Wait(K, Arc<Initialization<V>>),
    Start(Arc<Initialization<V>>),
}

impl<V> Initialization<V> {
    fn new(hash: u64) -> Self {
        Self {
            hash,
            state: Mutex::new(InitializationState::Running(WaitSet::new())),
        }
    }

    fn complete(&self, completion: Completion<V>) -> WaitSet {
        let mut state = self.state.lock();
        let InitializationState::Running(waiters) =
            mem::replace(&mut *state, InitializationState::Complete(completion))
        else {
            unreachable!("pending entry completed more than once")
        };
        waiters
    }

    fn wait(&self) -> InitializationWait<'_, V> {
        InitializationWait {
            initialization: self,
            token: None,
        }
    }
}

struct InitializationWait<'a, V> {
    initialization: &'a Initialization<V>,
    token: Option<WakerToken>,
}

impl<V> Future for InitializationWait<'_, V> {
    type Output = Completion<V>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self {
            initialization,
            token,
        } = self.get_mut();
        let replaced = {
            let mut state = initialization.state.lock();
            match &mut *state {
                InitializationState::Complete(Completion::Value(value)) => {
                    return Poll::Ready(Completion::Value(Arc::clone(value)));
                }
                InitializationState::Complete(Completion::Retry) => {
                    return Poll::Ready(Completion::Retry);
                }
                InitializationState::Running(waiters) => waiters.register_waker(token, cx),
            }
        };
        drop(replaced);
        Poll::Pending
    }
}

impl<V> Drop for InitializationWait<'_, V> {
    fn drop(&mut self) {
        let removed = {
            let mut state = self.initialization.state.lock();
            match &mut *state {
                InitializationState::Running(waiters) => waiters.unregister_waker(&mut self.token),
                InitializationState::Complete(_) => None,
            }
        };
        drop(removed);
    }
}

/// A hash map that runs computation only once for each key and stores the result.
///
/// Note that this always clones the value out of the underlying map. Because of this, it's common
/// to wrap the `V` in an `Arc<V>` to make cloning cheap.
pub struct OnceMap<K, V, S = RandomState> {
    entries: Mutex<HashTable<Entry<K, V>>>,
    hasher: S,
}

impl<K, V, S> fmt::Debug for OnceMap<K, V, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let entries = self.entries.lock();
        let pending = entries
            .iter()
            .filter(|entry| matches!(entry.state, EntryState::Pending(_)))
            .count();
        f.debug_struct("OnceMap")
            .field("len", &entries.len())
            .field("pending", &pending)
            .finish()
    }
}

impl<K, V, S> OnceMap<K, V, S> {
    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.lock().len()
    }
}

impl<K, V, S> OnceMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn lookup(&self, key: K) -> Lookup<K, V> {
        let hash = self.hasher.hash_one(&key);
        let mut entries = self.entries.lock();
        if let Some(entry) = entries.find(hash, |entry| entry.key.eq(&key)) {
            return match &entry.state {
                EntryState::Ready(value) => Lookup::Ready(Arc::clone(value)),
                EntryState::Pending(initialization) => {
                    Lookup::Wait(key, Arc::clone(initialization))
                }
            };
        }

        let initialization = Arc::new(Initialization::new(hash));
        entries.insert_unique(
            hash,
            Entry {
                hash,
                key,
                state: EntryState::Pending(Arc::clone(&initialization)),
            },
            |entry| entry.hash,
        );
        Lookup::Start(initialization)
    }

    fn finish_initialization(
        &self,
        initialization: &Arc<Initialization<V>>,
        completion: Completion<V>,
    ) -> (WaitSet, Option<Entry<K, V>>) {
        let mut stored_initialization = None;
        let mut detached_entry = None;
        {
            let mut entries = self.entries.lock();
            let current = |entry: &Entry<K, V>| {
                matches!(
                    &entry.state,
                    EntryState::Pending(stored) if Arc::ptr_eq(stored, initialization)
                )
            };
            if let Completion::Value(value) = &completion {
                if let Some(entry) = entries.find_mut(initialization.hash, current) {
                    let EntryState::Pending(pending) =
                        mem::replace(&mut entry.state, EntryState::Ready(Arc::clone(value)))
                    else {
                        unreachable!()
                    };
                    stored_initialization = Some(pending);
                }
            } else if let Ok(occupied) = entries.find_entry(initialization.hash, current) {
                detached_entry = Some(occupied.remove().0);
            }
        }
        // Never destroy table-owned state while holding the table lock.
        drop(stored_initialization);

        (initialization.complete(completion), detached_entry)
    }

    fn get_value<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        V: Clone,
    {
        let hash = self.hasher.hash_one(key);
        let value = {
            let entries = self.entries.lock();
            let entry = entries.find(hash, |entry| entry.key.borrow() == key)?;
            match &entry.state {
                EntryState::Ready(value) => Some(Arc::clone(value)),
                EntryState::Pending(_) => None,
            }
        }?;
        Some(value.as_ref().clone())
    }

    fn detach<Q>(&self, key: &Q) -> Option<Arc<V>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let removed = {
            let mut entries = self.entries.lock();
            let occupied = entries
                .find_entry(hash, |entry| entry.key.borrow() == key)
                .ok()?;
            occupied.remove().0
        };

        let Entry { key, state, .. } = removed;
        drop(key);
        match state {
            EntryState::Ready(value) => Some(value),
            EntryState::Pending(_) => None,
        }
    }

    async fn resolve<E, F>(&self, mut lookup: Lookup<K, V>, func: F) -> Result<V, E>
    where
        V: Clone,
        F: AsyncFnOnce() -> Result<V, E>,
    {
        loop {
            match lookup {
                Lookup::Ready(value) => return Ok(value.as_ref().clone()),
                Lookup::Wait(key, initialization) => match initialization.wait().await {
                    Completion::Value(value) => return Ok(value.as_ref().clone()),
                    Completion::Retry => lookup = self.lookup(key),
                },
                Lookup::Start(initialization) => {
                    let guard = InitializationGuard::new(self, initialization);
                    let value = Arc::new(func().await?);
                    guard.publish(Arc::clone(&value));
                    return Ok(value.as_ref().clone());
                }
            }
        }
    }
}

struct InitializationGuard<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    map: &'a OnceMap<K, V, S>,
    initialization: Option<Arc<Initialization<V>>>,
}

impl<'a, K, V, S> InitializationGuard<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(map: &'a OnceMap<K, V, S>, initialization: Arc<Initialization<V>>) -> Self {
        Self {
            map,
            initialization: Some(initialization),
        }
    }

    fn publish(mut self, value: Arc<V>) {
        let (mut wakers, detached_entry) = self.map.finish_initialization(
            self.initialization.as_ref().unwrap(),
            Completion::Value(value),
        );
        self.initialization.take();
        wake_all(wakers.take_wakers());
        drop(detached_entry);
    }
}

impl<K, V, S> Drop for InitializationGuard<'_, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        let Some(initialization) = self.initialization.take() else {
            return;
        };
        let (mut waiters, detached_entry) = self
            .map
            .finish_initialization(&initialization, Completion::Retry);
        // Cleanup runs during cancellation and may already be unwinding from user code.
        let _ = panic::catch_unwind(AssertUnwindSafe(|| wake_all(waiters.take_wakers())));
        drop(detached_entry);
    }
}

impl<K, V, S> FromIterator<(K, V)> for OnceMap<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: BuildHasher + Default,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let hasher = S::default();
        let mut entries: HashTable<Entry<K, V>> = HashTable::with_capacity(iter.size_hint().0);
        for (key, value) in iter {
            let hash = hasher.hash_one(&key);
            let replaced = entries
                .find_entry(hash, |entry| entry.key.eq(&key))
                .ok()
                .map(|occupied| occupied.remove().0);
            entries.insert_unique(
                hash,
                Entry {
                    hash,
                    key,
                    state: EntryState::Ready(Arc::new(value)),
                },
                |entry| entry.hash,
            );
            drop(replaced);
        }
        Self {
            entries: Mutex::new(entries),
            hasher,
        }
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
        Self::with_hasher(RandomState::new())
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
            entries: Mutex::new(HashTable::new()),
            hasher,
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
        match self.lookup(key) {
            Lookup::Ready(value) => value.as_ref().clone(),
            pending => match self
                .resolve(pending, async || Ok::<V, Infallible>(func().await))
                .await
            {
                Ok(value) => value,
            },
        }
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
        match self.lookup(key) {
            Lookup::Ready(value) => Ok(value.as_ref().clone()),
            pending => self.resolve(pending, func).await,
        }
    }

    /// Get a clone of the value for the given key if exists.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get_value(key)
    }

    /// Remove the given key from the map.
    ///
    /// If you need to get the value that has been removed, use the [`remove`] method instead.
    ///
    /// An in-flight computation is detached but continues for callers that already joined it; its
    /// result is not stored in the map.
    ///
    /// [`remove`]: Self::remove
    pub fn discard<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        drop(self.detach(key));
    }

    /// Remove the given key from the map and return a *clone* of the value if exists.
    ///
    /// If you do not need to get the value that has been removed, use the [`discard`] method
    /// instead.
    ///
    /// An in-flight computation is detached but continues for callers that already joined it; its
    /// result is not stored in the map.
    ///
    /// [`discard`]: Self::discard
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let value = self.detach(key)?;
        Some(value.as_ref().clone())
    }
}
