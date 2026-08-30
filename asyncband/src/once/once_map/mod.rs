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
use std::cell::Cell;
use std::cell::RefCell;
use std::fmt;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::RandomState;
use std::marker::PhantomData;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

use hashbrown::HashTable;

use crate::internal::mutex::Mutex;
use crate::once::OnceCell;

#[cfg(test)]
mod tests;

const INITIAL_READY_CAPACITY: usize = 16;
const READY_LOAD_NUMERATOR: usize = 3;
const READY_LOAD_DENOMINATOR: usize = 4;

// Each synchronous lookup publishes the map it is reading in one thread-local record. Removal
// blocks new lookups and waits for matching records to clear before freeing table allocations.
// Table growth retains previous generations and therefore does not need to block readers.
//
// Publication, blocking, verification, and registry scans are sequentially consistent. A reader
// therefore either verifies before removal starts and appears in the writer's scan, or observes
// the block and retries without dereferencing the ready table.
struct ReadBarrier {
    blocked: AtomicBool,
}

struct ReaderRecord {
    barrier: AtomicPtr<()>,
    claimed: AtomicBool,
    next: AtomicPtr<ReaderRecord>,
}

impl ReaderRecord {
    const fn claimed() -> Self {
        Self {
            barrier: AtomicPtr::new(ptr::null_mut()),
            claimed: AtomicBool::new(true),
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }
}

// Records are never unlinked, so removal can scan the registry without protecting the registry
// itself. A thread keeps the records needed by its deepest nested lookup and releases its claims
// when the thread exits, allowing later threads to reuse them.
static READER_RECORDS: AtomicPtr<ReaderRecord> = AtomicPtr::new(ptr::null_mut());

struct ThreadReaders {
    records: RefCell<Vec<&'static ReaderRecord>>,
    depth: Cell<usize>,
}

impl Drop for ThreadReaders {
    fn drop(&mut self) {
        for record in self.records.get_mut().drain(..) {
            release_reader(record);
        }
    }
}

thread_local! {
    static THREAD_READERS: ThreadReaders = const { ThreadReaders {
        records: RefCell::new(Vec::new()),
        depth: Cell::new(0),
    } };
}

fn claim_reader() -> &'static ReaderRecord {
    let mut current = READER_RECORDS.load(Ordering::Acquire);
    while !current.is_null() {
        // SAFETY: Reader records are allocated once and never freed or unlinked.
        let record = unsafe { &*current };
        if record
            .claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return record;
        }
        current = record.next.load(Ordering::Relaxed);
    }

    let record: &'static ReaderRecord = Box::leak(Box::new(ReaderRecord::claimed()));
    let raw = ptr::from_ref(record).cast_mut();
    let mut head = READER_RECORDS.load(Ordering::SeqCst);
    loop {
        record.next.store(head, Ordering::Relaxed);
        match READER_RECORDS.compare_exchange_weak(head, raw, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return record,
            Err(actual) => head = actual,
        }
    }
}

fn release_reader(record: &'static ReaderRecord) {
    debug_assert!(record.barrier.load(Ordering::Relaxed).is_null());
    record.claimed.store(false, Ordering::Release);
}

struct ReaderClaim(&'static ReaderRecord);

impl Drop for ReaderClaim {
    fn drop(&mut self) {
        release_reader(self.0);
    }
}

fn with_thread_reader<R>(f: impl FnOnce(&ReaderRecord) -> R) -> R {
    let mut f = Some(f);
    match THREAD_READERS.try_with(|thread| {
        let depth = thread.depth.get();
        thread.depth.set(depth + 1);
        let depth_guard = ReaderDepth {
            depth: &thread.depth,
            previous: depth,
        };
        let record = {
            let mut records = thread.records.borrow_mut();
            if depth == records.len() {
                records.push(claim_reader());
            }
            records[depth]
        };
        let result = f.take().unwrap()(record);
        drop(depth_guard);
        result
    }) {
        Ok(result) => result,
        Err(_) => {
            let record = ReaderClaim(claim_reader());
            f.take().unwrap()(record.0)
        }
    }
}

struct ReaderDepth<'a> {
    depth: &'a Cell<usize>,
    previous: usize,
}

impl Drop for ReaderDepth<'_> {
    fn drop(&mut self) {
        self.depth.set(self.previous);
    }
}

struct ActiveReader<'a> {
    record: &'a ReaderRecord,
}

impl<'a> ActiveReader<'a> {
    fn new(record: &'a ReaderRecord, barrier: *mut ()) -> Self {
        record.barrier.store(barrier, Ordering::SeqCst);
        Self { record }
    }
}

impl Drop for ActiveReader<'_> {
    fn drop(&mut self) {
        self.record.barrier.store(ptr::null_mut(), Ordering::SeqCst);
    }
}

struct BlockedReaders<'a> {
    barrier: &'a ReadBarrier,
}

impl Drop for BlockedReaders<'_> {
    fn drop(&mut self) {
        self.barrier.blocked.store(false, Ordering::SeqCst);
    }
}

impl ReadBarrier {
    const fn new() -> Self {
        Self {
            blocked: AtomicBool::new(false),
        }
    }

    fn marker(&self) -> *mut () {
        ptr::from_ref(self).cast_mut().cast()
    }

    fn read<R>(&self, f: impl FnOnce() -> R) -> R {
        let mut f = Some(f);
        with_thread_reader(|record| {
            loop {
                let mut spins = 0;
                while self.blocked.load(Ordering::SeqCst) {
                    if spins < 16 {
                        std::hint::spin_loop();
                        spins += 1;
                    } else {
                        std::thread::yield_now();
                    }
                }

                let active = ActiveReader::new(record, self.marker());
                if self.blocked.load(Ordering::SeqCst) {
                    drop(active);
                    continue;
                }

                let result = f.take().unwrap()();
                drop(active);
                return result;
            }
        })
    }

    // The caller must serialize blockers and every operation that can free protected data.
    fn block(&self) -> BlockedReaders<'_> {
        let marker = self.marker();
        // Trait implementations invoked by a read are expected not to reenter the map. Detect this
        // misuse rather than waiting forever for the current thread's own reader record to clear.
        assert!(
            !current_thread_reads(marker),
            "OnceMap removal cannot run reentrantly during a read of the same map"
        );
        let was_blocked = self.blocked.swap(true, Ordering::SeqCst);
        assert!(!was_blocked, "nested OnceMap removal is not supported");

        let mut spins = 0;
        while has_active_reader(marker) {
            if spins < 16 {
                std::hint::spin_loop();
                spins += 1;
            } else {
                std::thread::yield_now();
            }
        }

        BlockedReaders { barrier: self }
    }
}

fn current_thread_reads(marker: *mut ()) -> bool {
    THREAD_READERS
        .try_with(|thread| {
            let depth = thread.depth.get();
            thread
                .records
                .borrow()
                .iter()
                .take(depth)
                .any(|record| record.barrier.load(Ordering::Relaxed) == marker)
        })
        .unwrap_or(false)
}

fn has_active_reader(marker: *mut ()) -> bool {
    let mut current = READER_RECORDS.load(Ordering::SeqCst);
    while !current.is_null() {
        // SAFETY: Reader records are allocated once and never freed or unlinked.
        let record = unsafe { &*current };
        if record.barrier.load(Ordering::SeqCst) == marker {
            return true;
        }
        current = record.next.load(Ordering::Relaxed);
    }
    false
}

struct ReadyIndex<K, V> {
    root: AtomicPtr<ReadyTable<K, V>>,
    ownership: PhantomData<Box<ReadyTable<K, V>>>,
}

// Ready tables use open addressing and never delete entries in place. A nonzero tag publishes the
// pointer in the matching slot: writers store the pointer before the tag with release ordering,
// while readers acquire the tag before dereferencing the pointer. A replacement table owns the
// previous generation until removal can drain readers and reclaim the complete chain.
struct ReadyTable<K, V> {
    tags: Box<[AtomicU8]>,
    slots: Box<[AtomicPtr<Entry<K, V>>]>,
    owners: Mutex<Vec<Arc<Entry<K, V>>>>,
    previous: Option<Box<ReadyTable<K, V>>>,
}

impl<K, V> ReadyIndex<K, V> {
    const fn new() -> Self {
        Self {
            root: AtomicPtr::new(ptr::null_mut()),
            ownership: PhantomData,
        }
    }

    fn has_root(&self) -> bool {
        !self.root.load(Ordering::Acquire).is_null()
    }

    // SAFETY: The caller must either hold the map's read barrier or serialize deletion with the
    // map's writer lock. Growth may run concurrently because it retains every published table.
    unsafe fn find<R>(
        &self,
        hash: u64,
        matches: impl Fn(&Entry<K, V>) -> bool,
        found: impl Fn(&Entry<K, V>) -> R,
    ) -> Option<R> {
        let table = self.root.load(Ordering::Acquire);
        if table.is_null() {
            return None;
        }

        // SAFETY: The caller protects the root generation, which owns every entry it references.
        unsafe { (&*table).find(hash, matches, found) }
    }

    // SAFETY: The caller must serialize writers. This operation never frees published data.
    unsafe fn insert(&self, entry: Arc<Entry<K, V>>) {
        let current = self.root.load(Ordering::Acquire);
        if current.is_null() {
            let table = Box::new(ReadyTable::new(INITIAL_READY_CAPACITY));
            table.insert(entry);
            self.root.store(Box::into_raw(table), Ordering::Release);
            return;
        }

        // SAFETY: The writer lock retains the current table, and insertion cannot free it.
        let current_table = unsafe { &*current };
        if !current_table.needs_grow() {
            current_table.insert(entry);
            return;
        }

        let capacity = current_table
            .capacity()
            .checked_mul(2)
            .expect("OnceMap ready table capacity overflow");
        let mut replacement = Box::new(ReadyTable::new(capacity));
        replacement.copy_from(current_table);
        replacement.insert(entry);

        // No operation below can unwind: transfer ownership of the current generation into its
        // replacement, then publish the fully initialized table.
        // SAFETY: `root` uniquely owns the current table allocation, and the replacement assumes
        // that ownership without moving the allocation readers may still be traversing.
        replacement.previous = Some(unsafe { Box::from_raw(current) });
        self.root
            .store(Box::into_raw(replacement), Ordering::Release);
    }

    // SAFETY: The caller must either hold exclusive map access or serialize writers, block new
    // readers, and wait for active readers before calling this method.
    unsafe fn remove<Q>(&self, hash: u64, key: &Q) -> Option<Arc<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let current = self.root.load(Ordering::Relaxed);
        if current.is_null() {
            return None;
        }

        // SAFETY: The caller has drained readers and serialized writers.
        let current_table = unsafe { &*current };
        // SAFETY: The current table and all its entries remain exclusively protected.
        let removed = unsafe { current_table.find_ptr(hash, |entry| entry.key.borrow() == key) }?;

        let remaining = current_table.len() - 1;
        let replacement = if remaining == 0 {
            ptr::null_mut()
        } else {
            let replacement = Box::new(ReadyTable::new(current_table.capacity()));
            replacement.copy_except(current_table, removed);
            Box::into_raw(replacement)
        };

        // Keep the removed entry alive independently of all table generations before reclaiming
        // their strong references.
        let removed = current_table.clone_owner(removed);

        self.root.store(replacement, Ordering::Release);
        // SAFETY: Readers are drained, the root no longer exposes this allocation, and retained
        // entries have independent references in the replacement table.
        drop(unsafe { Box::from_raw(current) });
        Some(removed)
    }

    // SAFETY: The caller must hold the map's read barrier or serialize removal.
    unsafe fn for_each(&self, mut visit: impl FnMut(&Entry<K, V>)) {
        let root = self.root.load(Ordering::Acquire);
        if !root.is_null() {
            // SAFETY: The caller prevents the current table and its entries from being freed.
            unsafe { (&*root).for_each(&mut visit) };
        }
    }
}

impl<K, V> Drop for ReadyIndex<K, V> {
    fn drop(&mut self) {
        let root = *self.root.get_mut();
        if !root.is_null() {
            // SAFETY: Exclusive access proves that no reader remains, and the root owns every
            // retained table generation.
            drop(unsafe { Box::from_raw(root) });
        }
    }
}

impl<K, V> ReadyTable<K, V> {
    fn new(capacity: usize) -> Self {
        debug_assert!(capacity >= INITIAL_READY_CAPACITY);
        debug_assert!(capacity.is_power_of_two());
        Self {
            tags: std::iter::repeat_with(|| AtomicU8::new(0))
                .take(capacity)
                .collect(),
            slots: std::iter::repeat_with(|| AtomicPtr::new(ptr::null_mut()))
                .take(capacity)
                .collect(),
            owners: Mutex::new(Vec::with_capacity(max_ready_len(capacity))),
            previous: None,
        }
    }

    fn capacity(&self) -> usize {
        self.slots.len()
    }

    fn len(&self) -> usize {
        self.owners.lock().len()
    }

    fn needs_grow(&self) -> bool {
        self.len() + 1 > max_ready_len(self.capacity())
    }

    fn insert(&self, entry: Arc<Entry<K, V>>) {
        let hash = entry.hash;
        let index = self
            .vacant_index(hash)
            .expect("ready table must retain an empty slot");
        let raw = Arc::as_ptr(&entry).cast_mut();
        self.owners.lock().push(entry);
        self.slots[index].store(raw, Ordering::Relaxed);
        self.tags[index].store(hash_tag(hash), Ordering::Release);
    }

    fn vacant_index(&self, hash: u64) -> Option<usize> {
        let mask = self.capacity() - 1;
        let mut index = hash as usize & mask;
        for _ in 0..self.capacity() {
            if self.tags[index].load(Ordering::Relaxed) == 0 {
                return Some(index);
            }
            index = (index + 1) & mask;
        }
        None
    }

    // SAFETY: The caller must protect this table and its entries from reclamation.
    unsafe fn find<R>(
        &self,
        hash: u64,
        matches: impl Fn(&Entry<K, V>) -> bool,
        found: impl Fn(&Entry<K, V>) -> R,
    ) -> Option<R> {
        let entry = unsafe { self.find_ptr(hash, matches) }?;
        // SAFETY: `find_ptr` returned an entry owned by this protected table.
        Some(found(unsafe { &*entry }))
    }

    // SAFETY: The caller must protect this table and its entries from reclamation.
    unsafe fn find_ptr(
        &self,
        hash: u64,
        matches: impl Fn(&Entry<K, V>) -> bool,
    ) -> Option<*mut Entry<K, V>> {
        let mask = self.capacity() - 1;
        let mut index = hash as usize & mask;
        let tag = hash_tag(hash);
        for _ in 0..self.capacity() {
            let stored_tag = self.tags[index].load(Ordering::Acquire);
            if stored_tag == 0 {
                return None;
            }
            if stored_tag != tag {
                index = (index + 1) & mask;
                continue;
            }
            let entry = self.slots[index].load(Ordering::Relaxed);
            debug_assert!(!entry.is_null());
            // SAFETY: The caller protects every non-null slot from reclamation.
            let entry_ref = unsafe { &*entry };
            if entry_ref.hash == hash && matches(entry_ref) {
                return Some(entry);
            }
            index = (index + 1) & mask;
        }
        None
    }

    fn copy_from(&self, source: &Self) {
        self.copy_except(source, ptr::null_mut());
    }

    fn copy_except(&self, source: &Self, excluded: *mut Entry<K, V>) {
        let source = source.owners.lock();
        let mut owners = self.owners.lock();
        for entry in source.iter() {
            let raw = Arc::as_ptr(entry).cast_mut();
            if raw == excluded {
                continue;
            }

            let hash = entry.hash;
            let index = self
                .vacant_index(hash)
                .expect("replacement ready table must retain an empty slot");
            owners.push(Arc::clone(entry));
            self.slots[index].store(raw, Ordering::Relaxed);
            self.tags[index].store(hash_tag(hash), Ordering::Release);
        }
    }

    fn clone_owner(&self, target: *mut Entry<K, V>) -> Arc<Entry<K, V>> {
        self.owners
            .lock()
            .iter()
            .find(|entry| Arc::as_ptr(entry).cast_mut() == target)
            .cloned()
            .expect("ready slot must have a table owner")
    }

    // SAFETY: The caller must protect this table and its entries from reclamation.
    unsafe fn for_each(&self, visit: &mut impl FnMut(&Entry<K, V>)) {
        for (tag, slot) in self.tags.iter().zip(&self.slots) {
            if tag.load(Ordering::Acquire) != 0 {
                let entry = slot.load(Ordering::Relaxed);
                debug_assert!(!entry.is_null());
                // SAFETY: The caller protects every non-null slot from reclamation.
                visit(unsafe { &*entry });
            }
        }
    }
}

fn hash_tag(hash: u64) -> u8 {
    ((hash >> (u64::BITS - u8::BITS)) as u8).max(1)
}

fn max_ready_len(capacity: usize) -> usize {
    capacity / READY_LOAD_DENOMINATOR * READY_LOAD_NUMERATOR
}

struct Entry<K, V> {
    hash: u64,
    key: K,
    cell: OnceCell<V>,
}

struct WriteState<K, V> {
    pending: HashTable<Arc<Entry<K, V>>>,
}

enum Lookup<K, V> {
    Ready(V),
    Pending(Arc<Entry<K, V>>),
}

/// A hash map that runs computation only once for each key and stores the result.
///
/// Note that this always clones the value out of the underlying map. Because of this, it's common
/// to wrap the `V` in an `Arc<V>` to make cloning cheap.
pub struct OnceMap<K, V, S = RandomState> {
    // Successful values live in a lazily allocated atomic table. The write lock protects table
    // mutation and the separate set of computations that have not completed yet.
    ready: ReadyIndex<K, V>,
    readers: ReadBarrier,
    write: Mutex<WriteState<K, V>>,
    hasher: S,
}

impl<K, V, S> fmt::Debug for OnceMap<K, V, S>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Write::write_str(f, "OnceMap ")?;
        let mut debug_map = f.debug_map();
        self.readers.read(|| {
            // SAFETY: The read barrier protects the current table and every referenced entry.
            unsafe {
                self.ready.for_each(|entry| {
                    debug_map.entry(&entry.key, &entry.cell);
                });
            }
        });
        let write = self.write.lock();
        for entry in write.pending.iter() {
            debug_map.entry(&entry.key, &entry.cell);
        }
        debug_map.finish()
    }
}

impl<K, V, S> OnceMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn get_or_insert(&self, key: K) -> Lookup<K, V>
    where
        V: Clone,
    {
        let hash = self.hasher.hash_one(&key);
        if let Some(value) = self.get_ready(hash, |entry| entry.key.eq(&key)) {
            return Lookup::Ready(value);
        }

        let mut write = self.write.lock();
        // SAFETY: The write lock prevents deletion while the fast-path miss is rechecked.
        if let Some(value) = unsafe {
            self.ready.find(
                hash,
                |entry| entry.key.eq(&key),
                |entry| entry.cell.get().cloned(),
            )
        }
        .flatten()
        {
            return Lookup::Ready(value);
        }

        if let Some(entry) = write.pending.find(hash, |entry| entry.key.eq(&key)) {
            return Lookup::Pending(Arc::clone(entry));
        }

        let entry = Arc::new(Entry {
            hash,
            key,
            cell: OnceCell::new(),
        });
        write
            .pending
            .insert_unique(hash, Arc::clone(&entry), |entry| entry.hash);
        Lookup::Pending(entry)
    }

    fn get_ready(&self, hash: u64, matches: impl Fn(&Entry<K, V>) -> bool) -> Option<V>
    where
        V: Clone,
    {
        // A null root is safe to observe without reader registration: there is no allocation to
        // protect, and a concurrent insertion can linearize immediately after this miss.
        if !self.ready.has_root() {
            return None;
        }
        self.readers.read(|| {
            // SAFETY: The read barrier protects the current table and every referenced entry.
            unsafe {
                self.ready.find(hash, matches, |entry| {
                    entry
                        .cell
                        .get()
                        .expect("ready entry is initialized")
                        .clone()
                })
            }
        })
    }

    fn publish_if_current(&self, entry: &Arc<Entry<K, V>>) {
        debug_assert!(entry.cell.initialized());
        let mut write = self.write.lock();
        let Some(stored) = remove_pending_if(&mut write.pending, entry.hash, |stored| {
            Arc::ptr_eq(stored, entry)
        }) else {
            return;
        };

        // SAFETY: The write lock serializes table growth. Insertion only publishes new allocations.
        unsafe { self.ready.insert(stored) };
    }

    fn detach<Q>(&self, key: &Q) -> Option<Arc<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let mut write = self.write.lock();
        let blocked = self.readers.block();
        // SAFETY: The write lock serializes deletion, and the barrier has drained every reader.
        let ready = unsafe { self.ready.remove(hash, key) };
        let pending = if ready.is_none() {
            remove_pending_if(&mut write.pending, hash, |entry| entry.key.borrow() == key)
        } else {
            None
        };
        drop(blocked);
        drop(write);
        ready.or(pending)
    }

    fn cleanup_abandoned_entry(&self, entry: Arc<Entry<K, V>>) {
        let removed = {
            let mut write = self.write.lock();
            let Ok(occupied) = write
                .pending
                .find_entry(entry.hash, |stored| Arc::ptr_eq(stored, &entry))
            else {
                drop(write);
                drop(entry);
                return;
            };

            // With table ownership confirmed and the writer locked against new callers, two
            // owners means the table and this cleanup guard are the only remaining references.
            if Arc::strong_count(&entry) == 2 && !entry.cell.initialized() {
                Some(occupied.remove().0)
            } else {
                // A waiting cleanup must observe this call's reference being released while no new
                // caller can clone the table's reference.
                drop(entry);
                None
            }
        };
        drop(removed);
    }

    fn insert(&mut self, key: K, value: V) {
        let hash = self.hasher.hash_one(&key);
        let entry = Arc::new(Entry {
            hash,
            key,
            cell: OnceCell::from_value(value),
        });

        let mut write = self.write.lock();
        // SAFETY: Exclusive map access proves that no reader or competing writer exists.
        let ready = unsafe { self.ready.remove(hash, &entry.key) };
        let pending = if ready.is_none() {
            remove_pending_if(&mut write.pending, hash, |stored| stored.key.eq(&entry.key))
        } else {
            None
        };
        // SAFETY: Exclusive map access serializes table growth.
        unsafe { self.ready.insert(entry) };
        drop(ready.or(pending));
    }
}

fn remove_pending_if<K, V>(
    pending: &mut HashTable<Arc<Entry<K, V>>>,
    hash: u64,
    matches: impl Fn(&Arc<Entry<K, V>>) -> bool,
) -> Option<Arc<Entry<K, V>>> {
    Some(pending.find_entry(hash, matches).ok()?.remove().0)
}

impl<K, V, S> FromIterator<(K, V)> for OnceMap<K, V, S>
where
    K: Eq + Hash,
    V: Clone,
    S: BuildHasher + Default,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut map = Self::with_hasher(S::default());
        for (key, value) in iter {
            map.insert(key, value);
        }
        map
    }
}

// Holds one call's entry so Drop can remove an abandoned computation from the pending table.
struct ComputeCleanupGuard<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    once_map: &'a OnceMap<K, V, S>,
    entry: Option<Arc<Entry<K, V>>>,
}

impl<'a, K, V, S> ComputeCleanupGuard<'a, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(once_map: &'a OnceMap<K, V, S>, entry: Arc<Entry<K, V>>) -> Self {
        Self {
            once_map,
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

impl<K, V, S> Drop for ComputeCleanupGuard<'_, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn drop(&mut self) {
        let Some(entry) = self.entry.take() else {
            return;
        };
        self.once_map.cleanup_abandoned_entry(entry);
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
            ready: ReadyIndex::new(),
            readers: ReadBarrier::new(),
            write: Mutex::new(WriteState {
                pending: HashTable::new(),
            }),
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
        let entry = match self.get_or_insert(key) {
            Lookup::Ready(value) => return value,
            Lookup::Pending(entry) => entry,
        };

        let guard = ComputeCleanupGuard::new(self, entry);
        let result = guard.entry().cell.get_or_init(func).await;
        self.publish_if_current(guard.entry());
        let result = result.clone();
        guard.dismiss();
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
        let entry = match self.get_or_insert(key) {
            Lookup::Ready(value) => return Ok(value),
            Lookup::Pending(entry) => entry,
        };

        let guard = ComputeCleanupGuard::new(self, entry);
        let result = guard.entry().cell.get_or_try_init(func).await?;
        self.publish_if_current(guard.entry());
        let result = result.clone();
        guard.dismiss();
        Ok(result)
    }

    /// Get a clone of the value for the given key if exists.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        self.get_ready(hash, |entry| entry.key.borrow() == key)
    }

    /// Remove the given key from the map.
    ///
    /// If you need to get the value that has been removed, use the [`remove`] method instead.
    ///
    /// This may wait for concurrent lookups that are already reading the map. An in-flight
    /// computation is detached but continues for callers that already joined it; its result is not
    /// stored in the map.
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
    /// This may wait for concurrent lookups that are already reading the map. An in-flight
    /// computation is detached but continues for callers that already joined it; its result is not
    /// stored in the map.
    ///
    /// [`discard`]: Self::discard
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let entry = self.detach(key)?;
        entry.cell.get().cloned()
    }
}
