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
use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;

use hashbrown::HashTable;

use crate::internal::mutex::Mutex;
use crate::once::OnceCell;

#[cfg(test)]
mod tests;

const TRIE_BITS: u32 = 4;
const TRIE_FANOUT: usize = 1 << TRIE_BITS;
const TRIE_MASK: u64 = TRIE_FANOUT as u64 - 1;

// Each synchronous lookup publishes the map it is reading in one thread-local record. Removal
// blocks new lookups and waits for matching records to clear before freeing trie allocations.
// Trie growth never frees published allocations and therefore does not need to block readers.
//
// Publication, blocking, verification, and registry scans are sequentially consistent. A reader
// therefore either verifies before removal starts and appears in the writer's scan, or observes
// the block and retries without dereferencing the trie.
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

struct ReadyTrie<K, V> {
    root: AtomicPtr<TrieNode<K, V>>,
    ownership: PhantomData<Box<TrieNode<K, V>>>,
}

enum TrieNode<K, V> {
    Leaf(TrieLeaf<K, V>),
    Branch(TrieBranch<K, V>),
}

struct TrieBranch<K, V> {
    slots: [AtomicPtr<TrieNode<K, V>>; TRIE_FANOUT],
}

struct TrieLeaf<K, V> {
    hash: u64,
    entries: AtomicPtr<ReadyEntry<K, V>>,
    ownership: PhantomData<Box<ReadyEntry<K, V>>>,
}

struct ReadyEntry<K, V> {
    entry: Arc<Entry<K, V>>,
    next: AtomicPtr<ReadyEntry<K, V>>,
}

impl<K, V> ReadyTrie<K, V> {
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
    // map's writer lock. Growth may run concurrently because it only publishes initialized nodes.
    unsafe fn find<R>(
        &self,
        hash: u64,
        matches: impl Fn(&Entry<K, V>) -> bool,
        found: impl Fn(&Entry<K, V>) -> R,
    ) -> Option<R> {
        let mut node = self.root.load(Ordering::Acquire);
        let mut shift = 0;
        while !node.is_null() {
            // SAFETY: The caller prevents deletion, and published nodes remain allocated during
            // concurrent growth.
            match unsafe { &*node } {
                TrieNode::Branch(branch) => {
                    let index = ((hash >> shift) & TRIE_MASK) as usize;
                    node = branch.slots[index].load(Ordering::Acquire);
                    shift += TRIE_BITS;
                }
                TrieNode::Leaf(leaf) if leaf.hash == hash => {
                    let mut current = leaf.entries.load(Ordering::Acquire);
                    while !current.is_null() {
                        // SAFETY: Entry links have the same lifetime as their containing leaf while
                        // deletion is excluded.
                        let ready = unsafe { &*current };
                        if matches(&ready.entry) {
                            return Some(found(&ready.entry));
                        }
                        current = ready.next.load(Ordering::Acquire);
                    }
                    return None;
                }
                TrieNode::Leaf(_) => return None,
            }
        }
        None
    }

    // SAFETY: The caller must serialize trie writers. This operation never frees published data.
    unsafe fn insert(&self, entry: Arc<Entry<K, V>>) {
        let hash = entry.hash;
        let mut owner = &self.root;
        let mut shift = 0;
        loop {
            let node = owner.load(Ordering::Acquire);
            if node.is_null() {
                owner.store(new_leaf(entry), Ordering::Release);
                return;
            }

            // SAFETY: Serialized writers retain ownership of every published node, and insertion
            // never frees nodes that a reader may be traversing.
            match unsafe { &*node } {
                TrieNode::Branch(branch) => {
                    let index = ((hash >> shift) & TRIE_MASK) as usize;
                    owner = &branch.slots[index];
                    shift += TRIE_BITS;
                }
                TrieNode::Leaf(leaf) if leaf.hash == hash => {
                    let head = leaf.entries.load(Ordering::Relaxed);
                    let ready = Box::new(ReadyEntry {
                        entry,
                        next: AtomicPtr::new(head),
                    });
                    leaf.entries.store(Box::into_raw(ready), Ordering::Release);
                    return;
                }
                TrieNode::Leaf(leaf) => {
                    let replacement = split_leaves(node, leaf.hash, new_leaf(entry), hash, shift);
                    owner.store(replacement, Ordering::Release);
                    return;
                }
            }
        }
    }

    // SAFETY: The caller must either hold exclusive map access or serialize trie writers, block new
    // readers, and wait for active readers before calling this method.
    unsafe fn remove<Q>(&self, hash: u64, key: &Q) -> Option<Arc<Entry<K, V>>>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let mut owner = &self.root;
        let mut node = owner.load(Ordering::Relaxed);
        let mut shift = 0;
        while !node.is_null() {
            // SAFETY: The read barrier is blocked and the writer is serialized.
            match unsafe { &*node } {
                TrieNode::Branch(branch) => {
                    let index = ((hash >> shift) & TRIE_MASK) as usize;
                    owner = &branch.slots[index];
                    node = owner.load(Ordering::Relaxed);
                    shift += TRIE_BITS;
                }
                TrieNode::Leaf(leaf) if leaf.hash == hash => {
                    let mut link_owner = &leaf.entries;
                    let mut current = link_owner.load(Ordering::Relaxed);
                    while !current.is_null() {
                        // SAFETY: No reader or writer can access this link concurrently.
                        let ready = unsafe { &*current };
                        if ready.entry.key.borrow() == key {
                            let next = ready.next.load(Ordering::Relaxed);
                            link_owner.store(next, Ordering::Release);
                            // SAFETY: `link_owner` transferred this link's unique ownership here.
                            let removed = unsafe { Box::from_raw(current) };
                            let ReadyEntry { entry, .. } = *removed;

                            if leaf.entries.load(Ordering::Relaxed).is_null() {
                                owner.store(ptr::null_mut(), Ordering::Release);
                                // SAFETY: The parent slot transferred this now-empty leaf's unique
                                // ownership here.
                                drop(unsafe { Box::from_raw(node) });
                            }
                            return Some(entry);
                        }
                        link_owner = &ready.next;
                        current = link_owner.load(Ordering::Relaxed);
                    }
                    return None;
                }
                TrieNode::Leaf(_) => return None,
            }
        }
        None
    }

    // SAFETY: The caller must hold the map's read barrier or serialize deletion.
    unsafe fn for_each(&self, mut visit: impl FnMut(&Arc<Entry<K, V>>)) {
        let root = self.root.load(Ordering::Acquire);
        if !root.is_null() {
            // SAFETY: The caller prevents the root from being freed during traversal.
            unsafe { for_each_node(root, &mut visit) };
        }
    }
}

impl<K, V> Drop for ReadyTrie<K, V> {
    fn drop(&mut self) {
        let root = *self.root.get_mut();
        if !root.is_null() {
            // SAFETY: Exclusive access proves that no reader remains, and the root owns the whole
            // trie.
            drop(unsafe { Box::from_raw(root) });
        }
    }
}

impl<K, V> TrieBranch<K, V> {
    fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
        }
    }
}

impl<K, V> Drop for TrieBranch<K, V> {
    fn drop(&mut self) {
        for slot in &mut self.slots {
            let node = *slot.get_mut();
            if !node.is_null() {
                // SAFETY: Each non-null branch slot uniquely owns its child.
                drop(unsafe { Box::from_raw(node) });
            }
        }
    }
}

impl<K, V> Drop for TrieLeaf<K, V> {
    fn drop(&mut self) {
        let mut current = *self.entries.get_mut();
        while !current.is_null() {
            // SAFETY: The leaf uniquely owns every link in its list.
            let ready = unsafe { Box::from_raw(current) };
            current = ready.next.load(Ordering::Relaxed);
            drop(ready);
        }
    }
}

fn new_leaf<K, V>(entry: Arc<Entry<K, V>>) -> *mut TrieNode<K, V> {
    let hash = entry.hash;
    let ready = Box::into_raw(Box::new(ReadyEntry {
        entry,
        next: AtomicPtr::new(ptr::null_mut()),
    }));
    Box::into_raw(Box::new(TrieNode::Leaf(TrieLeaf {
        hash,
        entries: AtomicPtr::new(ready),
        ownership: PhantomData,
    })))
}

fn split_leaves<K, V>(
    old: *mut TrieNode<K, V>,
    old_hash: u64,
    new: *mut TrieNode<K, V>,
    new_hash: u64,
    shift: u32,
) -> *mut TrieNode<K, V> {
    debug_assert_ne!(old_hash, new_hash);
    debug_assert!(shift < u64::BITS);

    // Until the new path is published, `old` still belongs to its current parent. Leaking an
    // incomplete path during unwinding is safe; dropping it would free `old` through two owners.
    let branch = ManuallyDrop::new(TrieBranch::new());
    let old_index = ((old_hash >> shift) & TRIE_MASK) as usize;
    let new_index = ((new_hash >> shift) & TRIE_MASK) as usize;
    if old_index == new_index {
        let child = split_leaves(old, old_hash, new, new_hash, shift + TRIE_BITS);
        branch.slots[old_index].store(child, Ordering::Relaxed);
    } else {
        branch.slots[old_index].store(old, Ordering::Relaxed);
        branch.slots[new_index].store(new, Ordering::Relaxed);
    }
    Box::into_raw(Box::new(TrieNode::Branch(ManuallyDrop::into_inner(branch))))
}

// SAFETY: `node` must remain protected for the entire recursive traversal.
unsafe fn for_each_node<K, V>(
    node: *mut TrieNode<K, V>,
    visit: &mut impl FnMut(&Arc<Entry<K, V>>),
) {
    // SAFETY: The caller guarantees that `node` remains allocated.
    match unsafe { &*node } {
        TrieNode::Branch(branch) => {
            for slot in &branch.slots {
                let child = slot.load(Ordering::Acquire);
                if !child.is_null() {
                    // SAFETY: Children share the protected lifetime of their parent.
                    unsafe { for_each_node(child, visit) };
                }
            }
        }
        TrieNode::Leaf(leaf) => {
            let mut current = leaf.entries.load(Ordering::Acquire);
            while !current.is_null() {
                // SAFETY: Entry links share the protected lifetime of their leaf.
                let ready = unsafe { &*current };
                visit(&ready.entry);
                current = ready.next.load(Ordering::Acquire);
            }
        }
    }
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
    // Successful values live in a lazily allocated trie. The write lock protects trie mutation and
    // the separate table of computations that have not completed yet.
    ready: ReadyTrie<K, V>,
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
            // SAFETY: The read barrier protects every allocation visited by the trie.
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
            // SAFETY: The read barrier protects every allocation visited by the trie.
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

        // SAFETY: The write lock serializes trie growth. Insertion only publishes new allocations.
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
        // SAFETY: Exclusive map access serializes trie growth.
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
            ready: ReadyTrie::new(),
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
