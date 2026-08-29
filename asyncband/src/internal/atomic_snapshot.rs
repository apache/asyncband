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

//! Atomic publication and reclamation of immutable, `Arc`-backed snapshots.
//!
//! [`AtomicSnapshot`] owns the currently published snapshot. A reader temporarily announces the
//! raw pointer in a thread-local hazard record, verifies that it is still current, and may then
//! borrow it for the duration of a callback. Replacement publishes the new snapshot immediately
//! and transfers ownership of the old one into [`ReplacedSnapshot`].
//!
//! Logical replacement and physical destruction are deliberately separate:
//!
//! - dropping `ReplacedSnapshot` waits for current readers and destroys it promptly;
//! - `ReplacedSnapshot::defer` queues it in a per-owner [`SnapshotReclaimer`], which amortizes
//!   hazard scans but may retain an unprotected snapshot until the next batch or explicit flush.
//!
//! The caller therefore chooses reclamation policy at each mutation instead of hiding a global
//! lifetime policy inside the atomic pointer.

use std::cell::Cell;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::internal::mutex::Mutex;

struct HazardRecord {
    pointer: AtomicPtr<()>,
    claimed: AtomicBool,
    next: AtomicPtr<HazardRecord>,
}

impl HazardRecord {
    const fn claimed() -> Self {
        Self {
            pointer: AtomicPtr::new(ptr::null_mut()),
            claimed: AtomicBool::new(true),
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }
}

// Records are never unlinked, so a scan may follow their pointers without protecting the registry
// itself. Threads cache enough records for their deepest nesting and release their claims on exit;
// later threads reuse those records, so sequential thread churn does not grow the registry.
static HAZARDS: AtomicPtr<HazardRecord> = AtomicPtr::new(ptr::null_mut());
static HAZARD_COUNT: AtomicUsize = AtomicUsize::new(0);

// Reclamation scans all hazard records, so amortize each scan over a fixed minimum batch and at
// least two retired snapshots per registered record.
const MIN_RECLAIM_BATCH: usize = 32;

struct ThreadHazards {
    hazards: RefCell<Vec<&'static HazardRecord>>,
    depth: Cell<usize>,
}

impl Drop for ThreadHazards {
    fn drop(&mut self) {
        for hazard in self.hazards.get_mut().drain(..) {
            release_hazard(hazard);
        }
    }
}

thread_local! {
    static THREAD_HAZARDS: ThreadHazards = const { ThreadHazards {
        hazards: RefCell::new(Vec::new()),
        depth: Cell::new(0),
    } };
}

fn claim_hazard() -> &'static HazardRecord {
    let mut current = HAZARDS.load(Ordering::Acquire);
    while !current.is_null() {
        // SAFETY: Hazard records are allocated once and never freed or unlinked.
        let hazard = unsafe { &*current };
        if hazard
            .claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return hazard;
        }
        current = hazard.next.load(Ordering::Relaxed);
    }

    let hazard: &'static HazardRecord = Box::leak(Box::new(HazardRecord::claimed()));
    let raw = ptr::from_ref(hazard).cast_mut();
    let mut head = HAZARDS.load(Ordering::SeqCst);
    loop {
        hazard.next.store(head, Ordering::Relaxed);
        match HAZARDS.compare_exchange_weak(head, raw, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => {
                HAZARD_COUNT.fetch_add(1, Ordering::Relaxed);
                return hazard;
            }
            Err(actual) => head = actual,
        }
    }
}

fn release_hazard(hazard: &'static HazardRecord) {
    debug_assert!(hazard.pointer.load(Ordering::Relaxed).is_null());
    hazard.claimed.store(false, Ordering::Release);
}

struct HazardClaim(&'static HazardRecord);

impl Drop for HazardClaim {
    fn drop(&mut self) {
        release_hazard(self.0);
    }
}

fn with_thread_hazard<R>(f: impl FnOnce(&HazardRecord) -> R) -> R {
    let mut f = Some(f);
    match THREAD_HAZARDS.try_with(|thread| {
        let depth = thread.depth.get();
        thread.depth.set(depth + 1);
        let depth_guard = DepthGuard {
            depth: &thread.depth,
            previous: depth,
        };
        let hazard = {
            let mut hazards = thread.hazards.borrow_mut();
            if depth == hazards.len() {
                hazards.push(claim_hazard());
            }
            hazards[depth]
        };
        let result = f.take().unwrap()(hazard);
        drop(depth_guard);
        result
    }) {
        Ok(result) => result,
        Err(_) => {
            let hazard = HazardClaim(claim_hazard());
            f.take().unwrap()(hazard.0)
        }
    }
}

struct DepthGuard<'a> {
    depth: &'a Cell<usize>,
    previous: usize,
}

impl Drop for DepthGuard<'_> {
    fn drop(&mut self) {
        self.depth.set(self.previous);
    }
}

struct ReadProtection<'a> {
    hazard: &'a HazardRecord,
}

impl<'a> ReadProtection<'a> {
    fn new(hazard: &'a HazardRecord, pointer: *mut ()) -> Self {
        hazard.pointer.store(pointer, Ordering::SeqCst);
        Self { hazard }
    }
}

impl Drop for ReadProtection<'_> {
    fn drop(&mut self) {
        self.hazard
            .pointer
            .store(ptr::null_mut(), Ordering::Release);
    }
}

fn is_protected(pointer: *mut ()) -> bool {
    let mut current = HAZARDS.load(Ordering::SeqCst);
    while !current.is_null() {
        // SAFETY: Hazard records are allocated once and never freed or unlinked.
        let hazard = unsafe { &*current };
        if hazard.pointer.load(Ordering::SeqCst) == pointer {
            return true;
        }
        current = hazard.next.load(Ordering::Relaxed);
    }
    false
}

fn protected_pointers() -> Vec<*mut ()> {
    let mut pointers = Vec::with_capacity(HAZARD_COUNT.load(Ordering::Relaxed));
    let mut current = HAZARDS.load(Ordering::SeqCst);
    while !current.is_null() {
        // SAFETY: Hazard records are allocated once and never freed or unlinked.
        let hazard = unsafe { &*current };
        let pointer = hazard.pointer.load(Ordering::SeqCst);
        if !pointer.is_null() {
            pointers.push(pointer);
        }
        current = hazard.next.load(Ordering::Relaxed);
    }
    pointers.sort_unstable();
    pointers
}

/// An optional immutable snapshot that readers can borrow while writers replace it.
///
/// `read` protects the published allocation with a thread-local hazard record and limits the
/// `&T` borrow to the callback. `load_owned` converts that short protected borrow into an owned
/// `Arc`. `replace` publishes a new snapshot immediately and returns ownership of the previous one.
/// The returned [`ReplacedSnapshot`] either waits for its readers when dropped or can be handed to
/// a [`SnapshotReclaimer`] for batched, deferred reclamation.
///
/// Registry publication, hazard publication, pointer verification, replacement, and hazard scans
/// are sequentially consistent. A newly registered reader therefore either appears in a later
/// scan or verifies the source pointer after its replacement. In the latter case it retries rather
/// than dereferencing the removed allocation. The initial source load can be relaxed because the
/// verification load also acquires the published snapshot.
pub struct AtomicSnapshot<T> {
    pointer: AtomicPtr<T>,
    ownership: PhantomData<Arc<T>>,
}

// Keeps the original `Arc` pointer provenance while limiting access to the protected callback.
struct ProtectedSnapshot<'a, T> {
    pointer: *mut T,
    lifetime: PhantomData<&'a T>,
}

impl<'a, T> ProtectedSnapshot<'a, T> {
    fn into_ref(self) -> &'a T {
        // SAFETY: The higher-ranked callback cannot let the reference outlive hazard protection.
        unsafe { &*self.pointer }
    }

    fn into_owned(self) -> Arc<T> {
        // SAFETY: The hazard protection remains active during this increment.
        unsafe { Arc::increment_strong_count(self.pointer) };
        // SAFETY: The increment above created exactly one owned reference.
        unsafe { Arc::from_raw(self.pointer) }
    }
}

/// Ownership of a snapshot that has been removed from an [`AtomicSnapshot`].
///
/// Dropping this value is deterministic: it waits until no reader protects the snapshot and then
/// releases it. `defer` moves the snapshot into a reclaimer instead, allowing the replacing thread
/// to continue without waiting for a reader that is still using the old value.
pub struct ReplacedSnapshot<T> {
    value: Option<Arc<T>>,
}

impl<T> ReplacedSnapshot<T> {
    /// Transfers this snapshot to `reclaimer` without waiting for current readers.
    pub fn defer(mut self, reclaimer: &SnapshotReclaimer<T>) {
        reclaimer.retire(self.value.take().unwrap());
    }
}

impl<T> Drop for ReplacedSnapshot<T> {
    fn drop(&mut self) {
        let Some(value) = self.value.take() else {
            return;
        };

        let pointer = Arc::as_ptr(&value).cast_mut().cast();
        let mut spins = 0;
        while is_protected(pointer) {
            if spins < 16 {
                std::hint::spin_loop();
                spins += 1;
            } else {
                std::thread::yield_now();
            }
        }
        drop(value);
    }
}

/// Snapshots awaiting a hazard-pointer scan.
///
/// Retired snapshots are reclaimed in batches once the queue reaches a threshold proportional to
/// the hazard registry. This amortizes scans, but makes destruction nondeterministic: a partial
/// batch can remain alive until a later replacement, an explicit `flush`, or reclaimer drop.
/// Each owner should therefore keep its own typed reclaimer. Besides avoiding cross-owner
/// contention, that permits borrowed values without imposing a `T: 'static` bound.
pub struct SnapshotReclaimer<T> {
    retired: Mutex<Vec<Arc<T>>>,
}

impl<T> SnapshotReclaimer<T> {
    pub const fn new() -> Self {
        Self {
            retired: Mutex::new(Vec::new()),
        }
    }

    fn retire(&self, snapshot: Arc<T>) {
        let should_reclaim = {
            let mut retired = self.retired.lock();
            retired.push(snapshot);
            retired.len() >= reclaim_threshold()
        };
        if should_reclaim {
            self.reclaim();
        }
    }

    fn reclaim(&self) {
        let mut candidates = {
            let mut retired = self.retired.lock();
            std::mem::take(&mut *retired)
        };
        if candidates.is_empty() {
            return;
        }

        let protected = protected_pointers();
        candidates.retain(|snapshot| {
            let pointer = Arc::as_ptr(snapshot).cast_mut().cast();
            protected.binary_search(&pointer).is_ok()
        });

        if !candidates.is_empty() {
            self.retired.lock().append(&mut candidates);
        }
    }

    /// Waits until every snapshot currently owned by this reclaimer is safe to destroy.
    pub fn flush(&self) {
        loop {
            self.reclaim();
            if self.retired.lock().is_empty() {
                return;
            }
            std::thread::yield_now();
        }
    }
}

impl<T> Drop for SnapshotReclaimer<T> {
    fn drop(&mut self) {
        self.flush();
    }
}

fn reclaim_threshold() -> usize {
    MIN_RECLAIM_BATCH.max(HAZARD_COUNT.load(Ordering::Relaxed).saturating_mul(2))
}

impl<T> AtomicSnapshot<T> {
    pub const fn empty() -> Self {
        Self {
            pointer: AtomicPtr::new(ptr::null_mut()),
            ownership: PhantomData,
        }
    }

    fn with_protected<R>(
        &self,
        f: impl for<'a> FnOnce(Option<ProtectedSnapshot<'a, T>>) -> R,
    ) -> R {
        with_thread_hazard(|hazard| {
            loop {
                let pointer = self.pointer.load(Ordering::Relaxed);
                if pointer.is_null() {
                    return f(None);
                }

                let protection = ReadProtection::new(hazard, pointer.cast());
                let verified = self.pointer.load(Ordering::SeqCst);
                if verified != pointer {
                    drop(protection);
                    continue;
                }

                // Use the verified pointer rather than the first load. If an allocation is
                // reclaimed and its address reused between the two loads, this carries the
                // currently published allocation's provenance. `protection` prevents that
                // allocation from being reclaimed for the duration of the callback.
                let result = f(Some(ProtectedSnapshot {
                    pointer: verified,
                    lifetime: PhantomData,
                }));
                drop(protection);
                return result;
            }
        })
    }

    pub fn read<R>(&self, f: impl for<'a> FnOnce(Option<&'a T>) -> R) -> R {
        self.with_protected(|snapshot| f(snapshot.map(ProtectedSnapshot::into_ref)))
    }

    pub fn load_owned(&self) -> Option<Arc<T>> {
        self.with_protected(|snapshot| snapshot.map(ProtectedSnapshot::into_owned))
    }

    /// Publishes the first snapshot and panics if a value was already present.
    pub fn initialize(&self, value: Arc<T>) {
        let pointer = Arc::into_raw(value).cast_mut();
        if self
            .pointer
            .compare_exchange(ptr::null_mut(), pointer, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            // SAFETY: The failed comparison did not publish or consume this owned reference.
            drop(unsafe { Arc::from_raw(pointer) });
            panic!("atomic snapshot was already initialized");
        }
    }

    /// Publishes `value` and transfers ownership of the previous snapshot to the caller.
    pub fn replace(&self, value: Option<Arc<T>>) -> Option<ReplacedSnapshot<T>> {
        let new_pointer = value.map(Arc::into_raw).unwrap_or(ptr::null()).cast_mut();
        let old_pointer = self.pointer.swap(new_pointer, Ordering::SeqCst);
        if old_pointer.is_null() {
            return None;
        }

        Some(ReplacedSnapshot {
            // SAFETY: `old_pointer` was created by `Arc::into_raw`, and replacement transfers the
            // atomic's one owned reference into the returned value without releasing it.
            value: Some(unsafe { Arc::from_raw(old_pointer) }),
        })
    }
}

impl<T> Drop for AtomicSnapshot<T> {
    fn drop(&mut self) {
        let pointer = *self.pointer.get_mut();
        if !pointer.is_null() {
            // SAFETY: Exclusive access proves that no safe reader can be using the atomic. This is
            // the one owned reference installed by `Arc::into_raw` and not transferred by
            // `replace`.
            drop(unsafe { Arc::from_raw(pointer) });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::AtomicSnapshot;
    use super::SnapshotReclaimer;

    #[test]
    fn read_load_and_replace_preserve_arc_ownership() {
        let first = Arc::new(1);
        let atomic = AtomicSnapshot::empty();

        atomic.initialize(Arc::clone(&first));
        assert_eq!(atomic.read(|value| value.copied()), Some(1));
        assert_eq!(*atomic.load_owned().unwrap(), 1);
        assert_eq!(Arc::strong_count(&first), 2);

        let second = Arc::new(2);
        drop(atomic.replace(Some(Arc::clone(&second))).unwrap());
        assert_eq!(Arc::strong_count(&first), 1);
        assert_eq!(*atomic.load_owned().unwrap(), 2);

        drop(atomic.replace(None).unwrap());
        assert_eq!(Arc::strong_count(&second), 1);
        assert!(atomic.load_owned().is_none());
    }

    #[test]
    fn deferred_reclamation_does_not_wait_for_a_reader() {
        struct Tracked(Arc<AtomicUsize>);

        impl Drop for Tracked {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let atomic = Arc::new(AtomicSnapshot::empty());
        let reclaimer = SnapshotReclaimer::new();
        atomic.initialize(Arc::new(Tracked(Arc::clone(&drops))));
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));

        let reader = {
            let atomic = Arc::clone(&atomic);
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            std::thread::spawn(move || {
                atomic.read(|value| {
                    assert!(value.is_some());
                    entered.wait();
                    release.wait();
                });
            })
        };

        entered.wait();
        atomic.replace(None).unwrap().defer(&reclaimer);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        reclaimer.reclaim();
        assert_eq!(drops.load(Ordering::Relaxed), 0);

        release.wait();
        reader.join().unwrap();
        reclaimer.reclaim();
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn reclaimer_accepts_borrowed_snapshots() {
        let value = 1;
        let atomic = AtomicSnapshot::empty();
        let reclaimer = SnapshotReclaimer::new();

        atomic.initialize(Arc::new(&value));
        atomic.replace(None).unwrap().defer(&reclaimer);

        reclaimer.reclaim();
    }

    #[test]
    fn concurrent_reads_and_replacements_reclaim_every_value() {
        const ITERATIONS: usize = if cfg!(miri) { 32 } else { 10_000 };

        struct Tracked {
            generation: usize,
            drops: Arc<AtomicUsize>,
        }

        impl Drop for Tracked {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let atomic = Arc::new(AtomicSnapshot::empty());
        let reclaimer = SnapshotReclaimer::new();
        atomic.initialize(Arc::new(Tracked {
            generation: 0,
            drops: Arc::clone(&drops),
        }));
        let start = Arc::new(Barrier::new(3));

        let readers: Vec<_> = (0..2)
            .map(|_| {
                let atomic = Arc::clone(&atomic);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    for _ in 0..ITERATIONS {
                        atomic.read(|value| {
                            let value = value.unwrap();
                            assert!(value.generation <= ITERATIONS);
                            std::hint::black_box(value);
                        });
                        std::thread::yield_now();
                    }
                })
            })
            .collect();

        start.wait();
        for generation in 1..=ITERATIONS {
            atomic
                .replace(Some(Arc::new(Tracked {
                    generation,
                    drops: Arc::clone(&drops),
                })))
                .unwrap()
                .defer(&reclaimer);
            std::thread::yield_now();
        }
        for reader in readers {
            reader.join().unwrap();
        }

        reclaimer.reclaim();
        drop(atomic);
        drop(reclaimer);
        assert_eq!(drops.load(Ordering::Relaxed), ITERATIONS + 1);
    }
}
