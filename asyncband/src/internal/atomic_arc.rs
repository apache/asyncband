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

use std::cell::Cell;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;

struct Hazard {
    pointer: AtomicPtr<()>,
    claimed: AtomicBool,
    next: AtomicPtr<Hazard>,
}

impl Hazard {
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
static HAZARDS: AtomicPtr<Hazard> = AtomicPtr::new(ptr::null_mut());

struct ThreadHazards {
    hazards: RefCell<Vec<&'static Hazard>>,
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

fn claim_hazard() -> &'static Hazard {
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

    let hazard: &'static Hazard = Box::leak(Box::new(Hazard::claimed()));
    let raw = ptr::from_ref(hazard).cast_mut();
    let mut head = HAZARDS.load(Ordering::SeqCst);
    loop {
        hazard.next.store(head, Ordering::Relaxed);
        match HAZARDS.compare_exchange_weak(head, raw, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return hazard,
            Err(actual) => head = actual,
        }
    }
}

fn release_hazard(hazard: &'static Hazard) {
    debug_assert!(hazard.pointer.load(Ordering::Relaxed).is_null());
    hazard.claimed.store(false, Ordering::Release);
}

struct HazardClaim(&'static Hazard);

impl Drop for HazardClaim {
    fn drop(&mut self) {
        release_hazard(self.0);
    }
}

fn with_thread_hazard<R>(f: impl FnOnce(&Hazard) -> R) -> R {
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

struct Protection<'a> {
    hazard: &'a Hazard,
}

impl<'a> Protection<'a> {
    fn new(hazard: &'a Hazard, pointer: *mut ()) -> Self {
        hazard.pointer.store(pointer, Ordering::SeqCst);
        Self { hazard }
    }
}

impl Drop for Protection<'_> {
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

/// An atomically replaceable optional `Arc`.
///
/// This deliberately small implementation supports protected reads, owned loads, and replacement.
/// A reader publishes the raw pointer in a thread-local hazard slot before dereferencing it. A
/// writer keeps the replaced `Arc` alive until no hazard slot refers to it.
///
/// Registry publication, hazard publication, pointer verification and replacement, and hazard
/// scans are sequentially consistent. A newly registered reader therefore either appears in the
/// registry scan or verifies the pointer after its replacement. Likewise, if a writer scans a
/// record before the reader publishes its hazard, the replacement precedes the reader's
/// verification and that verification cannot still observe the old pointer. Otherwise the scan
/// cannot overlook the published hazard. The initial pointer load can be relaxed because the
/// verification load also acquires the published value.
pub struct AtomicArcOption<T> {
    pointer: AtomicPtr<T>,
    ownership: PhantomData<Arc<T>>,
}

/// A reference protected from reclamation by an `AtomicArcOption` read.
pub struct Protected<'a, T> {
    value: &'a T,
    pointer: *mut T,
}

impl<T> Protected<'_, T> {
    pub fn to_arc(&self) -> Arc<T> {
        // SAFETY: The `Protected` value can only be constructed while its pointer is published in
        // a hazard slot, so a writer cannot reclaim the atomic's owned reference during this call.
        unsafe { Arc::increment_strong_count(self.pointer) };
        // SAFETY: The increment above created exactly one owned reference for this raw pointer.
        unsafe { Arc::from_raw(self.pointer) }
    }
}

impl<T> Deref for Protected<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}

impl<T> AtomicArcOption<T> {
    pub const fn empty() -> Self {
        Self {
            pointer: AtomicPtr::new(ptr::null_mut()),
            ownership: PhantomData,
        }
    }

    pub fn with<R>(&self, f: impl for<'a> FnOnce(Option<Protected<'a, T>>) -> R) -> R {
        with_thread_hazard(|hazard| {
            loop {
                let pointer = self.pointer.load(Ordering::Relaxed);
                if pointer.is_null() {
                    return f(None);
                }

                let protection = Protection::new(hazard, pointer.cast());
                let verified = self.pointer.load(Ordering::SeqCst);
                if verified != pointer {
                    drop(protection);
                    continue;
                }

                // Use the verified pointer rather than the first load. If an allocation is
                // reclaimed and its address reused between the two loads, this carries the
                // currently published allocation's provenance. `protection` prevents that
                // allocation from being reclaimed for the duration of the callback, and the
                // higher-ranked callback cannot return the borrowed reference in its result.
                let result = f(Some(Protected {
                    // SAFETY: `verified` came from `Arc::into_raw` and remains protected.
                    value: unsafe { &*verified },
                    pointer: verified,
                }));
                drop(protection);
                return result;
            }
        })
    }

    pub fn load(&self) -> Option<Arc<T>> {
        self.with(|value| value.map(|value| value.to_arc()))
    }

    pub fn store(&self, value: Option<Arc<T>>) {
        drop(self.swap(value));
    }

    pub fn swap(&self, value: Option<Arc<T>>) -> Option<Arc<T>> {
        let new_pointer = value.map(Arc::into_raw).unwrap_or(ptr::null()).cast_mut();
        let old_pointer = self.pointer.swap(new_pointer, Ordering::SeqCst);
        if old_pointer.is_null() {
            return None;
        }

        let erased = old_pointer.cast();
        let mut spins = 0;
        while is_protected(erased) {
            if spins < 16 {
                std::hint::spin_loop();
                spins += 1;
            } else {
                std::thread::yield_now();
            }
        }

        // SAFETY: `old_pointer` was created by `Arc::into_raw`, and the atomic transferred its one
        // owned reference to this return value. The hazard scan establishes that no reader can
        // still be between loading this pointer and incrementing its own strong count.
        Some(unsafe { Arc::from_raw(old_pointer) })
    }
}

impl<T> Drop for AtomicArcOption<T> {
    fn drop(&mut self) {
        let pointer = *self.pointer.get_mut();
        if !pointer.is_null() {
            // SAFETY: Exclusive access proves that no safe reader can be using the atomic. This is
            // the one owned reference installed by `Arc::into_raw` and not transferred by `swap`.
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

    use super::AtomicArcOption;

    #[test]
    fn load_store_and_swap_preserve_arc_ownership() {
        let first = Arc::new(1);
        let atomic = AtomicArcOption::empty();

        atomic.store(Some(Arc::clone(&first)));
        assert_eq!(*atomic.load().unwrap(), 1);
        assert_eq!(Arc::strong_count(&first), 2);

        let second = Arc::new(2);
        let replaced = atomic.swap(Some(Arc::clone(&second))).unwrap();
        assert!(Arc::ptr_eq(&replaced, &first));
        assert_eq!(*atomic.load().unwrap(), 2);

        assert!(Arc::ptr_eq(&atomic.swap(None).unwrap(), &second));
        assert!(atomic.load().is_none());
    }

    #[test]
    fn concurrent_loads_and_swaps_reclaim_every_value() {
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
        let atomic = Arc::new(AtomicArcOption::empty());
        atomic.store(Some(Arc::new(Tracked {
            generation: 0,
            drops: Arc::clone(&drops),
        })));
        let start = Arc::new(Barrier::new(3));

        let readers: Vec<_> = (0..2)
            .map(|_| {
                let atomic = Arc::clone(&atomic);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    for _ in 0..ITERATIONS {
                        let value = atomic.load().unwrap();
                        assert!(value.generation <= ITERATIONS);
                        std::hint::black_box(value);
                        std::thread::yield_now();
                    }
                })
            })
            .collect();

        start.wait();
        for generation in 1..=ITERATIONS {
            drop(atomic.swap(Some(Arc::new(Tracked {
                generation,
                drops: Arc::clone(&drops),
            }))));
            std::thread::yield_now();
        }
        for reader in readers {
            reader.join().unwrap();
        }

        drop(atomic);
        assert_eq!(drops.load(Ordering::Relaxed), ITERATIONS + 1);
    }
}
