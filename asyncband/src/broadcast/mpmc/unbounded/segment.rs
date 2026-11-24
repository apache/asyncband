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

use std::alloc;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::panic::RefUnwindSafe;
use std::panic::UnwindSafe;
use std::process;
use std::ptr;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::AtomicPtr;
#[cfg(target_has_atomic = "64")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[cfg(not(target_has_atomic = "64"))]
use crate::internal::mutex::Mutex;

pub const INLINE_CAP: usize = 8;
pub const HEAP_CAP: usize = 256;
pub const EPOCHS: usize = 3;

/// Uninitialized 128-byte separator that guarantees adjacent fields never share a 128-byte
/// coherence sector for any allocator base alignment without over-aligning `Shared<T>` or dirtying
/// separator cache lines on channel creation.
pub struct CacheSep(#[expect(dead_code)] MaybeUninit<[u8; 128]>);

impl CacheSep {
    pub const fn new() -> Self {
        Self(MaybeUninit::uninit())
    }
}

#[cfg(target_has_atomic = "64")]
pub struct CounterU64(AtomicU64);

#[cfg(target_has_atomic = "64")]
impl CounterU64 {
    pub const fn new(initial: u64) -> Self {
        Self(AtomicU64::new(initial))
    }

    #[inline(always)]
    pub fn load(&self, ordering: Ordering) -> u64 {
        self.0.load(ordering)
    }

    #[inline(always)]
    pub fn store(&self, value: u64, ordering: Ordering) {
        self.0.store(value, ordering);
    }

    #[inline(always)]
    pub fn fetch_add(&self, delta: u64, ordering: Ordering) {
        self.0.fetch_add(delta, ordering);
    }

    #[inline(always)]
    pub fn advance_or_abort(&self, ordering: Ordering) {
        if self.0.fetch_add(1, ordering) == u64::MAX {
            process::abort();
        }
    }

    #[inline(always)]
    pub fn claim_next(&self) -> Option<u64> {
        // `SeqCst` pairs with `has_waiters` in `register_waiter` without extra instructions over
        // `AcqRel` on ARM64 (`ldaddal`) or x86_64 (`lock xadd`).
        let prev = self.0.fetch_add(1, Ordering::SeqCst);
        if prev == u64::MAX {
            self.restore_max();
            None
        } else {
            Some(prev)
        }
    }

    #[cold]
    #[inline(never)]
    fn restore_max(&self) {
        self.0.store(u64::MAX, Ordering::SeqCst);
    }
}

#[cfg(not(target_has_atomic = "64"))]
pub struct CounterU64(Mutex<u64>);

#[cfg(not(target_has_atomic = "64"))]
impl CounterU64 {
    pub const fn new(initial: u64) -> Self {
        Self(Mutex::new(initial))
    }

    #[inline]
    pub fn load(&self, _ordering: Ordering) -> u64 {
        *self.0.lock()
    }

    #[inline]
    pub fn store(&self, value: u64, _ordering: Ordering) {
        *self.0.lock() = value;
    }

    #[inline]
    pub fn fetch_add(&self, delta: u64, _ordering: Ordering) {
        *self.0.lock() += delta;
    }

    #[inline]
    pub fn advance_or_abort(&self, _ordering: Ordering) {
        let mut guard = self.0.lock();
        if *guard == u64::MAX {
            process::abort();
        }
        *guard += 1;
    }

    #[inline]
    pub fn claim_next(&self) -> Option<u64> {
        let mut guard = self.0.lock();
        let prev = *guard;
        if prev == u64::MAX {
            None
        } else {
            *guard = prev + 1;
            Some(prev)
        }
    }
}

impl UnwindSafe for CounterU64 {}
impl RefUnwindSafe for CounterU64 {}

pub struct MsgSlot<T> {
    /// `0` while unpublished or after reclamation; `k >= 1` while `k` eligible subscriptions
    /// still need to read or drop past this slot.
    pub state: AtomicUsize,
    pub msg: UnsafeCell<MaybeUninit<Arc<T>>>,
}

impl<T> MsgSlot<T> {
    const fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            msg: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

impl<T> UnwindSafe for MsgSlot<T> {}
impl<T> RefUnwindSafe for MsgSlot<T> {}

#[repr(C)]
pub struct Segment<T> {
    pub base: CounterU64,
    pub cap: usize,
    pub refs: AtomicUsize,
    pub prev: AtomicPtr<Segment<T>>,
    pub next: AtomicPtr<Segment<T>>,
    pub is_inline: bool,
    slots: [MsgSlot<T>; 0],
}

impl<T> UnwindSafe for Segment<T> {}
impl<T> RefUnwindSafe for Segment<T> {}

#[repr(C)]
pub struct InlineSegment<T> {
    pub header: Segment<T>,
    slots: [MsgSlot<T>; INLINE_CAP],
}

impl<T> InlineSegment<T> {
    pub const fn new() -> Self {
        Self {
            header: Segment {
                base: CounterU64::new(0),
                cap: INLINE_CAP,
                refs: AtomicUsize::new(2),
                prev: AtomicPtr::new(ptr::null_mut()),
                next: AtomicPtr::new(ptr::null_mut()),
                is_inline: true,
                slots: [],
            },
            slots: [const { MsgSlot::new() }; INLINE_CAP],
        }
    }
}

#[repr(C)]
struct HeapSegment<T> {
    header: Segment<T>,
    slots: [MsgSlot<T>; HEAP_CAP],
}

impl<T> Segment<T> {
    pub fn alloc_heap(base: u64, refs: usize, prev: *mut Self) -> NonNull<Self> {
        let layout = alloc::Layout::new::<HeapSegment<T>>();
        // SAFETY: `HeapSegment<T>` is non-zero-sized; `alloc_zeroed` initializes every slot's
        // `state` to `0` (`AtomicUsize`) while letting the allocator back fresh segments with
        // zeroed pages instead of looping across all `HEAP_CAP` slots.
        let raw = unsafe { alloc::alloc_zeroed(layout).cast::<HeapSegment<T>>() };
        let Some(non_null) = NonNull::new(raw) else {
            alloc::handle_alloc_error(layout);
        };
        let ptr = non_null.as_ptr().cast::<Self>();
        unsafe {
            ptr::addr_of_mut!((*ptr).base).write(CounterU64::new(base));
            ptr::addr_of_mut!((*ptr).cap).write(HEAP_CAP);
            ptr::addr_of_mut!((*ptr).refs).write(AtomicUsize::new(refs));
            ptr::addr_of_mut!((*ptr).prev).write(AtomicPtr::new(prev));
            ptr::addr_of_mut!((*ptr).next).write(AtomicPtr::new(ptr::null_mut()));
            ptr::addr_of_mut!((*ptr).is_inline).write(false);
        }
        unsafe { NonNull::new_unchecked(ptr) }
    }

    pub unsafe fn dealloc_heap(ptr: NonNull<Self>) {
        debug_assert!(!unsafe { (*ptr.as_ptr()).is_inline });
        let layout = alloc::Layout::new::<HeapSegment<T>>();
        // SAFETY: `ptr` was allocated in `alloc_heap` and all initialized slots were consumed.
        unsafe {
            alloc::dealloc(ptr.as_ptr().cast::<u8>(), layout);
        }
    }

    #[inline]
    pub unsafe fn reset_for_reuse(ptr: NonNull<Self>, base: u64, refs: usize, prev: *mut Self) {
        let raw = ptr.as_ptr();
        // SAFETY: Caller has exclusive ownership of `ptr` after 3-epoch quiescence, and all
        // previously published slots were returned to `state == 0` upon reclamation.
        unsafe {
            (*raw).base.store(base, Ordering::Relaxed);
            (*raw).refs.store(refs, Ordering::Relaxed);
            (*raw).prev.store(prev, Ordering::Relaxed);
            (*raw).next.store(ptr::null_mut(), Ordering::Relaxed);
        }
    }

    #[inline(always)]
    pub unsafe fn slot_ptr_at(seg: *mut Self, offset: usize) -> NonNull<MsgSlot<T>> {
        // SAFETY: `seg` points to an `InlineSegment<T>` (`cap == INLINE_CAP`) or `HeapSegment<T>`
        // (`cap == HEAP_CAP`), and projecting from `(*seg).slots` preserves full segment
        // provenance across `0..cap`.
        unsafe {
            debug_assert!(offset < (*seg).cap);
            let raw = ptr::addr_of!((*seg).slots)
                .cast::<MsgSlot<T>>()
                .cast_mut()
                .add(offset);
            NonNull::new_unchecked(raw)
        }
    }

    #[inline(always)]
    pub unsafe fn slot_at<'a>(seg: *mut Self, offset: usize) -> &'a MsgSlot<T> {
        unsafe { &*Self::slot_ptr_at(seg, offset).as_ptr() }
    }
}

#[cfg(target_has_atomic = "64")]
pub struct Shard {
    /// Packs three 16-bit active publisher counts (`0..16`, `16..32`, `32..48`) into one word so
    /// computing `epoch << 4` is a single barrel shift instead of an integer multiply.
    active: AtomicU64,
}

#[cfg(target_has_atomic = "64")]
impl Shard {
    const SHIFT_BITS: usize = 4;
    const EPOCH_MASK: u64 = 0xFFFF;

    pub const fn new() -> Self {
        Self {
            active: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn enter(&self, epoch: usize) {
        let shift = epoch << Self::SHIFT_BITS;
        let prev = self.active.fetch_add(1u64 << shift, Ordering::SeqCst);
        if ((prev >> shift) & Self::EPOCH_MASK) == Self::EPOCH_MASK {
            process::abort();
        }
    }

    #[inline(always)]
    pub fn exit(&self, epoch: usize) {
        self.active
            .fetch_sub(1u64 << (epoch << Self::SHIFT_BITS), Ordering::Release);
    }

    #[inline]
    pub fn is_all_idle(&self) -> bool {
        self.active.load(Ordering::SeqCst) == 0
    }

    #[inline]
    pub fn is_epoch_idle(&self, epoch: usize) -> bool {
        ((self.active.load(Ordering::Acquire) >> (epoch << Self::SHIFT_BITS)) & Self::EPOCH_MASK)
            == 0
    }
}

#[cfg(not(target_has_atomic = "64"))]
pub struct Shard {
    active: [AtomicUsize; EPOCHS],
}

#[cfg(not(target_has_atomic = "64"))]
impl Shard {
    pub const fn new() -> Self {
        Self {
            active: [
                AtomicUsize::new(0),
                AtomicUsize::new(0),
                AtomicUsize::new(0),
            ],
        }
    }

    #[inline]
    pub fn enter(&self, epoch: usize) {
        self.active[epoch].fetch_add(1, Ordering::SeqCst);
    }

    #[inline]
    pub fn exit(&self, epoch: usize) {
        self.active[epoch].fetch_sub(1, Ordering::Release);
    }

    #[inline]
    pub fn is_all_idle(&self) -> bool {
        self.active.iter().all(|c| c.load(Ordering::SeqCst) == 0)
    }

    #[inline]
    pub fn is_epoch_idle(&self, epoch: usize) -> bool {
        self.active[epoch].load(Ordering::Acquire) == 0
    }
}

impl UnwindSafe for Shard {}
impl RefUnwindSafe for Shard {}

pub struct SegmentPool<T> {
    pub active_heap_segments: usize,
    pub cycle_peak_heap: usize,
    pub free_list: Vec<NonNull<Segment<T>>>,
    pub retired: [Vec<NonNull<Segment<T>>>; EPOCHS],
}

impl<T> SegmentPool<T> {
    pub fn new() -> Self {
        Self {
            active_heap_segments: 0,
            cycle_peak_heap: 0,
            free_list: Vec::new(),
            retired: [Vec::new(), Vec::new(), Vec::new()],
        }
    }

    #[inline]
    pub fn cached_len(&self) -> usize {
        self.free_list.len() + self.retired[0].len() + self.retired[1].len() + self.retired[2].len()
    }
}

impl<T> Drop for SegmentPool<T> {
    fn drop(&mut self) {
        for ptr in self.free_list.drain(..) {
            // SAFETY: Pool owns all heap segments in `free_list`.
            unsafe { Segment::dealloc_heap(ptr) };
        }
        for bucket in &mut self.retired {
            for ptr in bucket.drain(..) {
                // SAFETY: `Shared::drop` runs only once all senders and receivers have dropped.
                unsafe { Segment::dealloc_heap(ptr) };
            }
        }
    }
}
