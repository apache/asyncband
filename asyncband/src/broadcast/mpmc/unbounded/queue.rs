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

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem;
use std::mem::ManuallyDrop;
use std::panic::RefUnwindSafe;
use std::panic::UnwindSafe;
use std::ptr;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;

use crate::broadcast::mpmc::common::Received;
use crate::broadcast::mpmc::error::RecvError;
use crate::broadcast::mpmc::error::TryRecvError;
use crate::broadcast::mpmc::unbounded::segment::CacheSep;
use crate::broadcast::mpmc::unbounded::segment::CounterU64;
use crate::broadcast::mpmc::unbounded::segment::EPOCHS;
#[cfg(test)]
use crate::broadcast::mpmc::unbounded::segment::HEAP_CAP;
use crate::broadcast::mpmc::unbounded::segment::INLINE_CAP;
use crate::broadcast::mpmc::unbounded::segment::InlineSegment;
use crate::broadcast::mpmc::unbounded::segment::MsgSlot;
use crate::broadcast::mpmc::unbounded::segment::Segment;
use crate::broadcast::mpmc::unbounded::segment::SegmentPool;
use crate::broadcast::mpmc::unbounded::segment::Shard;
use crate::broadcast::mpmc::unbounded::waiters::WaitReg;
use crate::broadcast::mpmc::unbounded::waiters::Waiters;
use crate::internal::mutex::Mutex;
use crate::internal::waker_batch::WakerBatch;

const SHARDS: usize = 4;

#[repr(C)]
struct ExtraShardSlot {
    shard: Shard,
    _sep: CacheSep,
}

type ExtraShards = [ExtraShardSlot; SHARDS - 1];

pub struct Subscription<T> {
    pub cursor: u64,
    pub seg: NonNull<Segment<T>>,
    pub slot_ptr: NonNull<MsgSlot<T>>,
    pub rem_in_seg: u32,
    pub wait_gen_hi: u32,
}

impl<T> Clone for Subscription<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Subscription<T> {}

struct DrainBatch<T> {
    first: Option<Arc<T>>,
    rest: Vec<Arc<T>>,
}

impl<T> DrainBatch<T> {
    const fn new() -> Self {
        Self {
            first: None,
            rest: Vec::new(),
        }
    }

    #[inline]
    fn push(&mut self, msg: Arc<T>) {
        if self.first.is_none() {
            self.first = Some(msg);
        } else {
            self.rest.push(msg);
        }
    }
}

struct SlotPark {
    notified: Mutex<bool>,
    condvar: Condvar,
}

impl SlotPark {
    const fn new() -> Self {
        Self {
            notified: Mutex::new(false),
            condvar: Condvar::new(),
        }
    }

    fn wait(&self) {
        let mut notified = self.notified.lock();
        while !*notified {
            notified = self
                .condvar
                .wait(notified)
                .unwrap_or_else(PoisonError::into_inner);
        }
        *notified = false;
    }

    fn notify(&self) {
        *self.notified.lock() = true;
        self.condvar.notify_one();
    }
}

impl Wake for SlotPark {
    fn wake(self: Arc<Self>) {
        self.notify();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}

#[cold]
#[inline(never)]
fn init_extra_shards_box(slot: &AtomicPtr<ExtraShards>) -> *mut ExtraShards {
    let boxed: Box<ExtraShards> = Box::new([
        ExtraShardSlot {
            shard: Shard::new(),
            _sep: CacheSep::new(),
        },
        ExtraShardSlot {
            shard: Shard::new(),
            _sep: CacheSep::new(),
        },
        ExtraShardSlot {
            shard: Shard::new(),
            _sep: CacheSep::new(),
        },
    ]);
    let raw = Box::into_raw(boxed);
    match slot.compare_exchange(ptr::null_mut(), raw, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => raw,
        Err(winner) => {
            // SAFETY: Another thread won the initialization race; reclaim our unused `Box`.
            unsafe {
                drop(Box::from_raw(raw));
            }
            winner
        }
    }
}

#[repr(C)]
pub struct Shared<T> {
    // Read-mostly header: stays in shared L1 state across steady-state publish and receive.
    live_receivers: AtomicUsize,
    paused: AtomicBool,
    has_heap_pool: AtomicBool,
    has_excess_pool: AtomicBool,
    epoch: AtomicUsize,
    next_shard: AtomicUsize,
    senders: AtomicUsize,
    tail_seg: AtomicPtr<Segment<T>>,
    extra_shards: AtomicPtr<ExtraShards>,
    _marker: PhantomData<Arc<T>>,
    _sep0: CacheSep,

    tail: CounterU64,
    _sep1: CacheSep,

    /// Monotonic count of versions whose backlog slots have been reclaimed (plus any versions
    /// skipped while zero receivers existed). Paired with `tail` so `tail - reclaimed` gives exact
    /// retained backlog length without splitting state across multiple counters.
    reclaimed: CounterU64,
    _sep2: CacheSep,

    pub shard0: Shard,
    _sep3: CacheSep,

    // Waiter registration state: untouched when receivers do not park, and mutated together on a
    // single cache line when waiters register, wake, or cancel without invalidating the header.
    has_waiters: AtomicBool,
    pub wake_gen: CounterU64,
    waiters: Mutex<Waiters>,
    pool: Mutex<Option<Box<SegmentPool<T>>>>,
    control: Mutex<()>,
    inline_seg0: UnsafeCell<InlineSegment<T>>,
}

// SAFETY: `Shared<T>` distributes `Arc<T>` across threads, requiring `T: Send + Sync` just like
// `Arc<T>` itself.
unsafe impl<T: Send + Sync> Send for Shared<T> {}
unsafe impl<T: Send + Sync> Sync for Shared<T> {}
impl<T> UnwindSafe for Shared<T> {}
impl<T> RefUnwindSafe for Shared<T> {}

impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        let tail_seg = *self.tail_seg.get_mut();
        if !tail_seg.is_null() {
            let inline_raw = self.inline_seg0.get().cast::<Segment<T>>();
            if ptr::eq(tail_seg, inline_raw) {
                let next_raw = *self.inline_seg0.get_mut().header.next.get_mut();
                if let Some(next) = NonNull::new(next_raw) {
                    // SAFETY: `inline_seg0.next` owned one reference to heap segment `next`.
                    unsafe {
                        self.release_seg_ref(next);
                    }
                }
            } else {
                // SAFETY: `tail_seg` is a heap segment disjoint from `self` and owned one ref.
                unsafe {
                    self.release_seg_ref(NonNull::new_unchecked(tail_seg));
                }
            }
        }
        let extra_shards = *self.extra_shards.get_mut();
        if !extra_shards.is_null() {
            // SAFETY: `extra_shards` was allocated with `Box::into_raw` in `init_extra_shards_box`.
            unsafe {
                drop(Box::from_raw(extra_shards));
            }
        }
    }
}

impl<T> Shared<T> {
    pub fn with_first_subscription() -> (Arc<Self>, Subscription<T>) {
        let shared = Arc::new(Self {
            live_receivers: AtomicUsize::new(1),
            paused: AtomicBool::new(false),
            has_heap_pool: AtomicBool::new(false),
            has_excess_pool: AtomicBool::new(false),
            epoch: AtomicUsize::new(0),
            next_shard: AtomicUsize::new(1),
            senders: AtomicUsize::new(1),
            tail_seg: AtomicPtr::new(ptr::null_mut()),
            extra_shards: AtomicPtr::new(ptr::null_mut()),
            _marker: PhantomData,
            _sep0: CacheSep::new(),
            tail: CounterU64::new(0),
            _sep1: CacheSep::new(),
            reclaimed: CounterU64::new(0),
            _sep2: CacheSep::new(),
            shard0: Shard::new(),
            _sep3: CacheSep::new(),
            has_waiters: AtomicBool::new(false),
            wake_gen: CounterU64::new(0),
            waiters: Mutex::new(Waiters::new()),
            pool: Mutex::new(None),
            control: Mutex::new(()),
            inline_seg0: UnsafeCell::new(InlineSegment::new()),
        });

        let seg0_raw = shared.inline_seg0.get().cast::<Segment<T>>();
        shared.tail_seg.store(seg0_raw, Ordering::Relaxed);
        let (seg0, slot_ptr) = unsafe {
            (
                NonNull::new_unchecked(seg0_raw),
                Segment::slot_ptr_at(seg0_raw, 0),
            )
        };

        let subscription = Subscription {
            cursor: 0,
            seg: seg0,
            slot_ptr,
            rem_in_seg: INLINE_CAP as u32,
            wait_gen_hi: 0,
        };
        (shared, subscription)
    }

    #[inline]
    pub fn clone_sender(&self) -> NonNull<Shard> {
        // Caller clones `Arc<Shared<T>>` first, which already enforces `isize::MAX` refcount cap.
        self.senders.fetch_add(1, Ordering::Relaxed);
        let idx = self.next_shard.fetch_add(1, Ordering::Relaxed) & (SHARDS - 1);
        if idx == 0 {
            NonNull::from(&self.shard0)
        } else {
            let mut extra = self.extra_shards.load(Ordering::Acquire);
            if extra.is_null() {
                extra = init_extra_shards_box(&self.extra_shards);
            }
            // SAFETY: `extra` stays valid until `Shared::drop` and `idx - 1 < SHARDS - 1`.
            unsafe { NonNull::from(&(*extra)[idx - 1].shard) }
        }
    }

    #[inline]
    pub fn drop_sender(&self) {
        if self.senders.fetch_sub(1, Ordering::SeqCst) == 1
            && self.has_waiters.load(Ordering::SeqCst)
        {
            self.wake_all_waiters();
        }
    }

    #[cold]
    #[inline(never)]
    fn wake_all_waiters(&self) {
        let mut waiters = self.waiters.lock();
        if waiters.overflow_len == 0 {
            let Some(waker) = waiters.slot0.take() else {
                self.has_waiters.store(false, Ordering::Release);
                return;
            };
            self.wake_gen.advance_or_abort(Ordering::Release);
            self.has_waiters.store(false, Ordering::Release);
            drop(waiters);
            waker.wake();
            return;
        }

        let mut batch = WakerBatch::new();
        if let Some(w0) = waiters.slot0.take() {
            batch.push(w0);
        }
        waiters.drain_overflow_into(&mut batch);
        self.wake_gen.advance_or_abort(Ordering::Release);
        self.has_waiters.store(false, Ordering::Release);
        drop(waiters);
        batch.wake_all();
    }

    #[inline]
    pub fn is_disconnected(&self) -> bool {
        self.senders.load(Ordering::Acquire) == 0
    }

    #[inline]
    pub fn unread_from(&self, cursor: u64) -> usize {
        let tail = self.tail.load(Ordering::Acquire);
        usize::try_from(tail.saturating_sub(cursor))
            .expect("unread broadcast message count exceeds usize")
    }

    pub fn retained_message_count(&self) -> usize {
        if self.live_receivers.load(Ordering::Acquire) == 0 {
            return 0;
        }
        let reclaimed = self.reclaimed.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        usize::try_from(tail.saturating_sub(reclaimed))
            .expect("retained broadcast message count exceeds usize")
    }

    #[inline(always)]
    fn enter_shard(&self, shard_ptr: NonNull<Shard>) -> (&Shard, usize) {
        // SAFETY: `shard_ptr` points inside `self.shard0` or `self.extra_shards`, both valid for
        // the lifetime of `&self`.
        let shard = unsafe { shard_ptr.as_ref() };
        if !self.paused.load(Ordering::Relaxed) {
            let e = self.epoch.load(Ordering::Relaxed);
            shard.enter(e);
            if !self.paused.load(Ordering::SeqCst) {
                return (shard, e);
            }
            shard.exit(e);
        }
        self.enter_shard_slow(shard)
    }

    #[cold]
    #[inline(never)]
    fn enter_shard_slow<'a>(&'a self, shard: &'a Shard) -> (&'a Shard, usize) {
        loop {
            drop(self.control.lock());
            if self.paused.load(Ordering::Relaxed) {
                continue;
            }
            let e = self.epoch.load(Ordering::Relaxed);
            shard.enter(e);
            if !self.paused.load(Ordering::SeqCst) {
                return (shard, e);
            }
            shard.exit(e);
        }
    }

    #[inline]
    fn wait_shard_quiescent(shard: &Shard) {
        while !shard.is_all_idle() {
            thread::yield_now();
        }
    }

    fn all_shards_idle(&self) -> bool {
        if !self.shard0.is_all_idle() {
            return false;
        }
        let extra = self.extra_shards.load(Ordering::Acquire);
        if !extra.is_null() {
            // SAFETY: `extra` stays valid until `Shared::drop`.
            for slot in unsafe { &*extra } {
                if !slot.shard.is_all_idle() {
                    return false;
                }
            }
        }
        true
    }

    fn pause_publishers_locked(&self) {
        self.paused.store(true, Ordering::SeqCst);
        Self::wait_shard_quiescent(&self.shard0);
        let extra = self.extra_shards.load(Ordering::Acquire);
        if !extra.is_null() {
            // SAFETY: `extra` stays valid until `Shared::drop`.
            for slot in unsafe { &*extra } {
                Self::wait_shard_quiescent(&slot.shard);
            }
        }
    }

    fn unpause_publishers_locked(&self) {
        self.paused.store(false, Ordering::Release);
    }

    #[inline(always)]
    pub fn send(&self, shard_ptr: NonNull<Shard>, msg: T) {
        if self.live_receivers.load(Ordering::Relaxed) == 0 {
            self.send_cold(shard_ptr, msg);
            return;
        }

        let arc_msg = Arc::new(msg);
        let (shard, e) = self.enter_shard(shard_ptr);
        let live_rcvs = self.live_receivers.load(Ordering::Acquire);
        if live_rcvs == 0 {
            self.finish_discard_arc(shard, e, arc_msg);
            return;
        }

        let Some(version) = self.tail.claim_next() else {
            shard.exit(e);
            Self::drop_and_panic_overflow(arc_msg);
        };

        // SAFETY: Active shard epoch keeps `tail_seg` valid, and any segment holding an unwritten
        // `version` remains pinned until `slot.state` becomes non-zero.
        unsafe {
            let seg = self.tail_seg.load(Ordering::Acquire);
            let base = (*seg).base.load(Ordering::Relaxed);
            if version >= base && version - base < (*seg).cap as u64 {
                let slot = Segment::slot_at(seg, (version - base) as usize);
                (*slot.msg.get()).write(arc_msg);
                slot.state.store(live_rcvs, Ordering::SeqCst);
            } else {
                self.publish_slow(seg, version, live_rcvs, arc_msg);
            }
        }

        shard.exit(e);

        if self.has_waiters.load(Ordering::SeqCst) {
            self.wake_all_waiters();
        }
    }

    #[cold]
    #[inline(never)]
    fn send_cold(&self, shard_ptr: NonNull<Shard>, msg: T) {
        if self.tail.load(Ordering::Relaxed) == u64::MAX {
            Self::drop_and_panic_overflow(msg);
        }
        let (shard, e) = self.enter_shard(shard_ptr);
        let live_rcvs = self.live_receivers.load(Ordering::Acquire);
        if live_rcvs == 0 {
            let claimed = self.tail.claim_next();
            if claimed.is_some() {
                self.reclaimed.fetch_add(1, Ordering::Relaxed);
            }
            shard.exit(e);
            drop(msg);
            assert!(
                claimed.is_some(),
                "broadcast channel version counter overflowed"
            );
            return;
        }
        shard.exit(e);
        self.send(shard_ptr, msg);
    }

    #[cold]
    #[inline(never)]
    fn finish_discard_arc(&self, shard: &Shard, e: usize, arc_msg: Arc<T>) {
        let claimed = self.tail.claim_next();
        if claimed.is_some() {
            self.reclaimed.fetch_add(1, Ordering::Relaxed);
        }
        shard.exit(e);
        drop(arc_msg);
        assert!(
            claimed.is_some(),
            "broadcast channel version counter overflowed"
        );
    }

    #[cold]
    #[inline(never)]
    fn drop_and_panic_overflow<V>(value: V) -> ! {
        drop(value);
        panic!("broadcast channel version counter overflowed");
    }

    #[cold]
    #[inline(never)]
    unsafe fn publish_slow(
        &self,
        mut seg: *mut Segment<T>,
        version: u64,
        live_rcvs: usize,
        arc_msg: Arc<T>,
    ) {
        unsafe {
            while version < (*seg).base.load(Ordering::Relaxed) {
                seg = (*seg).prev.load(Ordering::Acquire);
                debug_assert!(!seg.is_null());
            }
            loop {
                let base = (*seg).base.load(Ordering::Relaxed);
                let cap = (*seg).cap as u64;
                if version - base < cap {
                    let slot = Segment::slot_at(seg, (version - base) as usize);
                    (*slot.msg.get()).write(arc_msg);
                    slot.state.store(live_rcvs, Ordering::SeqCst);
                    return;
                }
                seg = self.advance_or_get_next_for_pub(seg);
            }
        }
    }

    #[cold]
    unsafe fn advance_or_get_next_for_pub(&self, cur: *mut Segment<T>) -> *mut Segment<T> {
        let next = unsafe { self.get_or_init_next(cur) };
        if ptr::eq(self.tail_seg.load(Ordering::Acquire), cur) {
            unsafe {
                (*next).refs.fetch_add(1, Ordering::Relaxed);
            }
            if self
                .tail_seg
                .compare_exchange(cur, next, Ordering::SeqCst, Ordering::Acquire)
                .is_ok()
            {
                unsafe {
                    self.release_seg_ref(NonNull::new_unchecked(cur));
                }
            } else {
                unsafe {
                    self.release_seg_ref(NonNull::new_unchecked(next));
                }
            }
        }
        next
    }

    pub fn subscribe(&self) -> Subscription<T> {
        let control = self.control.lock();
        let has_senders = !self.is_disconnected();
        if has_senders {
            self.pause_publishers_locked();
        }

        let start = self.tail.load(Ordering::Relaxed);
        let prev_live = self.live_receivers.load(Ordering::Relaxed);

        // SAFETY: All publishers are paused under `control` (or zero senders remain).
        let (seg, slot_ptr, rem_in_seg) = unsafe {
            let seg = if prev_live == 0 {
                let cur = self.tail_seg.load(Ordering::Acquire);
                let base = (*cur).base.load(Ordering::Relaxed);
                let cap = (*cur).cap as u64;
                if (*cur).refs.load(Ordering::Acquire) == 1
                    && (*cur).next.load(Ordering::Acquire).is_null()
                {
                    if start < base || start - base >= cap {
                        (*cur).base.store(start, Ordering::Relaxed);
                    }
                    (*cur).prev.store(ptr::null_mut(), Ordering::Relaxed);
                    (*cur).refs.fetch_add(1, Ordering::Relaxed);
                    NonNull::new_unchecked(cur)
                } else {
                    let fresh = self.alloc_heap_segment(start, 2, ptr::null_mut());
                    self.tail_seg.store(fresh.as_ptr(), Ordering::SeqCst);
                    self.release_seg_ref(NonNull::new_unchecked(cur));
                    fresh
                }
            } else {
                let mut cur = self.tail_seg.load(Ordering::Acquire);
                loop {
                    let base = (*cur).base.load(Ordering::Relaxed);
                    let cap = (*cur).cap as u64;
                    if start < base {
                        cur = (*cur).prev.load(Ordering::Acquire);
                        continue;
                    }
                    if start - base < cap {
                        break;
                    }
                    cur = self.advance_or_get_next_for_pub(cur);
                }
                (*cur).refs.fetch_add(1, Ordering::Relaxed);
                NonNull::new_unchecked(cur)
            };
            let raw = seg.as_ptr();
            let base = (*raw).base.load(Ordering::Relaxed);
            let cap = (*raw).cap;
            let offset = (start - base) as usize;
            (
                seg,
                Segment::slot_ptr_at(raw, offset),
                (cap - offset) as u32,
            )
        };

        self.live_receivers.store(prev_live + 1, Ordering::Release);

        if has_senders {
            self.unpause_publishers_locked();
        }
        drop(control);

        Subscription {
            cursor: start,
            seg,
            slot_ptr,
            rem_in_seg,
            wait_gen_hi: 0,
        }
    }

    pub fn drop_receiver(&self, mut state: Subscription<T>) {
        let (end, became_zero_receivers) =
            if self.is_disconnected() && self.live_receivers.load(Ordering::Acquire) == 1 {
                self.live_receivers.store(0, Ordering::Release);
                (self.tail.load(Ordering::Relaxed), true)
            } else {
                let _control = self.control.lock();
                let has_senders = !self.is_disconnected();
                if has_senders {
                    self.pause_publishers_locked();
                }

                let end = self.tail.load(Ordering::Relaxed);
                let prev_live = self.live_receivers.load(Ordering::Relaxed);
                self.live_receivers.store(prev_live - 1, Ordering::Release);

                if has_senders {
                    self.unpause_publishers_locked();
                }
                (end, prev_live == 1)
            };

        let mut reclaimed = DrainBatch::new();
        let mut reclaimed_count = 0u64;
        while state.cursor < end {
            // SAFETY: `state.seg` holds this receiver's `+1` reference, `state.slot_ptr` points to
            // the current in-bounds slot, and every `cursor < end` was claimed before pause.
            unsafe {
                let slot = state.slot_ptr.as_ref();

                let mut slot_state = slot.state.load(Ordering::Acquire);
                if slot_state == 0 {
                    slot_state = self.wait_for_inflight_slot(slot);
                }
                if slot_state == 1 {
                    let msg = (*slot.msg.get()).assume_init_read();
                    slot.state.store(0, Ordering::Relaxed);
                    reclaimed.push(msg);
                    reclaimed_count += 1;
                } else if slot.state.fetch_sub(1, Ordering::AcqRel) == 1 {
                    let msg = (*slot.msg.get()).assume_init_read();
                    reclaimed.push(msg);
                    reclaimed_count += 1;
                }

                state.cursor += 1;
                state.rem_in_seg -= 1;
                if state.rem_in_seg != 0 {
                    state.slot_ptr = state.slot_ptr.add(1);
                } else {
                    self.advance_receiver_seg(&mut state);
                }
            }
        }
        if reclaimed_count != 0 {
            self.reclaimed.fetch_add(reclaimed_count, Ordering::Release);
        }

        // Reclaim segment references and any half-closed backing allocations before dropping
        // user payloads so backing memory is returned even if a payload destructor unwinds.
        unsafe {
            self.release_seg_ref(state.seg);
        }
        if became_zero_receivers && self.has_heap_pool.load(Ordering::Acquire) {
            self.reclaim_all_heap_when_no_receivers();
        }
        drop(reclaimed);
    }

    /// Immediately releases all heap segments when no receivers remain so idle `UnboundedSender`
    /// handles never retain backing segment buffers.
    #[cold]
    #[inline(never)]
    fn reclaim_all_heap_when_no_receivers(&self) {
        let mut to_free = Vec::new();
        {
            let _control = self.control.lock();
            if self.live_receivers.load(Ordering::Acquire) != 0 {
                return;
            }
            let has_senders = !self.is_disconnected();
            if has_senders {
                self.pause_publishers_locked();
            }

            let cur_tail = self.tail.load(Ordering::Relaxed);
            let seg0_raw = self.inline_seg0.get().cast::<Segment<T>>();
            let displaced = if !ptr::eq(self.tail_seg.load(Ordering::Acquire), seg0_raw) {
                // SAFETY: Publishers are paused and `live_receivers == 0`, so `inline_seg0` has
                // `refs == 0` and all its slots are already `state == 0`.
                unsafe {
                    (*seg0_raw).base.store(cur_tail, Ordering::Relaxed);
                    (*seg0_raw).refs.store(1, Ordering::Relaxed);
                    (*seg0_raw).prev.store(ptr::null_mut(), Ordering::Relaxed);
                    (*seg0_raw).next.store(ptr::null_mut(), Ordering::Relaxed);
                }
                NonNull::new(self.tail_seg.swap(seg0_raw, Ordering::SeqCst))
            } else {
                None
            };

            if has_senders {
                self.unpause_publishers_locked();
            }

            if let Some(old_tail) = displaced {
                // SAFETY: `self.tail_seg` owned one reference to `old_tail`.
                unsafe {
                    self.release_seg_ref(old_tail);
                }
            }

            if self.live_receivers.load(Ordering::Acquire) == 0 {
                let mut pool_guard = self.pool.lock();
                if let Some(mut pool) = pool_guard.take() {
                    if pool.active_heap_segments == 0 && self.all_shards_idle() {
                        to_free.append(&mut pool.free_list);
                        for bucket in &mut pool.retired {
                            to_free.append(bucket);
                        }
                        self.has_heap_pool.store(false, Ordering::Release);
                        self.has_excess_pool.store(false, Ordering::Relaxed);
                    } else {
                        self.try_advance_epoch_locked(&mut pool);
                        *pool_guard = Some(pool);
                    }
                }
            }
        }
        for seg in to_free {
            // SAFETY: Zero references and zero active shard epochs remain for `seg`.
            unsafe {
                Segment::dealloc_heap(seg);
            }
        }
    }

    #[inline(always)]
    pub fn try_recv(&self, sub: &mut Subscription<T>) -> Result<Received<T>, TryRecvError> {
        let cur_ver = sub.cursor;
        // SAFETY: `sub.seg` owns this receiver's `+1` segment reference, and `sub.slot_ptr` points
        // directly to the in-bounds `MsgSlot<T>` for `cur_ver`.
        let slot = unsafe { sub.slot_ptr.as_ref() };

        let mut state = slot.state.load(Ordering::Acquire);
        if state == 0 {
            if self.tail.load(Ordering::Acquire) <= cur_ver {
                if !self.is_disconnected() {
                    return Err(TryRecvError::Empty);
                }
                if self.tail.load(Ordering::Acquire) <= cur_ver {
                    return Err(TryRecvError::Disconnected);
                }
            }
            state = self.wait_for_inflight_slot(slot);
        }

        Ok(self.consume_ready_slot(sub, cur_ver, slot, state))
    }

    #[inline(always)]
    fn consume_ready_slot(
        &self,
        sub: &mut Subscription<T>,
        cur_ver: u64,
        slot: &MsgSlot<T>,
        state: usize,
    ) -> Received<T> {
        // Hold `msg` in `ManuallyDrop` across the cold segment/pool helpers so LLVM promotes both
        // `msg` and `reclaimed` into SSA registers without stack-spilling `Arc<T>` for unwind glue.
        let (msg, reclaimed) = if state == 1 {
            // SAFETY: `state == 1` with `Acquire` synchronized with any earlier readers' `AcqRel`
            // decrements, and no other live subscription has `cursor <= cur_ver`.
            let msg = unsafe { ManuallyDrop::new((*slot.msg.get()).assume_init_read()) };
            slot.state.store(0, Ordering::Relaxed);
            (msg, true)
        } else {
            // SAFETY: `state >= 2` means our own claim keeps `slot.msg` initialized through the
            // `Arc::clone` below until `fetch_sub` releases that claim.
            let msg = unsafe { ManuallyDrop::new((*slot.msg.get()).assume_init_ref().clone()) };
            if slot.state.fetch_sub(1, Ordering::AcqRel) == 1 {
                unsafe {
                    Self::drop_duplicate_slot_arc(slot);
                }
                (msg, true)
            } else {
                (msg, false)
            }
        };

        let next_ver = cur_ver + 1;
        sub.cursor = next_ver;

        if reclaimed {
            self.reclaimed.fetch_add(1, Ordering::Relaxed);
        }

        sub.rem_in_seg -= 1;
        if sub.rem_in_seg != 0 {
            // SAFETY: `rem_in_seg` was at least 2 before decrementing, so `slot_ptr.add(1)` stays
            // strictly within `0..cap` of `sub.seg`.
            sub.slot_ptr = unsafe { sub.slot_ptr.add(1) };
        } else {
            unsafe {
                self.advance_receiver_seg(sub);
            }
        }

        if reclaimed
            && (self.has_excess_pool.load(Ordering::Relaxed)
                || (self.has_heap_pool.load(Ordering::Relaxed) && self.is_disconnected()))
        {
            self.post_reclaim_cold(sub, next_ver);
        }

        Received {
            msg: ManuallyDrop::into_inner(msg),
            reclaimed,
        }
    }

    #[cold]
    #[inline(never)]
    fn wait_for_inflight_slot(&self, slot: &MsgSlot<T>) -> usize {
        let state = slot.state.load(Ordering::Acquire);
        if state != 0 {
            return state;
        }

        // Park on a private `Mutex<bool>` + `Condvar` waker instead of looping `yield_now` or
        // calling `std::thread::park`, leaving any outer executor's `Thread::unpark` token intact.
        let parker = Arc::new(SlotPark::new());
        let waker = Waker::from(parker.clone());
        let mut wait_slot = None;
        let mut wait_gen_hi = 0u32;

        loop {
            {
                let mut waiters = self.waiters.lock();
                let current_gen = self.wake_gen.load(Ordering::Relaxed);
                let retired =
                    waiters.register(&mut wait_slot, &mut wait_gen_hi, current_gen, &waker);
                self.has_waiters.store(true, Ordering::SeqCst);

                let ready = slot.state.load(Ordering::SeqCst);
                if ready != 0 {
                    let cleared = wait_slot
                        .take()
                        .and_then(|reg| waiters.remove(reg, wait_gen_hi, current_gen));
                    if waiters.is_empty() {
                        self.has_waiters.store(false, Ordering::Release);
                    }
                    drop(waiters);
                    drop(retired);
                    drop(cleared);
                    return ready;
                }
                drop(waiters);
                drop(retired);
            }

            parker.wait();
            let ready = slot.state.load(Ordering::Acquire);
            if ready != 0 {
                self.clear_wait_slot_fast(wait_gen_hi, &mut wait_slot);
                return ready;
            }
        }
    }

    #[cold]
    #[inline(never)]
    unsafe fn drop_duplicate_slot_arc(slot: &MsgSlot<T>) {
        // SAFETY: Caller already cloned `msg` and won the final `fetch_sub(1, AcqRel) == 1` tie,
        // so the caller's local `msg` clone keeps strong_count >= 2 (`T::drop` cannot run here).
        unsafe {
            (*slot.msg.get()).assume_init_drop();
        }
    }

    #[cold]
    #[inline(never)]
    fn post_reclaim_cold(&self, sub: &mut Subscription<T>, next_ver: u64) {
        if self.tail.load(Ordering::Acquire) != next_ver {
            return;
        }
        if self.is_disconnected() {
            self.reclaim_when_drained_and_disconnected(sub, next_ver);
        } else if self.has_excess_pool.load(Ordering::Relaxed) {
            self.maybe_shrink_pool();
        }
    }

    /// Immediately reclaims heap segment backing memory once all senders have disconnected and
    /// receivers finish draining the backlog, even while disconnected `UnboundedReceiver` handles
    /// remain alive.
    #[cold]
    #[inline(never)]
    fn reclaim_when_drained_and_disconnected(&self, sub: &mut Subscription<T>, next_ver: u64) {
        let mut to_free = Vec::new();
        {
            let _control = self.control.lock();
            let seg0_raw = self.inline_seg0.get().cast::<Segment<T>>();
            let old_tail_raw = self.tail_seg.load(Ordering::Acquire);
            if self.live_receivers.load(Ordering::Acquire) == 1
                && !(ptr::eq(old_tail_raw, seg0_raw) && ptr::eq(sub.seg.as_ptr(), seg0_raw))
            {
                // SAFETY: Zero senders exist permanently, `sub` is the sole live subscription, and
                // every buffered slot up to `next_ver` is drained. Releasing both `tail_seg` and
                // `sub.seg` drops all active segment references (whether mid-segment or right
                // across a segment boundary) to zero before resetting `inline_seg0`.
                unsafe {
                    if let Some(old_tail) = NonNull::new(old_tail_raw) {
                        self.release_seg_ref(old_tail);
                    }
                    self.release_seg_ref(sub.seg);
                    (*seg0_raw).base.store(next_ver, Ordering::Relaxed);
                    (*seg0_raw).refs.store(2, Ordering::Relaxed);
                    (*seg0_raw).prev.store(ptr::null_mut(), Ordering::Relaxed);
                    (*seg0_raw).next.store(ptr::null_mut(), Ordering::Relaxed);
                    self.tail_seg.store(seg0_raw, Ordering::SeqCst);
                    sub.seg = NonNull::new_unchecked(seg0_raw);
                    sub.slot_ptr = Segment::slot_ptr_at(seg0_raw, 0);
                    sub.rem_in_seg = INLINE_CAP as u32;
                }
            }

            let mut pool_guard = self.pool.lock();
            if let Some(mut pool) = pool_guard.take() {
                for _ in 0..EPOCHS {
                    self.try_advance_epoch_locked(&mut pool);
                }
                to_free.append(&mut pool.free_list);
                if pool.active_heap_segments == 0 && pool.cached_len() == 0 {
                    self.has_heap_pool.store(false, Ordering::Release);
                    self.has_excess_pool.store(false, Ordering::Relaxed);
                } else {
                    self.has_excess_pool
                        .store(pool.cached_len() > 1, Ordering::Relaxed);
                    *pool_guard = Some(pool);
                }
            }
        }
        for seg in to_free {
            unsafe {
                Segment::dealloc_heap(seg);
            }
        }
    }

    #[cold]
    #[inline(never)]
    unsafe fn advance_receiver_seg(&self, sub: &mut Subscription<T>) {
        unsafe {
            let next_raw = self.get_or_init_next(sub.seg.as_ptr());
            let next = NonNull::new_unchecked(next_raw);
            (*next_raw).refs.fetch_add(1, Ordering::Relaxed);
            let prev = mem::replace(&mut sub.seg, next);
            sub.slot_ptr = Segment::slot_ptr_at(next_raw, 0);
            sub.rem_in_seg = (*next_raw).cap as u32;
            self.release_seg_ref(prev);
        }
    }

    #[inline]
    pub fn poll_recv(
        &self,
        sub: &mut Subscription<T>,
        wait_slot: &mut Option<WaitReg>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Received<T>, RecvError>> {
        loop {
            let cur_ver = sub.cursor;
            // SAFETY: `sub.seg` owns this receiver's `+1` segment reference, and `sub.slot_ptr`
            // points directly to the in-bounds `MsgSlot<T>` for `cur_ver`.
            let slot = unsafe { sub.slot_ptr.as_ref() };
            let state = slot.state.load(Ordering::Acquire);
            if state != 0 {
                let received = self.consume_ready_slot(sub, cur_ver, slot, state);
                self.clear_wait_slot_fast(sub.wait_gen_hi, wait_slot);
                return Poll::Ready(Ok(received));
            }
            if self.tail.load(Ordering::Acquire) <= cur_ver
                && self.is_disconnected()
                && self.tail.load(Ordering::Acquire) <= cur_ver
            {
                self.clear_wait_slot_fast(sub.wait_gen_hi, wait_slot);
                return Poll::Ready(Err(RecvError::Disconnected));
            }
            // Never spin or yield inside `Future::poll`: even if a concurrent publisher has
            // claimed `cur_ver` (`tail > cur_ver`) and was preempted before storing `slot.state`,
            // register the task waker and return `Poll::Pending` so the publisher wakes us once
            // `slot.state.store(..., SeqCst)` completes.
            if !self.register_waiter(cur_ver, slot, &mut sub.wait_gen_hi, wait_slot, cx) {
                return Poll::Pending;
            }
        }
    }

    #[inline]
    fn clear_wait_slot_fast(&self, wait_gen_hi: u32, wait_slot: &mut Option<WaitReg>) {
        if let Some(reg) = *wait_slot {
            if reg.full_gen(wait_gen_hi) == self.wake_gen.load(Ordering::Acquire) {
                self.unregister_recv(wait_gen_hi, wait_slot);
            } else {
                *wait_slot = None;
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn register_waiter(
        &self,
        cur_ver: u64,
        slot: &MsgSlot<T>,
        wait_gen_hi: &mut u32,
        wait_slot: &mut Option<WaitReg>,
        cx: &mut Context<'_>,
    ) -> bool {
        let mut waiters = self.waiters.lock();
        let current_gen = self.wake_gen.load(Ordering::Relaxed);
        let retired = waiters.register(wait_slot, wait_gen_hi, current_gen, cx.waker());
        self.has_waiters.store(true, Ordering::SeqCst);

        let slot_ready = slot.state.load(Ordering::SeqCst) != 0;
        let disconnected_and_empty = !slot_ready
            && self.senders.load(Ordering::SeqCst) == 0
            && self.tail.load(Ordering::SeqCst) <= cur_ver;
        if slot_ready || disconnected_and_empty {
            let cleared = wait_slot
                .take()
                .and_then(|reg| waiters.remove(reg, *wait_gen_hi, current_gen));
            if waiters.is_empty() {
                self.has_waiters.store(false, Ordering::Release);
            }
            drop(waiters);
            drop(retired);
            drop(cleared);
            return true;
        }

        drop(waiters);
        drop(retired);
        false
    }

    #[cold]
    #[inline(never)]
    pub fn unregister_recv(&self, wait_gen_hi: u32, wait_slot: &mut Option<WaitReg>) {
        let Some(reg) = wait_slot.take() else {
            return;
        };
        if reg.full_gen(wait_gen_hi) != self.wake_gen.load(Ordering::Acquire) {
            return;
        }
        let mut waiters = self.waiters.lock();
        let current_gen = self.wake_gen.load(Ordering::Relaxed);
        let waker = waiters.remove(reg, wait_gen_hi, current_gen);
        if waiters.is_empty() {
            self.has_waiters.store(false, Ordering::Release);
        }
        drop(waiters);
        drop(waker);
    }

    #[cold]
    unsafe fn get_or_init_next(&self, cur: *mut Segment<T>) -> *mut Segment<T> {
        let existing = unsafe { (*cur).next.load(Ordering::Acquire) };
        if !existing.is_null() {
            return existing;
        }

        let next_base = unsafe {
            (*cur)
                .base
                .load(Ordering::Relaxed)
                .saturating_add((*cur).cap as u64)
        };
        let cand = self.alloc_heap_segment(next_base, 1, cur);
        match unsafe {
            (*cur).next.compare_exchange(
                ptr::null_mut(),
                cand.as_ptr(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
        } {
            Ok(_) => cand.as_ptr(),
            Err(winner) => {
                self.put_unused_heap_segment(cand);
                winner
            }
        }
    }

    fn alloc_heap_segment(
        &self,
        base: u64,
        refs: usize,
        prev: *mut Segment<T>,
    ) -> NonNull<Segment<T>> {
        let reused = {
            let mut pool_guard = self.pool.lock();
            let pool = pool_guard.get_or_insert_with(|| {
                self.has_heap_pool.store(true, Ordering::Release);
                Box::new(SegmentPool::new())
            });
            self.try_advance_epoch_locked(pool);
            let popped = pool.free_list.pop();
            pool.active_heap_segments += 1;
            pool.cycle_peak_heap = pool.cycle_peak_heap.max(pool.active_heap_segments);
            popped
        };
        if let Some(ptr) = reused {
            unsafe {
                Segment::reset_for_reuse(ptr, base, refs, prev);
            }
            ptr
        } else {
            Segment::alloc_heap(base, refs, prev)
        }
    }

    fn put_unused_heap_segment(&self, seg: NonNull<Segment<T>>) {
        let mut pool_guard = self.pool.lock();
        let pool = pool_guard
            .as_mut()
            .expect("heap segment requires initialized SegmentPool");
        pool.active_heap_segments -= 1;
        pool.free_list.push(seg);
    }

    unsafe fn release_seg_ref(&self, mut seg: NonNull<Segment<T>>) {
        loop {
            let raw = seg.as_ptr();
            if unsafe { (*raw).refs.fetch_sub(1, Ordering::AcqRel) } != 1 {
                return;
            }
            let next_raw = unsafe { (*raw).next.swap(ptr::null_mut(), Ordering::Acquire) };
            let is_inline = unsafe { (*raw).is_inline };
            if !is_inline {
                let mut pool_guard = self.pool.lock();
                let pool = pool_guard
                    .as_mut()
                    .expect("heap segment requires initialized SegmentPool");
                pool.active_heap_segments -= 1;
                let cur_epoch = self.epoch.load(Ordering::Relaxed);
                pool.retired[cur_epoch].push(seg);
                self.try_advance_epoch_locked(pool);
                if pool.cached_len() > 1 {
                    self.has_excess_pool.store(true, Ordering::Relaxed);
                }
            }
            let Some(next) = NonNull::new(next_raw) else {
                return;
            };
            seg = next;
        }
    }

    fn shard_epoch_idle(&self, epoch_idx: usize) -> bool {
        if !self.shard0.is_epoch_idle(epoch_idx) {
            return false;
        }
        let extra = self.extra_shards.load(Ordering::Acquire);
        if !extra.is_null() {
            // SAFETY: `extra` stays valid until `Shared::drop`.
            for slot in unsafe { &*extra } {
                if !slot.shard.is_epoch_idle(epoch_idx) {
                    return false;
                }
            }
        }
        true
    }

    fn try_advance_epoch_locked(&self, pool: &mut SegmentPool<T>) {
        let steps = if self.all_shards_idle() { EPOCHS } else { 2 };
        for _ in 0..steps {
            let cur = self.epoch.load(Ordering::Relaxed);
            let prev = (cur + EPOCHS - 1) % EPOCHS;
            if !self.shard_epoch_idle(prev) {
                break;
            }
            let next = (cur + 1) % EPOCHS;
            self.epoch.store(next, Ordering::SeqCst);
            pool.free_list.append(&mut pool.retired[next]);
        }
    }

    #[cold]
    #[inline(never)]
    fn maybe_shrink_pool(&self) {
        if self.retained_message_count() != 0 {
            return;
        }
        let mut to_free = Vec::new();
        {
            let mut pool_guard = self.pool.lock();
            let Some(pool) = pool_guard.as_mut() else {
                self.has_excess_pool.store(false, Ordering::Relaxed);
                return;
            };
            self.try_advance_epoch_locked(pool);
            let cached = pool.cached_len();
            let active = pool.active_heap_segments;
            let total = active + cached;
            let peak = mem::replace(&mut pool.cycle_peak_heap, active);
            if total > 1 && peak <= total / 4 {
                let keep_cached = (peak * 2).max(1).saturating_sub(active);
                while pool.free_list.len() > keep_cached {
                    if let Some(seg) = pool.free_list.pop() {
                        to_free.push(seg);
                    }
                }
            }
            if pool.cached_len() <= 1 {
                self.has_excess_pool.store(false, Ordering::Relaxed);
            }
        }
        for seg in to_free {
            unsafe {
                Segment::dealloc_heap(seg);
            }
        }
    }

    #[cfg(test)]
    pub fn set_tail(&self, tail: u64) {
        let _control = self.control.lock();
        assert_eq!(self.live_receivers.load(Ordering::Acquire), 0);
        self.tail.store(tail, Ordering::SeqCst);
        self.reclaimed.store(tail, Ordering::Release);
        let cur = self.tail_seg.load(Ordering::Acquire);
        unsafe {
            (*cur).base.store(tail, Ordering::Relaxed);
        }
    }

    #[cfg(test)]
    pub fn is_unlocked_for_test(&self) -> bool {
        self.control.try_lock().is_some()
            && self.pool.try_lock().is_some()
            && self.waiters.try_lock().is_some()
    }

    #[cfg(test)]
    pub fn buffer_capacity(&self) -> usize {
        let mut pool_guard = self.pool.lock();
        let Some(pool) = pool_guard.as_mut() else {
            return INLINE_CAP;
        };
        self.try_advance_epoch_locked(pool);
        let heap_segments = pool.active_heap_segments + pool.cached_len();
        INLINE_CAP + heap_segments * HEAP_CAP
    }

    #[cfg(test)]
    pub fn has_waiters_for_test(&self) -> bool {
        self.has_waiters.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub fn begin_inflight_publish_for_test(&self) -> (usize, u64, usize) {
        let (_, e) = self.enter_shard(NonNull::from(&self.shard0));
        let live_rcvs = self.live_receivers.load(Ordering::Acquire);
        let version = self.tail.claim_next().expect("version overflow in test");
        (e, version, live_rcvs)
    }

    #[cfg(test)]
    pub fn finish_inflight_publish_for_test(
        &self,
        epoch: usize,
        version: u64,
        live_rcvs: usize,
        msg: T,
    ) {
        let arc_msg = Arc::new(msg);
        unsafe {
            let seg = self.tail_seg.load(Ordering::Acquire);
            let base = (*seg).base.load(Ordering::Relaxed);
            let slot = Segment::slot_at(seg, (version - base) as usize);
            (*slot.msg.get()).write(arc_msg);
            slot.state.store(live_rcvs, Ordering::SeqCst);
        }
        self.shard0.exit(epoch);
        if self.has_waiters.load(Ordering::SeqCst) {
            self.wake_all_waiters();
        }
    }
}
