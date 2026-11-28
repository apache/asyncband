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

//! Segment-backed MPMC unbounded broadcast engine.
//!
//! Versions `[0, 8)` live inline in [`Shared`] so short-lived or idle channels allocate zero heap
//! segments. Versions `>= 8` live in uniform `Arc`-owned [`Segment`] blocks of [`SEG_CAP`] slots
//! (`[8, 256)` in the `base = 0` segment, then `[256 * k, 256 * (k + 1))`). Publishers borrow the
//! active tail segment and subscriber count under a 4-shard reader-writer lock (`ShardedRwLock`),
//! while subscription changes and segment tail advances take exclusive write access so retired
//! segments drop immediately without epoch garbage collection.

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::ops::DerefMut;
use std::panic::RefUnwindSafe;
use std::panic::UnwindSafe;
use std::process;
use std::ptr;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
#[cfg(target_has_atomic = "64")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::thread;

use crate::broadcast::mpmc::common::Received;
use crate::broadcast::mpmc::error::RecvError;
use crate::broadcast::mpmc::error::TryRecvError;
use crate::broadcast::mpmc::unbounded::waiters::BroadcastWake;
use crate::broadcast::mpmc::unbounded::waiters::WakeToken;
use crate::internal::mutex::Mutex;

const INLINE_CAP: usize = 8;
pub const SEG_CAP: usize = 256;
const SEG_MASK: usize = SEG_CAP - 1;
const SHARDS: usize = 4;

#[cfg(target_has_atomic = "64")]
pub struct CounterU64(AtomicU64);

#[cfg(target_has_atomic = "64")]
impl CounterU64 {
    const FAST_LIMIT: u64 = u64::MAX - (1 << 32);

    pub const fn new(val: u64) -> Self {
        Self(AtomicU64::new(val))
    }

    #[inline(always)]
    pub fn load(&self, ordering: Ordering) -> u64 {
        self.0.load(ordering)
    }

    #[inline(always)]
    pub fn store(&self, val: u64, ordering: Ordering) {
        self.0.store(val, ordering);
    }

    #[inline(always)]
    pub fn fetch_add(&self, delta: u64, ordering: Ordering) {
        self.0.fetch_add(delta, ordering);
    }

    #[inline(always)]
    pub fn claim_next(&self) -> Option<u64> {
        let claimed = if self.0.load(Ordering::Relaxed) < Self::FAST_LIMIT {
            let prev = self.0.fetch_add(1, Ordering::SeqCst);
            if prev == u64::MAX {
                process::abort();
            }
            prev
        } else {
            self.claim_next_slow()
        };
        (claimed != u64::MAX).then_some(claimed)
    }

    #[cold]
    #[inline(never)]
    fn claim_next_slow(&self) -> u64 {
        let mut cur = self.0.load(Ordering::Relaxed);
        loop {
            let Some(next) = cur.checked_add(1) else {
                return u64::MAX;
            };
            match self
                .0
                .compare_exchange_weak(cur, next, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => return cur,
                Err(actual) => cur = actual,
            }
        }
    }
}

#[cfg(not(target_has_atomic = "64"))]
pub struct CounterU64(Mutex<u64>);

#[cfg(not(target_has_atomic = "64"))]
impl CounterU64 {
    pub const fn new(val: u64) -> Self {
        Self(Mutex::new(val))
    }

    #[inline]
    pub fn load(&self, _ordering: Ordering) -> u64 {
        *self.0.lock()
    }

    #[inline]
    pub fn store(&self, val: u64, _ordering: Ordering) {
        *self.0.lock() = val;
    }

    #[inline]
    pub fn fetch_add(&self, delta: u64, _ordering: Ordering) {
        let mut guard = self.0.lock();
        *guard = guard.wrapping_add(delta);
    }

    #[inline]
    pub fn claim_next(&self) -> Option<u64> {
        let mut guard = self.0.lock();
        let prev = *guard;
        *guard = prev.checked_add(1)?;
        Some(prev)
    }
}

#[repr(C)]
pub struct MsgSlot<T> {
    pub state: AtomicUsize,
    pub msg: UnsafeCell<MaybeUninit<Arc<T>>>,
}

impl<T> MsgSlot<T> {
    const fn empty() -> Self {
        Self {
            state: AtomicUsize::new(0),
            msg: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    #[inline]
    unsafe fn reclaim_unread(&self, state: usize) -> Option<Arc<T>> {
        if state == 1 {
            let msg = unsafe { (*self.msg.get()).assume_init_read() };
            self.state.store(0, Ordering::Relaxed);
            Some(msg)
        } else if self.state.fetch_sub(1, Ordering::AcqRel) == 1 {
            Some(unsafe { (*self.msg.get()).assume_init_read() })
        } else {
            None
        }
    }
}

struct Segment<T> {
    base: u64,
    next: OnceLock<Arc<Segment<T>>>,
    slots: [MsgSlot<T>; SEG_CAP],
    #[cfg(test)]
    live_slots: Arc<AtomicUsize>,
}

impl<T> Segment<T> {
    #[inline(always)]
    fn slot_ptr(seg: &Arc<Self>, offset: usize) -> NonNull<MsgSlot<T>> {
        debug_assert!(offset < SEG_CAP);
        // SAFETY: `Arc::as_ptr` preserves provenance over `slots`, and `offset < SEG_CAP`.
        unsafe {
            let slots = (&raw const (*Arc::as_ptr(seg)).slots).cast::<MsgSlot<T>>();
            NonNull::new_unchecked(slots.add(offset).cast_mut())
        }
    }

    #[inline]
    fn next_or_init(&self, shared: &Shared<T>) -> Arc<Self> {
        self.next
            .get_or_init(|| shared.alloc_segment(self.base + SEG_CAP as u64))
            .clone()
    }
}

impl<T> Drop for Segment<T> {
    fn drop(&mut self) {
        #[cfg(test)]
        self.live_slots.fetch_sub(
            if self.base == 0 {
                SEG_CAP - INLINE_CAP
            } else {
                SEG_CAP
            },
            Ordering::AcqRel,
        );
        // Unlink iteratively so dropping a long lagging chain cannot overflow the stack.
        let mut cur = self.next.take();
        while let Some(seg) = cur {
            match Arc::try_unwrap(seg) {
                Ok(mut owned) => cur = owned.next.take(),
                Err(_) => break,
            }
        }
    }
}

#[repr(C)]
struct Shard {
    readers: AtomicUsize,
    pad: MaybeUninit<[u8; 64 - size_of::<AtomicUsize>()]>,
}

impl Shard {
    const fn new() -> Self {
        Self {
            readers: AtomicUsize::new(0),
            pad: MaybeUninit::uninit(),
        }
    }
}

/// Reader-biased sharded reader-writer lock that avoids reader cache-line contention across
/// concurrent publishers while giving subscription updates and tail advances exclusive access.
#[repr(C)]
struct ShardedRwLock<T> {
    writer_lock: Mutex<()>,
    writing: AtomicBool,
    used_shards: AtomicUsize,
    data: UnsafeCell<T>,
    shards: [Shard; SHARDS],
}

impl<T> ShardedRwLock<T> {
    const fn new(value: T) -> Self {
        Self {
            writer_lock: Mutex::new(()),
            writing: AtomicBool::new(false),
            used_shards: AtomicUsize::new(1),
            data: UnsafeCell::new(value),
            shards: [const { Shard::new() }; SHARDS],
        }
    }

    #[inline]
    fn assign_shard(&self) -> usize {
        self.used_shards.fetch_add(1, Ordering::SeqCst) & (SHARDS - 1)
    }

    #[inline(always)]
    fn read(&self, shard_idx: usize) -> ReadGuard<'_, T> {
        let shard = &self.shards[shard_idx];
        if !self.writing.load(Ordering::Relaxed) {
            if shard.readers.fetch_add(1, Ordering::SeqCst) > isize::MAX as usize {
                process::abort();
            }
            if !self.writing.load(Ordering::SeqCst) {
                return ReadGuard { lock: self, shard };
            }
            shard.readers.fetch_sub(1, Ordering::Release);
        }
        self.read_slow(shard)
    }

    #[cold]
    #[inline(never)]
    fn read_slow<'a>(&'a self, shard: &'a Shard) -> ReadGuard<'a, T> {
        loop {
            drop(self.writer_lock.lock());
            if !self.writing.load(Ordering::Relaxed) {
                if shard.readers.fetch_add(1, Ordering::SeqCst) > isize::MAX as usize {
                    process::abort();
                }
                if !self.writing.load(Ordering::SeqCst) {
                    return ReadGuard { lock: self, shard };
                }
                shard.readers.fetch_sub(1, Ordering::Release);
            }
        }
    }

    fn write(&self) -> WriteGuard<'_, T> {
        let guard = self.writer_lock.lock();
        self.writing.store(true, Ordering::SeqCst);
        let count = self.used_shards.load(Ordering::SeqCst).min(SHARDS);
        for shard in &self.shards[..count] {
            while shard.readers.load(Ordering::SeqCst) != 0 {
                thread::yield_now();
            }
        }
        WriteGuard { lock: self, guard }
    }

    fn try_write(&self) -> Option<WriteGuard<'_, T>> {
        let guard = self.writer_lock.try_lock()?;
        self.writing.store(true, Ordering::SeqCst);
        let count = self.used_shards.load(Ordering::SeqCst).min(SHARDS);
        if self.shards[..count]
            .iter()
            .any(|s| s.readers.load(Ordering::SeqCst) != 0)
        {
            self.writing.store(false, Ordering::Release);
            return None;
        }
        Some(WriteGuard { lock: self, guard })
    }

    #[cfg(test)]
    fn is_unlocked(&self) -> bool {
        self.try_write().is_some()
    }
}

struct ReadGuard<'a, T> {
    lock: &'a ShardedRwLock<T>,
    shard: &'a Shard,
}

impl<T> Deref for ReadGuard<'_, T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &T {
        // SAFETY: Holding a non-zero shard reader count prevents any active `WriteGuard`.
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> Drop for ReadGuard<'_, T> {
    #[inline(always)]
    fn drop(&mut self) {
        self.shard.readers.fetch_sub(1, Ordering::Release);
    }
}

struct WriteGuard<'a, T> {
    lock: &'a ShardedRwLock<T>,
    #[expect(dead_code, reason = "holds the writer mutex until WriteGuard drops")]
    guard: MutexGuard<'a, ()>,
}

impl<T> Deref for WriteGuard<'_, T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &T {
        // SAFETY: Exclusive `writer_lock` with all shard reader counts drained to zero.
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> DerefMut for WriteGuard<'_, T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: Exclusive `writer_lock` with all shard reader counts drained to zero.
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T> Drop for WriteGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        self.lock.writing.store(false, Ordering::Release);
    }
}

struct State<T> {
    live_receivers: usize,
    tail_seg: Option<Arc<Segment<T>>>,
}

pub struct Subscription<T> {
    pub cursor: u64,
    seg: Option<Arc<Segment<T>>>,
    slot_ptr: NonNull<MsgSlot<T>>,
    wait_token: WakeToken,
}

impl<T> UnwindSafe for Subscription<T> {}
impl<T> RefUnwindSafe for Subscription<T> {}

#[repr(C)]
pub struct Shared<T> {
    live_receivers: AtomicUsize,
    inline_subs: AtomicUsize,
    senders: AtomicUsize,
    has_tail_seg: AtomicBool,
    pub wake: BroadcastWake,
    detached_slot: MsgSlot<T>,
    first_next: Mutex<Option<Arc<Segment<T>>>>,
    #[cfg(test)]
    live_slots: Arc<AtomicUsize>,
    marker: PhantomData<Arc<T>>,

    // Inline `[0, 8)` slots are cold once heap segments start, isolating cold metadata from
    // `tail`.
    first_slots: [MsgSlot<T>; INLINE_CAP],

    tail: CounterU64,
    pad_tail: MaybeUninit<[u8; 128 - size_of::<CounterU64>()]>,

    state: ShardedRwLock<State<T>>,
    pad_state: MaybeUninit<[u8; 64]>,

    reclaimed: CounterU64,
}

unsafe impl<T: Send + Sync> Send for Shared<T> {}
unsafe impl<T: Send + Sync> Sync for Shared<T> {}
impl<T> UnwindSafe for Shared<T> {}
impl<T> RefUnwindSafe for Shared<T> {}

impl<T> Shared<T> {
    pub fn channel() -> (Arc<Self>, Subscription<T>) {
        let shared = Arc::new(Self {
            live_receivers: AtomicUsize::new(1),
            inline_subs: AtomicUsize::new(1),
            senders: AtomicUsize::new(1),
            has_tail_seg: AtomicBool::new(false),
            wake: BroadcastWake::new(),
            detached_slot: MsgSlot::empty(),
            first_next: Mutex::new(None),
            #[cfg(test)]
            live_slots: Arc::new(AtomicUsize::new(0)),
            marker: PhantomData,
            first_slots: [const { MsgSlot::empty() }; INLINE_CAP],
            tail: CounterU64::new(0),
            pad_tail: MaybeUninit::uninit(),
            state: ShardedRwLock::new(State {
                live_receivers: 1,
                tail_seg: None,
            }),
            pad_state: MaybeUninit::uninit(),
            reclaimed: CounterU64::new(0),
        });
        let sub = Subscription {
            cursor: 0,
            seg: None,
            slot_ptr: shared.first_slot_ptr(0),
            wait_token: WakeToken::new(),
        };
        (shared, sub)
    }

    #[inline(always)]
    fn first_slot_ptr(&self, offset: usize) -> NonNull<MsgSlot<T>> {
        debug_assert!(offset < INLINE_CAP);
        // SAFETY: `self.first_slots.as_ptr()` carries provenance over all `INLINE_CAP` slots.
        unsafe { NonNull::new_unchecked(self.first_slots.as_ptr().add(offset).cast_mut()) }
    }

    #[inline(always)]
    fn is_detached(&self, sub: &Subscription<T>) -> bool {
        ptr::eq(sub.slot_ptr.as_ptr(), &self.detached_slot)
    }

    #[inline]
    fn alloc_segment(&self, base: u64) -> Arc<Segment<T>> {
        #[cfg(test)]
        self.live_slots.fetch_add(
            if base == 0 {
                SEG_CAP - INLINE_CAP
            } else {
                SEG_CAP
            },
            Ordering::AcqRel,
        );
        Arc::new(Segment {
            base,
            next: OnceLock::new(),
            slots: [const { MsgSlot::empty() }; SEG_CAP],
            #[cfg(test)]
            live_slots: self.live_slots.clone(),
        })
    }

    #[inline]
    fn first_heap_or_init(&self) -> Arc<Segment<T>> {
        self.first_next
            .lock()
            .get_or_insert_with(|| self.alloc_segment(0))
            .clone()
    }

    #[inline]
    pub fn clone_sender(&self) -> usize {
        self.senders.fetch_add(1, Ordering::Relaxed);
        self.state.assign_shard()
    }

    #[inline]
    pub fn drop_sender(&self) {
        if self.senders.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.wake.notify_if_waiting();
            if self.has_tail_seg.load(Ordering::SeqCst) {
                self.release_tail_on_disconnect();
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn release_tail_on_disconnect(&self) {
        let (old_tail, old_first) = {
            let mut state = self.state.write();
            self.has_tail_seg.store(false, Ordering::Release);
            let old_first = if self.inline_subs.load(Ordering::Acquire) == 0 {
                self.first_next.lock().take()
            } else {
                None
            };
            (state.tail_seg.take(), old_first)
        };
        drop((old_tail, old_first));
    }

    #[inline]
    pub fn is_disconnected(&self) -> bool {
        self.senders.load(Ordering::Acquire) == 0
    }

    #[inline]
    pub fn unread_from(&self, cursor: u64) -> usize {
        usize::try_from(self.tail.load(Ordering::Acquire).saturating_sub(cursor))
            .unwrap_or(usize::MAX)
    }

    pub fn retained_message_count(&self) -> usize {
        if self.live_receivers.load(Ordering::Acquire) == 0 {
            return 0;
        }
        let reclaimed = self.reclaimed.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        usize::try_from(tail.saturating_sub(reclaimed)).unwrap_or(usize::MAX)
    }

    #[inline(always)]
    pub fn send(&self, shard: usize, msg: T) {
        if self.live_receivers.load(Ordering::Relaxed) == 0 {
            self.send_zero_receivers_cold(shard, msg);
            return;
        }
        self.send_arc(shard, Arc::new(msg));
    }

    #[inline(always)]
    fn send_arc(&self, shard: usize, arc_msg: Arc<T>) {
        let state = self.state.read(shard);
        let live_rcvs = state.live_receivers;
        if live_rcvs == 0 {
            self.finish_discard(state, arc_msg);
            return;
        }
        let Some(version) = self.tail.claim_next() else {
            drop(state);
            Self::drop_and_panic_overflow(arc_msg);
        };

        if let Some(seg) = state.tail_seg.as_deref() {
            let offset = version - seg.base;
            if offset < SEG_CAP as u64 {
                let slot = &seg.slots[offset as usize];
                unsafe { (*slot.msg.get()).write(arc_msg) };
                slot.state.store(live_rcvs, Ordering::SeqCst);
                drop(state);
                self.wake.notify_if_waiting();
                return;
            }
            let target = self.publish_slow(Some(seg), version, live_rcvs, arc_msg);
            drop(state);
            self.wake.notify_if_waiting();
            self.advance_tail_seg(target);
            return;
        }

        if version < INLINE_CAP as u64 {
            let slot = &self.first_slots[version as usize];
            unsafe { (*slot.msg.get()).write(arc_msg) };
            slot.state.store(live_rcvs, Ordering::SeqCst);
            drop(state);
            self.wake.notify_if_waiting();
            return;
        }

        let target = self.publish_slow(None, version, live_rcvs, arc_msg);
        drop(state);
        self.wake.notify_if_waiting();
        self.advance_tail_seg(target);
    }

    #[inline(always)]
    fn claim_discarded<V>(&self, state: ReadGuard<'_, State<T>>, val: V) {
        let ok = self.tail.claim_next().is_some();
        if ok {
            self.reclaimed.fetch_add(1, Ordering::Relaxed);
        }
        drop(state);
        drop(val);
        assert!(ok, "broadcast channel version counter overflowed");
    }

    #[cold]
    #[inline(never)]
    fn send_zero_receivers_cold(&self, shard: usize, msg: T) {
        let state = self.state.read(shard);
        if state.live_receivers == 0 {
            self.claim_discarded(state, msg);
            return;
        }
        drop(state);
        self.send_arc(shard, Arc::new(msg));
    }

    #[cold]
    #[inline(never)]
    fn finish_discard(&self, state: ReadGuard<'_, State<T>>, arc_msg: Arc<T>) {
        self.claim_discarded(state, arc_msg);
    }

    #[cold]
    #[inline(never)]
    fn drop_and_panic_overflow<V>(value: V) -> ! {
        drop(value);
        panic!("broadcast channel version counter overflowed");
    }

    #[cold]
    #[inline(never)]
    fn publish_slow(
        &self,
        seg: Option<&Segment<T>>,
        version: u64,
        live_rcvs: usize,
        arc_msg: Arc<T>,
    ) -> Arc<Segment<T>> {
        let mut cur = match seg {
            Some(seg) => seg.next_or_init(self),
            None => self.first_heap_or_init(),
        };
        loop {
            let offset = version - cur.base;
            if offset < SEG_CAP as u64 {
                let slot = &cur.slots[offset as usize];
                unsafe { (*slot.msg.get()).write(arc_msg) };
                slot.state.store(live_rcvs, Ordering::SeqCst);
                return cur;
            }
            cur = cur.next_or_init(self);
        }
    }

    #[cold]
    #[inline(never)]
    fn advance_tail_seg(&self, target: Arc<Segment<T>>) {
        let (old_tail, old_first) = {
            let mut state = self.state.write();
            if state.live_receivers == 0
                || state
                    .tail_seg
                    .as_ref()
                    .is_some_and(|cur| target.base <= cur.base)
            {
                return;
            }
            let old_tail = state.tail_seg.replace(target);
            self.has_tail_seg.store(true, Ordering::Release);
            let old_first = if self.inline_subs.load(Ordering::Acquire) == 0 {
                self.first_next.lock().take()
            } else {
                None
            };
            (old_tail, old_first)
        };
        drop((old_tail, old_first));
    }

    pub fn subscribe(&self) -> Subscription<T> {
        let mut state = self.state.write();
        let start = self.tail.load(Ordering::Relaxed);
        if self.senders.load(Ordering::SeqCst) == 0 {
            return Subscription {
                cursor: start,
                seg: None,
                slot_ptr: NonNull::from(&self.detached_slot),
                wait_token: WakeToken::new(),
            };
        }

        let (seg, slot_ptr) = if start < INLINE_CAP as u64 {
            self.inline_subs.fetch_add(1, Ordering::AcqRel);
            (None, self.first_slot_ptr(start as usize))
        } else {
            let target_base = start & !(SEG_MASK as u64);
            let mut cur = state.tail_seg.clone().unwrap_or_else(|| {
                let fresh = if target_base == 0 {
                    if self.inline_subs.load(Ordering::Acquire) > 0 {
                        self.first_heap_or_init()
                    } else {
                        self.first_next
                            .lock()
                            .take()
                            .unwrap_or_else(|| self.alloc_segment(0))
                    }
                } else {
                    self.alloc_segment(target_base)
                };
                state.tail_seg = Some(fresh.clone());
                self.has_tail_seg.store(true, Ordering::Release);
                fresh
            });
            while cur.base < target_base {
                cur = cur.next_or_init(self);
            }
            let slot_ptr = Segment::slot_ptr(&cur, (start - target_base) as usize);
            (Some(cur), slot_ptr)
        };

        state.live_receivers += 1;
        self.live_receivers
            .store(state.live_receivers, Ordering::Release);
        drop(state);

        Subscription {
            cursor: start,
            seg,
            slot_ptr,
            wait_token: WakeToken::new(),
        }
    }

    pub fn cancel_recv(&self, sub: &mut Subscription<T>) {
        self.wake.clear_async(&mut sub.wait_token);
    }

    pub fn drop_receiver(&self, sub: &mut Subscription<T>) {
        self.wake.clear_async(&mut sub.wait_token);
        if self.is_detached(sub) {
            return;
        }
        // Fast path: disconnected and already caught up (e.g. `cancel_pending`, fanout drop).
        if self.is_disconnected()
            && self.tail.load(Ordering::Acquire) == sub.cursor
            && (sub.seg.is_some() || !self.has_tail_seg.load(Ordering::Acquire))
        {
            sub.slot_ptr = NonNull::from(&self.detached_slot);
            let seg = sub.seg.take();
            if seg.is_none() {
                self.inline_subs.fetch_sub(1, Ordering::AcqRel);
            }
            self.live_receivers.fetch_sub(1, Ordering::AcqRel);
            drop(seg);
            return;
        }

        sub.slot_ptr = NonNull::from(&self.detached_slot);
        let was_inline = sub.seg.is_none();
        let mut cur_seg = sub.seg.take();
        let (end, old_tail, old_first) = {
            let mut state = self.state.write();
            let end = self.tail.load(Ordering::Relaxed);
            state.live_receivers -= 1;
            self.live_receivers
                .store(state.live_receivers, Ordering::Release);
            let inline_after = if was_inline {
                self.inline_subs.fetch_sub(1, Ordering::AcqRel) - 1
            } else {
                self.inline_subs.load(Ordering::Acquire)
            };
            let mut old_tail = None;
            let mut old_first = None;
            if state.live_receivers == 0 {
                self.has_tail_seg.store(false, Ordering::Release);
                old_tail = state.tail_seg.take();
                if end >= INLINE_CAP as u64 {
                    let first = self.first_next.lock().take();
                    if was_inline && end > INLINE_CAP as u64 {
                        cur_seg = first;
                    } else {
                        old_first = first;
                    }
                }
            } else if was_inline {
                if end > INLINE_CAP as u64 {
                    cur_seg = self.first_next.lock().clone();
                }
                if inline_after == 0 && (state.tail_seg.is_some() || self.is_disconnected()) {
                    old_first = self.first_next.lock().take();
                }
            }
            (end, old_tail, old_first)
        };

        let mut reclaimed = Vec::new();
        if sub.cursor < end {
            let mut active_seg = if sub.cursor < INLINE_CAP as u64 {
                None
            } else {
                cur_seg.take()
            };
            while sub.cursor < end {
                let slot = if sub.cursor < INLINE_CAP as u64 {
                    &self.first_slots[sub.cursor as usize]
                } else {
                    let seg = active_seg.get_or_insert_with(|| {
                        cur_seg
                            .take()
                            .expect("backlog past inline segment requires allocated heap segment")
                    });
                    &seg.slots[(sub.cursor - seg.base) as usize]
                };
                let mut slot_state = slot.state.load(Ordering::Acquire);
                if slot_state == 0 {
                    slot_state = self.wait_for_inflight_slot(slot);
                }
                if let Some(msg) = unsafe { slot.reclaim_unread(slot_state) } {
                    reclaimed.push(msg);
                }
                sub.cursor += 1;
                if sub.cursor < end && (sub.cursor as usize & SEG_MASK) == 0 {
                    active_seg = active_seg.map(|s| s.next_or_init(self));
                }
            }
            drop(active_seg);
        }
        if !reclaimed.is_empty() {
            self.reclaimed
                .fetch_add(reclaimed.len() as u64, Ordering::Release);
        }

        // Release all segment memory before running payload destructors, which may unwind.
        drop((cur_seg, old_tail, old_first));
        drop(reclaimed);
    }

    #[inline(always)]
    pub fn try_recv(&self, sub: &mut Subscription<T>) -> Result<Received<T>, TryRecvError> {
        let cur_ver = sub.cursor;
        let slot = unsafe { sub.slot_ptr.as_ref() };
        let mut state = slot.state.load(Ordering::Acquire);
        if state == 0 {
            if self.tail.load(Ordering::Acquire) <= cur_ver && !self.is_disconnected() {
                return Err(TryRecvError::Empty);
            }
            state = self.try_recv_slow(sub, cur_ver, slot)?;
        }
        Ok(self.consume_ready_slot(sub, cur_ver, slot, state))
    }

    #[cold]
    #[inline(never)]
    fn try_recv_slow(
        &self,
        sub: &mut Subscription<T>,
        cur_ver: u64,
        slot: &MsgSlot<T>,
    ) -> Result<usize, TryRecvError> {
        if self.is_detached(sub) {
            return Err(TryRecvError::Disconnected);
        }
        if self.tail.load(Ordering::Acquire) <= cur_ver {
            if !self.is_disconnected() {
                return Err(TryRecvError::Empty);
            }
            if self.tail.load(Ordering::Acquire) <= cur_ver {
                self.detach_disconnected_sub(sub);
                return Err(TryRecvError::Disconnected);
            }
        }
        Ok(self.wait_for_inflight_slot(slot))
    }

    #[inline]
    pub fn poll_recv(
        &self,
        sub: &mut Subscription<T>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Received<T>, RecvError>> {
        loop {
            let cur_ver = sub.cursor;
            let slot = unsafe { sub.slot_ptr.as_ref() };
            let state = slot.state.load(Ordering::Acquire);
            if state != 0 {
                let received = self.consume_ready_slot(sub, cur_ver, slot, state);
                self.wake.clear_async(&mut sub.wait_token);
                return Poll::Ready(Ok(received));
            }
            if self.is_detached(sub) {
                return Poll::Ready(Err(RecvError::Disconnected));
            }
            if self.tail.load(Ordering::Acquire) <= cur_ver
                && self.is_disconnected()
                && self.tail.load(Ordering::Acquire) <= cur_ver
            {
                self.wake.clear_async(&mut sub.wait_token);
                self.detach_disconnected_sub(sub);
                return Poll::Ready(Err(RecvError::Disconnected));
            }
            if !self.wake.park_async(&mut sub.wait_token, cx, || {
                slot.state.load(Ordering::SeqCst) != 0
                    || (self.senders.load(Ordering::SeqCst) == 0
                        && self.tail.load(Ordering::SeqCst) <= cur_ver)
            }) {
                return Poll::Pending;
            }
        }
    }

    #[inline(always)]
    fn consume_ready_slot(
        &self,
        sub: &mut Subscription<T>,
        cur_ver: u64,
        slot: &MsgSlot<T>,
        state: usize,
    ) -> Received<T> {
        let (msg, reclaimed) = if state == 1 {
            let msg = unsafe { ManuallyDrop::new((*slot.msg.get()).assume_init_read()) };
            slot.state.store(0, Ordering::Relaxed);
            (msg, true)
        } else {
            let msg = unsafe { ManuallyDrop::new((*slot.msg.get()).assume_init_ref().clone()) };
            if slot.state.fetch_sub(1, Ordering::AcqRel) == 1 {
                unsafe { (*slot.msg.get()).assume_init_drop() };
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

        if (next_ver as usize & SEG_MASK) != 0 && next_ver != INLINE_CAP as u64 {
            sub.slot_ptr = unsafe { sub.slot_ptr.add(1) };
            if reclaimed && self.senders.load(Ordering::Relaxed) == 0 {
                self.maybe_detach_drained_disconnected(sub, next_ver);
            }
        } else {
            self.advance_receiver_seg(sub, next_ver);
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
        self.wake.park_sync(|| {
            let ready = slot.state.load(Ordering::SeqCst);
            (ready != 0).then_some(ready)
        })
    }

    #[cold]
    #[inline(never)]
    fn maybe_detach_drained_disconnected(&self, sub: &mut Subscription<T>, next_ver: u64) {
        if self.is_disconnected() && self.tail.load(Ordering::Acquire) == next_ver {
            self.detach_disconnected_sub(sub);
        }
    }

    #[cold]
    #[inline(never)]
    fn detach_disconnected_sub(&self, sub: &mut Subscription<T>) {
        if self.is_detached(sub) {
            return;
        }
        let prev_seg = sub.seg.take();
        sub.slot_ptr = NonNull::from(&self.detached_slot);
        self.live_receivers.fetch_sub(1, Ordering::AcqRel);
        let old_first =
            if prev_seg.is_none() && self.inline_subs.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.first_next.lock().take()
            } else {
                None
            };
        drop((prev_seg, old_first));
    }

    #[cold]
    #[inline(never)]
    fn advance_receiver_seg(&self, sub: &mut Subscription<T>, next_ver: u64) {
        if self.is_disconnected() && self.tail.load(Ordering::Acquire) == next_ver {
            self.detach_disconnected_sub(sub);
            return;
        }
        if let Some(prev) = &sub.seg {
            let next = prev.next_or_init(self);
            sub.slot_ptr = Segment::slot_ptr(&next, 0);
            sub.seg = Some(next);
            return;
        }

        // Transitioning out of inline `[0, 8)` onto the `base = 0` heap segment at offset 8.
        let next = self.first_heap_or_init();
        sub.slot_ptr = Segment::slot_ptr(&next, INLINE_CAP);
        sub.seg = Some(next);

        if self.inline_subs.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Never block or yield inside `poll_recv`; if a publisher holds a read guard, the next
            // `advance_tail_seg` or `drop_receiver` clears `first_next`.
            let old_first = if self.is_disconnected() {
                self.first_next.lock().take()
            } else if let Some(state) = self.state.try_write() {
                if self.inline_subs.load(Ordering::Acquire) == 0 && state.tail_seg.is_some() {
                    self.first_next.lock().take()
                } else {
                    None
                }
            } else {
                None
            };
            drop(old_first);
        }
    }

    #[cfg(test)]
    pub fn set_tail(&self, tail: u64) {
        let state = self.state.write();
        assert_eq!(state.live_receivers, 0);
        self.tail.store(tail, Ordering::SeqCst);
        self.reclaimed.store(tail, Ordering::Release);
    }

    #[cfg(test)]
    pub fn is_unlocked_for_test(&self) -> bool {
        self.state.is_unlocked()
    }

    #[cfg(test)]
    pub fn buffer_capacity(&self) -> usize {
        let (inline_active, old_first) = {
            let state = self.state.write();
            let inline = self.inline_subs.load(Ordering::Acquire);
            let old_first = if inline == 0 && (state.tail_seg.is_some() || self.is_disconnected()) {
                self.first_next.lock().take()
            } else {
                None
            };
            (
                inline > 0 && self.live_receivers.load(Ordering::Acquire) > 0,
                old_first,
            )
        };
        drop(old_first);
        usize::from(inline_active) * INLINE_CAP + self.live_slots.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub fn begin_inflight_publish_for_test(&self) -> (u64, usize) {
        let state = self.state.read(0);
        let live_rcvs = state.live_receivers;
        let version = self.tail.claim_next().expect("version overflow in test");
        (version, live_rcvs)
    }

    #[cfg(test)]
    pub fn finish_inflight_publish_for_test(&self, version: u64, live_rcvs: usize, msg: T) {
        let state = self.state.read(0);
        let slot = if version < INLINE_CAP as u64 {
            &self.first_slots[version as usize]
        } else {
            let seg = state
                .tail_seg
                .as_deref()
                .expect("tail segment must exist for heap version");
            &seg.slots[(version - seg.base) as usize]
        };
        unsafe { (*slot.msg.get()).write(Arc::new(msg)) };
        slot.state.store(live_rcvs, Ordering::SeqCst);
        drop(state);
        self.wake.notify_if_waiting();
    }
}
