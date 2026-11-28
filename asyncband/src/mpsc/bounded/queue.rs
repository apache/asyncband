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
use std::marker::PhantomData;
use std::mem;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::panic::RefUnwindSafe;
use std::panic::UnwindSafe;
use std::process;
use std::ptr;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicU8;
#[cfg(target_has_atomic = "64")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use crate::internal::grant_waiter::GrantWaiter;
use crate::internal::mutex::Mutex;
use crate::internal::register_waker;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::mpsc::RecvError;
use crate::mpsc::SendError;
use crate::mpsc::TryRecvError;
use crate::mpsc::TrySendError;

const CACHE_LINE: usize = 128;
const CLOSED_BIT: u64 = 1 << 63;
const RX_WAITING_BIT: u64 = 1 << 62;
const TICKET_MASK: u64 = (1 << 62) - 1;
const ACQ_CLOSED_BIT: u64 = 1 << 63;
const ACQ_WAITERS_BIT: u64 = 1 << 62;
const ACQ_COUNT_MASK: u64 = (1 << 62) - 1;
const RX_WAITERS_BIT: u64 = 1 << 63;
const RX_REL_MASK: u64 = (1 << 63) - 1;
const SLOT_EMPTY: u8 = 0;
const SLOT_READY: u8 = 1;
const SLOT_RX_WAITING: u8 = 2;

#[repr(align(128))]
struct CachePadded<T>(T);

impl<T> Deref for CachePadded<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &T {
        &self.0
    }
}

#[cfg(target_has_atomic = "64")]
struct Atomic64(AtomicU64);

#[cfg(target_has_atomic = "64")]
impl Atomic64 {
    const fn new(val: u64) -> Self {
        Self(AtomicU64::new(val))
    }

    #[inline(always)]
    fn load(&self, ordering: Ordering) -> u64 {
        self.0.load(ordering)
    }

    #[inline(always)]
    fn fetch_add(&self, delta: u64, ordering: Ordering) -> u64 {
        self.0.fetch_add(delta, ordering)
    }

    #[inline(always)]
    fn fetch_sub(&self, delta: u64, ordering: Ordering) -> u64 {
        self.0.fetch_sub(delta, ordering)
    }

    #[inline]
    fn fetch_or(&self, bits: u64, ordering: Ordering) -> u64 {
        self.0.fetch_or(bits, ordering)
    }

    #[inline]
    fn fetch_and(&self, mask: u64, ordering: Ordering) -> u64 {
        self.0.fetch_and(mask, ordering)
    }

    #[inline]
    fn fetch_max(&self, val: u64, ordering: Ordering) -> u64 {
        self.0.fetch_max(val, ordering)
    }

    #[inline]
    fn compare_exchange(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.0.compare_exchange(current, new, success, failure)
    }

    #[inline]
    fn compare_exchange_weak(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.0.compare_exchange_weak(current, new, success, failure)
    }
}

#[cfg(not(target_has_atomic = "64"))]
struct Atomic64(Mutex<u64>);

#[cfg(not(target_has_atomic = "64"))]
impl Atomic64 {
    const fn new(val: u64) -> Self {
        Self(Mutex::new(val))
    }

    #[inline]
    fn update(&self, f: impl FnOnce(u64) -> u64) -> u64 {
        let mut guard = self.0.lock();
        let prev = *guard;
        *guard = f(prev);
        prev
    }

    #[inline]
    fn load(&self, _ordering: Ordering) -> u64 {
        *self.0.lock()
    }

    #[inline]
    fn fetch_add(&self, delta: u64, _ordering: Ordering) -> u64 {
        self.update(|prev| prev.wrapping_add(delta))
    }

    #[inline]
    fn fetch_sub(&self, delta: u64, _ordering: Ordering) -> u64 {
        self.update(|prev| prev.wrapping_sub(delta))
    }

    #[inline]
    fn fetch_or(&self, bits: u64, _ordering: Ordering) -> u64 {
        self.update(|prev| prev | bits)
    }

    #[inline]
    fn fetch_and(&self, mask: u64, _ordering: Ordering) -> u64 {
        self.update(|prev| prev & mask)
    }

    #[inline]
    fn fetch_max(&self, val: u64, _ordering: Ordering) -> u64 {
        self.update(|prev| prev.max(val))
    }

    #[inline]
    fn compare_exchange(
        &self,
        current: u64,
        new: u64,
        _success: Ordering,
        _failure: Ordering,
    ) -> Result<u64, u64> {
        let mut guard = self.0.lock();
        let actual = *guard;
        if actual == current {
            *guard = new;
            Ok(actual)
        } else {
            Err(actual)
        }
    }

    #[inline]
    fn compare_exchange_weak(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.compare_exchange(current, new, success, failure)
    }
}

struct ProducerState {
    acquired: Atomic64,
    cached_rx_rel: Atomic64,
    tail: Atomic64,
}

struct SlotLayout {
    align: usize,
    value_offset: usize,
    stride: usize,
}

// Keep `state: AtomicU8` at byte offset 0 of every slot while isolating payloads `>= 64 B` onto
// dedicated 128B cache-line multiples without wasting an extra 128B line before `T`.
const fn compute_slot_layout<T>() -> SlotLayout {
    if size_of::<T>() == 0 {
        return SlotLayout {
            align: 1,
            value_offset: 0,
            stride: 0,
        };
    }
    let raw = align_of::<T>();
    let (align, value_offset) = if size_of::<T>() >= 64 {
        (
            if raw < CACHE_LINE { CACHE_LINE } else { raw },
            if raw < 16 { 16 } else { raw },
        )
    } else {
        (raw, raw)
    };
    let Some(end) = value_offset.checked_add(size_of::<T>()) else {
        panic!("mpsc bounded channel slot size exceeds allocation size limit");
    };
    let Some(stride) = end.checked_next_multiple_of(align) else {
        panic!("mpsc bounded channel slot size exceeds allocation size limit");
    };
    SlotLayout {
        align,
        value_offset,
        stride,
    }
}

#[repr(C)]
struct SlotRing<T> {
    slots_ptr: AtomicPtr<u8>,
    raw_ptr: AtomicPtr<u8>,
    capacity: NonZeroUsize,
    index_mask: usize,
    marker: PhantomData<UnsafeCell<T>>,
}

impl<T> Drop for SlotRing<T> {
    fn drop(&mut self) {
        self.dealloc_slots();
    }
}

impl<T> SlotRing<T> {
    const LAYOUT: SlotLayout = compute_slot_layout::<T>();

    #[track_caller]
    fn alloc_layout(buffer: usize) -> alloc::Layout {
        // Request `align_of::<T>()` so Miri proves `T` alignment while `System::alloc_zeroed`
        // preserves OS zero-page `calloc` instead of faulting every page on over-aligned layouts.
        let alloc_align = align_of::<T>();
        let Some(alloc_bytes) = Self::LAYOUT
            .stride
            .checked_mul(buffer)
            .and_then(|bytes| bytes.checked_add(Self::LAYOUT.align - alloc_align))
        else {
            panic!("mpsc bounded channel capacity exceeds allocation size limit");
        };
        let Ok(layout) = alloc::Layout::from_size_align(alloc_bytes, alloc_align) else {
            panic!("mpsc bounded channel capacity exceeds allocation size limit");
        };
        layout
    }

    #[track_caller]
    fn new(capacity: NonZeroUsize) -> Self {
        let buffer = capacity.get();
        let (slots_ptr, raw_ptr) = if size_of::<T>() == 0 {
            (ptr::null_mut(), ptr::null_mut())
        } else {
            let layout = Self::alloc_layout(buffer);
            // SAFETY: `buffer > 0` and `size_of::<T>() > 0`, so `layout.size() > 0`.
            let raw = unsafe { alloc::alloc_zeroed(layout) };
            if raw.is_null() {
                alloc::handle_alloc_error(layout);
            }
            let offset = raw.addr().wrapping_neg() & (Self::LAYOUT.align - 1);
            // SAFETY: `offset < Self::LAYOUT.align`, keeping all slots within `raw`'s allocation.
            (unsafe { raw.add(offset) }, raw)
        };
        Self {
            slots_ptr: AtomicPtr::new(slots_ptr),
            raw_ptr: AtomicPtr::new(raw_ptr),
            capacity,
            index_mask: if buffer.is_power_of_two() {
                buffer - 1
            } else {
                usize::MAX
            },
            marker: PhantomData,
        }
    }

    fn dealloc_slots(&self) {
        if size_of::<T>() == 0 {
            return;
        }
        let raw = self.raw_ptr.swap(ptr::null_mut(), Ordering::AcqRel);
        if !raw.is_null() {
            self.slots_ptr.store(ptr::null_mut(), Ordering::Release);
            let layout = Self::alloc_layout(self.capacity.get());
            // SAFETY: `raw` was allocated with `layout` in `SlotRing::new`, drained before free,
            // and swapped to null so it is freed at most once.
            unsafe { alloc::dealloc(raw, layout) };
        }
    }

    #[inline(always)]
    fn slot_base(&self, ticket: u64) -> *mut u8 {
        let idx = if self.index_mask != usize::MAX {
            (ticket as usize) & self.index_mask
        } else {
            (ticket % (self.capacity.get() as u64)) as usize
        };
        let ptr = self.slots_ptr.load(Ordering::Relaxed);
        debug_assert!(!ptr.is_null() && idx < self.capacity.get());
        // SAFETY: Called only while the receiver is alive or draining pre-close tickets.
        unsafe { ptr.add(idx * Self::LAYOUT.stride) }
    }

    #[inline(always)]
    unsafe fn slot_state_at<'a>(base: *mut u8) -> &'a AtomicU8 {
        // SAFETY: Offset 0 of every slot is zero-initialized and valid for atomic byte access.
        unsafe { AtomicU8::from_ptr(base) }
    }

    #[inline(always)]
    unsafe fn slot_value_at(base: *mut u8) -> *mut T {
        // SAFETY: `value_offset` is aligned to a multiple of `align_of::<T>()` within the slot.
        unsafe { base.add(Self::LAYOUT.value_offset).cast::<T>() }
    }
}

struct SlotPark {
    notified: Mutex<bool>,
    condvar: Condvar,
}

impl SlotPark {
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
}

impl Wake for SlotPark {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        *self.notified.lock() = true;
        self.condvar.notify_one();
    }
}

struct EndpointState {
    senders: AtomicUsize,
    rx_alive: AtomicBool,
    control: Mutex<Control>,
}

struct Control {
    rx_alive: bool,
    recv_waker: Option<Waker>,
    recv_slot_ticket: Option<u64>,
    send_waiters: WaitList<GrantWaiter>,
}

struct DrainGuard<'a, T> {
    shared: &'a Shared<T>,
    cursor: u64,
    end: u64,
}

impl<T> DrainGuard<'_, T> {
    fn drain(&mut self) {
        if !mem::needs_drop::<T>() && (size_of::<T>() == 0 || !self.shared.has_senders()) {
            self.cursor = self.end;
        }
        while self.cursor < self.end {
            let ticket = self.cursor;
            self.cursor += 1;
            if size_of::<T>() == 0 {
                // SAFETY: `T` is zero-sized and one published instance was forgotten per ticket.
                unsafe { NonNull::<T>::dangling().as_ptr().drop_in_place() };
            } else {
                let base = self.shared.ring.slot_base(ticket);
                self.shared.wait_for_slot_ready(ticket, base);
                // SAFETY: `wait_for_slot_ready` observed `SLOT_READY` with `Acquire` on `base`,
                // whose ticket `< end` was claimed before `CLOSED_BIT` was set.
                unsafe { SlotRing::<T>::slot_value_at(base).drop_in_place() };
            }
        }
        self.shared.ring.dealloc_slots();
    }
}

impl<T> Drop for DrainGuard<'_, T> {
    fn drop(&mut self) {
        self.drain();
    }
}

// `#[repr(C)]` keeps immutable ring geometry on the leading cache line, separate from the
// producer-hot permit/tail line, receiver-release line, and cold endpoint control line.
#[repr(C)]
pub struct Shared<T> {
    ring: SlotRing<T>,
    producer: CachePadded<ProducerState>,
    rx_state: CachePadded<Atomic64>,
    endpoint: CachePadded<EndpointState>,
}

// SAFETY: `Shared<T>` transfers ownership of `T` values from producers to the single receiver with
// Acquire/Release synchronization on per-slot readiness and never exposes shared `&T` references.
unsafe impl<T: Send> Send for Shared<T> {}
unsafe impl<T: Send> Sync for Shared<T> {}
impl<T> UnwindSafe for Shared<T> {}
impl<T> RefUnwindSafe for Shared<T> {}

impl<T> Shared<T> {
    #[track_caller]
    pub fn new(capacity: NonZeroUsize) -> Arc<Self> {
        Arc::new(Self {
            ring: SlotRing::new(capacity),
            producer: CachePadded(ProducerState {
                acquired: Atomic64::new(0),
                cached_rx_rel: Atomic64::new(0),
                tail: Atomic64::new(0),
            }),
            rx_state: CachePadded(Atomic64::new(0)),
            endpoint: CachePadded(EndpointState {
                senders: AtomicUsize::new(1),
                rx_alive: AtomicBool::new(true),
                control: Mutex::new(Control {
                    rx_alive: true,
                    recv_waker: None,
                    recv_slot_ticket: None,
                    send_waiters: WaitList::new(),
                }),
            }),
        })
    }

    #[inline]
    pub fn clone_sender(&self) {
        // `BoundedSender::clone` clones `Arc<Shared<T>>` first, which aborts before overflow.
        self.endpoint.senders.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn drop_sender(&self) {
        let prev = self.endpoint.senders.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "a live sender must own one sender count");
        if prev == 1 {
            self.notify_last_sender_dropped();
        }
    }

    #[cold]
    fn notify_last_sender_dropped(&self) {
        if let Some(waker) = self.take_recv_waker() {
            waker.wake();
        }
    }

    #[cold]
    fn take_recv_waker(&self) -> Option<Waker> {
        let mut control = self.endpoint.control.lock();
        if size_of::<T>() == 0 {
            self.producer
                .tail
                .fetch_and(!RX_WAITING_BIT, Ordering::Release);
        }
        control.recv_slot_ticket = None;
        control.recv_waker.take()
    }

    pub fn drop_receiver(&self, head: u64) {
        let (final_tail, recv_waker, mut waiters) = {
            let mut control = self.endpoint.control.lock();
            control.rx_alive = false;
            // Seal `acquired` and `tail` under `control` before storing `rx_alive = false` so no
            // concurrent send on a non-full channel can report `Full` during teardown, and any
            // caller observing disconnection immediately observes `has_receiver() == false`.
            let p = &*self.producer;
            p.acquired.fetch_or(ACQ_CLOSED_BIT, Ordering::SeqCst);
            p.acquired.fetch_and(!ACQ_WAITERS_BIT, Ordering::SeqCst);
            self.rx_state.fetch_and(!RX_WAITERS_BIT, Ordering::SeqCst);
            let final_tail = p.tail.fetch_or(CLOSED_BIT, Ordering::AcqRel) & TICKET_MASK;
            if size_of::<T>() == 0 {
                p.tail.fetch_and(!RX_WAITING_BIT, Ordering::Release);
            }
            self.endpoint.rx_alive.store(false, Ordering::Release);
            control.recv_slot_ticket = None;
            let recv_waker = control.recv_waker.take();
            let waiters = mem::replace(&mut control.send_waiters, WaitList::new());
            (final_tail, recv_waker, waiters)
        };

        // Notify blocked senders before destroying buffered values, whose destructors may panic.
        while let Some(waker) = waiters.take_waiting_waker() {
            waker.wake();
        }
        drop((waiters, recv_waker));

        let mut guard = DrainGuard {
            shared: self,
            cursor: head,
            end: final_tail,
        };
        guard.drain();
    }

    #[inline]
    pub fn has_receiver(&self) -> bool {
        self.endpoint.rx_alive.load(Ordering::Acquire)
    }

    #[inline]
    pub fn has_senders(&self) -> bool {
        self.endpoint.senders.load(Ordering::Acquire) != 0
    }

    #[inline]
    pub fn is_empty_at(&self, head: u64) -> bool {
        (self.producer.tail.load(Ordering::Acquire) & TICKET_MASK) == head
    }

    #[inline]
    fn sync_waiting_flag_locked(&self, control: &Control) {
        if control.send_waiters.is_empty() {
            self.rx_state.fetch_and(!RX_WAITERS_BIT, Ordering::Release);
            self.producer
                .acquired
                .fetch_and(!ACQ_WAITERS_BIT, Ordering::Release);
        }
    }

    #[inline]
    pub fn try_acquire_capacity(&self) -> Result<(), TrySendError<()>> {
        let cap = self.ring.capacity.get() as u64;
        let cached_rx = self.producer.cached_rx_rel.load(Ordering::Acquire);
        let mut raw_acq = self.producer.acquired.load(Ordering::Relaxed);
        loop {
            if raw_acq & ACQ_CLOSED_BIT != 0 {
                self.on_closed_tail();
                return Err(TrySendError::Disconnected(()));
            }
            let acq = raw_acq & ACQ_COUNT_MASK;
            if acq.saturating_sub(cached_rx) >= cap {
                return self.try_acquire_slow::<false>(cap, cached_rx, raw_acq);
            }
            if acq >= ACQ_COUNT_MASK - 1 {
                process::abort();
            }
            // Slot-reuse happens-before edges come from `cached_rx_rel.load(Acquire)` and the
            // subsequent `tail.fetch_add(1, AcqRel)`, so advancing `acquired` needs `Relaxed`.
            match self.producer.acquired.compare_exchange_weak(
                raw_acq,
                raw_acq + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => raw_acq = actual,
            }
        }
    }

    #[cold]
    fn try_acquire_slow<const SEQCST: bool>(
        &self,
        cap: u64,
        mut cached_rx: u64,
        mut raw_acq: u64,
    ) -> Result<(), TrySendError<()>> {
        let load_ord = if SEQCST {
            Ordering::SeqCst
        } else {
            Ordering::Acquire
        };
        let cas_succ = if SEQCST {
            Ordering::SeqCst
        } else {
            Ordering::Relaxed
        };
        loop {
            let true_rx = self.rx_state.load(load_ord) & RX_REL_MASK;
            if !SEQCST && true_rx != cached_rx {
                self.producer
                    .cached_rx_rel
                    .fetch_max(true_rx, Ordering::Release);
            }
            cached_rx = true_rx;
            let fresh_acq = self.producer.acquired.load(load_ord);
            if fresh_acq & ACQ_CLOSED_BIT != 0 {
                if !SEQCST {
                    self.on_closed_tail();
                }
                return Err(TrySendError::Disconnected(()));
            }
            let acq = fresh_acq & ACQ_COUNT_MASK;
            if acq.saturating_sub(cached_rx) < cap {
                if acq >= ACQ_COUNT_MASK - 1 {
                    process::abort();
                }
                match self.producer.acquired.compare_exchange(
                    fresh_acq,
                    fresh_acq + 1,
                    cas_succ,
                    load_ord,
                ) {
                    Ok(_) => return Ok(()),
                    Err(actual) => raw_acq = actual,
                }
            } else if !SEQCST && fresh_acq != raw_acq {
                raw_acq = fresh_acq;
            } else {
                let verify_rx = self.rx_state.load(load_ord) & RX_REL_MASK;
                if verify_rx == cached_rx {
                    return Err(TrySendError::Full(()));
                }
                if !SEQCST {
                    self.producer
                        .cached_rx_rel
                        .fetch_max(verify_rx, Ordering::Release);
                }
                cached_rx = verify_rx;
            }
        }
    }

    pub fn poll_acquire_capacity_slow(
        &self,
        waiter_slot: &mut Option<WaiterId>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), SendError<()>>> {
        let mut control = self.endpoint.control.lock();
        if !control.rx_alive {
            *waiter_slot = None;
            return Poll::Ready(Err(SendError::new(())));
        }

        if let Some(id) = *waiter_slot {
            if matches!(control.send_waiters.waiter_mut(id), GrantWaiter::Granted) {
                control.send_waiters.remove_unlinked_waiter(id);
                *waiter_slot = None;
                return Poll::Ready(Ok(()));
            }
        } else if control.send_waiters.is_empty() {
            self.rx_state.fetch_or(RX_WAITERS_BIT, Ordering::SeqCst);
            self.producer
                .acquired
                .fetch_or(ACQ_WAITERS_BIT, Ordering::SeqCst);
            if self
                .try_acquire_slow::<true>(self.ring.capacity.get() as u64, 0, 0)
                .is_ok()
            {
                self.sync_waiting_flag_locked(&control);
                return Poll::Ready(Ok(()));
            }
        }

        let old = control
            .send_waiters
            .register_waiter(waiter_slot, cx.waker());
        drop(control);
        drop(old);
        Poll::Pending
    }

    #[cold]
    pub fn cancel_waiter_id(&self, id: WaiterId) {
        let (waiter, wake) = {
            let mut control = self.endpoint.control.lock();
            if !control.rx_alive {
                return;
            }
            let waiter = control.send_waiters.remove(id);
            let wake = match &waiter {
                GrantWaiter::Waiting(_) => {
                    self.sync_waiting_flag_locked(&control);
                    None
                }
                GrantWaiter::Granted => self.release_locked(&mut control, || {
                    self.producer.acquired.fetch_sub(1, Ordering::AcqRel);
                }),
            };
            (waiter, wake)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        drop(waiter);
    }

    fn release_locked(&self, control: &mut Control, on_no_waiter: impl FnOnce()) -> Option<Waker> {
        if !control.rx_alive {
            return None;
        }
        let wake = control.send_waiters.grant_one();
        self.sync_waiting_flag_locked(control);
        if wake.is_none() {
            on_no_waiter();
        }
        wake
    }

    #[cold]
    fn release_slow(&self, on_no_waiter: impl FnOnce()) {
        let wake = self.release_locked(&mut self.endpoint.control.lock(), on_no_waiter);
        if let Some(waker) = wake {
            waker.wake();
        }
    }

    #[inline]
    fn release_from_receiver(&self) {
        let mut cur = self.rx_state.load(Ordering::Relaxed);
        loop {
            if (cur & RX_WAITERS_BIT) != 0 {
                self.release_slow(|| {
                    if (self.rx_state.fetch_add(1, Ordering::Release) & RX_REL_MASK)
                        >= RX_REL_MASK - 1
                    {
                        process::abort();
                    }
                });
                return;
            }
            if (cur & RX_REL_MASK) >= RX_REL_MASK - 1 {
                process::abort();
            }
            match self.rx_state.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }

    #[inline]
    pub fn release_capacity(&self) {
        let mut cur = self.producer.acquired.load(Ordering::Relaxed);
        loop {
            if cur & ACQ_CLOSED_BIT != 0 {
                return;
            }
            if cur & ACQ_WAITERS_BIT != 0 {
                self.release_slow(|| {
                    self.producer.acquired.fetch_sub(1, Ordering::AcqRel);
                });
                return;
            }
            debug_assert!((cur & ACQ_COUNT_MASK) > 0);
            match self.producer.acquired.compare_exchange_weak(
                cur,
                cur - 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Copies `*src` into a pre-claimed capacity slot, taking logical ownership of `*src` when
    /// returning `true`. Returns `false` without reading `*src` if the receiver has disconnected.
    #[inline]
    pub unsafe fn publish_from_ptr(&self, src: *const T) -> bool {
        // `AcqRel` carries slot-reuse happens-before edges across out-of-order `Permit` claims.
        let raw = self.producer.tail.fetch_add(1, Ordering::AcqRel);
        if (raw & TICKET_MASK) >= TICKET_MASK - 1 {
            process::abort();
        }
        if raw & CLOSED_BIT != 0 {
            self.on_closed_tail();
            return false;
        }

        let ticket = raw & TICKET_MASK;
        if size_of::<T>() > 0 {
            let base = self.ring.slot_base(ticket);
            // SAFETY: Capacity bounds in-flight and buffered slots to `capacity`, and the previous
            // occupant reset `slot_state` to `SLOT_EMPTY` before releasing capacity. `Release`
            // publishes `*src` to the receiver; `wake_receiver_for_slot` acquires `control`.
            let prev_slot = unsafe {
                ptr::copy_nonoverlapping(src, SlotRing::<T>::slot_value_at(base), 1);
                SlotRing::<T>::slot_state_at(base).swap(SLOT_READY, Ordering::Release)
            };
            if prev_slot == SLOT_RX_WAITING {
                self.wake_receiver_for_slot(ticket);
            }
            return true;
        }

        if raw & RX_WAITING_BIT != 0 {
            self.wake_receiver_on_tail();
        }
        true
    }

    #[cold]
    fn on_closed_tail(&self) {
        // Synchronize with `drop_receiver` so callers immediately see `has_receiver() == false`.
        drop(self.endpoint.control.lock());
    }

    #[cold]
    fn wake_receiver_for_slot(&self, ticket: u64) {
        let waker = {
            let mut control = self.endpoint.control.lock();
            if control.recv_slot_ticket == Some(ticket) {
                control.recv_slot_ticket = None;
                control.recv_waker.take()
            } else {
                None
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    // Tail-based wake registration is used only for zero-sized channels without slot storage.
    #[cold]
    fn wake_receiver_on_tail(&self) {
        if self.producer.tail.load(Ordering::Acquire) & RX_WAITING_BIT == 0 {
            return;
        }
        let waker = {
            let mut control = self.endpoint.control.lock();
            self.producer
                .tail
                .fetch_and(!RX_WAITING_BIT, Ordering::Release);
            control
                .recv_slot_ticket
                .is_none()
                .then(|| control.recv_waker.take())
                .flatten()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Registers `waker` on `ticket`'s slot (empty or in-flight), returning `Poll::Ready(Ok(()))`
    /// if already ready or `Poll::Ready(Err(RecvError::Disconnected))` when `check_disconnect` is
    /// enabled and no sender remains to fill `ticket`.
    #[cold]
    fn arm_slot_wait(
        &self,
        ticket: u64,
        state: &AtomicU8,
        waker: &Waker,
        check_disconnect: bool,
    ) -> Poll<Result<(), RecvError>> {
        if state.load(Ordering::Acquire) == SLOT_READY {
            return Poll::Ready(Ok(()));
        }

        let mut control = self.endpoint.control.lock();
        control.recv_slot_ticket = Some(ticket);
        let old_waker = register_waker(&mut control.recv_waker, waker);
        let (outcome, cleared) = match state.compare_exchange(
            SLOT_EMPTY,
            SLOT_RX_WAITING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(SLOT_RX_WAITING)
                if check_disconnect && !self.has_senders() && self.is_empty_at(ticket) =>
            {
                control.recv_slot_ticket = None;
                (
                    Poll::Ready(Err(RecvError::Disconnected)),
                    control.recv_waker.take(),
                )
            }
            Ok(_) | Err(SLOT_RX_WAITING) => (Poll::Pending, None),
            Err(SLOT_READY) => {
                control.recv_slot_ticket = None;
                (Poll::Ready(Ok(())), control.recv_waker.take())
            }
            Err(_) => unreachable!("invalid bounded mpsc slot state"),
        };
        drop(control);
        drop((old_waker, cleared));
        outcome
    }

    #[inline]
    pub fn claim_read_slot(&self, head: u64) -> Result<*mut u8, TryRecvError> {
        if size_of::<T>() == 0 {
            self.ensure_non_empty(head)?;
            return Ok(NonNull::<u8>::dangling().as_ptr());
        }

        let base = self.ring.slot_base(head);
        // SAFETY: `base` points to a valid allocated slot in `0..self.ring.capacity`.
        if unsafe { SlotRing::<T>::slot_state_at(base) }.load(Ordering::Acquire) != SLOT_READY {
            self.claim_read_slot_slow(head, base)?;
        }
        Ok(base)
    }

    #[inline]
    pub fn poll_claim_read_slot(
        &self,
        head: u64,
        cx: &mut Context<'_>,
    ) -> Poll<Result<*mut u8, RecvError>> {
        if size_of::<T>() == 0 {
            return self.poll_claim_zst_slot(head, cx);
        }

        let base = self.ring.slot_base(head);
        // SAFETY: `base` points to a valid allocated slot in `0..self.ring.capacity`.
        if unsafe { SlotRing::<T>::slot_state_at(base) }.load(Ordering::Acquire) == SLOT_READY {
            return Poll::Ready(Ok(base));
        }
        self.poll_claim_read_slot_slow(head, base, cx)
    }

    #[cold]
    fn poll_claim_zst_slot(
        &self,
        head: u64,
        cx: &mut Context<'_>,
    ) -> Poll<Result<*mut u8, RecvError>> {
        loop {
            match self.ensure_non_empty(head) {
                Ok(()) => return Poll::Ready(Ok(NonNull::<u8>::dangling().as_ptr())),
                Err(TryRecvError::Disconnected) => {
                    if self.producer.tail.load(Ordering::Relaxed) & RX_WAITING_BIT != 0 {
                        drop(self.take_recv_waker());
                    }
                    return Poll::Ready(Err(RecvError::Disconnected));
                }
                Err(TryRecvError::Empty) => {
                    let mut control = self.endpoint.control.lock();
                    control.recv_slot_ticket = None;
                    let old_waker = register_waker(&mut control.recv_waker, cx.waker());
                    let prev_tail = self
                        .producer
                        .tail
                        .fetch_or(RX_WAITING_BIT, Ordering::AcqRel)
                        & TICKET_MASK;
                    let (retry, cleared) = if prev_tail != head || !self.has_senders() {
                        self.producer
                            .tail
                            .fetch_and(!RX_WAITING_BIT, Ordering::Release);
                        (true, control.recv_waker.take())
                    } else {
                        (false, None)
                    };
                    drop(control);
                    drop((old_waker, cleared));
                    if !retry {
                        return Poll::Pending;
                    }
                }
            }
        }
    }

    #[cold]
    fn poll_claim_read_slot_slow(
        &self,
        head: u64,
        base: *mut u8,
        cx: &mut Context<'_>,
    ) -> Poll<Result<*mut u8, RecvError>> {
        // SAFETY: `base` points to a valid allocated slot in `0..self.ring.capacity`.
        let state = unsafe { SlotRing::<T>::slot_state_at(base) };
        self.arm_slot_wait(head, state, cx.waker(), true)
            .map_ok(|()| base)
    }

    #[inline]
    fn ensure_non_empty(&self, head: u64) -> Result<(), TryRecvError> {
        if !self.is_empty_at(head) {
            return Ok(());
        }
        if self.has_senders() {
            return Err(TryRecvError::Empty);
        }
        if self.is_empty_at(head) {
            Err(TryRecvError::Disconnected)
        } else {
            Ok(())
        }
    }

    #[cold]
    fn wait_for_slot_ready(&self, ticket: u64, base: *mut u8) {
        // SAFETY: `base` points to a valid allocated slot in `0..self.ring.capacity`.
        let state = unsafe { SlotRing::<T>::slot_state_at(base) };
        if state.load(Ordering::Acquire) == SLOT_READY {
            return;
        }
        let parker = Arc::new(SlotPark {
            notified: Mutex::new(false),
            condvar: Condvar::new(),
        });
        let waker = Waker::from(parker.clone());
        while self
            .arm_slot_wait(ticket, state, &waker, false)
            .is_pending()
        {
            parker.wait();
            if state.load(Ordering::Acquire) == SLOT_READY {
                return;
            }
        }
    }

    #[cold]
    fn claim_read_slot_slow(&self, head: u64, base: *mut u8) -> Result<(), TryRecvError> {
        self.ensure_non_empty(head)?;
        self.wait_for_slot_ready(head, base);
        Ok(())
    }

    #[inline(always)]
    pub unsafe fn read_claimed_slot_with<R>(&self, base: *mut u8, map: impl FnOnce(T) -> R) -> R {
        if size_of::<T>() == 0 {
            self.release_from_receiver();
            // SAFETY: `T` is zero-sized and one published instance was forgotten in `publish`.
            return map(unsafe { NonNull::<T>::dangling().as_ptr().read() });
        }
        // SAFETY: Caller observed `SLOT_READY` with `Acquire` and holds exclusive `&mut` access.
        let out = map(unsafe { SlotRing::<T>::slot_value_at(base).read() });
        // `release_from_receiver()` publishes this reset before any producer can reuse `base`.
        unsafe { SlotRing::<T>::slot_state_at(base).store(SLOT_EMPTY, Ordering::Relaxed) };
        self.release_from_receiver();
        out
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::pin::pin;
    use std::sync::mpsc;
    use std::thread;
    #[cfg(not(miri))]
    use std::time::Duration;

    use super::*;
    use crate::mpsc::BoundedReceiver;
    use crate::mpsc::BoundedSender;

    struct CountWake(AtomicUsize);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn try_recv_parks_without_consuming_caller_thread_token() {
        let shared = Shared::<u64>::new(NonZeroUsize::new(4).unwrap());
        let _tx = BoundedSender::new(shared.clone());
        let mut rx = BoundedReceiver::new(shared.clone());

        shared.try_acquire_capacity().unwrap();
        let raw = shared.producer.0.tail.fetch_add(1, Ordering::AcqRel);
        assert_eq!(raw & TICKET_MASK, 0);

        let base = shared.ring.slot_base(0);
        let shared_producer = shared.clone();
        let publisher = thread::spawn(move || {
            let state =
                unsafe { SlotRing::<u64>::slot_state_at(shared_producer.ring.slot_base(0)) };
            while state.load(Ordering::Acquire) != SLOT_RX_WAITING {
                thread::yield_now();
            }
            let base = shared_producer.ring.slot_base(0);
            let prev = unsafe {
                SlotRing::<u64>::slot_value_at(base).write(77);
                SlotRing::<u64>::slot_state_at(base).swap(SLOT_READY, Ordering::Release)
            };
            assert_eq!(prev, SLOT_RX_WAITING);
            shared_producer.wake_receiver_for_slot(0);
        });

        let (done_tx, done_rx) = mpsc::channel();
        let consumer = thread::spawn(move || {
            // Pre-arm the caller thread's unpark token and verify `try_recv` leaves it intact.
            thread::current().unpark();
            assert_eq!(rx.try_recv(), Ok(77));
            thread::park();
            done_tx.send(()).unwrap();
            rx
        });

        #[cfg(not(miri))]
        if done_rx.recv_timeout(Duration::from_secs(5)).is_err() {
            consumer.thread().unpark();
            panic!("try_recv timed out or consumed the caller thread's unpark token");
        }
        #[cfg(miri)]
        done_rx.recv().unwrap();

        let rx = consumer.join().unwrap();
        assert_eq!(
            unsafe { SlotRing::<u64>::slot_state_at(base) }.load(Ordering::Acquire),
            SLOT_EMPTY
        );
        publisher.join().unwrap();
        drop(rx);
    }

    #[test]
    fn recv_poll_suspends_without_spinning_when_head_producer_is_in_flight() {
        let shared = Shared::<u64>::new(NonZeroUsize::new(4).unwrap());
        let tx = BoundedSender::new(shared.clone());
        let mut rx = BoundedReceiver::new(shared.clone());

        shared.try_acquire_capacity().unwrap();
        let raw = shared.producer.0.tail.fetch_add(1, Ordering::AcqRel);
        assert_eq!(raw & TICKET_MASK, 0);

        let wake1 = Arc::new(CountWake(AtomicUsize::new(0)));
        let waker1 = Waker::from(wake1.clone());
        let mut cx1 = Context::from_waker(&waker1);

        let wake2 = Arc::new(CountWake(AtomicUsize::new(0)));
        let waker2 = Waker::from(wake2.clone());
        let mut cx2 = Context::from_waker(&waker2);

        {
            let mut fut = pin!(rx.recv());
            assert_eq!(fut.as_mut().poll(&mut cx1), Poll::Pending);

            let base = shared.ring.slot_base(0);
            assert_eq!(
                unsafe { SlotRing::<u64>::slot_state_at(base) }.load(Ordering::Acquire),
                SLOT_RX_WAITING
            );

            // Re-polling with a replaced waker keeps `SLOT_RX_WAITING` armed and updates the waker.
            assert_eq!(fut.as_mut().poll(&mut cx2), Poll::Pending);
            assert_eq!(wake1.0.load(Ordering::SeqCst), 0);
            assert_eq!(wake2.0.load(Ordering::SeqCst), 0);

            let prev_slot = unsafe {
                SlotRing::<u64>::slot_value_at(base).write(99);
                SlotRing::<u64>::slot_state_at(base).swap(SLOT_READY, Ordering::Release)
            };
            assert_eq!(prev_slot, SLOT_RX_WAITING);
            shared.wake_receiver_for_slot(0);

            assert_eq!(wake1.0.load(Ordering::SeqCst), 0);
            assert_eq!(wake2.0.load(Ordering::SeqCst), 1);
            assert_eq!(fut.as_mut().poll(&mut cx2), Poll::Ready(Ok(99)));
        }

        tx.try_send(100).unwrap();
        assert_eq!(rx.try_recv(), Ok(100));
    }

    #[test]
    fn slot_layout_packs_cache_line_payloads_without_extra_line_padding() {
        assert_eq!(compute_slot_layout::<[u64; 8]>().stride, CACHE_LINE);
        assert_eq!(compute_slot_layout::<Option<[u64; 8]>>().stride, CACHE_LINE);
        assert_eq!(compute_slot_layout::<[u8; 1024]>().stride, 9 * CACHE_LINE);
    }

    #[test]
    fn receiver_drop_deallocates_slot_ring_while_senders_survive() {
        let shared = Shared::<[u8; 1024]>::new(NonZeroUsize::new(16).unwrap());
        let tx = BoundedSender::new(shared.clone());
        let rx = BoundedReceiver::new(shared.clone());
        tx.try_send([1u8; 1024]).unwrap();
        assert!(!shared.ring.slots_ptr.load(Ordering::Acquire).is_null());
        drop(rx);
        assert!(shared.ring.slots_ptr.load(Ordering::Acquire).is_null());
        assert_eq!(
            tx.try_send([2u8; 1024]),
            Err(TrySendError::Disconnected([2u8; 1024]))
        );
    }

    #[cfg(panic = "unwind")]
    #[test]
    fn receiver_drop_deallocates_slot_ring_after_destructor_panic() {
        struct PanicOnFirstDrop(bool);

        impl Drop for PanicOnFirstDrop {
            fn drop(&mut self) {
                assert!(!self.0, "first queued destructor panicked");
            }
        }

        let shared = Shared::new(NonZeroUsize::new(4).unwrap());
        let tx = BoundedSender::new(shared.clone());
        let rx = BoundedReceiver::new(shared.clone());
        tx.try_send(PanicOnFirstDrop(true)).unwrap();
        tx.try_send(PanicOnFirstDrop(false)).unwrap();
        assert!(catch_unwind(AssertUnwindSafe(|| drop(rx))).is_err());
        assert!(shared.ring.slots_ptr.load(Ordering::Acquire).is_null());
    }

    #[test]
    fn empty_recv_arms_head_slot_without_marking_tail_and_handles_cancel_and_disconnect() {
        let shared = Shared::<u64>::new(NonZeroUsize::new(4).unwrap());
        let tx = BoundedSender::new(shared.clone());
        let mut rx = BoundedReceiver::new(shared.clone());

        let wake = Arc::new(CountWake(AtomicUsize::new(0)));
        let waker = Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);

        // Polling an empty non-ZST channel arms `slot[0]` without touching `tail`'s wait bit.
        {
            let mut fut = pin!(rx.recv());
            assert_eq!(fut.as_mut().poll(&mut cx), Poll::Pending);
        }
        assert_eq!(
            shared.producer.0.tail.load(Ordering::Acquire) & RX_WAITING_BIT,
            0
        );
        assert_eq!(
            unsafe { SlotRing::<u64>::slot_state_at(shared.ring.slot_base(0)) }
                .load(Ordering::Acquire),
            SLOT_RX_WAITING
        );

        // Cancelling the pending recv leaves `try_recv` reporting Empty and subsequent sends clean.
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        tx.try_send(42).unwrap();
        assert_eq!(wake.0.load(Ordering::SeqCst), 1);
        assert_eq!(rx.try_recv(), Ok(42));

        // Dropping the last sender wakes a slot-armed recv and transitions it to Disconnected.
        let mut fut = pin!(rx.recv());
        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Pending);
        drop(tx);
        assert_eq!(wake.0.load(Ordering::SeqCst), 2);
        assert_eq!(
            fut.as_mut().poll(&mut cx),
            Poll::Ready(Err(RecvError::Disconnected))
        );
        assert!(shared.endpoint.0.control.lock().recv_waker.is_none());
    }
}
