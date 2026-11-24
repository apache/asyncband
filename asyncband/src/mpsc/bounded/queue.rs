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
use std::hint;
use std::marker::PhantomData;
use std::mem;
use std::num::NonZeroUsize;
use std::panic::RefUnwindSafe;
use std::panic::UnwindSafe;
use std::process;
use std::ptr;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
#[cfg(target_has_atomic = "64")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread;

use crate::internal::mutex::Mutex;
use crate::internal::register_waker;
use crate::internal::update_waker;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::waker_batch::WakerBatch;
use crate::mpsc::RecvError;
use crate::mpsc::SendError;
use crate::mpsc::TryRecvError;
use crate::mpsc::TrySendError;

const CACHE_LINE: usize = 128;
const CLOSED_BIT: u64 = 1 << 63;
const RX_WAITING_BIT: u64 = 1 << 62;
const TICKET_MASK: u64 = (1 << 62) - 1;
const MAX_FAST_CAPACITY: usize = (isize::MAX as usize) >> 1;
const DISCONNECTED_BIAS: usize = MAX_FAST_CAPACITY + 1;
const RX_CREDIT_WAITERS_BIT: usize = 1usize << (usize::BITS - 1);

#[repr(align(128))]
struct CachePadded<T>(T);

#[cfg(target_has_atomic = "64")]
struct Tail(AtomicU64);

#[cfg(target_has_atomic = "64")]
impl Tail {
    const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    #[inline]
    fn load(&self, ordering: Ordering) -> u64 {
        self.0.load(ordering)
    }

    #[inline]
    fn claim(&self) -> u64 {
        // A sender or `Permit` may acquire capacity before an earlier occupant of the same ring
        // slot is consumed while other permits cycle `tail` forward by `capacity`, so claiming a
        // ticket must be `AcqRel` to carry slot-reuse happens-before edges across `tail`.
        self.0.fetch_add(1, Ordering::AcqRel)
    }

    #[inline]
    fn set_rx_waiting(&self) -> u64 {
        self.0.fetch_or(RX_WAITING_BIT, Ordering::AcqRel)
    }

    #[inline]
    fn clear_rx_waiting(&self) {
        self.0.fetch_and(!RX_WAITING_BIT, Ordering::Release);
    }

    #[inline]
    fn close(&self) -> u64 {
        self.0.fetch_or(CLOSED_BIT, Ordering::AcqRel)
    }
}

#[cfg(not(target_has_atomic = "64"))]
struct Tail(Mutex<u64>);

#[cfg(not(target_has_atomic = "64"))]
impl Tail {
    const fn new() -> Self {
        Self(Mutex::new(0))
    }

    #[inline]
    fn load(&self, _ordering: Ordering) -> u64 {
        *self.0.lock()
    }

    #[inline]
    fn claim(&self) -> u64 {
        let mut guard = self.0.lock();
        let prev = *guard;
        *guard = prev + 1;
        prev
    }

    #[inline]
    fn set_rx_waiting(&self) -> u64 {
        let mut guard = self.0.lock();
        let prev = *guard;
        *guard = prev | RX_WAITING_BIT;
        prev
    }

    #[inline]
    fn clear_rx_waiting(&self) {
        *self.0.lock() &= !RX_WAITING_BIT;
    }

    #[inline]
    fn close(&self) -> u64 {
        let mut guard = self.0.lock();
        let prev = *guard;
        *guard = prev | CLOSED_BIT;
        prev
    }
}

struct SlotLayout {
    align: usize,
    value_offset: usize,
    stride: usize,
}

// Keep `ready: AtomicBool` at byte offset 0 of every slot for zero-offset `ldarb`/`stlrb`
// addressing while aligning payloads `>= 64 B` (such as `[u8; 1024]`) to a dedicated 128B line.
const fn compute_slot_layout<T>() -> SlotLayout {
    if size_of::<T>() == 0 {
        return SlotLayout {
            align: 1,
            value_offset: 0,
            stride: 0,
        };
    }
    let raw_align = align_of::<T>();
    let align = if size_of::<T>() >= 64 && raw_align < CACHE_LINE {
        CACHE_LINE
    } else {
        raw_align
    };
    let Some(end) = align.checked_add(size_of::<T>()) else {
        panic!("mpsc bounded channel slot size exceeds allocation size limit");
    };
    let Some(stride) = end.checked_next_multiple_of(align) else {
        panic!("mpsc bounded channel slot size exceeds allocation size limit");
    };
    SlotLayout {
        align,
        value_offset: align,
        stride,
    }
}

#[track_caller]
fn slot_buffer_layout<T>(buffer: usize) -> alloc::Layout {
    let slot = compute_slot_layout::<T>();
    let Some(total_size) = slot.stride.checked_mul(buffer) else {
        panic!("mpsc bounded channel capacity exceeds allocation size limit");
    };
    let Ok(layout) = alloc::Layout::from_size_align(total_size, slot.align) else {
        panic!("mpsc bounded channel capacity exceeds allocation size limit");
    };
    layout
}

#[repr(C)]
struct SlotRing<T> {
    slots_ptr: NonNull<u8>,
    capacity: NonZeroUsize,
    index_mask: usize,
    _marker: PhantomData<UnsafeCell<T>>,
}

impl<T> Drop for SlotRing<T> {
    fn drop(&mut self) {
        if size_of::<T>() == 0 {
            return;
        }
        let layout = slot_buffer_layout::<T>(self.capacity.get());
        // SAFETY: `slots_ptr` was allocated with `layout`, and all initialized payloads were
        // drained when `BoundedReceiver` dropped before the final `Arc<Shared<T>>` drop.
        unsafe {
            alloc::dealloc(self.slots_ptr.as_ptr(), layout);
        }
    }
}

impl<T> SlotRing<T> {
    const LAYOUT: SlotLayout = compute_slot_layout::<T>();

    #[track_caller]
    fn new(capacity: NonZeroUsize) -> Self {
        let buffer = capacity.get();
        let slots_ptr = if size_of::<T>() == 0 {
            NonNull::dangling()
        } else {
            let layout = slot_buffer_layout::<T>(buffer);
            // SAFETY: `buffer > 0` and `size_of::<T>() > 0`, so `layout.size() > 0`. Zeroed bytes
            // initialize `ready: AtomicBool` at offset 0 to `false` without staging `T` on stack.
            let ptr = unsafe { alloc::alloc_zeroed(layout) };
            NonNull::new(ptr).unwrap_or_else(|| alloc::handle_alloc_error(layout))
        };
        let index_mask = if buffer.is_power_of_two() {
            buffer - 1
        } else {
            usize::MAX
        };
        Self {
            slots_ptr,
            capacity,
            index_mask,
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    fn slot_base(&self, ticket: u64) -> *mut u8 {
        let idx = if self.index_mask != usize::MAX {
            let raw = (ticket as usize) & self.index_mask;
            // Swapping bits `0..3` and `3..6` spreads consecutive small-slot tickets across
            // distinct 128B cache lines on power-of-two rings `>= 64`.
            if Self::LAYOUT.stride <= 64 && self.capacity.get() >= 64 {
                ((raw & 0b00_0111) << 3) | ((raw & 0b11_1000) >> 3) | (raw & !0b11_1111)
            } else {
                raw
            }
        } else {
            (ticket % (self.capacity.get() as u64)) as usize
        };
        debug_assert!(idx < self.capacity.get());
        // SAFETY: `idx < self.capacity` and `SlotRing::new` allocated `capacity * stride` bytes.
        unsafe { self.slots_ptr.as_ptr().add(idx * Self::LAYOUT.stride) }
    }

    #[inline(always)]
    unsafe fn slot_ready_at<'a>(base: *mut u8) -> &'a AtomicBool {
        // SAFETY: Offset 0 of every allocated slot holds a valid zero-initialized `AtomicBool`.
        unsafe { &*base.cast::<AtomicBool>() }
    }

    #[inline(always)]
    unsafe fn slot_value_at(base: *mut u8) -> *mut T {
        // SAFETY: `value_offset` is aligned to a multiple of `align_of::<T>()` within the slot.
        unsafe { base.add(Self::LAYOUT.value_offset).cast::<T>() }
    }

    #[inline]
    fn wait_until_ready(ready: &AtomicBool) {
        let mut spins = 0u32;
        while !ready.load(Ordering::Acquire) {
            if spins < 128 && !cfg!(miri) {
                hint::spin_loop();
                spins += 1;
            } else {
                thread::yield_now();
            }
        }
    }
}

struct EndpointState {
    senders: AtomicUsize,
    rx_alive: AtomicBool,
    has_waiting_senders: AtomicBool,
    control: Mutex<Control>,
}

struct Control {
    rx_alive: bool,
    recv_waker: Option<Waker>,
    send_waiters: WaitList<SendWaiter>,
}

enum SendWaiter {
    Waiting(Waker),
    Granted,
}

impl WaitList<SendWaiter> {
    fn grant_one(&mut self) -> Option<Waker> {
        let (_, waiter) = self.unlink_first_waiter()?;
        let SendWaiter::Waiting(waker) = mem::replace(waiter, SendWaiter::Granted) else {
            unreachable!("only waiting senders remain linked");
        };
        Some(waker)
    }

    fn take_waiting_waker(&mut self) -> Option<Waker> {
        let (id, _) = self.unlink_first_waiter()?;
        let SendWaiter::Waiting(waker) = self.remove_unlinked_waiter(id) else {
            unreachable!("only waiting senders remain linked");
        };
        Some(waker)
    }
}

struct DrainGuard<'a, T> {
    shared: &'a Shared<T>,
    cursor: u64,
    end: u64,
}

impl<T> DrainGuard<'_, T> {
    fn drain(&mut self) {
        while self.cursor < self.end {
            let ticket = self.cursor;
            self.cursor += 1;
            if size_of::<T>() == 0 {
                // SAFETY: `T` is zero-sized and one published instance was forgotten per ticket.
                unsafe {
                    NonNull::<T>::dangling().as_ptr().drop_in_place();
                }
            } else {
                let base = self.shared.ring.slot_base(ticket);
                // SAFETY: `base` points to a valid slot whose ticket `< end` was claimed before
                // `CLOSED_BIT` was set, and `BoundedReceiver::drop` owns all unconsumed slots.
                unsafe {
                    SlotRing::<T>::wait_until_ready(SlotRing::<T>::slot_ready_at(base));
                    SlotRing::<T>::slot_value_at(base).drop_in_place();
                }
            }
        }
    }
}

impl<T> Drop for DrainGuard<'_, T> {
    fn drop(&mut self) {
        self.drain();
    }
}

// `#[repr(C)]` keeps immutable ring geometry on the leading cache line, separate from contended
// permit, receiver-credit, producer-tail, and endpoint lines.
#[repr(C)]
pub struct Shared<T> {
    ring: SlotRing<T>,
    credit_limit: usize,
    permits: CachePadded<AtomicUsize>,
    rx_credit: CachePadded<AtomicUsize>,
    tail: CachePadded<Tail>,
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
        let buffer = capacity.get();
        let ring = SlotRing::new(capacity);
        let credit_limit = if (8..=MAX_FAST_CAPACITY).contains(&buffer) {
            (buffer / 4).min(32)
        } else {
            0
        };
        Arc::new(Self {
            ring,
            credit_limit,
            permits: CachePadded(AtomicUsize::new(buffer)),
            rx_credit: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(Tail::new()),
            endpoint: CachePadded(EndpointState {
                senders: AtomicUsize::new(1),
                rx_alive: AtomicBool::new(true),
                has_waiting_senders: AtomicBool::new(false),
                control: Mutex::new(Control {
                    rx_alive: true,
                    recv_waker: None,
                    send_waiters: WaitList::new(),
                }),
            }),
        })
    }

    #[inline]
    pub fn clone_sender(&self) {
        if self.endpoint.0.senders.fetch_add(1, Ordering::Relaxed) >= MAX_FAST_CAPACITY {
            process::abort();
        }
    }

    #[inline]
    pub fn drop_sender(&self) {
        let prev = self.endpoint.0.senders.fetch_sub(1, Ordering::AcqRel);
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
        let mut control = self.endpoint.0.control.lock();
        self.tail.0.clear_rx_waiting();
        control.recv_waker.take()
    }

    pub fn drop_receiver(&self, head: u64) {
        let (final_tail, recv_waker, mut waiters) = {
            let mut control = self.endpoint.0.control.lock();
            control.rx_alive = false;
            // Seal `tail` and poison `permits` under `control` before storing `rx_alive = false`
            // so any caller observing disconnection immediately sees `has_receiver() == false`.
            let final_tail = self.tail.0.close() & TICKET_MASK;
            self.tail.0.clear_rx_waiting();
            self.rx_credit.0.store(0, Ordering::SeqCst);
            if self.uses_cas_permits() {
                self.permits.0.store(0, Ordering::SeqCst);
            } else {
                self.permits
                    .0
                    .fetch_sub(DISCONNECTED_BIAS, Ordering::SeqCst);
            }
            self.endpoint
                .0
                .has_waiting_senders
                .store(false, Ordering::Release);
            self.endpoint.0.rx_alive.store(false, Ordering::Release);
            let recv_waker = control.recv_waker.take();
            let waiters = mem::replace(&mut control.send_waiters, WaitList::new());
            (final_tail, recv_waker, waiters)
        };

        // Notify blocked senders before destroying buffered values, whose destructors may panic.
        while let Some(waker) = waiters.take_waiting_waker() {
            waker.wake();
        }
        drop(waiters);
        drop(recv_waker);

        let mut guard = DrainGuard {
            shared: self,
            cursor: head,
            end: final_tail,
        };
        guard.drain();
    }

    #[inline]
    pub fn has_receiver(&self) -> bool {
        self.endpoint.0.rx_alive.load(Ordering::Acquire)
    }

    #[inline]
    pub fn has_senders(&self) -> bool {
        self.endpoint.0.senders.load(Ordering::Acquire) != 0
    }

    #[inline]
    pub fn is_empty_at(&self, head: u64) -> bool {
        (self.tail.0.load(Ordering::Acquire) & TICKET_MASK) == head
    }

    #[inline(always)]
    fn uses_cas_permits(&self) -> bool {
        size_of::<T>() == 0 && self.ring.capacity.get() > MAX_FAST_CAPACITY
    }

    #[inline]
    fn has_positive_permits(&self, raw: usize) -> bool {
        if self.uses_cas_permits() {
            raw != 0
        } else {
            (raw as isize) > 0
        }
    }

    #[inline]
    fn sync_waiting_flag_locked(&self, control: &Control) {
        if control.send_waiters.is_empty() {
            if self.credit_limit != 0 {
                self.rx_credit
                    .0
                    .fetch_and(!RX_CREDIT_WAITERS_BIT, Ordering::Release);
            }
            self.endpoint
                .0
                .has_waiting_senders
                .store(false, Ordering::Release);
        }
    }

    #[inline]
    fn flush_rx_credit_raw(&self) -> (isize, bool) {
        let prev = self
            .rx_credit
            .0
            .fetch_and(RX_CREDIT_WAITERS_BIT, Ordering::SeqCst);
        let had_waiters = (prev & RX_CREDIT_WAITERS_BIT) != 0;
        let credit = prev & !RX_CREDIT_WAITERS_BIT;
        if credit == 0 {
            return (0, had_waiters);
        }
        let after = self
            .permits
            .0
            .fetch_add(credit, Ordering::SeqCst)
            .wrapping_add(credit) as isize;
        (after, had_waiters)
    }

    #[inline]
    fn flush_rx_credit(&self) {
        let (after, had_waiters) = self.flush_rx_credit_raw();
        if after > 0 && (had_waiters || self.endpoint.0.has_waiting_senders.load(Ordering::SeqCst))
        {
            self.reclaim_for_waiters();
        }
    }

    #[inline]
    pub fn try_acquire_capacity(&self) -> Result<(), TrySendError<()>> {
        if self.uses_cas_permits() {
            return self.try_acquire_cas();
        }
        if (self.permits.0.load(Ordering::Relaxed) as isize) <= 0
            && !self.try_refill_from_rx_credit()
        {
            return Err(self.full_or_disconnected());
        }
        if (self.permits.0.fetch_sub(1, Ordering::Acquire) as isize) > 0 {
            Ok(())
        } else {
            self.rollback_failed_acquire()
        }
    }

    #[inline]
    fn try_refill_from_rx_credit(&self) -> bool {
        if self.credit_limit == 0
            || (self.rx_credit.0.load(Ordering::Acquire) & !RX_CREDIT_WAITERS_BIT) == 0
        {
            return false;
        }
        self.flush_rx_credit();
        (self.permits.0.load(Ordering::Relaxed) as isize) > 0
    }

    #[inline]
    fn full_or_disconnected(&self) -> TrySendError<()> {
        if self.has_receiver() {
            TrySendError::Full(())
        } else {
            TrySendError::Disconnected(())
        }
    }

    #[cold]
    fn rollback_failed_acquire(&self) -> Result<(), TrySendError<()>> {
        let restored = self.permits.0.fetch_add(1, Ordering::SeqCst) as isize;
        if restored >= 0 && self.endpoint.0.has_waiting_senders.load(Ordering::SeqCst) {
            self.reclaim_for_waiters();
        }
        Err(self.full_or_disconnected())
    }

    #[cold]
    fn try_acquire_cas(&self) -> Result<(), TrySendError<()>> {
        if self.has_receiver() && self.try_decrement_permits(Ordering::AcqRel, Ordering::Acquire) {
            Ok(())
        } else {
            Err(self.full_or_disconnected())
        }
    }

    #[inline]
    fn try_decrement_permits(&self, success: Ordering, failure: Ordering) -> bool {
        let mut cur = self.permits.0.load(failure);
        loop {
            if !self.has_positive_permits(cur) {
                return false;
            }
            match self
                .permits
                .0
                .compare_exchange_weak(cur, cur.wrapping_sub(1), success, failure)
            {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    pub fn poll_acquire_capacity_slow(
        &self,
        waiter_slot: &mut Option<WaiterId>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), SendError<()>>> {
        let mut control = self.endpoint.0.control.lock();
        if !control.rx_alive {
            *waiter_slot = None;
            return Poll::Ready(Err(SendError::new(())));
        }

        if let Some(id) = *waiter_slot {
            if let SendWaiter::Waiting(waker) = control.send_waiters.waiter_mut(id) {
                let old = update_waker(waker, cx.waker());
                drop(control);
                drop(old);
                return Poll::Pending;
            }
            control.send_waiters.remove_unlinked_waiter(id);
            *waiter_slot = None;
            return Poll::Ready(Ok(()));
        }

        if control.send_waiters.is_empty() {
            self.endpoint
                .0
                .has_waiting_senders
                .store(true, Ordering::SeqCst);
            if self.credit_limit != 0 {
                self.rx_credit
                    .0
                    .fetch_or(RX_CREDIT_WAITERS_BIT, Ordering::SeqCst);
            }
        }
        if self.credit_limit != 0 {
            self.flush_rx_credit_raw();
        }
        if control.send_waiters.is_empty()
            && self.try_decrement_permits(Ordering::SeqCst, Ordering::SeqCst)
        {
            self.sync_waiting_flag_locked(&control);
            return Poll::Ready(Ok(()));
        }

        let waiter = SendWaiter::Waiting(cx.waker().clone());
        *waiter_slot = Some(control.send_waiters.push_back(waiter));
        let need_reclaim = self.has_positive_permits(self.permits.0.load(Ordering::SeqCst));
        drop(control);
        if need_reclaim {
            self.reclaim_for_waiters();
        }
        Poll::Pending
    }

    #[cold]
    pub fn cancel_waiter_id(&self, id: WaiterId) {
        let (waiter, wake) = {
            let mut control = self.endpoint.0.control.lock();
            if !control.rx_alive {
                return;
            }
            let waiter = control.send_waiters.remove(id);
            let wake = match &waiter {
                SendWaiter::Waiting(_) => {
                    self.sync_waiting_flag_locked(&control);
                    None
                }
                SendWaiter::Granted => self.release_capacity_locked(&mut control),
            };
            (waiter, wake)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        drop(waiter);
    }

    #[inline]
    fn release_from_receiver(&self) {
        if self.credit_limit == 0 {
            self.release_capacity();
            return;
        }
        // `RX_CREDIT_WAITERS_BIT` occupies the high bit, so one unsigned check flushes both at
        // `credit_limit` and immediately whenever any sender is waiting for capacity.
        let prev = self.rx_credit.0.fetch_add(1, Ordering::Release);
        if prev.wrapping_add(1) >= self.credit_limit {
            self.flush_rx_credit();
        }
    }

    #[inline]
    pub fn release_capacity(&self) {
        if !self.has_receiver() {
            return;
        }
        if self.endpoint.0.has_waiting_senders.load(Ordering::Acquire) {
            self.release_capacity_slow();
            return;
        }
        let prev = self.permits.0.fetch_add(1, Ordering::SeqCst);
        let became_available = if self.uses_cas_permits() {
            prev == 0
        } else {
            (prev as isize) >= 0
        };
        if became_available && self.endpoint.0.has_waiting_senders.load(Ordering::SeqCst) {
            self.reclaim_for_waiters();
        }
    }

    #[cold]
    fn release_capacity_slow(&self) {
        let wake = {
            let mut control = self.endpoint.0.control.lock();
            self.release_capacity_locked(&mut control)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }

    #[cold]
    fn reclaim_for_waiters(&self) {
        loop {
            let mut batch = WakerBatch::new();
            let done = {
                let mut control = self.endpoint.0.control.lock();
                if !control.rx_alive {
                    return;
                }
                while !batch.will_spill() && !control.send_waiters.is_empty() {
                    if !self.try_decrement_permits(Ordering::SeqCst, Ordering::SeqCst) {
                        break;
                    }
                    let waker = control
                        .send_waiters
                        .grant_one()
                        .expect("checked non-empty send_waiters");
                    self.sync_waiting_flag_locked(&control);
                    batch.push(waker);
                }
                control.send_waiters.is_empty()
                    || !self.has_positive_permits(self.permits.0.load(Ordering::SeqCst))
            };
            batch.wake_all();
            if done {
                return;
            }
        }
    }

    fn release_capacity_locked(&self, control: &mut Control) -> Option<Waker> {
        if !control.rx_alive {
            return None;
        }
        if let Some(waker) = control.send_waiters.grant_one() {
            self.sync_waiting_flag_locked(control);
            return Some(waker);
        }
        self.permits.0.fetch_add(1, Ordering::SeqCst);
        None
    }

    /// Copies `*src` into a pre-claimed capacity slot, taking logical ownership of `*src` when
    /// returning `true`. Returns `false` without reading `*src` if the receiver has disconnected.
    #[inline]
    pub unsafe fn publish_from_ptr(&self, src: *const T) -> bool {
        let raw = self.tail.0.claim();
        if raw & CLOSED_BIT != 0 {
            self.on_closed_tail();
            return false;
        }

        if size_of::<T>() > 0 {
            let base = self.ring.slot_base(raw & TICKET_MASK);
            // SAFETY: Capacity accounting bounds in-flight and buffered slots to `capacity`, and
            // the previous occupant cleared `ready` before releasing its capacity unit.
            unsafe {
                ptr::copy_nonoverlapping(src, SlotRing::<T>::slot_value_at(base), 1);
                SlotRing::<T>::slot_ready_at(base).store(true, Ordering::Release);
            }
        }

        if raw & RX_WAITING_BIT != 0 {
            self.wake_receiver();
        }
        true
    }

    #[cold]
    fn on_closed_tail(&self) {
        // Synchronize with `drop_receiver` so any caller observing `SendError` immediately
        // observes `has_receiver() == false` and disconnected reservations.
        drop(self.endpoint.0.control.lock());
    }

    #[cold]
    fn wake_receiver(&self) {
        if self.tail.0.load(Ordering::Acquire) & RX_WAITING_BIT == 0 {
            return;
        }
        if let Some(waker) = self.take_recv_waker() {
            waker.wake();
        }
    }

    #[cold]
    pub fn clear_recv_waker_if_waiting(&self) {
        if self.tail.0.load(Ordering::Relaxed) & RX_WAITING_BIT != 0 {
            drop(self.take_recv_waker());
        }
    }

    #[cold]
    pub fn register_recv_wait(&self, head: u64, cx: &mut Context<'_>) -> bool {
        let mut control = self.endpoint.0.control.lock();
        let old_waker = register_waker(&mut control.recv_waker, cx.waker());
        let prev_tail = self.tail.0.set_rx_waiting() & TICKET_MASK;

        if prev_tail != head || !self.has_senders() {
            self.tail.0.clear_rx_waiting();
            let cleared = control.recv_waker.take();
            drop(control);
            drop(old_waker);
            drop(cleared);
            return true;
        }

        drop(control);
        drop(old_waker);
        false
    }

    #[inline]
    pub fn claim_read_slot(&self, head: u64) -> Result<*mut u8, TryRecvError> {
        if size_of::<T>() == 0 {
            self.ensure_non_empty(head)?;
            return Ok(NonNull::<u8>::dangling().as_ptr());
        }

        let base = self.ring.slot_base(head);
        // SAFETY: `base` points to a valid allocated slot in `0..self.ring.capacity`.
        if !unsafe { SlotRing::<T>::slot_ready_at(base) }.load(Ordering::Acquire) {
            self.claim_read_slot_slow(head, base)?;
        }
        Ok(base)
    }

    #[inline]
    fn ensure_non_empty(&self, head: u64) -> Result<(), TryRecvError> {
        if (self.tail.0.load(Ordering::Acquire) & TICKET_MASK) != head {
            return Ok(());
        }
        if self.has_senders() {
            return Err(TryRecvError::Empty);
        }
        if (self.tail.0.load(Ordering::Acquire) & TICKET_MASK) == head {
            Err(TryRecvError::Disconnected)
        } else {
            Ok(())
        }
    }

    #[cold]
    fn claim_read_slot_slow(&self, head: u64, base: *mut u8) -> Result<(), TryRecvError> {
        self.ensure_non_empty(head)?;
        // SAFETY: `base` points to a valid allocated slot in `0..self.ring.capacity`.
        SlotRing::<T>::wait_until_ready(unsafe { SlotRing::<T>::slot_ready_at(base) });
        Ok(())
    }

    #[inline]
    pub unsafe fn read_claimed_slot(&self, base: *mut u8) -> T {
        if size_of::<T>() == 0 {
            self.release_from_receiver();
            // SAFETY: `T` is zero-sized and one published instance was forgotten in `publish`.
            return unsafe { NonNull::<T>::dangling().as_ptr().read() };
        }

        // SAFETY: `claim_read_slot` observed `ready == true` with `Acquire`, synchronizing with
        // the producer's write of `T`, and `BoundedReceiver` holds exclusive `&mut` access.
        let value = unsafe { SlotRing::<T>::slot_value_at(base).read() };
        unsafe {
            SlotRing::<T>::slot_ready_at(base).store(false, Ordering::Release);
        }
        self.release_from_receiver();
        value
    }

    #[inline]
    pub unsafe fn read_claimed_slot_poll(&self, base: *mut u8) -> Poll<Result<T, RecvError>> {
        if size_of::<T>() == 0 {
            self.release_from_receiver();
            // SAFETY: `T` is zero-sized and one published instance was forgotten in `publish`.
            return Poll::Ready(Ok(unsafe { NonNull::<T>::dangling().as_ptr().read() }));
        }

        // Materialize `Poll::Ready(Ok(value))` directly in the caller's return slot before
        // releasing capacity so LLVM does not stage large payloads through an extra stack buffer.
        // SAFETY: `claim_read_slot` observed `ready == true` with `Acquire`.
        let out = Poll::Ready(Ok(unsafe { SlotRing::<T>::slot_value_at(base).read() }));
        unsafe {
            SlotRing::<T>::slot_ready_at(base).store(false, Ordering::Release);
        }
        self.release_from_receiver();
        out
    }
}
