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

#[repr(align(128))]
struct CachePadded<T>(T);

#[cfg(target_has_atomic = "64")]
struct Atomic64(AtomicU64);

#[cfg(target_has_atomic = "64")]
impl Atomic64 {
    const fn new(val: u64) -> Self {
        Self(AtomicU64::new(val))
    }

    #[inline]
    fn load(&self, ordering: Ordering) -> u64 {
        self.0.load(ordering)
    }

    #[inline]
    fn fetch_add(&self, delta: u64, ordering: Ordering) -> u64 {
        self.0.fetch_add(delta, ordering)
    }

    #[inline]
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
    fn load(&self, _ordering: Ordering) -> u64 {
        *self.0.lock()
    }

    #[inline]
    fn fetch_add(&self, delta: u64, _ordering: Ordering) -> u64 {
        let mut guard = self.0.lock();
        let prev = *guard;
        *guard = prev.wrapping_add(delta);
        prev
    }

    #[inline]
    fn fetch_sub(&self, delta: u64, _ordering: Ordering) -> u64 {
        let mut guard = self.0.lock();
        let prev = *guard;
        *guard = prev.wrapping_sub(delta);
        prev
    }

    #[inline]
    fn fetch_or(&self, bits: u64, _ordering: Ordering) -> u64 {
        let mut guard = self.0.lock();
        let prev = *guard;
        *guard = prev | bits;
        prev
    }

    #[inline]
    fn fetch_and(&self, mask: u64, _ordering: Ordering) -> u64 {
        let mut guard = self.0.lock();
        let prev = *guard;
        *guard = prev & mask;
        prev
    }

    #[inline]
    fn fetch_max(&self, val: u64, _ordering: Ordering) -> u64 {
        let mut guard = self.0.lock();
        let prev = *guard;
        if val > prev {
            *guard = val;
        }
        prev
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

struct Tail(Atomic64);

impl Tail {
    const fn new() -> Self {
        Self(Atomic64::new(0))
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
        let raw = self.0.fetch_add(1, Ordering::AcqRel);
        if (raw & TICKET_MASK) >= TICKET_MASK - 1 {
            process::abort();
        }
        raw
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

struct ProducerPermits {
    acquired: Atomic64,
    cached_rx_rel: Atomic64,
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
// producer-permit, receiver-release, producer-tail, and endpoint lines.
#[repr(C)]
pub struct Shared<T> {
    ring: SlotRing<T>,
    permits: CachePadded<ProducerPermits>,
    rx_state: CachePadded<Atomic64>,
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
        let ring = SlotRing::new(capacity);
        Arc::new(Self {
            ring,
            permits: CachePadded(ProducerPermits {
                acquired: Atomic64::new(0),
                cached_rx_rel: Atomic64::new(0),
            }),
            rx_state: CachePadded(Atomic64::new(0)),
            tail: CachePadded(Tail::new()),
            endpoint: CachePadded(EndpointState {
                senders: AtomicUsize::new(1),
                rx_alive: AtomicBool::new(true),
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
        // `BoundedSender::clone` clones `Arc<Shared<T>>` first, which already aborts at
        // `isize::MAX` before `senders` can overflow.
        self.endpoint.0.senders.fetch_add(1, Ordering::Relaxed);
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
            // Seal `acquired` and `tail` under `control` before storing `rx_alive = false` so no
            // concurrent send on a non-full channel can report `Full` during teardown, and any
            // caller observing disconnection immediately observes `has_receiver() == false`.
            self.permits
                .0
                .acquired
                .fetch_or(ACQ_CLOSED_BIT, Ordering::SeqCst);
            self.permits
                .0
                .acquired
                .fetch_and(!ACQ_WAITERS_BIT, Ordering::SeqCst);
            self.rx_state.0.fetch_and(!RX_WAITERS_BIT, Ordering::SeqCst);
            let final_tail = self.tail.0.close() & TICKET_MASK;
            self.tail.0.clear_rx_waiting();
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

    #[inline]
    fn sync_waiting_flag_locked(&self, control: &Control) {
        if control.send_waiters.is_empty() {
            self.rx_state
                .0
                .fetch_and(!RX_WAITERS_BIT, Ordering::Release);
            self.permits
                .0
                .acquired
                .fetch_and(!ACQ_WAITERS_BIT, Ordering::Release);
        }
    }

    #[inline]
    pub fn try_acquire_capacity(&self) -> Result<(), TrySendError<()>> {
        let cap = self.ring.capacity.get() as u64;
        let cached_rx = self.permits.0.cached_rx_rel.load(Ordering::Acquire);
        let mut raw_acq = self.permits.0.acquired.load(Ordering::Relaxed);
        loop {
            if raw_acq & ACQ_CLOSED_BIT != 0 {
                self.on_closed_tail();
                return Err(TrySendError::Disconnected(()));
            }
            let acq = raw_acq & ACQ_COUNT_MASK;
            if acq.saturating_sub(cached_rx) >= cap {
                return self.try_acquire_capacity_reload(cap, cached_rx, raw_acq);
            }
            if acq >= ACQ_COUNT_MASK - 1 {
                process::abort();
            }
            match self.permits.0.acquired.compare_exchange_weak(
                raw_acq,
                raw_acq + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => raw_acq = actual,
            }
        }
    }

    #[cold]
    fn try_acquire_capacity_reload(
        &self,
        cap: u64,
        mut cached_rx: u64,
        mut raw_acq: u64,
    ) -> Result<(), TrySendError<()>> {
        loop {
            let true_rx = self.rx_state.0.load(Ordering::Acquire) & RX_REL_MASK;
            if true_rx != cached_rx {
                self.permits
                    .0
                    .cached_rx_rel
                    .fetch_max(true_rx, Ordering::Release);
                cached_rx = true_rx;
            }
            let fresh_acq = self.permits.0.acquired.load(Ordering::Acquire);
            if fresh_acq & ACQ_CLOSED_BIT != 0 {
                self.on_closed_tail();
                return Err(TrySendError::Disconnected(()));
            }
            let acq = fresh_acq & ACQ_COUNT_MASK;
            if acq.saturating_sub(cached_rx) < cap {
                if acq >= ACQ_COUNT_MASK - 1 {
                    process::abort();
                }
                match self.permits.0.acquired.compare_exchange(
                    fresh_acq,
                    fresh_acq + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Ok(()),
                    Err(actual) => {
                        raw_acq = actual;
                        continue;
                    }
                }
            }
            if fresh_acq != raw_acq {
                raw_acq = fresh_acq;
                continue;
            }
            let verify_rx = self.rx_state.0.load(Ordering::Acquire) & RX_REL_MASK;
            if verify_rx == cached_rx {
                return Err(TrySendError::Full(()));
            }
            self.permits
                .0
                .cached_rx_rel
                .fetch_max(verify_rx, Ordering::Release);
            cached_rx = verify_rx;
        }
    }

    fn try_acquire_permit_seqcst(&self) -> bool {
        let cap = self.ring.capacity.get() as u64;
        let mut true_rx = self.rx_state.0.load(Ordering::SeqCst) & RX_REL_MASK;
        let mut raw_acq = self.permits.0.acquired.load(Ordering::SeqCst);
        loop {
            if raw_acq & ACQ_CLOSED_BIT != 0 {
                return false;
            }
            let acq = raw_acq & ACQ_COUNT_MASK;
            if acq.saturating_sub(true_rx) >= cap {
                let latest_rx = self.rx_state.0.load(Ordering::SeqCst) & RX_REL_MASK;
                if latest_rx != true_rx {
                    true_rx = latest_rx;
                    raw_acq = self.permits.0.acquired.load(Ordering::SeqCst);
                    continue;
                }
                return false;
            }
            if acq >= ACQ_COUNT_MASK - 1 {
                process::abort();
            }
            match self.permits.0.acquired.compare_exchange(
                raw_acq,
                raw_acq + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(actual) => raw_acq = actual,
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
            self.rx_state.0.fetch_or(RX_WAITERS_BIT, Ordering::SeqCst);
            self.permits
                .0
                .acquired
                .fetch_or(ACQ_WAITERS_BIT, Ordering::SeqCst);
            if self.try_acquire_permit_seqcst() {
                self.sync_waiting_flag_locked(&control);
                return Poll::Ready(Ok(()));
            }
        }

        let waiter = SendWaiter::Waiting(cx.waker().clone());
        *waiter_slot = Some(control.send_waiters.push_back(waiter));
        drop(control);
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
        let mut cur = self.rx_state.0.load(Ordering::Relaxed);
        loop {
            if (cur & RX_WAITERS_BIT) != 0 {
                self.release_from_receiver_slow();
                return;
            }
            if (cur & RX_REL_MASK) >= RX_REL_MASK - 1 {
                process::abort();
            }
            match self.rx_state.0.compare_exchange_weak(
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

    #[cold]
    fn release_from_receiver_slow(&self) {
        let wake = {
            let mut control = self.endpoint.0.control.lock();
            if !control.rx_alive {
                None
            } else if let Some(waker) = control.send_waiters.grant_one() {
                // Transfer the consumed message's `acquired` unit directly to the oldest queued
                // waiter without exposing free capacity to concurrent `try_send` callers.
                self.sync_waiting_flag_locked(&control);
                Some(waker)
            } else {
                self.sync_waiting_flag_locked(&control);
                let prev = self.rx_state.0.fetch_add(1, Ordering::Release);
                if (prev & RX_REL_MASK) >= RX_REL_MASK - 1 {
                    process::abort();
                }
                None
            }
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }

    #[inline]
    pub fn release_capacity(&self) {
        let mut cur = self.permits.0.acquired.load(Ordering::Relaxed);
        loop {
            if cur & ACQ_CLOSED_BIT != 0 {
                return;
            }
            if cur & ACQ_WAITERS_BIT != 0 {
                self.release_capacity_slow();
                return;
            }
            debug_assert!((cur & ACQ_COUNT_MASK) > 0);
            match self.permits.0.acquired.compare_exchange_weak(
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

    fn release_capacity_locked(&self, control: &mut Control) -> Option<Waker> {
        if !control.rx_alive {
            return None;
        }
        if let Some(waker) = control.send_waiters.grant_one() {
            self.sync_waiting_flag_locked(control);
            return Some(waker);
        }
        self.sync_waiting_flag_locked(control);
        self.permits.0.acquired.fetch_sub(1, Ordering::AcqRel);
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
