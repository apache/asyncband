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

//! Shared synchronization state for retryable one-time initialization.
//!
//! Unlike a fixed-permit semaphore, this state machine separates exclusive initializer selection
//! from completion notification: an aborted attempt hands leadership to at most one queued
//! candidate in FIFO order, while successful completion broadcasts to every waiting candidate and
//! observer at once.

use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::update_waker;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::wakerset::WakerSet;
use crate::internal::wakerset::WakerToken;

const UNINIT: u8 = 0;
const RUNNING: u8 = 1 << 0;
const HAS_WAITERS: u8 = 1 << 1;
const COMPLETE: u8 = 1 << 2;

enum TryBegin {
    Completed,
    Leading,
    Contended,
}

#[derive(Default)]
enum InlineSlot {
    #[default]
    Empty,
    Observer(Waker),
    Initializer(Waker),
    Granted,
}

enum OverflowInitWaiter {
    Waiting(Waker),
    Granted,
}

struct OverflowWaiters {
    initializers: WaitList<OverflowInitWaiter>,
    observers: WakerSet,
}

impl Default for OverflowWaiters {
    fn default() -> Self {
        Self {
            initializers: WaitList::new(),
            observers: WakerSet::new(),
        }
    }
}

#[derive(Default)]
struct Waiters {
    // Handles the common single-waiter contention case without allocating an arena or box.
    // An initializer uses `inline` only when `overflow` holds no queued initializers, preserving
    // strict FIFO handoff across cancellations and new arrivals.
    inline: InlineSlot,
    overflow: Option<Box<OverflowWaiters>>,
}

impl Waiters {
    const fn new() -> Self {
        Self {
            inline: InlineSlot::Empty,
            overflow: None,
        }
    }

    fn is_unused(&self) -> bool {
        matches!(self.inline, InlineSlot::Empty)
            && self.overflow.as_ref().is_none_or(|overflow| {
                !overflow.initializers.has_occupied_slots() && overflow.observers.is_empty()
            })
    }

    fn has_no_overflow_initializers(&self) -> bool {
        self.overflow
            .as_ref()
            .is_none_or(|overflow| overflow.initializers.is_empty())
    }

    fn overflow_or_init(&mut self) -> &mut OverflowWaiters {
        self.overflow.get_or_insert_default()
    }

    fn overflow_mut(&mut self) -> &mut OverflowWaiters {
        self.overflow
            .as_mut()
            .expect("overflow registration must have allocated waiter storage")
    }

    fn wake_all(self) {
        match self.inline {
            InlineSlot::Observer(waker) | InlineSlot::Initializer(waker) => waker.wake(),
            InlineSlot::Empty | InlineSlot::Granted => {}
        }
        if let Some(overflow) = self.overflow {
            for waiter in overflow.initializers.into_values() {
                if let OverflowInitWaiter::Waiting(waker) = waiter {
                    waker.wake();
                }
            }
            overflow.observers.wake_all();
        }
    }
}

pub struct OnceState {
    state: AtomicU8,
    waiters: Mutex<Waiters>,
}

impl OnceState {
    pub const fn new(completed: bool) -> Self {
        Self {
            state: AtomicU8::new(if completed { COMPLETE } else { UNINIT }),
            waiters: Mutex::new(Waiters::new()),
        }
    }

    #[inline]
    pub fn is_completed(&self) -> bool {
        self.state.load(Ordering::Acquire) == COMPLETE
    }

    #[inline]
    pub fn complete_mut(&mut self) {
        let prev = mem::replace(self.state.get_mut(), COMPLETE);
        if prev & HAS_WAITERS != 0 {
            mem::take(self.waiters.get_mut()).wake_all();
        }
    }

    #[inline]
    pub fn reset_mut(&mut self) {
        let prev = mem::replace(self.state.get_mut(), UNINIT);
        if prev & HAS_WAITERS != 0 {
            drop(mem::take(self.waiters.get_mut()));
        }
    }

    pub fn acquire_init(&self) -> impl Future<Output = Option<InitGuard<'_>>> + '_ {
        AcquireInit {
            once: Some(self),
            registration: InitRegistration::Unregistered,
        }
    }

    pub fn wait(&self) -> impl Future<Output = ()> + '_ {
        OnceWait {
            once: self,
            registration: ObserverRegistration::Unregistered,
        }
    }

    #[inline]
    fn try_begin_init(&self) -> TryBegin {
        let mut s = self.state.load(Ordering::Acquire);
        loop {
            if s == COMPLETE {
                return TryBegin::Completed;
            }
            if s & RUNNING != 0 {
                return TryBegin::Contended;
            }
            match self.state.compare_exchange_weak(
                s,
                s | RUNNING,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => return TryBegin::Leading,
                Err(actual) => s = actual,
            }
        }
    }

    #[inline]
    fn complete_init(&self) {
        if self
            .state
            .compare_exchange(RUNNING, COMPLETE, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }

        self.complete_init_slow();
    }

    #[cold]
    fn complete_init_slow(&self) {
        let mut waiters = self.waiters.lock();
        let drained = mem::take(&mut *waiters);
        self.state.store(COMPLETE, Ordering::Release);
        drop(waiters);
        drained.wake_all();
    }

    #[inline]
    fn abort_init(&self) {
        if self
            .state
            .compare_exchange(RUNNING, UNINIT, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }

        self.abort_init_slow();
    }

    #[cold]
    fn abort_init_slow(&self) {
        let mut waiters = self.waiters.lock();
        let (next_waker, retired_overflow) = self.pass_or_release_leadership(&mut waiters);
        drop(waiters);
        drop(retired_overflow);
        if let Some(waker) = next_waker {
            waker.wake();
        }
    }

    fn pass_or_release_leadership(
        &self,
        waiters: &mut Waiters,
    ) -> (Option<Waker>, Option<Box<OverflowWaiters>>) {
        if matches!(waiters.inline, InlineSlot::Initializer(_)) {
            let InlineSlot::Initializer(waker) =
                mem::replace(&mut waiters.inline, InlineSlot::Granted)
            else {
                unreachable!();
            };
            return (Some(waker), None);
        }

        if let Some(overflow) = waiters.overflow.as_mut() {
            if let Some((_, slot)) = overflow.initializers.unlink_first_waiter() {
                let OverflowInitWaiter::Waiting(waker) =
                    mem::replace(slot, OverflowInitWaiter::Granted)
                else {
                    unreachable!("only waiting initializers remain linked");
                };
                return (Some(waker), None);
            }
        }

        // `RUNNING` is currently held by the caller and `waiters` is locked, so no other thread
        // can transition `self.state` concurrently.
        if waiters.is_unused() {
            let retired_overflow = waiters.overflow.take();
            self.state.store(UNINIT, Ordering::Release);
            (None, retired_overflow)
        } else {
            self.state.store(HAS_WAITERS, Ordering::Release);
            (None, None)
        }
    }

    #[must_use = "drop any retired overflow storage after releasing the waiter lock"]
    fn reclaim_if_unused(&self, waiters: &mut Waiters) -> Option<Box<OverflowWaiters>> {
        if waiters.is_unused() {
            self.clear_has_waiters();
            waiters.overflow.take()
        } else {
            None
        }
    }

    fn clear_has_waiters(&self) {
        let mut s = self.state.load(Ordering::Relaxed);
        while s & HAS_WAITERS != 0 && s != COMPLETE {
            match self.state.compare_exchange_weak(
                s,
                s & !HAS_WAITERS,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => s = actual,
            }
        }
    }
}

pub struct InitGuard<'a> {
    once: &'a OnceState,
}

impl InitGuard<'_> {
    #[inline]
    pub fn complete(self) {
        let once = self.once;
        mem::forget(self);
        once.complete_init();
    }
}

impl Drop for InitGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.once.abort_init();
    }
}

#[derive(Default)]
enum InitRegistration {
    #[default]
    Unregistered,
    Inline,
    Overflow(WaiterId),
}

struct AcquireInit<'a> {
    once: Option<&'a OnceState>,
    registration: InitRegistration,
}

impl<'a> Future for AcquireInit<'a> {
    type Output = Option<InitGuard<'a>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let once = this
            .once
            .expect("Once initialization polled after completion");

        if once.is_completed() {
            this.once = None;
            this.registration = InitRegistration::Unregistered;
            return Poll::Ready(None);
        }

        if matches!(this.registration, InitRegistration::Unregistered) {
            match once.try_begin_init() {
                TryBegin::Completed => {
                    this.once = None;
                    return Poll::Ready(None);
                }
                TryBegin::Leading => {
                    this.once = None;
                    return Poll::Ready(Some(InitGuard { once }));
                }
                TryBegin::Contended => {}
            }
        }

        let mut waiters = once.waiters.lock();
        if once.is_completed() {
            this.once = None;
            this.registration = InitRegistration::Unregistered;
            return Poll::Ready(None);
        }

        match &this.registration {
            InitRegistration::Inline => match &mut waiters.inline {
                InlineSlot::Granted => {
                    waiters.inline = InlineSlot::Empty;
                    let retired_overflow = once.reclaim_if_unused(&mut waiters);
                    drop(waiters);
                    drop(retired_overflow);
                    this.once = None;
                    this.registration = InitRegistration::Unregistered;
                    return Poll::Ready(Some(InitGuard { once }));
                }
                InlineSlot::Initializer(current) => {
                    let retired = update_waker(current, cx.waker());
                    drop(waiters);
                    drop(retired);
                    return Poll::Pending;
                }
                _ => unreachable!("inline initializer registration must occupy the inline slot"),
            },
            InitRegistration::Overflow(id) => {
                let id = *id;
                match waiters.overflow_mut().initializers.waiter_mut(id) {
                    OverflowInitWaiter::Granted => {
                        waiters
                            .overflow_mut()
                            .initializers
                            .remove_unlinked_waiter(id);
                        let retired_overflow = once.reclaim_if_unused(&mut waiters);
                        drop(waiters);
                        drop(retired_overflow);
                        this.once = None;
                        this.registration = InitRegistration::Unregistered;
                        return Poll::Ready(Some(InitGuard { once }));
                    }
                    OverflowInitWaiter::Waiting(current) => {
                        let retired = update_waker(current, cx.waker());
                        drop(waiters);
                        drop(retired);
                        return Poll::Pending;
                    }
                }
            }
            InitRegistration::Unregistered => {}
        }

        let mut s = once.state.load(Ordering::Acquire);
        loop {
            if s == COMPLETE {
                this.once = None;
                return Poll::Ready(None);
            }
            if s & RUNNING == 0 {
                match once.state.compare_exchange_weak(
                    s,
                    s | RUNNING,
                    Ordering::Acquire,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        this.once = None;
                        return Poll::Ready(Some(InitGuard { once }));
                    }
                    Err(actual) => {
                        s = actual;
                        continue;
                    }
                }
            }
            if s & HAS_WAITERS != 0 {
                break;
            }
            match once.state.compare_exchange_weak(
                s,
                s | HAS_WAITERS,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => s = actual,
            }
        }

        if matches!(waiters.inline, InlineSlot::Empty) && waiters.has_no_overflow_initializers() {
            waiters.inline = InlineSlot::Initializer(cx.waker().clone());
            this.registration = InitRegistration::Inline;
        } else {
            let id = waiters
                .overflow_or_init()
                .initializers
                .push_back(OverflowInitWaiter::Waiting(cx.waker().clone()));
            this.registration = InitRegistration::Overflow(id);
        }
        drop(waiters);
        Poll::Pending
    }
}

impl Drop for AcquireInit<'_> {
    fn drop(&mut self) {
        let Some(once) = self.once else {
            return;
        };
        if matches!(self.registration, InitRegistration::Unregistered) {
            return;
        }
        if once.is_completed() {
            return;
        }

        let mut waiters = once.waiters.lock();
        if once.is_completed() {
            return;
        }

        let mut removed_waker = None;
        let mut was_granted = false;
        match mem::take(&mut self.registration) {
            InitRegistration::Unregistered => unreachable!(),
            InitRegistration::Inline => match mem::take(&mut waiters.inline) {
                InlineSlot::Initializer(waker) => removed_waker = Some(waker),
                InlineSlot::Granted => was_granted = true,
                _ => unreachable!("inline initializer registration must occupy the inline slot"),
            },
            InitRegistration::Overflow(id) => {
                match waiters.overflow_mut().initializers.remove(id) {
                    OverflowInitWaiter::Waiting(waker) => removed_waker = Some(waker),
                    OverflowInitWaiter::Granted => was_granted = true,
                }
            }
        }

        let (next_waker, retired_overflow) = if was_granted {
            once.pass_or_release_leadership(&mut waiters)
        } else {
            (None, once.reclaim_if_unused(&mut waiters))
        };
        drop(waiters);
        drop(removed_waker);
        drop(retired_overflow);
        if let Some(waker) = next_waker {
            waker.wake();
        }
    }
}

#[derive(Default)]
enum ObserverRegistration {
    #[default]
    Unregistered,
    Inline,
    Overflow(WakerToken),
}

struct OnceWait<'a> {
    once: &'a OnceState,
    registration: ObserverRegistration,
}

impl Future for OnceWait<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let Self { once, registration } = self.get_mut();
        if once.is_completed() {
            *registration = ObserverRegistration::Unregistered;
            return Poll::Ready(());
        }

        let mut waiters = once.waiters.lock();
        if once.is_completed() {
            *registration = ObserverRegistration::Unregistered;
            return Poll::Ready(());
        }

        let mut s = once.state.load(Ordering::Acquire);
        while s & HAS_WAITERS == 0 {
            if s == COMPLETE {
                *registration = ObserverRegistration::Unregistered;
                return Poll::Ready(());
            }
            match once.state.compare_exchange_weak(
                s,
                s | HAS_WAITERS,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => s = actual,
            }
        }

        let retired_waker = match registration {
            ObserverRegistration::Unregistered if matches!(waiters.inline, InlineSlot::Empty) => {
                waiters.inline = InlineSlot::Observer(cx.waker().clone());
                *registration = ObserverRegistration::Inline;
                None
            }
            ObserverRegistration::Unregistered => {
                let token = waiters.overflow_or_init().observers.insert(cx.waker());
                *registration = ObserverRegistration::Overflow(token);
                None
            }
            ObserverRegistration::Inline => match &mut waiters.inline {
                InlineSlot::Observer(current) => update_waker(current, cx.waker()),
                _ => unreachable!("inline observer registration must occupy the inline slot"),
            },
            ObserverRegistration::Overflow(token) => {
                waiters.overflow_mut().observers.update(token, cx.waker())
            }
        };
        drop(waiters);
        drop(retired_waker);
        Poll::Pending
    }
}

impl Drop for OnceWait<'_> {
    fn drop(&mut self) {
        if matches!(self.registration, ObserverRegistration::Unregistered) {
            return;
        }
        if self.once.is_completed() {
            return;
        }

        let mut waiters = self.once.waiters.lock();
        if self.once.is_completed() {
            return;
        }

        let removed_waker = match mem::take(&mut self.registration) {
            ObserverRegistration::Unregistered => unreachable!(),
            ObserverRegistration::Inline => match mem::take(&mut waiters.inline) {
                InlineSlot::Observer(waker) => waker,
                _ => unreachable!("inline observer registration must occupy the inline slot"),
            },
            ObserverRegistration::Overflow(token) => waiters.overflow_mut().observers.remove(token),
        };
        let retired_overflow = self.once.reclaim_if_unused(&mut waiters);
        drop(waiters);
        drop(removed_waker);
        drop(retired_overflow);
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use super::*;

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn reclaims_overflow_storage_when_all_waiters_leave() {
        let once = OnceState::new(false);

        // Observer burst cancelling before any initializer runs must release overflow storage.
        let mut observers = (0..8).map(|_| Box::pin(once.wait())).collect::<Vec<_>>();
        for observer in &mut observers {
            assert!(poll_once(observer.as_mut()).is_pending());
        }
        assert!(once.waiters.lock().overflow.is_some());
        drop(observers);
        assert_eq!(once.state.load(Ordering::Acquire), UNINIT);
        assert!(once.waiters.lock().overflow.is_none());

        // Overflow candidate claiming a grant as the last waiter must also release overflow storage
        // before completion takes the lock-free fast path.
        let Poll::Ready(Some(first_guard)) = poll_once(pin!(once.acquire_init())) else {
            panic!("first caller should acquire leadership");
        };
        let mut inline_candidate = Box::pin(once.acquire_init());
        let mut overflow_candidate = pin!(once.acquire_init());
        assert!(poll_once(inline_candidate.as_mut()).is_pending());
        assert!(poll_once(overflow_candidate.as_mut()).is_pending());
        assert!(once.waiters.lock().overflow.is_some());

        drop(inline_candidate);
        drop(first_guard);
        let Poll::Ready(Some(second_guard)) = poll_once(overflow_candidate.as_mut()) else {
            panic!("remaining candidate should receive leadership");
        };
        assert_eq!(once.state.load(Ordering::Acquire), RUNNING);
        assert!(once.waiters.lock().overflow.is_none());

        second_guard.complete();
        assert!(once.is_completed());
        assert!(once.waiters.lock().overflow.is_none());
    }
}
