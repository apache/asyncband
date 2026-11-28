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

//! Generation-tracked broadcast wake coordinator for async tasks and synchronous in-flight waits.

use std::mem;
use std::num::NonZeroUsize;
use std::process;
use std::sync::Condvar;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Waker;

use crate::broadcast::mpmc::unbounded::queue::CounterU64;
use crate::internal::mutex::Mutex;
use crate::internal::register_waker;
use crate::internal::waker_batch::WakerBatch;
use crate::internal::wakerset::WakerSet;
use crate::internal::wakerset::WakerToken;

const SLOT_NONE: usize = 0;
const SLOT_INLINE: usize = usize::MAX;

/// Opaque 2-word registration handle owned by a broadcast subscriber.
pub struct WakeToken {
    wake_gen: u64,
    slot: usize,
}

impl WakeToken {
    pub const fn new() -> Self {
        Self {
            wake_gen: 0,
            slot: SLOT_NONE,
        }
    }
}

struct Waiters {
    slot0: Option<Waker>,
    overflow: WakerSet,
    sync_seq: u64,
    sync_active: usize,
}

impl Waiters {
    const fn new() -> Self {
        Self {
            slot0: None,
            overflow: WakerSet::new(),
            sync_seq: 0,
            sync_active: 0,
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.slot0.is_none() && self.overflow.is_empty() && self.sync_active == 0
    }

    #[inline]
    fn overflow_token(raw: usize) -> WakerToken {
        WakerToken::from_raw(NonZeroUsize::new(raw).expect("overflow token must be non-zero"))
    }

    #[inline]
    #[must_use = "drop the returned waker after releasing the waiters lock"]
    fn register(&mut self, token: &mut WakeToken, cur_gen: u64, waker: &Waker) -> Option<Waker> {
        if token.slot != SLOT_NONE && token.wake_gen == cur_gen {
            return if token.slot == SLOT_INLINE {
                register_waker(&mut self.slot0, waker)
            } else {
                self.overflow
                    .update(&Self::overflow_token(token.slot), waker)
            };
        }
        token.wake_gen = cur_gen;
        if self.slot0.is_none() {
            self.slot0 = Some(waker.clone());
            token.slot = SLOT_INLINE;
        } else {
            token.slot = self.overflow.insert(waker).into_raw().get();
        }
        None
    }

    #[inline]
    #[must_use = "drop the returned waker after releasing the waiters lock"]
    fn remove(&mut self, token: &mut WakeToken, cur_gen: u64) -> Option<Waker> {
        let prev = mem::replace(&mut token.slot, SLOT_NONE);
        if prev == SLOT_NONE || token.wake_gen != cur_gen {
            None
        } else if prev == SLOT_INLINE {
            self.slot0.take()
        } else {
            Some(self.overflow.remove(Self::overflow_token(prev)))
        }
    }
}

pub struct BroadcastWake {
    has_waiters: AtomicBool,
    wake_gen: CounterU64,
    waiters: Mutex<Waiters>,
    sync_condvar: Condvar,
}

impl BroadcastWake {
    pub const fn new() -> Self {
        Self {
            has_waiters: AtomicBool::new(false),
            wake_gen: CounterU64::new(0),
            waiters: Mutex::new(Waiters::new()),
            sync_condvar: Condvar::new(),
        }
    }

    #[inline(always)]
    pub fn notify_if_waiting(&self) {
        if self.has_waiters.load(Ordering::SeqCst) {
            self.notify_all_cold();
        }
    }

    #[inline(always)]
    pub fn clear_async(&self, token: &mut WakeToken) {
        if token.slot != SLOT_NONE {
            if token.wake_gen != self.wake_gen.load(Ordering::Acquire) {
                token.slot = SLOT_NONE;
            } else {
                self.clear_async_locked(token);
            }
        }
    }

    #[inline(never)]
    pub fn park_async(
        &self,
        token: &mut WakeToken,
        cx: &mut Context<'_>,
        can_proceed: impl FnOnce() -> bool,
    ) -> bool {
        let mut waiters = self.waiters.lock();
        let cur_gen = self.wake_gen.load(Ordering::Relaxed);
        let retired = waiters.register(token, cur_gen, cx.waker());
        self.has_waiters.store(true, Ordering::SeqCst);
        let proceed = can_proceed();
        let cleared = if proceed {
            let removed = waiters.remove(token, cur_gen);
            if waiters.is_empty() {
                self.has_waiters.store(false, Ordering::Release);
            }
            removed
        } else {
            None
        };
        drop(waiters);
        drop((retired, cleared));
        proceed
    }

    #[cold]
    #[inline(never)]
    pub fn park_sync<R>(&self, mut poll_ready: impl FnMut() -> Option<R>) -> R {
        struct SyncGuard<'a>(&'a BroadcastWake);
        impl Drop for SyncGuard<'_> {
            fn drop(&mut self) {
                let mut waiters = self.0.waiters.lock();
                waiters.sync_active -= 1;
                if waiters.is_empty() {
                    self.0.has_waiters.store(false, Ordering::Release);
                }
            }
        }

        let mut seen = {
            let mut waiters = self.waiters.lock();
            waiters.sync_active += 1;
            self.has_waiters.store(true, Ordering::SeqCst);
            waiters.sync_seq
        };
        let _guard = SyncGuard(self);
        loop {
            if let Some(ready) = poll_ready() {
                return ready;
            }
            let mut waiters = self.waiters.lock();
            if waiters.sync_seq == seen {
                waiters = self
                    .sync_condvar
                    .wait(waiters)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            seen = waiters.sync_seq;
        }
    }

    #[cold]
    #[inline(never)]
    fn clear_async_locked(&self, token: &mut WakeToken) {
        let removed = {
            let mut waiters = self.waiters.lock();
            let removed = waiters.remove(token, self.wake_gen.load(Ordering::Relaxed));
            if waiters.is_empty() {
                self.has_waiters.store(false, Ordering::Release);
            }
            removed
        };
        drop(removed);
    }

    #[inline]
    fn finish_notify_locked(&self, waiters: &Waiters, bump_gen: bool) {
        if bump_gen {
            let Some(next) = self.wake_gen.load(Ordering::Relaxed).checked_add(1) else {
                process::abort();
            };
            self.wake_gen.store(next, Ordering::Release);
        }
        if waiters.is_empty() {
            self.has_waiters.store(false, Ordering::Release);
        }
    }

    #[inline(never)]
    fn notify_all_cold(&self) {
        let mut waiters = self.waiters.lock();
        let wake_sync = waiters.sync_active != 0;
        if wake_sync {
            waiters.sync_seq = waiters.sync_seq.wrapping_add(1);
        }
        if waiters.overflow.is_empty() {
            let waker = waiters.slot0.take();
            self.finish_notify_locked(&waiters, waker.is_some());
            drop(waiters);
            // Wake synchronous in-flight waiters ahead of external async `Waker::wake` callbacks.
            if wake_sync {
                self.sync_condvar.notify_all();
            }
            if let Some(w) = waker {
                w.wake();
            }
            return;
        }
        let mut batch = WakerBatch::new();
        if let Some(w0) = waiters.slot0.take() {
            batch.push(w0);
        }
        waiters.overflow.drain_into(&mut batch);
        self.finish_notify_locked(&waiters, true);
        drop(waiters);
        if wake_sync {
            self.sync_condvar.notify_all();
        }
        batch.wake_all();
    }

    #[cfg(test)]
    pub fn has_waiters(&self) -> bool {
        self.has_waiters.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub fn sync_waiter_count(&self) -> usize {
        self.waiters.lock().sync_active
    }
}
