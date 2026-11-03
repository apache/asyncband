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
use std::mem;
use std::panic::RefUnwindSafe;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::wakerset::WakerSet;
use crate::internal::wakerset::WakerToken;

const EMPTY: u8 = 0;
const REGISTERED: u8 = 1;
const BUSY: u8 = 2;
const CLOSED: u8 = 4;
const OVERFLOW: u8 = 8;

pub enum Registration {
    First,
    Set(Option<WakerToken>),
}

impl Default for Registration {
    fn default() -> Self {
        Self::Set(None)
    }
}

#[derive(Default)]
pub struct Waiters {
    // EMPTY and REGISTERED can be claimed as BUSY. Closing is permanent. When the closer
    // sees BUSY it leaves notification to the registrar instead of waiting for it.
    phase: AtomicU8,
    first: UnsafeCell<Option<Waker>>,
    extra: OnceLock<Box<Mutex<WakerSet>>>,
}

// SAFETY: the BUSY owner or the thread that closes a REGISTERED slot has exclusive access.
unsafe impl Sync for Waiters {}

// A panic in user work never exposes the slot. Executor waker operations must not panic.
impl RefUnwindSafe for Waiters {}

impl Waiters {
    fn extra(&self) -> &Mutex<WakerSet> {
        self.extra
            .get_or_init(|| Box::new(Mutex::new(WakerSet::new())))
    }

    fn claim(&self, expected: u8, mut phase: u8) -> bool {
        while phase & !OVERFLOW == expected {
            match self.phase.compare_exchange_weak(
                phase,
                BUSY | (phase & OVERFLOW),
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => phase = actual,
            }
        }
        false
    }

    pub fn register(&self, token: &mut Registration, waker: &Waker) -> Poll<()> {
        let phase = self.phase.load(Ordering::Acquire);
        match token {
            Registration::First if !self.claim(REGISTERED, phase) => {
                // Only this observer can change its registration. A failed claim observed CLOSED.
                *token = Registration::default();
                return Poll::Ready(());
            }
            Registration::First => {}
            Registration::Set(None) if self.claim(EMPTY, phase) => {}
            Registration::Set(id) => return self.register_extra(id, waker, phase),
        }

        // SAFETY: only the BUSY owner can access the cell until it publishes REGISTERED.
        let slot = unsafe { &mut *self.first.get() };
        let retired = if slot.as_ref().is_some_and(|old| old.will_wake(waker)) {
            None
        } else {
            slot.replace(waker.clone())
        };
        *token = Registration::First;
        if self.phase.fetch_sub(BUSY - REGISTERED, Ordering::AcqRel) & CLOSED != 0 {
            // The closer saw BUSY and delegated the slot to us. It will never be reused.
            // SAFETY: closing BUSY left exclusive access with this registrar.
            let removed = unsafe { &mut *self.first.get() }.take();
            *token = Registration::Set(None);
            if let Some(waker) = removed {
                waker.wake();
            }
            drop(retired);
            Poll::Ready(())
        } else {
            drop(retired);
            Poll::Pending
        }
    }

    fn register_extra(&self, id: &mut Option<WakerToken>, waker: &Waker, phase: u8) -> Poll<()> {
        // Announce overflow before taking its lock. Closing either sees this bit and takes the
        // same lock, or wins this RMW and prevents registration.
        let phase = if phase & OVERFLOW == 0 {
            self.phase.fetch_or(OVERFLOW, Ordering::AcqRel)
        } else {
            phase
        };
        if phase & CLOSED != 0 {
            *id = None;
            return Poll::Ready(());
        }
        let mut set = self.extra().lock();
        if self.phase.load(Ordering::Acquire) & CLOSED != 0 {
            *id = None;
            return Poll::Ready(());
        }
        let retired = set.register(id, waker);
        drop(set);
        drop(retired);
        Poll::Pending
    }

    pub fn unregister(&self, token: &mut Registration) {
        match mem::take(token) {
            Registration::Set(None) => {}
            Registration::First if self.claim(REGISTERED, self.phase.load(Ordering::Acquire)) => {
                // SAFETY: we own BUSY. Release the cell before dropping its waker.
                let removed = unsafe { &mut *self.first.get() }.take();
                self.phase.fetch_sub(BUSY, Ordering::Release);
                drop(removed);
            }
            Registration::First => {}
            Registration::Set(mut id) => {
                let mut set = self.extra().lock();
                if self.phase.load(Ordering::Acquire) & CLOSED == 0 {
                    let removed = set.unregister(&mut id);
                    drop(set);
                    drop(removed);
                }
            }
        }
    }

    pub fn wake_all(&self) {
        let previous = self.phase.fetch_or(CLOSED, Ordering::AcqRel);
        if previous & !OVERFLOW == REGISTERED {
            // SAFETY: closing REGISTERED takes the slot and permanently prevents further claims.
            if let Some(waker) = unsafe { &mut *self.first.get() }.take() {
                waker.wake();
            }
        }
        if previous & OVERFLOW != 0 {
            let wakers = mem::take(&mut *self.extra().lock());
            wakers.wake_all();
        }
    }
}
