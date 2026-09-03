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

use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use crate::internal::mutex::Mutex;
use crate::internal::wake_all;
use crate::internal::wakerset::WakerSet;
use crate::internal::wakerset::WakerToken;

#[derive(Debug)]
pub struct CountdownState {
    state: AtomicU32,
    waiters: Mutex<WakerSet>,
}

impl CountdownState {
    pub const fn new(count: u32) -> Self {
        Self {
            state: AtomicU32::new(count),
            waiters: Mutex::new(WakerSet::new()),
        }
    }

    /// Loads the current count, acquiring state published before a transition to zero.
    pub fn state(&self) -> u32 {
        self.state.load(Ordering::Acquire)
    }

    /// Attempts to replace `current` with `new`, publishing the new count on success.
    ///
    /// A spurious or contended failure returns the observed count so the caller can retry.
    fn cas_state(&self, current: u32, new: u32) -> Result<(), u32> {
        self.state
            .compare_exchange_weak(current, new, Ordering::Release, Ordering::Relaxed)
            .map(|_| ())
    }

    /// Drains the waiter set under its lock, then wakes every waiter after releasing the lock.
    pub fn wake_all(&self) {
        let wakers = {
            let mut waiters = self.waiters.lock();
            waiters.take_all()
        };

        wake_all(wakers);
    }

    /// Polls for zero, registering the current waker if the countdown is still active.
    pub fn poll_wait(&self, token: &mut Option<WakerToken>, cx: &mut Context<'_>) -> Poll<()> {
        if self.try_wait().is_ok() {
            // The zero transition owns detaching every registration. Avoid taking the waiter lock
            // again when the completed future is dropped.
            *token = None;
            return Poll::Ready(());
        }

        let retired_waker = {
            let mut waiters = self.waiters.lock();
            if self.state() == 0 {
                // A concurrent zero transition will drain after this lock is released.
                *token = None;
                return Poll::Ready(());
            }
            waiters.register(token, cx.waker())
        };
        drop(retired_waker);
        Poll::Pending
    }

    #[inline]
    pub fn unregister(&self, token: &mut Option<WakerToken>) {
        if token.is_some() {
            let removed_waker = {
                let mut waiters = self.waiters.lock();
                if self.state() == 0 {
                    // The terminal zero transition owns this registration or has already detached
                    // it. No later countdown generation can reuse its slot.
                    *token = None;
                    None
                } else {
                    waiters.unregister(token)
                }
            };
            drop(removed_waker);
        }
    }

    /// Returns `Ok(())` if the counter is zero, otherwise returns the current counter value.
    pub fn try_wait(&self) -> Result<(), u32> {
        match self.state() {
            0 => Ok(()),
            s => Err(s),
        }
    }

    /// Decrements the counter by `n`, returning whether the caller should wake up all waiters.
    pub fn decrement(&self, n: u32) -> bool {
        let mut cnt = self.state();
        loop {
            if cnt == 0 {
                // Only the operation that performs the transition to zero owns waiter notification.
                return false;
            }

            let new_cnt = cnt.saturating_sub(n);
            match self.cas_state(cnt, new_cnt) {
                Ok(_) => return new_cnt == 0,
                Err(x) => cnt = x,
            }
        }
    }
}
