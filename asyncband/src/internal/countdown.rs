// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::task::Context;

use crate::internal::Mutex;
use crate::internal::WaitSet;

#[derive(Debug)]
pub(crate) struct CountdownState {
    state: AtomicU32,
    waiters: Mutex<WaitSet>,
}

impl CountdownState {
    pub(crate) const fn new(count: u32) -> Self {
        Self {
            state: AtomicU32::new(count),
            waiters: Mutex::new(WaitSet::new()),
        }
    }

    /// Performs volatile read on `state`.
    ///
    /// All other writes to `state` should be at least [`Ordering::Release`].
    pub(crate) fn state(&self) -> u32 {
        self.state.load(Ordering::Acquire)
    }

    /// Performs volatile CAS on `state`.
    ///
    /// If the comparison succeeds, performs read-modify-write operation with [`Ordering::Relaxed`]
    /// for read, and [`Ordering::Release`] for write; if the comparison fails, performs load
    /// operation with [`Ordering::Relaxed`].
    ///
    /// @see https://doc.rust-lang.org/std/sync/atomic/struct.AtomicU32.html#method.compare_exchange_weak
    /// @see https://en.cppreference.com/w/cpp/atomic/atomic_compare_exchange
    fn cas_state(&self, current: u32, new: u32) -> Result<(), u32> {
        self.state
            .compare_exchange_weak(current, new, Ordering::Release, Ordering::Relaxed)
            .map(|_| ())
    }

    /// Drain and wake up all waiters.
    pub(crate) fn wake_all(&self) {
        let wakers = {
            let mut waiters = self.waiters.lock();
            waiters.take_wakers()
        };

        for waker in wakers {
            waker.wake();
        }
    }

    /// Registers a waker to be woken up when the countdown reaches zero.
    ///
    /// `idx` must be `None` when the waker is not registered, or `Some(key)` where `key` is
    /// a value previously returned by this method.
    pub(crate) fn register_waker(&self, idx: &mut Option<usize>, cx: &mut Context<'_>) {
        let mut waiters = self.waiters.lock();
        waiters.register_waker(idx, cx);
    }

    /// Returns `Ok(())` if the counter is zero, otherwise returns `Err(s)` where `s` is the current
    /// counter value.
    pub(crate) fn spin_wait(&self, n: usize) -> Result<(), u32> {
        for _ in 0..n {
            if self.state() == 0 {
                return Ok(());
            }
            std::hint::spin_loop();
        }

        match self.state() {
            0 => Ok(()),
            s => Err(s),
        }
    }

    /// Increments the counter by `n`.
    ///
    /// Returns `true` without changing the counter if the operation would overflow.
    pub(crate) fn increment(&self, n: u32) -> bool {
        let mut cnt = self.state();
        loop {
            let Some(new_cnt) = cnt.checked_add(n) else {
                return true;
            };
            match self.cas_state(cnt, new_cnt) {
                Ok(_) => return false,
                Err(x) => cnt = x,
            }
        }
    }

    /// Decrements the counter by `n`, returning whether the caller should wake up all waiters.
    pub(crate) fn decrement(&self, n: u32) -> bool {
        let mut cnt = self.state();
        loop {
            if cnt == 0 {
                // the one who decrements the counter to zero should wake up all waiters, not this
                // one
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn increment_reports_overflow_without_changing_state() {
        let state = CountdownState::new(u32::MAX);

        assert!(state.increment(1));
        assert_eq!(state.state(), u32::MAX);
    }
}
