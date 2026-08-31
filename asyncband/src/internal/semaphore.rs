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

use std::future::Future;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::ptr;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;

/// The internal semaphore that provides low-level async primitives.
#[derive(Debug)]
pub struct Semaphore {
    /// The current number of available permits in the semaphore.
    permits: AtomicUsize,
    waiters: Mutex<WaitList<WaitNode>>,
}

#[derive(Debug)]
struct WaitNode {
    permits: usize,
    /// A linked node without a waker is permit debt owned by the queue. An acquire node only loses
    /// its waker while being detached, after which its future still owns the node.
    waker: Option<Waker>,
}

const WAKE_BATCH_SIZE: usize = 32;

/// The initialized entries in `wakers` are exactly `start..end`.
struct WakeBatch {
    wakers: [MaybeUninit<Waker>; WAKE_BATCH_SIZE],
    start: usize,
    end: usize,
}

impl WakeBatch {
    fn new() -> Self {
        const UNINIT: MaybeUninit<Waker> = MaybeUninit::uninit();
        Self {
            wakers: [UNINIT; WAKE_BATCH_SIZE],
            start: 0,
            end: 0,
        }
    }

    fn push(&mut self, waker: Waker) {
        debug_assert_eq!(self.start, 0);
        debug_assert!(self.end < WAKE_BATCH_SIZE);
        self.wakers[self.end].write(waker);
        self.end += 1;
    }

    fn is_full(&self) -> bool {
        self.end == WAKE_BATCH_SIZE
    }

    fn wake_all(&mut self) {
        while self.start < self.end {
            let index = self.start;
            self.start += 1;
            // SAFETY: `index` was within the initialized range before advancing `start`.
            unsafe { self.wakers[index].assume_init_read() }.wake();
        }
        self.start = 0;
        self.end = 0;
    }
}

impl Drop for WakeBatch {
    fn drop(&mut self) {
        let start = self.wakers[self.start..self.end]
            .as_mut_ptr()
            .cast::<Waker>();
        let remaining = ptr::slice_from_raw_parts_mut(start, self.end - self.start);
        // SAFETY: The initialized entries are exactly `start..end`.
        unsafe { ptr::drop_in_place(remaining) };
    }
}

impl Semaphore {
    pub const fn new(permits: usize) -> Self {
        Self {
            permits: AtomicUsize::new(permits),
            waiters: Mutex::new(WaitList::new()),
        }
    }

    /// Returns the current number of available permits.
    pub fn available_permits(&self) -> usize {
        self.permits.load(Ordering::Acquire)
    }

    /// Tries to acquire `n` permits from the semaphore.
    ///
    /// Returns `true` if the permits were acquired, `false` otherwise.
    pub fn try_acquire(&self, n: usize) -> bool {
        let mut current = self.permits.load(Ordering::Acquire);
        loop {
            if current < n {
                return false;
            }

            let next = current - n;
            match self
                .permits
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Drains up to `up_to` permits that are currently available.
    ///
    /// Returns the number of permits that were actually drained.
    pub fn drain_permits(&self, up_to: usize) -> usize {
        if up_to == 0 {
            return 0;
        }

        let mut current = self.permits.load(Ordering::Acquire);
        loop {
            let new = current.saturating_sub(up_to);
            match self.permits.compare_exchange_weak(
                current,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return up_to.min(current),
                Err(actual) => current = actual,
            }
        }
    }

    /// Reduces the semaphore's logical permit balance by exactly `n`.
    ///
    /// If fewer than `n` permits are available, a queue-head debt node consumes future releases.
    pub fn reduce_permits(&self, n: usize) {
        acquired_or_enqueue(self, n, None, None, false);
    }

    /// Acquires `n` permits from the semaphore.
    pub async fn acquire(&self, n: usize) {
        let fut = Acquire {
            permits: n,
            index: None,
            semaphore: self,
            done: false,
        };
        fut.await
    }

    /// Returns a future that is resolved when acquired `n` permits from the semaphore.
    pub fn poll_acquire(&self, n: usize) -> Acquire<'_> {
        Acquire {
            permits: n,
            index: None,
            semaphore: self,
            done: false,
        }
    }

    /// Adds `n` permits to the semaphore.
    pub fn release(&self, n: usize) {
        if n != 0 {
            self.insert_permits_with_lock(n, self.waiters.lock());
        }
    }

    /// Adds `n` permits to the semaphore if there is any waiter.
    pub fn release_if_nonempty(&self, n: usize) {
        let waiters = self.waiters.lock();
        if !waiters.is_empty() {
            self.insert_permits_with_lock(n, waiters);
        }
    }

    /// Adds as many permits until there is no waiter.
    pub fn notify_all(&self) {
        let mut waiters = self.waiters.lock();
        let mut wakers = vec![];
        loop {
            match waiters.unlink_first_waiter(|node| {
                node.permits = 0;
                true
            }) {
                None => break,
                Some((id, waiter)) => {
                    let remove_now = waiter.waker.is_none();
                    if let Some(waker) = waiter.waker.take() {
                        wakers.push(waker);
                    }
                    if remove_now {
                        waiters.remove_unlinked_waiter(id);
                    }
                }
            }
        }
        drop(waiters);
        for waker in wakers {
            waker.wake();
        }
    }

    fn insert_permits_with_lock(
        &self,
        mut rem: usize,
        waiters: MutexGuard<'_, WaitList<WaitNode>>,
    ) {
        let mut wakers = WakeBatch::new();

        let mut lock = Some(waiters);
        while rem > 0 {
            let mut waiters = lock.take().unwrap_or_else(|| self.waiters.lock());
            while !wakers.is_full() {
                match waiters.unlink_first_waiter(|node| {
                    if node.permits <= rem {
                        rem -= node.permits;
                        node.permits = 0;
                        true
                    } else {
                        node.permits -= rem;
                        rem = 0;
                        false
                    }
                }) {
                    None => break,
                    Some((id, waiter)) => {
                        let remove_now = waiter.waker.is_none();
                        if let Some(waker) = waiter.waker.take() {
                            wakers.push(waker);
                        }
                        if remove_now {
                            waiters.remove_unlinked_waiter(id);
                        }
                    }
                }
            }

            if rem > 0 && waiters.is_empty() {
                // Holding `waiters` serializes all permit additions. Concurrent operations can
                // only remove permits, so the count cannot grow between this check and fetch_add.
                let current = self.permits.load(Ordering::Relaxed);
                assert!(
                    current.checked_add(rem).is_some(),
                    "number of added permits ({rem}) would overflow usize::MAX (prev: {current})"
                );
                self.permits.fetch_add(rem, Ordering::Release);
                rem = 0;
            }

            drop(waiters);
            wakers.wake_all();
        }
    }

    #[cfg(test)]
    pub fn num_waiter_nodes(&self) -> usize {
        self.waiters.lock().occupied_len()
    }
}

#[derive(Debug)]
pub struct Acquire<'a> {
    permits: usize,
    index: Option<WaiterId>,
    semaphore: &'a Semaphore,
    done: bool,
}

impl Drop for Acquire<'_> {
    fn drop(&mut self) {
        if let Some(index) = self.index.take() {
            let mut waiters = self.semaphore.waiters.lock();
            let mut acquired = 0;
            waiters.unlink_waiter(index, |node| {
                acquired = self.permits - node.permits;
                node.permits = 0;
                true
            });
            let waiter = waiters.remove_unlinked_waiter(index);
            if acquired > 0 {
                self.semaphore.insert_permits_with_lock(acquired, waiters);
            } else {
                drop(waiters);
            }
            drop(waiter);
        }
    }
}

impl Acquire<'_> {
    pub fn poll_once(&mut self, waker: &Waker) -> Poll<()> {
        let Self {
            permits,
            index,
            semaphore,
            done,
        } = self;

        if *done {
            return Poll::Ready(());
        }

        let mut old_waker = None;
        match index {
            Some(idx) => {
                let mut waiters = semaphore.waiters.lock();
                let ready = {
                    let node = waiters.waiter_mut(*idx);
                    if node.permits > 0 {
                        let update_waker = node
                            .waker
                            .as_ref()
                            .is_none_or(|current| !current.will_wake(waker));
                        if update_waker {
                            old_waker = node.waker.replace(waker.clone());
                        }
                        false
                    } else {
                        true
                    }
                };
                if ready {
                    waiters.remove_unlinked_waiter(*idx);
                    *index = None;
                    *done = true;
                    return Poll::Ready(());
                }
            }
            None => {
                // not yet enqueued
                let needed = *permits;

                if acquired_or_enqueue(semaphore, needed, Some(index), Some(waker), true) {
                    *done = true;
                    return Poll::Ready(());
                }
            }
        };

        drop(old_waker);
        Poll::Pending
    }
}

impl Future for Acquire<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.poll_once(cx.waker())
    }
}

/// Returns `true` if successfully acquired the semaphore; `false` otherwise.
fn acquired_or_enqueue(
    sem: &Semaphore,
    needed: usize,
    index: Option<&mut Option<WaiterId>>,
    waker: Option<&Waker>,
    enqueue_last: bool,
) -> bool {
    assert_eq!(
        index.is_some(),
        waker.is_some(),
        "only acquire waiters have a future owner"
    );
    let mut current = sem.permits.load(Ordering::Acquire);
    let mut lock = None;

    loop {
        let (remaining, next) = if current >= needed {
            (0, current - needed)
        } else {
            (needed - current, 0)
        };

        if remaining > 0 && lock.is_none() {
            // No permits were immediately available, so this permit will
            // (probably) need to wait. We'll need to acquire a lock on the
            // wait queue before continuing. We need to do this _before_ the
            // CAS that sets the new value of the semaphore's `permits`
            // counter. Otherwise, if we subtract the permits and then
            // acquire the lock, we might miss additional permits being
            // added while waiting for the lock.
            lock = Some(sem.waiters.lock());
        }

        if let Err(actual) =
            sem.permits
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
        {
            // other thread changed the permits; retry
            current = actual;
            continue;
        }

        // all needed permits were acquired
        if remaining == 0 {
            return true;
        }

        // all available permits were acquired, but more are needed;
        // enqueue a waiter with the remaining needed permits

        let mut waiters = lock.take().unwrap_or_else(|| {
            unreachable!("lock must be acquired when remaining {remaining} > 0");
        });

        let node = WaitNode {
            permits: remaining,
            waker: waker.cloned(),
        };
        let id = if enqueue_last {
            waiters.push_back(node)
        } else {
            waiters.push_front(node)
        };
        if let Some(index) = index {
            assert!(
                index.replace(id).is_none(),
                "waiter must not be registered twice"
            );
        }

        return false;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Wake;

    use super::*;

    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn release_drains_more_than_one_wake_batch() {
        const WAITER_COUNT: usize = WAKE_BATCH_SIZE + 3;

        let semaphore = Semaphore::new(0);
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let mut acquires = (0..WAITER_COUNT)
            .map(|_| semaphore.poll_acquire(1))
            .collect::<Vec<_>>();

        for acquire in &mut acquires {
            assert!(acquire.poll_once(&waker).is_pending());
        }
        assert_eq!(semaphore.num_waiter_nodes(), WAITER_COUNT);

        semaphore.release(WAITER_COUNT);
        assert_eq!(counter.0.load(Ordering::Relaxed), WAITER_COUNT);

        for acquire in &mut acquires {
            assert!(acquire.poll_once(&waker).is_ready());
        }
        assert_eq!(semaphore.num_waiter_nodes(), 0);
    }
}
