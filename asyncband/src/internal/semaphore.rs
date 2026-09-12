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

// Portions of the permit-accounting algorithm originated from Tokio 1.42.0's batch semaphore.
// Copyright (c) Tokio Contributors
// The Tokio-derived portions remain licensed under the MIT License.
// Asyncband substantially replaced the waiter lifecycle with queue-owned WaitList nodes, supports
// queue-head permit debt for exact reductions, has no closed state or reserved flag bits, and uses
// its own cancellation, detachment, and waking machinery.
// Upstream source:
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/batch_semaphore.rs

use std::future::Future;
use std::pin::Pin;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::wake_all;

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
    #[cfg(any(feature = "broadcast", feature = "mpmc"))]
    pub fn release_if_nonempty(&self, n: usize) {
        let waiters = self.waiters.lock();
        if !waiters.is_empty() {
            self.insert_permits_with_lock(n, waiters);
        }
    }

    /// Adds as many permits until there is no waiter.
    #[cfg(any(feature = "broadcast", feature = "mpmc"))]
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
        wake_all(wakers.into_iter());
    }

    fn insert_permits_with_lock(
        &self,
        mut rem: usize,
        mut waiters: MutexGuard<'_, WaitList<WaitNode>>,
    ) {
        // A single-waiter handoff should not allocate a wake buffer.
        let mut first_waker = None;
        let mut wakers = vec![];
        while rem > 0 {
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
                        if first_waker.is_none() {
                            first_waker = Some(waker);
                        } else {
                            wakers.push(waker);
                        }
                    }
                    if remove_now {
                        waiters.remove_unlinked_waiter(id);
                    }
                }
            }
        }

        if rem > 0 {
            // Holding `waiters` serializes all permit additions. Concurrent operations can only
            // remove permits, so the count cannot grow between this check and fetch_add.
            let current = self.permits.load(Ordering::Relaxed);
            assert!(
                current.checked_add(rem).is_some(),
                "number of added permits ({rem}) would overflow usize::MAX (prev: {current})"
            );
            self.permits.fetch_add(rem, Ordering::Release);
        }

        // Finish all permit accounting before invoking callbacks. The shared helper attempts
        // every wake even if one panics, and propagates the first panic afterward.
        drop(waiters);
        wake_all(first_waker.into_iter().chain(wakers));
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
    use std::panic;
    use std::panic::AssertUnwindSafe;
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
    fn fulfilled_reduce_permits_debt_reclaims_its_waiter_node() {
        let semaphore = Semaphore::new(0);

        for _ in 0..3 {
            semaphore.reduce_permits(1);
            assert_eq!(semaphore.waiters.lock().occupied_len(), 1);

            semaphore.release(1);
            assert_eq!(semaphore.waiters.lock().occupied_len(), 0);
        }
    }

    #[test]
    fn release_distributes_permits_to_all_waiters() {
        const WAITER_COUNT: usize = 35;

        let semaphore = Semaphore::new(0);
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let mut acquires = (0..WAITER_COUNT)
            .map(|_| semaphore.poll_acquire(1))
            .collect::<Vec<_>>();

        for acquire in &mut acquires {
            assert!(acquire.poll_once(&waker).is_pending());
        }
        assert_eq!(semaphore.waiters.lock().occupied_len(), WAITER_COUNT);

        semaphore.release(WAITER_COUNT);
        assert_eq!(counter.0.load(Ordering::Relaxed), WAITER_COUNT);

        for acquire in &mut acquires {
            assert!(acquire.poll_once(&waker).is_ready());
        }
        assert_eq!(semaphore.waiters.lock().occupied_len(), 0);
    }

    #[test]
    fn panicking_wakes_preserve_permits_and_the_first_panic() {
        const WAITER_COUNT: usize = 65;

        struct TrackedWake {
            count: AtomicUsize,
            panic_message: Option<&'static str>,
        }

        impl Wake for TrackedWake {
            fn wake(self: Arc<Self>) {
                self.count.fetch_add(1, Ordering::Relaxed);
                if let Some(message) = self.panic_message {
                    panic::panic_any(message);
                }
            }
        }

        let semaphore = Semaphore::new(0);
        let trackers = (0..WAITER_COUNT)
            .map(|index| {
                Arc::new(TrackedWake {
                    count: AtomicUsize::new(0),
                    panic_message: if index == 0 {
                        Some("first wake panic")
                    } else if index == WAITER_COUNT / 2 {
                        Some("later wake panic")
                    } else {
                        None
                    },
                })
            })
            .collect::<Vec<_>>();
        let mut acquires = trackers
            .iter()
            .map(|tracker| {
                let mut acquire = semaphore.poll_acquire(1);
                assert!(
                    acquire
                        .poll_once(&Waker::from(tracker.clone()))
                        .is_pending()
                );
                acquire
            })
            .collect::<Vec<_>>();

        let payload = panic::catch_unwind(AssertUnwindSafe(|| semaphore.release(WAITER_COUNT + 2)))
            .expect_err("the original wake panic must reach the caller");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"first wake panic"));

        for tracker in trackers {
            assert_eq!(tracker.count.load(Ordering::Relaxed), 1);
        }
        for acquire in &mut acquires {
            assert!(acquire.poll_once(Waker::noop()).is_ready());
        }
        assert_eq!(semaphore.available_permits(), 2);
        assert_eq!(semaphore.waiters.lock().occupied_len(), 0);
    }
}
