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
// its own cancellation, detachment, and batched-waking machinery.
// Upstream source:
// https://github.com/tokio-rs/tokio/blob/bb9d57017e100985f86d8ca41ac105ee9140423e/tokio/src/sync/batch_semaphore.rs

use std::future::Future;
use std::pin::Pin;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::register_waker;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::waker_batch::WakerBatch;

/// The internal semaphore that provides low-level async primitives.
#[derive(Debug)]
pub struct Semaphore {
    /// The current number of available permits in the semaphore.
    available_permits: AtomicUsize,
    /// Set when a release holding the queue lock loses a CAS race, and cleared after it
    /// successfully updates the available count. Releases that observe this flag skip the
    /// lock-free attempt. This is a performance hint; stale reads do not affect correctness.
    contention_detected: AtomicBool,
    waiters: Mutex<WaitList<WaitNode>>,
}

#[derive(Debug)]
struct WaitNode {
    /// Permits still needed to satisfy the acquisition or repay the queued permit debt.
    needed_permits: usize,
    /// A linked node without a waker is permit debt owned by the queue. An acquire node only loses
    /// its waker while being detached, after which its future still owns the node.
    waker: Option<Waker>,
}

impl Semaphore {
    pub const fn new(permits: usize) -> Self {
        Self {
            available_permits: AtomicUsize::new(permits),
            contention_detected: AtomicBool::new(false),
            waiters: Mutex::new(WaitList::new()),
        }
    }

    /// Returns the current number of available permits.
    pub fn available_permits(&self) -> usize {
        self.available_permits.load(Ordering::Acquire)
    }

    /// Tries to acquire `n` permits from the semaphore.
    ///
    /// Returns `true` if the permits were acquired, `false` otherwise.
    pub fn try_acquire(&self, n: usize) -> bool {
        let mut available = self.available_permits.load(Ordering::Acquire);
        loop {
            if available < n {
                return false;
            }

            let next = available - n;
            match self.available_permits.compare_exchange(
                available,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => available = actual,
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

        let mut available = self.available_permits.load(Ordering::Acquire);
        loop {
            let next = available.saturating_sub(up_to);
            match self.available_permits.compare_exchange_weak(
                available,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return up_to.min(available),
                Err(actual) => available = actual,
            }
        }
    }

    /// Removes exactly `n` permits, consuming future releases if fewer are available.
    ///
    /// If fewer than `n` permits are available, a queue-head debt node consumes future releases.
    pub fn reduce_permits(&self, n: usize) {
        acquired_or_enqueue(self, n, None, None, false);
    }

    /// Acquires `n` permits from the semaphore.
    pub async fn acquire(&self, n: usize) {
        let fut = Acquire {
            requested_permits: n,
            index: None,
            semaphore: self,
            done: false,
        };
        fut.await
    }

    /// Returns a future that is resolved when acquired `n` permits from the semaphore.
    pub fn poll_acquire(&self, n: usize) -> Acquire<'_> {
        Acquire {
            requested_permits: n,
            index: None,
            semaphore: self,
            done: false,
        }
    }

    /// Releases `n` permits, attempting to update the available count without the queue lock.
    ///
    /// Acquires the queue lock if no permits are available, contention has been detected, or the
    /// compare-exchange fails.
    pub fn release(&self, n: usize) {
        if n == 0 {
            return;
        }

        // A waiter is linked only after all available permits have been taken. Releases make
        // permits available again only once the queue is empty, so available permits mean no
        // waiter is linked and the count can be increased without the queue lock.
        //
        // Try the exchange once before falling back to the locked path. Other threads can
        // still acquire or release permits without the lock, so an update under the lock may
        // also need to retry.
        let available = self.available_permits.load(Ordering::Relaxed);
        if available != 0
            && !self.contention_detected.load(Ordering::Relaxed)
            && self.try_update_permits(available, n).is_ok()
        {
            return;
        }

        self.distribute_permits(n, self.waiters.lock());
    }

    /// Releases `n` permits by acquiring the queue lock directly.
    ///
    /// Use this when no permits can be available, as when a mutex guard holds its only permit,
    /// to skip the speculative attempt in [`release`](Self::release).
    /// This method also works when permits are available.
    pub fn release_with_lock(&self, n: usize) {
        if n != 0 {
            self.distribute_permits(n, self.waiters.lock());
        }
    }

    /// Releases `n` permits only if the queue contains waiters when locked.
    #[cfg(feature = "broadcast")]
    pub fn release_if_waiters(&self, n: usize) {
        let waiters = self.waiters.lock();
        if !waiters.is_empty() {
            self.distribute_permits(n, waiters);
        }
    }

    /// Adds as many permits until there is no waiter.
    #[cfg(feature = "broadcast")]
    pub fn notify_all(&self) {
        let mut waiters = self.waiters.lock();
        let mut wakers = WakerBatch::new();
        while let Some((id, waiter)) = waiters.unlink_first_waiter(|node| {
            node.needed_permits = 0;
            true
        }) {
            if let Some(waker) = waiter.waker.take() {
                wakers.push(waker);
            } else {
                waiters.remove_unlinked_waiter(id);
            }
        }
        drop(waiters);
        wakers.wake_all();
    }

    /// Tries to update the available permit count from `expected` to `expected + released`.
    ///
    /// Returns the observed count if it does not match `expected`. Panics if the sum would
    /// overflow.
    ///
    /// The overflow check and addition must use the same count because other releases can add
    /// permits even while the queue lock is held. The exchange is the strong form because
    /// [`release`](Self::release) tries it only once.
    ///
    /// ORDERING: Release publishes the work protected by the released permits to the Acquire
    /// load or exchange that next observes this permit count.
    fn try_update_permits(&self, expected: usize, released: usize) -> Result<(), usize> {
        let next = expected.checked_add(released).unwrap_or_else(|| {
            panic!(
                "number of added permits ({released}) would overflow usize::MAX (prev: {expected})"
            )
        });
        self.available_permits
            .compare_exchange(expected, next, Ordering::Release, Ordering::Relaxed)
            .map(|_| ())
    }

    /// Assigns permits to queued waiters, then makes any surplus available.
    fn distribute_permits<'a>(
        &'a self,
        mut remaining: usize,
        mut waiters: MutexGuard<'a, WaitList<WaitNode>>,
    ) {
        let mut batch = WakerBatch::new();

        loop {
            while remaining > 0 && !batch.will_spill() {
                let Some((id, waiter)) = waiters.unlink_first_waiter(|node| {
                    let assigned = node.needed_permits.min(remaining);
                    node.needed_permits -= assigned;
                    remaining -= assigned;
                    node.needed_permits == 0
                }) else {
                    break;
                };
                if let Some(waker) = waiter.waker.take() {
                    batch.push(waker);
                } else {
                    waiters.remove_unlinked_waiter(id);
                }
            }

            if remaining > 0 && waiters.is_empty() {
                // Releases can add available permits without taking this lock, so the overflow
                // check and addition must use the same count. If none are available, other threads
                // cannot add any under this lock, but an RMW is still needed to extend the previous
                // release sequence.
                let mut available = self.available_permits.load(Ordering::Relaxed);
                if available == 0 {
                    self.available_permits
                        .fetch_add(remaining, Ordering::Release);
                } else {
                    while let Err(actual) = self.try_update_permits(available, remaining) {
                        // Another thread changed the available count despite our queue lock;
                        // ask subsequent releases to acquire the lock as well.
                        self.contention_detected.store(true, Ordering::Relaxed);
                        available = actual;
                    }
                    self.contention_detected.store(false, Ordering::Relaxed);
                }
                remaining = 0;
            }

            drop(waiters);
            batch.wake_all();
            if remaining == 0 {
                return;
            }
            waiters = self.waiters.lock();
        }
    }
}

#[derive(Debug)]
pub struct Acquire<'a> {
    requested_permits: usize,
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
                acquired = self.requested_permits - node.needed_permits;
                node.needed_permits = 0;
                true
            });
            let waiter = waiters.remove_unlinked_waiter(index);
            if acquired > 0 {
                self.semaphore.distribute_permits(acquired, waiters);
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
            requested_permits,
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
                    if node.needed_permits > 0 {
                        old_waker = register_waker(&mut node.waker, waker);
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
                let requested = *requested_permits;

                if acquired_or_enqueue(semaphore, requested, Some(index), Some(waker), true) {
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
    requested: usize,
    index: Option<&mut Option<WaiterId>>,
    waker: Option<&Waker>,
    enqueue_last: bool,
) -> bool {
    assert_eq!(
        index.is_some(),
        waker.is_some(),
        "only acquire waiters have a future owner"
    );
    let mut available = sem.available_permits.load(Ordering::Acquire);
    let mut lock = None;

    loop {
        let (needed, next) = if available >= requested {
            (0, available - requested)
        } else {
            (requested - available, 0)
        };

        if needed > 0 && lock.is_none() {
            // No permits were immediately available, so this permit will
            // (probably) need to wait. We'll need to acquire a lock on the
            // wait queue before continuing. We need to do this _before_ the
            // CAS that sets the new value of the semaphore's `available_permits`
            // counter. Otherwise, if we subtract the permits and then
            // acquire the lock, we might miss additional permits being
            // added while waiting for the lock.
            lock = Some(sem.waiters.lock());
        }

        if let Err(actual) = sem.available_permits.compare_exchange(
            available,
            next,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            // other thread changed the permits; retry
            available = actual;
            continue;
        }

        // all requested permits were acquired
        if needed == 0 {
            return true;
        }

        // all available permits were acquired, but more are needed;
        // enqueue a waiter for the permits still needed

        let mut waiters = lock.take().unwrap_or_else(|| {
            unreachable!("lock must be acquired when needed {needed} > 0");
        });

        let node = WaitNode {
            needed_permits: needed,
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
    fn release_uses_available_permits_without_queue_lock() {
        let semaphore = Semaphore::new(1);

        // Holding the queue lock deadlocks a release that needs it.
        let queue = semaphore.waiters.lock();
        semaphore.release(2);
        assert_eq!(semaphore.available_permits(), 3);
        assert!(semaphore.try_acquire(2));
        semaphore.release(2);
        assert_eq!(semaphore.available_permits(), 3);
        drop(queue);
    }

    #[test]
    fn release_takes_queue_lock_and_clears_detected_contention() {
        let semaphore = Semaphore::new(1);
        semaphore.contention_detected.store(true, Ordering::Relaxed);

        // Only the locked path clears this flag, so clearing it proves that the lock was taken.
        semaphore.release(1);
        assert_eq!(semaphore.available_permits(), 2);
        assert!(!semaphore.contention_detected.load(Ordering::Relaxed));
    }

    #[test]
    fn release_hands_permits_to_waiters_when_none_are_available() {
        let semaphore = Semaphore::new(0);
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let mut acquire = semaphore.poll_acquire(2);
        assert!(acquire.poll_once(&waker).is_pending());

        semaphore.release(3);
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
        assert_eq!(semaphore.available_permits(), 1);
        assert!(acquire.poll_once(&waker).is_ready());

        // The queue is empty and permits are available again: the lock is not needed.
        let queue = semaphore.waiters.lock();
        semaphore.release(1);
        drop(queue);
        assert_eq!(semaphore.available_permits(), 2);
    }

    #[test]
    fn distribute_permits_adds_to_available_permits() {
        let semaphore = Semaphore::new(2);

        // Another release can make permits available before this one acquires the lock.
        semaphore.distribute_permits(3, semaphore.waiters.lock());
        assert_eq!(semaphore.available_permits(), 5);
    }

    #[test]
    fn distribute_permits_overflow_preserves_available_permits() {
        let semaphore = Semaphore::new(usize::MAX);
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            semaphore.distribute_permits(1, semaphore.waiters.lock());
        }));
        assert!(result.is_err());
        assert_eq!(semaphore.available_permits(), usize::MAX);
        assert!(semaphore.waiters.lock().is_empty());
    }

    #[test]
    fn release_with_lock_hands_permits_to_waiters() {
        let semaphore = Semaphore::new(1);
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        assert!(semaphore.try_acquire(1));
        let mut acquire = semaphore.poll_acquire(1);
        assert!(acquire.poll_once(&waker).is_pending());

        semaphore.release_with_lock(1);
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
        assert_eq!(semaphore.available_permits(), 0);
        assert!(acquire.poll_once(&waker).is_ready());

        semaphore.release_with_lock(1);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn release_distributes_permits_across_multiple_batches() {
        const WAITER_COUNT: usize = WakerBatch::STACK_SIZE * 2 + 1;

        let semaphore = Semaphore::new(0);
        let counters = (0..WAITER_COUNT)
            .map(|_| Arc::new(WakeCounter(AtomicUsize::new(0))))
            .collect::<Vec<_>>();
        let wakers = counters
            .iter()
            .map(|counter| Waker::from(counter.clone()))
            .collect::<Vec<_>>();
        let mut acquires = (0..WAITER_COUNT)
            .map(|_| semaphore.poll_acquire(1))
            .collect::<Vec<_>>();

        for (acquire, waker) in acquires.iter_mut().zip(&wakers) {
            assert!(acquire.poll_once(waker).is_pending());
        }
        assert_eq!(semaphore.waiters.lock().occupied_len(), WAITER_COUNT);

        semaphore.release(WAITER_COUNT + 2);
        for counter in counters {
            assert_eq!(counter.0.load(Ordering::Relaxed), 1);
        }
        assert_eq!(semaphore.available_permits(), 2);

        for acquire in &mut acquires {
            assert!(acquire.poll_once(Waker::noop()).is_ready());
        }
        assert_eq!(semaphore.waiters.lock().occupied_len(), 0);
    }
}
