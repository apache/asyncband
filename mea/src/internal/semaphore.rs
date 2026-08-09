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

use std::future::Future;
use std::pin::Pin;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::Mutex;
use crate::internal::WaitList;
use crate::internal::WaiterId;

/// The internal semaphore that provides low-level async primitives.
#[derive(Debug)]
pub struct Semaphore {
    /// The current number of available permits in the semaphore.
    permits: AtomicUsize,
    waiters: Mutex<WaitList<WaitNode>>,
}

#[derive(Debug)]
enum WaitNode {
    /// State retained until the acquiring future observes completion or is dropped.
    Acquire {
        permits: usize,
        waker: Option<Waker>,
    },
    /// State owned by the queue and removed as soon as the permit debt is satisfied.
    Debt { permits: usize },
}

impl WaitNode {
    fn permits_mut(&mut self) -> &mut usize {
        match self {
            Self::Acquire { permits, .. } | Self::Debt { permits } => permits,
        }
    }

    fn take_waker(&mut self) -> Option<Waker> {
        match self {
            Self::Acquire { waker, .. } => waker.take(),
            Self::Debt { .. } => None,
        }
    }

    fn is_debt(&self) -> bool {
        matches!(self, Self::Debt { .. })
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

    /// Decrease the semaphore's permits by a maximum of `n`.
    ///
    /// Return the number of permits that were actually reduced.
    pub fn forget(&self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }

        let mut current = self.permits.load(Ordering::Acquire);
        loop {
            let new = current.saturating_sub(n);
            match self.permits.compare_exchange_weak(
                current,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return n.min(current),
                Err(actual) => current = actual,
            }
        }
    }

    /// Decrease the semaphore's permits by `n`.
    ///
    /// If the semaphore has not enough permits, enqueue front an empty waiter to consume the
    /// permits.
    pub fn forget_exact(&self, n: usize) {
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
        let mut wakers = Vec::new();
        loop {
            match waiters.unlink_first_waiter(|node| {
                *node.permits_mut() = 0;
                true
            }) {
                None => break,
                Some((id, waiter)) => {
                    let remove_now = waiter.is_debt();
                    if let Some(waker) = waiter.take_waker() {
                        wakers.push(waker);
                    }
                    if remove_now {
                        waiters.remove_unlinked_waiter(id);
                    }
                }
            }
        }
        drop(waiters);
        for w in wakers.drain(..) {
            w.wake();
        }
    }

    fn insert_permits_with_lock(
        &self,
        mut rem: usize,
        waiters: MutexGuard<'_, WaitList<WaitNode>>,
    ) {
        const NUM_WAKER: usize = 32;
        let mut wakers = Vec::with_capacity(NUM_WAKER);

        let mut lock = Some(waiters);
        while rem > 0 {
            let mut waiters = lock.take().unwrap_or_else(|| self.waiters.lock());
            while wakers.len() < NUM_WAKER {
                match waiters.unlink_first_waiter(|node| {
                    let permits = node.permits_mut();
                    if *permits <= rem {
                        rem -= *permits;
                        *permits = 0;
                        true
                    } else {
                        *permits -= rem;
                        rem = 0;
                        false
                    }
                }) {
                    None => break,
                    Some((id, waiter)) => {
                        let remove_now = waiter.is_debt();
                        if let Some(waker) = waiter.take_waker() {
                            wakers.push(waker);
                        }
                        if remove_now {
                            waiters.remove_unlinked_waiter(id);
                        }
                    }
                }
            }

            if rem > 0 && waiters.is_empty() {
                let permits = rem;
                let prev = self.permits.fetch_add(permits, Ordering::Release);
                assert!(
                    prev.checked_add(permits).is_some(),
                    "number of added permits ({permits}) would overflow usize::MAX (prev: {prev})"
                );
                rem = 0;
            }

            drop(waiters);
            for w in wakers.drain(..) {
                w.wake();
            }
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
                let WaitNode::Acquire { permits, .. } = node else {
                    unreachable!("acquire handle must refer to an acquire waiter");
                };
                acquired = self.permits - *permits;
                *permits = 0;
                true
            });
            waiters.remove_unlinked_waiter(index);
            if acquired > 0 {
                self.semaphore.insert_permits_with_lock(acquired, waiters);
            }
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

        match index {
            Some(idx) => {
                let mut waiters = semaphore.waiters.lock();
                let ready = {
                    let node = waiters.waiter_mut(*idx);
                    let WaitNode::Acquire {
                        permits,
                        waker: current_waker,
                    } = node
                    else {
                        unreachable!("acquire handle must refer to an acquire waiter");
                    };
                    if *permits > 0 {
                        let update_waker = current_waker
                            .as_ref()
                            .is_none_or(|current| !current.will_wake(waker));
                        if update_waker {
                            *current_waker = Some(waker.clone());
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

        let node = if let Some(waker) = waker {
            WaitNode::Acquire {
                permits: remaining,
                waker: Some(waker.clone()),
            }
        } else {
            WaitNode::Debt { permits: remaining }
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
