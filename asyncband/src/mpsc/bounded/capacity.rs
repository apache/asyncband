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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Waker;

use super::SEQUENCE_STEP;
use crate::internal::cache_padded::CachePadded;
use crate::internal::mutex::Mutex;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::wake_all;
use crate::internal::waker_batch::WakerBatch;
use crate::mpsc::TrySendError;

const CLOSED: usize = 1;

// A wake grants a retry, not a capacity permit. Keeping notified nodes until their future
// consumes the notification lets cancellation pass an unused retry to the next sender.
pub struct Capacity {
    claims: CachePadded<Claims>,
    consumed: CachePadded<AtomicUsize>,
    cancelled: CachePadded<AtomicUsize>,
    capacity: usize,
    waiting: AtomicBool,
    queue: Mutex<WaitList<Option<Waker>>>,
}

struct Claims {
    next: AtomicUsize,
    returned: AtomicUsize,
}

impl Capacity {
    pub fn new(capacity: usize) -> Self {
        Self {
            claims: CachePadded::new(Claims {
                next: AtomicUsize::new(0),
                returned: AtomicUsize::new(0),
            }),
            consumed: CachePadded::new(AtomicUsize::new(0)),
            cancelled: CachePadded::new(AtomicUsize::new(0)),
            capacity: capacity * SEQUENCE_STEP,
            waiting: AtomicBool::new(false),
            queue: Mutex::new(WaitList::new()),
        }
    }

    pub fn try_acquire(&self) -> Result<(), TrySendError<()>> {
        let mut claimed = self.claims.next.load(Ordering::Relaxed);
        let mut returned = self.claims.returned.load(Ordering::Acquire);
        loop {
            if claimed & CLOSED != 0 {
                return Err(TrySendError::Disconnected(()));
            }
            if claimed.wrapping_sub(returned) >= self.capacity {
                returned = self
                    .consumed
                    .load(Ordering::SeqCst)
                    .wrapping_add(self.cancelled.load(Ordering::SeqCst));
                if claimed.wrapping_sub(returned) >= self.capacity {
                    // A newer return can overtake our claim snapshot. Refresh the snapshot
                    // before reporting Full, including a concurrent close.
                    let current = self.claims.next.load(Ordering::Relaxed);
                    if current != claimed {
                        claimed = current;
                        continue;
                    }
                    return Err(TrySendError::Full(()));
                }
                // Carry the acquired consumption edge with the cached progress. Producers only
                // read the consumer's changing cache line when this capacity window runs out.
                self.claims.returned.store(returned, Ordering::Release);
            }
            match self.claims.next.compare_exchange_weak(
                claimed,
                claimed.wrapping_add(SEQUENCE_STEP),
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => claimed = actual,
            }
        }
    }

    pub fn consume(&self, head: usize) {
        // Only the consumer advances this cursor. Producers never modify its cache line.
        self.consumed.store(head, Ordering::SeqCst);
        self.notify_one();
    }

    pub fn cancel(&self) {
        self.cancelled.fetch_add(SEQUENCE_STEP, Ordering::SeqCst);
        self.notify_one();
    }

    pub fn close(&self) {
        self.claims.next.fetch_or(CLOSED, Ordering::SeqCst);
        self.notify_all();
    }

    pub fn waiter(&self) -> ReserveWaiter<'_> {
        ReserveWaiter {
            waiters: self,
            index: None,
        }
    }

    fn notify_one(&self) {
        // Releasing capacity precedes this SeqCst flag check. Registration publishes the flag
        // with SeqCst before rechecking the SeqCst consumed and cancelled cursors.
        // Either the receiver sees the registration or the sender sees the released capacity.
        if !self.waiting.load(Ordering::SeqCst) {
            return;
        }
        let waker = {
            let mut queue = self.queue.lock();
            let waker = queue
                .unlink_first_waiter(|_| true)
                .and_then(|(_, waker)| waker.take());
            self.waiting.store(!queue.is_empty(), Ordering::SeqCst);
            waker
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn notify_all(&self) {
        let mut wakers = WakerBatch::new();
        {
            let mut queue = self.queue.lock();
            while let Some((_, waker)) = queue.unlink_first_waiter(|_| true) {
                if let Some(waker) = waker.take() {
                    wakers.push(waker);
                }
            }
            self.waiting.store(false, Ordering::SeqCst);
        }
        wake_all(wakers.into_iter());
    }
}

pub struct ReserveWaiter<'a> {
    waiters: &'a Capacity,
    index: Option<WaiterId>,
}

impl ReserveWaiter<'_> {
    // The caller must retry sending after registration, before returning Pending.
    pub fn register(&mut self, waker: &Waker) {
        let mut new_waker = None;
        loop {
            let mut queue = self.waiters.queue.lock();
            if let Some(index) = self.index {
                if queue
                    .waiter_mut(index)
                    .as_ref()
                    .is_some_and(|current| current.will_wake(waker))
                {
                    return;
                }
            }
            let Some(waker) = new_waker.take() else {
                // Waker callbacks may reenter the channel, including clone and drop callbacks.
                drop(queue);
                new_waker = Some(waker.clone());
                continue;
            };
            let old_waker = if let Some(index) = self.index {
                let node = queue.waiter_mut(index);
                if node.is_some() {
                    node.replace(waker)
                } else {
                    queue.remove_unlinked_waiter(index);
                    self.index = Some(queue.push_back(Some(waker)));
                    None
                }
            } else {
                self.index = Some(queue.push_back(Some(waker)));
                None
            };
            self.waiters.waiting.store(true, Ordering::SeqCst);
            drop(queue);
            drop(old_waker);
            return;
        }
    }

    pub fn finish(&mut self) {
        if let Some(index) = self.index.take() {
            drop(self.remove(index));
        }
    }

    fn remove(&self, index: WaiterId) -> Option<Waker> {
        let mut queue = self.waiters.queue.lock();
        queue.unlink_waiter(index, |_| true);
        let waker = queue.remove_unlinked_waiter(index);
        self.waiters
            .waiting
            .store(!queue.is_empty(), Ordering::SeqCst);
        waker
    }
}

impl Drop for ReserveWaiter<'_> {
    fn drop(&mut self) {
        if let Some(index) = self.index.take() {
            let waker = self.remove(index);
            if waker.is_none() {
                self.waiters.notify_one();
            }
            drop(waker);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Capacity;
    use super::Ordering;
    use super::SEQUENCE_STEP;
    use super::TrySendError;

    #[test]
    fn consumption_and_cancellation_restore_capacity_across_counter_overflow() {
        let capacity = Capacity::new(3);
        // Simulate prior cancellations on the final lap without allocating billions of permits.
        let position = usize::MAX - 5;
        capacity.claims.next.store(position, Ordering::Relaxed);
        capacity.cancelled.store(position, Ordering::Relaxed);
        let mut consumed = 0;
        for _ in 0..3 {
            for _ in 0..3 {
                capacity.try_acquire().unwrap();
            }
            assert_eq!(capacity.try_acquire(), Err(TrySendError::Full(())));
            consumed += SEQUENCE_STEP;
            capacity.consume(consumed);
            capacity.cancel();
            capacity.cancel();
        }
        capacity.close();
        assert_eq!(capacity.try_acquire(), Err(TrySendError::Disconnected(())));
    }
}
