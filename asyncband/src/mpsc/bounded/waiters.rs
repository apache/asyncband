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
use std::sync::atomic::Ordering;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::wake_all;
use crate::internal::waker_batch::WakerBatch;

// A wake grants a retry, not a capacity permit. Keeping notified nodes until their future
// consumes the notification lets cancellation pass an unused retry to the next sender.
pub struct SendWaiters {
    waiting: AtomicBool,
    queue: Mutex<WaitList<Option<Waker>>>,
}

impl SendWaiters {
    pub fn new() -> Self {
        Self {
            waiting: AtomicBool::new(false),
            queue: Mutex::new(WaitList::new()),
        }
    }

    pub fn waiter(&self) -> SendWaiter<'_> {
        SendWaiter {
            waiters: self,
            index: None,
        }
    }

    pub fn notify_one(&self) {
        // The receiver publishes its head with SeqCst before checking this flag. Registration
        // publishes the flag with SeqCst before rechecking capacity (which uses a SeqCst fence).
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

    pub fn notify_all(&self) {
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

pub struct SendWaiter<'a> {
    waiters: &'a SendWaiters,
    index: Option<WaiterId>,
}

impl SendWaiter<'_> {
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

impl Drop for SendWaiter<'_> {
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
