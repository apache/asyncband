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

use std::mem;
use std::task::Waker;

use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;

/// A receiver notification makes the task runnable without reserving a value.
pub enum RecvWaiter {
    Waiting(Waker),
    Notified,
}

pub fn remove_waiter<W>(waiters: &mut WaitList<W>, id: WaiterId) -> W {
    // Unlinking is idempotent, so detached waiters are removed the same way as linked ones.
    waiters.unlink_waiter(id, |_| true);
    waiters.remove_unlinked_waiter(id)
}

impl WaitList<RecvWaiter> {
    pub fn notify_one(&mut self) -> Option<Waker> {
        let (_, waiter) = self.unlink_first_waiter(|_| true)?;
        let RecvWaiter::Waiting(waker) = mem::replace(waiter, RecvWaiter::Notified) else {
            unreachable!("only waiting operations remain linked");
        };
        Some(waker)
    }

    /// Registers a pending operation's waker or refreshes an existing registration.
    ///
    /// If the future finds no value after notification, its waiter rejoins the queue.
    #[must_use = "drop the replaced waker after releasing the queue lock"]
    pub fn register(&mut self, id: &mut Option<WaiterId>, current: &Waker) -> Option<Waker> {
        if let Some(queued) = *id {
            if let RecvWaiter::Waiting(waker) = self.waiter_mut(queued) {
                if waker.will_wake(current) {
                    return None;
                }
                return Some(mem::replace(waker, current.clone()));
            }
        }
        if let Some(notified) = id.take() {
            // The notification already took this node's waker, so nothing is retired.
            remove_waiter(self, notified);
        }
        *id = Some(self.push_back(RecvWaiter::Waiting(current.clone())));
        None
    }
}
