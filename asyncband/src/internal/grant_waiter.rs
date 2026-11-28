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

//! FIFO capacity-grant waiter state shared by bounded channels.

use std::mem;
use std::task::Waker;

use crate::internal::update_waker;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;

pub enum GrantWaiter {
    Waiting(Waker),
    Granted,
}

impl WaitList<GrantWaiter> {
    #[inline]
    #[must_use = "drop the replaced waker after releasing the queue lock"]
    pub fn register_waiter(&mut self, id: &mut Option<WaiterId>, current: &Waker) -> Option<Waker> {
        if let Some(queued) = *id {
            let GrantWaiter::Waiting(waker) = self.waiter_mut(queued) else {
                unreachable!("a granted waiter must be claimed before registration");
            };
            return update_waker(waker, current);
        }
        *id = Some(self.push_back(GrantWaiter::Waiting(current.clone())));
        None
    }

    #[inline]
    pub fn grant_one(&mut self) -> Option<Waker> {
        let (_, waiter) = self.unlink_first_waiter()?;
        let GrantWaiter::Waiting(waker) = mem::replace(waiter, GrantWaiter::Granted) else {
            unreachable!("only waiting operations remain linked");
        };
        Some(waker)
    }

    #[inline]
    pub fn take_waiting_waker(&mut self) -> Option<Waker> {
        let (id, _) = self.unlink_first_waiter()?;
        let GrantWaiter::Waiting(waker) = self.remove_unlinked_waiter(id) else {
            unreachable!("only waiting operations remain linked");
        };
        Some(waker)
    }
}
