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

use std::task::Waker;

use crate::internal::register_waker;
use crate::internal::waker_batch::WakerBatch;
use crate::internal::wakerset::WakerSet;
use crate::internal::wakerset::WakerToken;

pub struct Waiters {
    pub slot0: Option<Waker>,
    overflow: WakerSet,
}

impl Waiters {
    pub const fn new() -> Self {
        Self {
            slot0: None,
            overflow: WakerSet::new(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slot0.is_none() && self.overflow.is_empty()
    }

    #[inline]
    pub fn has_overflow(&self) -> bool {
        !self.overflow.is_empty()
    }

    /// Registers or updates `waker` when the registration belongs to `cur_gen`, or allocates a
    /// fresh inline or overflow slot.
    #[inline]
    #[must_use = "drop the returned waker after releasing the waiters lock"]
    pub fn register(
        &mut self,
        wait_gen: &mut u64,
        inline: &mut bool,
        overflow: &mut Option<WakerToken>,
        cur_gen: u64,
        waker: &Waker,
    ) -> Option<Waker> {
        if (*inline || overflow.is_some()) && *wait_gen == cur_gen {
            if *inline {
                debug_assert!(
                    self.slot0.is_some(),
                    "current-generation slot0 waiter must be occupied"
                );
                return register_waker(&mut self.slot0, waker);
            }
            let token = overflow
                .as_ref()
                .expect("current-generation overflow waiter must hold a token");
            return self.overflow.update(token, waker);
        }

        // Detached registrations from an earlier generation must be cleared locally rather than
        // passed back to `WakerSet`.
        *inline = false;
        *overflow = None;
        *wait_gen = cur_gen;

        if self.slot0.is_none() {
            self.slot0 = Some(waker.clone());
            *inline = true;
        } else {
            *overflow = Some(self.overflow.insert(waker));
        }
        None
    }

    #[inline]
    #[must_use = "drop the returned waker after releasing the waiters lock"]
    pub fn remove(
        &mut self,
        wait_gen: u64,
        inline: &mut bool,
        overflow: &mut Option<WakerToken>,
        cur_gen: u64,
    ) -> Option<Waker> {
        let was_inline = *inline;
        let was_overflow = overflow.take();
        *inline = false;
        if wait_gen != cur_gen {
            return None;
        }
        if was_inline {
            return self.slot0.take();
        }
        was_overflow.map(|token| self.overflow.remove(token))
    }

    #[inline]
    pub fn drain_overflow_into(&mut self, batch: &mut WakerBatch) {
        self.overflow.drain_into(batch);
    }
}
