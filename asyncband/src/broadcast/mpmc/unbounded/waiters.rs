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
use std::num::NonZeroU32;
use std::process;
use std::task::Waker;

use crate::internal::update_waker;
use crate::internal::waker_batch::WakerBatch;

/// Compact 8-byte waiter handle carrying a `NonZeroU32` niche and the low 32 bits of `wake_gen`
/// (paired with `Subscription::wait_gen_hi` for the full 64-bit generation) so `Option<WaitReg>`
/// fits in one 64-bit word and `Subscription<T>` stays at 32 bytes.
#[derive(Clone, Copy)]
pub struct WaitReg {
    gen_lo: u32,
    slot: NonZeroU32,
}

impl WaitReg {
    #[inline(always)]
    pub fn full_gen(self, hi: u32) -> u64 {
        ((hi as u64) << 32) | (self.gen_lo as u64)
    }
}

enum OverflowSlot {
    Occupied(Waker),
    Vacant(u32),
}

pub struct Waiters {
    pub slot0: Option<Waker>,
    overflow: Vec<OverflowSlot>,
    free_head: u32,
    pub overflow_len: usize,
}

impl Waiters {
    const VACANT_END: u32 = u32::MAX;

    pub const fn new() -> Self {
        Self {
            slot0: None,
            overflow: Vec::new(),
            free_head: Self::VACANT_END,
            overflow_len: 0,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slot0.is_none() && self.overflow_len == 0
    }

    /// Registers or updates `waker` when `reg` belongs to `cur_gen`, or allocates a fresh slot.
    #[inline]
    #[must_use = "drop the returned waker after releasing the waiters lock"]
    pub fn register(
        &mut self,
        reg: &mut Option<WaitReg>,
        reg_gen_hi: &mut u32,
        cur_gen: u64,
        waker: &Waker,
    ) -> Option<Waker> {
        if let Some(existing) = *reg {
            if existing.full_gen(*reg_gen_hi) == cur_gen {
                if existing.slot.get() == 1 {
                    let cur_waker = self
                        .slot0
                        .as_mut()
                        .expect("current-generation slot0 waiter must be occupied");
                    return update_waker(cur_waker, waker);
                }
                let idx = (existing.slot.get() - 2) as usize;
                let Some(OverflowSlot::Occupied(cur_waker)) = self.overflow.get_mut(idx) else {
                    unreachable!("current-generation overflow waiter must be occupied");
                };
                return update_waker(cur_waker, waker);
            }
        }

        *reg_gen_hi = (cur_gen >> 32) as u32;
        let gen_lo = cur_gen as u32;
        if self.slot0.is_none() {
            self.slot0 = Some(waker.clone());
            *reg = Some(WaitReg {
                gen_lo,
                slot: NonZeroU32::MIN,
            });
            return None;
        }

        let idx = if self.free_head != Self::VACANT_END {
            let idx = self.free_head as usize;
            let OverflowSlot::Vacant(next) = self.overflow[idx] else {
                unreachable!("free_head must point to a vacant waiter slot");
            };
            self.free_head = next;
            self.overflow[idx] = OverflowSlot::Occupied(waker.clone());
            idx
        } else {
            let idx = self.overflow.len();
            self.overflow.push(OverflowSlot::Occupied(waker.clone()));
            idx
        };
        self.overflow_len += 1;
        let Some(encoded) = u32::try_from(idx)
            .ok()
            .and_then(|v| v.checked_add(2))
            .and_then(NonZeroU32::new)
        else {
            process::abort();
        };
        *reg = Some(WaitReg {
            gen_lo,
            slot: encoded,
        });
        None
    }

    #[inline]
    #[must_use = "drop the returned waker after releasing the waiters lock"]
    pub fn remove(&mut self, reg: WaitReg, reg_gen_hi: u32, cur_gen: u64) -> Option<Waker> {
        if reg.full_gen(reg_gen_hi) != cur_gen {
            return None;
        }
        if reg.slot.get() == 1 {
            return self.slot0.take();
        }

        let idx = (reg.slot.get() - 2) as usize;
        if !matches!(self.overflow.get(idx), Some(OverflowSlot::Occupied(_))) {
            return None;
        }
        let prev = mem::replace(
            &mut self.overflow[idx],
            OverflowSlot::Vacant(self.free_head),
        );
        self.free_head = idx as u32;
        self.overflow_len -= 1;
        let OverflowSlot::Occupied(waker) = prev else {
            unreachable!()
        };
        Some(waker)
    }

    #[inline]
    pub fn drain_overflow_into(&mut self, batch: &mut WakerBatch) {
        if self.overflow_len == 0 {
            return;
        }
        for slot in self.overflow.drain(..) {
            if let OverflowSlot::Occupied(waker) = slot {
                batch.push(waker);
            }
        }
        self.free_head = Self::VACANT_END;
        self.overflow_len = 0;
    }
}
