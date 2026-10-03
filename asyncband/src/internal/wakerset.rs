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

//! Cancellable task wakers protected by the owning primitive's state lock.
//!
//! The primitive uses its generation or terminal state to recognize detached registrations;
//! their tokens must be cleared rather than passed back to the set. Returned wakers must be woken
//! or dropped after releasing the lock.

use std::mem;
use std::task::Waker;

use crate::internal::arena::Arena;
use crate::internal::arena::SlotId;
use crate::internal::waker_batch::WakerBatch;

/// An exclusive handle to one waker slot in a [`WakerSet`].
///
/// Removing the registration, calling [`WakerSet::take_all`], or replacing the set invalidates it.
#[derive(Debug)]
pub struct WakerToken(SlotId);

/// Cancellable waker storage without an implicit lifecycle or generation.
#[derive(Default, Debug)]
pub struct WakerSet {
    wakers: Arena<Waker>,
}

impl WakerSet {
    /// Constructs an empty waker set.
    pub const fn new() -> Self {
        Self {
            wakers: Arena::new(),
        }
    }

    /// Constructs an empty waker set with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            wakers: Arena::with_capacity(capacity),
        }
    }

    /// Collects registered wakers into an owned batch, retaining slot capacity for reuse.
    ///
    /// Moves each waker into the batch; up to [`WakerBatch::INLINE_CAPACITY`] fit without
    /// allocating.
    #[inline]
    pub fn take_all(&mut self) -> WakerBatch {
        if self.wakers.is_empty() {
            return WakerBatch::new();
        }
        self.wakers.take_all()
    }

    /// Consumes the set, transferring its backing allocation to an iterator.
    ///
    /// This avoids collecting a separate batch when capacity is no longer needed. The iterator
    /// releases the allocation when dropped.
    #[inline]
    pub fn into_iter(self) -> impl Iterator<Item = Waker> {
        self.wakers.into_iter()
    }

    /// Registers or updates a waker.
    ///
    /// Returns the previous waker only if it was replaced.
    #[inline]
    #[must_use = "drop the returned waker after releasing the waker set's state lock"]
    pub fn register(&mut self, token: &mut Option<WakerToken>, waker: &Waker) -> Option<Waker> {
        if let Some(token) = token {
            let current = self
                .wakers
                .get_mut(token.0)
                .expect("waker token must refer to an occupied slot");
            if current.will_wake(waker) {
                return None;
            }
            return Some(mem::replace(current, waker.clone()));
        }

        *token = Some(WakerToken(self.wakers.insert(waker.clone())));
        None
    }

    /// Removes and returns the waker identified by `token`, clearing the token.
    #[inline]
    #[must_use = "drop the returned waker after releasing the waker set's state lock"]
    pub fn unregister(&mut self, token: &mut Option<WakerToken>) -> Option<Waker> {
        token.take().map(|token| self.wakers.remove(token.0))
    }
}

#[cfg(test)]
mod tests {
    use super::WakerToken;

    #[test]
    fn waker_token_preserves_the_option_niche() {
        assert_eq!(size_of::<WakerToken>(), size_of::<usize>());
        assert_eq!(size_of::<WakerToken>(), size_of::<Option<WakerToken>>());
    }
}
