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

//! Cancellable storage for task wakers whose lifecycle is owned by the caller.
//!
//! A `WakerSet` is protected by the state lock of its owning primitive. The owner must clear a
//! token instead of unregistering it after an operation that detached the set. This lets each
//! primitive use its existing terminal state or generation to recognize stale registrations.

use std::mem;
use std::task::Waker;

use crate::internal::arena::Arena;
use crate::internal::arena::SlotId;
use crate::internal::waker_batch::WakerBatch;

/// An exclusive handle to one waker slot in a [`WakerSet`].
///
/// This token deliberately does not implement `Clone` or `Copy`. Its owner must not pass it back
/// to the set after the registration has been detached by [`WakerSet::drain`] or
/// [`WakerSet::take_all`].
#[derive(Debug)]
pub struct WakerToken(SlotId);

/// Cancellable waker storage without an implicit lifecycle or generation.
#[derive(Debug)]
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

    /// Drains all registered wakers into an owning batch while retaining slot capacity.
    ///
    /// The caller must invalidate every outstanding token and consume or drop the iterator after
    /// releasing the lock that protects this set.
    #[inline]
    pub fn drain(&mut self) -> impl Iterator<Item = Waker> + 'static {
        let mut wakers = WakerBatch::with_capacity(self.wakers.len());
        if self.wakers.is_empty() {
            return wakers.into_iter();
        }
        wakers.extend(self.wakers.drain());
        wakers.into_iter()
    }

    /// Takes all registered wakers together with the set's backing allocation.
    ///
    /// The caller must invalidate every outstanding token and consume or drop the iterator after
    /// releasing the lock that protects this set.
    #[inline]
    pub fn take_all(&mut self) -> impl Iterator<Item = Waker> + 'static {
        self.wakers.take_all()
    }

    /// Registers or updates a waker.
    ///
    /// If an existing waker is replaced, it is returned so the caller can drop it after releasing
    /// the lock that protects this set.
    #[inline]
    #[must_use = "drop the returned waker after releasing the waker set's state lock"]
    pub fn register(&mut self, token: &mut Option<WakerToken>, waker: &Waker) -> Option<Waker> {
        if let Some(current) = token.as_ref().map(|token| {
            self.wakers
                .get_mut(token.0)
                .expect("waker token must refer to an occupied slot")
        }) {
            if current.will_wake(waker) {
                return None;
            }
            return Some(mem::replace(current, waker.clone()));
        }

        *token = Some(WakerToken(self.wakers.insert(waker.clone())));
        None
    }

    /// Returns whether a live registration already wakes the given task, without cloning a waker.
    ///
    /// The caller must check completion before querying a potentially stale token.
    #[inline]
    pub fn will_wake(&self, token: &WakerToken, waker: &Waker) -> bool {
        self.wakers
            .get(token.0)
            .expect("waker token must refer to an occupied slot")
            .will_wake(waker)
    }

    /// Registers or replaces a waker cloned before taking the owner's state lock.
    ///
    /// Returns the previous waker so its destructor can run after releasing that lock.
    #[inline]
    #[must_use = "drop the returned waker after releasing the state lock"]
    pub fn register_owned(
        &mut self,
        token: &mut Option<WakerToken>,
        waker: Waker,
    ) -> Option<Waker> {
        if let Some(token) = token {
            let current = self
                .wakers
                .get_mut(token.0)
                .expect("waker token must refer to an occupied slot");
            return Some(mem::replace(current, waker));
        }

        *token = Some(WakerToken(self.wakers.insert(waker)));
        None
    }

    /// Removes the waker identified by `token`.
    ///
    /// The owner must clear stale tokens without calling this method after detaching the set. The
    /// returned waker must be dropped after releasing the lock that protects this set.
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
