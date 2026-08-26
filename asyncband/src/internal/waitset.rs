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
use std::task::Context;
use std::task::Waker;

use crate::internal::arena::Arena;
use crate::internal::arena::SlotId;

/// A single-owner token for a waker registered in a [`WaitSet`].
///
/// This deliberately does not implement `Clone` or `Copy`: duplicating a token could let a stale
/// token refer to a slot reused by another waiter in the same epoch.
#[derive(Debug)]
pub struct WakerToken {
    epoch: u64,
    slot: SlotId,
}

#[derive(Debug)]
pub struct WaitSet {
    epoch: u64,
    waiters: Arena<Waker>,
}

impl WaitSet {
    /// Construct a new, empty wait set.
    pub const fn new() -> Self {
        Self {
            epoch: 0,
            waiters: Arena::new(),
        }
    }

    /// Construct a new, empty wait set with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            epoch: 0,
            waiters: Arena::with_capacity(capacity),
        }
    }

    /// Takes all registered wakers as an owning iterator without waking them.
    #[inline]
    pub fn take_wakers(&mut self) -> impl Iterator<Item = Waker> + 'static {
        self.epoch = self.epoch.checked_add(1).expect("wait set epoch overflow");
        self.waiters.take_all()
    }

    /// Registers or updates a waker in the current wake epoch.
    ///
    /// If an existing waker is replaced, it is returned so the caller can drop it after releasing
    /// the lock that protects this wait set.
    #[inline]
    pub fn register_waker(
        &mut self,
        token: &mut Option<WakerToken>,
        cx: &mut Context<'_>,
    ) -> Option<Waker> {
        if let Some(current) = token.as_ref() {
            if current.epoch == self.epoch {
                let waker = self
                    .waiters
                    .get_mut(current.slot)
                    .expect("current waker token must refer to an occupied slot");
                if !waker.will_wake(cx.waker()) {
                    return Some(mem::replace(waker, cx.waker().clone()));
                }
                return None;
            }
        }

        *token = Some(WakerToken {
            epoch: self.epoch,
            slot: self.waiters.insert(cx.waker().clone()),
        });
        None
    }

    /// Removes the waker identified by `token` if it still belongs to the current wake epoch.
    ///
    /// The returned waker must be dropped after releasing the lock that protects this wait set.
    #[inline]
    pub fn unregister_waker(&mut self, token: &mut Option<WakerToken>) -> Option<Waker> {
        let token = token.take()?;
        if token.epoch == self.epoch {
            return Some(self.waiters.remove(token.slot));
        }
        None
    }

    #[cfg(test)]
    fn registered_len(&self) -> usize {
        self.waiters.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Wake;

    use super::*;

    #[test]
    fn waker_token_preserves_the_option_niche() {
        assert_eq!(
            std::mem::size_of::<WakerToken>(),
            std::mem::size_of::<Option<WakerToken>>()
        );
    }

    struct TrackWake(AtomicUsize);

    impl Wake for TrackWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct DropWake {
        dropped: Arc<AtomicBool>,
        wake_count: AtomicUsize,
    }

    impl Wake for DropWake {
        fn wake(self: Arc<Self>) {
            self.wake_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl Drop for DropWake {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
        }
    }

    fn drop_waker() -> (Waker, Arc<AtomicBool>) {
        let dropped = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(DropWake {
            dropped: dropped.clone(),
            wake_count: AtomicUsize::new(0),
        }));
        (waker, dropped)
    }

    fn register(
        waiters: &mut WaitSet,
        token: &mut Option<WakerToken>,
        waker: &Waker,
    ) -> Option<Waker> {
        waiters.register_waker(token, &mut Context::from_waker(waker))
    }

    #[test]
    fn stale_token_does_not_alias_a_reused_slot() {
        let mut waiters = WaitSet::new();
        let first_waker = Waker::from(Arc::new(TrackWake(AtomicUsize::new(0))));
        let second_waker = Waker::from(Arc::new(TrackWake(AtomicUsize::new(0))));
        let mut first = None;
        let mut second = None;

        register(&mut waiters, &mut first, &first_waker);
        assert_eq!(waiters.take_wakers().count(), 1);

        register(&mut waiters, &mut second, &second_waker);
        register(&mut waiters, &mut first, &first_waker);

        let registered = waiters.take_wakers().collect::<Vec<_>>();
        assert_eq!(registered.len(), 2);
        assert!(registered.iter().any(|waker| waker.will_wake(&first_waker)));
        assert!(
            registered
                .iter()
                .any(|waker| waker.will_wake(&second_waker))
        );
    }

    #[test]
    fn unregister_returns_the_waker_for_deferred_drop() {
        let mut waiters = WaitSet::new();
        let (waker, dropped) = drop_waker();
        let mut token = None;

        register(&mut waiters, &mut token, &waker);
        drop(waker);

        let removed = waiters.unregister_waker(&mut token);
        assert_eq!(waiters.registered_len(), 0);
        assert!(!dropped.load(Ordering::Relaxed));

        drop(removed);
        assert!(dropped.load(Ordering::Relaxed));
    }

    #[test]
    fn replacing_returns_the_old_waker_for_deferred_drop() {
        let mut waiters = WaitSet::new();
        let (old_waker, dropped) = drop_waker();
        let new_waker = Waker::from(Arc::new(TrackWake(AtomicUsize::new(0))));
        let mut token = None;

        assert!(register(&mut waiters, &mut token, &old_waker).is_none());
        drop(old_waker);

        let replaced = register(&mut waiters, &mut token, &new_waker);
        assert!(!dropped.load(Ordering::Relaxed));

        drop(replaced);
        assert!(dropped.load(Ordering::Relaxed));
    }
}
