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

//! Cancellable storage for task wakers.
//!
//! A `WaitSet` is protected by the state lock of its owning primitive. Registration clones a
//! borrowed [`Waker`], while replacement and removal return owned wakers so callers can drop or
//! wake them after unlocking.

use std::task::Waker;

use crate::internal::wakerset;
use crate::internal::wakerset::WakerSet;

/// An exclusive handle to one waiter slot in a [`WaitSet`].
///
/// The wait set owns the registered waker; this token only lets its future update or cancel that
/// registration. It deliberately does not implement `Clone` or `Copy`, because duplicating the
/// handle could let a stale token refer to a slot reused by another waiter in the same epoch.
#[derive(Debug)]
pub struct WakerToken {
    epoch: u64,
    token: wakerset::WakerToken,
}

#[derive(Debug)]
pub struct WaitSet {
    epoch: u64,
    wakers: WakerSet,
}

impl WaitSet {
    /// Construct a new, empty wait set.
    pub const fn new() -> Self {
        Self {
            epoch: 0,
            wakers: WakerSet::new(),
        }
    }

    /// Construct a new, empty wait set with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            epoch: 0,
            wakers: WakerSet::with_capacity(capacity),
        }
    }

    /// Drains all registered wakers as an owning iterator without waking them.
    ///
    /// A non-empty drain starts a new epoch so tokens retained by the drained futures cannot alias
    /// slots reused by later registrations. The caller must consume or drop the iterator after
    /// releasing the lock that protects this wait set.
    #[inline]
    pub fn drain(&mut self) -> impl Iterator<Item = Waker> + 'static {
        if self.wakers.is_empty() {
            return self.wakers.drain();
        }
        self.advance_epoch();
        self.wakers.drain()
    }

    /// Takes all registered wakers and the wait set's backing allocation without waking them.
    ///
    /// This is intended for terminal state transitions where the current allocation cannot be
    /// reused. The caller must consume or drop the iterator after releasing the lock that protects
    /// this wait set.
    #[inline]
    pub fn take_all(&mut self) -> impl Iterator<Item = Waker> + 'static {
        if !self.wakers.is_empty() {
            self.advance_epoch();
        }
        self.wakers.take_all()
    }

    /// Registers or updates a waker in the current wake epoch.
    ///
    /// If an existing waker is replaced, it is returned so the caller can drop it after releasing
    /// the lock that protects this wait set.
    #[inline]
    #[must_use = "drop the returned waker after releasing the wait set's state lock"]
    pub fn register(&mut self, token: &mut Option<WakerToken>, waker: &Waker) -> Option<Waker> {
        let mut current = match token.take() {
            Some(token) if token.epoch == self.epoch => Some(token.token),
            _ => None,
        };
        let retired = self.wakers.register(&mut current, waker);
        *token = current.map(|token| WakerToken {
            epoch: self.epoch,
            token,
        });
        retired
    }

    /// Removes the waker identified by `token` if it still belongs to the current wake epoch.
    ///
    /// The returned waker must be dropped after releasing the lock that protects this wait set.
    #[inline]
    #[must_use = "drop the returned waker after releasing the wait set's state lock"]
    pub fn unregister(&mut self, token: &mut Option<WakerToken>) -> Option<Waker> {
        let token = token.take()?;
        if token.epoch != self.epoch {
            // A drain advanced the epoch and already took ownership of this token's waker.
            return None;
        }
        let mut current = Some(token.token);
        self.wakers.unregister(&mut current)
    }

    fn advance_epoch(&mut self) {
        self.epoch = self.epoch.checked_add(1).expect("wait set epoch overflow");
    }

    #[cfg(test)]
    fn registered_len(&self) -> usize {
        self.wakers.registered_len()
    }
}

#[cfg(test)]
mod tests {
    use std::panic;
    use std::panic::AssertUnwindSafe;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Wake;

    use super::*;
    use crate::internal::wake_all;

    #[test]
    fn waker_token_preserves_the_option_niche() {
        assert_eq!(size_of::<WakerToken>(), size_of::<Option<WakerToken>>());
    }

    struct TrackWake(AtomicUsize);

    impl Wake for TrackWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct PanicWake;

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("wake failed");
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

    #[test]
    fn stale_token_does_not_alias_a_reused_slot() {
        let mut waiters = WaitSet::new();
        let first_task = Arc::new(TrackWake(AtomicUsize::new(0)));
        let first_waker = Waker::from(first_task.clone());
        let second_task = Arc::new(TrackWake(AtomicUsize::new(0)));
        let second_waker = Waker::from(second_task.clone());
        let mut first_token = None;
        let mut second_token = None;

        drop(waiters.register(&mut first_token, &first_waker));
        assert_eq!(waiters.drain().count(), 1);

        drop(waiters.register(&mut second_token, &second_waker));
        drop(waiters.register(&mut first_token, &first_waker));

        let registered = waiters.drain().collect::<Vec<_>>();
        assert_eq!(registered.len(), 2);
        wake_all(registered.into_iter());
        assert_eq!(first_task.0.load(Ordering::Relaxed), 1);
        assert_eq!(second_task.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn take_all_invalidates_existing_tokens() {
        let mut waiters = WaitSet::new();
        let first_task = Arc::new(TrackWake(AtomicUsize::new(0)));
        let first_waker = Waker::from(first_task.clone());
        let second_task = Arc::new(TrackWake(AtomicUsize::new(0)));
        let second_waker = Waker::from(second_task.clone());
        let mut first_token = None;
        let mut second_token = None;

        drop(waiters.register(&mut first_token, &first_waker));
        assert_eq!(waiters.take_all().count(), 1);

        drop(waiters.register(&mut second_token, &second_waker));
        drop(waiters.register(&mut first_token, &first_waker));

        let registered = waiters.take_all().collect::<Vec<_>>();
        assert_eq!(registered.len(), 2);
        wake_all(registered.into_iter());
        assert_eq!(first_task.0.load(Ordering::Relaxed), 1);
        assert_eq!(second_task.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn wake_all_notifies_remaining_waiters_after_a_panic() {
        let mut waiters = WaitSet::new();
        let panicking = Waker::from(Arc::new(PanicWake));
        let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
        let tracked = Waker::from(tracker.clone());
        let mut first = None;
        let mut second = None;
        let mut third = None;

        drop(waiters.register(&mut first, &panicking));
        drop(waiters.register(&mut second, &panicking));
        drop(waiters.register(&mut third, &tracked));

        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            wake_all(waiters.drain());
        }));
        assert!(result.is_err());
        assert_eq!(tracker.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn unregister_returns_the_waker_for_deferred_drop() {
        let mut waiters = WaitSet::new();
        let (waker, dropped) = drop_waker();
        let mut token = None;

        drop(waiters.register(&mut token, &waker));
        drop(waker);

        let removed = waiters.unregister(&mut token);
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

        assert!(waiters.register(&mut token, &old_waker).is_none());
        drop(old_waker);

        let replaced = waiters.register(&mut token, &new_waker);
        assert!(!dropped.load(Ordering::Relaxed));

        drop(replaced);
        assert!(dropped.load(Ordering::Relaxed));
    }
}
