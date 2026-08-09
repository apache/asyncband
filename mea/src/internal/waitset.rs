// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::task::Context;
use std::task::Waker;

use crate::internal::Arena;
use crate::internal::ArenaKey;

#[derive(Clone, Copy, Debug)]
pub(crate) struct WaitRegistration {
    epoch: u64,
    key: ArenaKey,
}

#[derive(Debug)]
pub(crate) struct WaitSet {
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

    /// Takes all registered wakers without waking them.
    pub(crate) fn take_wakers(&mut self) -> Vec<Waker> {
        self.epoch = self.epoch.checked_add(1).expect("wait set epoch overflow");
        self.waiters.take_all()
    }

    /// Registers or updates a waker in the current wake epoch.
    pub(crate) fn register_waker(
        &mut self,
        registration: &mut Option<WaitRegistration>,
        cx: &mut Context<'_>,
    ) {
        if let Some(current) = *registration {
            if current.epoch == self.epoch {
                let waker = self
                    .waiters
                    .get_mut(current.key)
                    .expect("current wait registration must be occupied");
                if !waker.will_wake(cx.waker()) {
                    waker.clone_from(cx.waker());
                }
                return;
            }
        }

        *registration = Some(WaitRegistration {
            epoch: self.epoch,
            key: self.waiters.insert(cx.waker().clone()),
        });
    }

    /// Removes a registration if it still belongs to the current wake epoch.
    pub(crate) fn unregister_waker(&mut self, registration: &mut Option<WaitRegistration>) {
        let Some(registration) = registration.take() else {
            return;
        };
        if registration.epoch == self.epoch {
            self.waiters.remove(registration.key);
        }
    }

    #[cfg(test)]
    fn registered_len(&self) -> usize {
        self.waiters.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Wake;

    use super::*;

    struct TrackWake(AtomicUsize);

    impl Wake for TrackWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn register(waiters: &mut WaitSet, registration: &mut Option<WaitRegistration>, waker: &Waker) {
        waiters.register_waker(registration, &mut Context::from_waker(waker));
    }

    #[test]
    fn stale_registration_does_not_alias_a_reused_slot() {
        let mut waiters = WaitSet::new();
        let first_waker = Waker::from(Arc::new(TrackWake(AtomicUsize::new(0))));
        let second_waker = Waker::from(Arc::new(TrackWake(AtomicUsize::new(0))));
        let mut first = None;
        let mut second = None;

        register(&mut waiters, &mut first, &first_waker);
        assert_eq!(waiters.take_wakers().len(), 1);

        register(&mut waiters, &mut second, &second_waker);
        register(&mut waiters, &mut first, &first_waker);

        let registered = waiters.take_wakers();
        assert_eq!(registered.len(), 2);
        assert!(registered.iter().any(|waker| waker.will_wake(&first_waker)));
        assert!(
            registered
                .iter()
                .any(|waker| waker.will_wake(&second_waker))
        );
    }

    #[test]
    fn unregister_releases_the_waker_immediately() {
        let mut waiters = WaitSet::new();
        let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
        let waker = Waker::from(tracker.clone());
        let mut registration = None;

        register(&mut waiters, &mut registration, &waker);
        assert_eq!(Arc::strong_count(&tracker), 3);

        waiters.unregister_waker(&mut registration);
        assert_eq!(waiters.registered_len(), 0);
        assert_eq!(Arc::strong_count(&tracker), 2);
    }
}
