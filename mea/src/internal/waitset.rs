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

use slab::Slab;

#[derive(Debug)]
pub(crate) struct WaitSet {
    waiters: Slab<Waker>,
}

impl WaitSet {
    /// Construct a new, empty wait set.
    pub const fn new() -> Self {
        Self {
            waiters: Slab::new(),
        }
    }

    /// Construct a new, empty wait set with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            waiters: Slab::with_capacity(capacity),
        }
    }

    /// Takes all registered wakers without waking them.
    pub(crate) fn take_wakers(&mut self) -> Vec<Waker> {
        self.waiters.drain().collect()
    }

    /// Registers a waker to the wait set.
    ///
    /// `idx` must be `None` when the waker is not registered, or `Some(key)` where `key` is
    /// a value previously returned by this method.
    pub(crate) fn register_waker(&mut self, idx: &mut Option<usize>, cx: &mut Context<'_>) {
        match *idx {
            None => {
                let key = self.waiters.insert(cx.waker().clone());
                *idx = Some(key);
            }
            Some(key) => {
                if self.waiters.contains(key) {
                    if !self.waiters[key].will_wake(cx.waker()) {
                        self.waiters[key] = cx.waker().clone();
                    }
                } else {
                    // DEFENSIVE NOTE:
                    //
                    // This is possible if latch/waitgroup is fired between the first and second
                    // state check.
                    //
                    // In this case, it does not harm to re-register the waker. Because
                    // the second state check will finish the future and the WaitSet gets
                    // dropped.
                    //
                    // Barrier holds the lock during check and register, so the race condition
                    // above won't happen.
                    let key = self.waiters.insert(cx.waker().clone());
                    *idx = Some(key);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Wake;
    use std::task::Waker;

    use super::WaitSet;

    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn take_wakers_does_not_wake() {
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let mut cx = Context::from_waker(&waker);
        let mut waiters = WaitSet::new();
        waiters.register_waker(&mut None, &mut cx);

        let wakers = waiters.take_wakers();
        assert_eq!(counter.0.load(Ordering::Relaxed), 0);
        assert_eq!(wakers.len(), 1);

        for waker in wakers {
            waker.wake();
        }
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    }
}
