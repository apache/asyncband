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

// This state machine is derived from the futures-rs `AtomicWaker`, licensed under
// Apache-2.0 OR MIT: https://github.com/rust-lang/futures-rs/blob/0.3.34/futures-core/src/task/__internal/atomic_waker.rs.

use std::cell::UnsafeCell;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::AcqRel;
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::Ordering::Release;
use std::task::Waker;

const WAITING: usize = 0;
const REGISTERING: usize = 0b01;
const WAKING: usize = 0b10;

/// A single-registerer, multi-notifier cell for task wake-up.
pub(crate) struct AtomicWaker {
    state: AtomicUsize,
    waker: UnsafeCell<Option<Waker>>,
}

// SAFETY: `state` grants exclusive access to `waker`, and losing concurrent registrations do not
// touch the slot. `Waker` itself is `Send + Sync`.
unsafe impl Sync for AtomicWaker {}

impl AtomicWaker {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self {
            state: AtomicUsize::new(WAITING),
            waker: UnsafeCell::new(None),
        }
    }

    /// Registers `waker`, replacing a previously registered task if it differs.
    ///
    /// Calls to this method must not overlap. It may run concurrently with any number of calls to
    /// [`wake`](Self::wake).
    #[inline]
    pub(crate) fn register(&self, waker: &Waker) {
        match self
            .state
            .compare_exchange(WAITING, REGISTERING, Acquire, Acquire)
            .unwrap_or_else(|state| state)
        {
            WAITING => {
                // SAFETY: changing WAITING to REGISTERING grants this thread exclusive access to
                // the waker slot until the state is returned to WAITING.
                unsafe { self.register_locked(waker) }
            }
            WAKING => {
                // A concurrent wake owns the slot. Self-waking ensures that this registration is
                // not lost even though it cannot replace the slot right now.
                waker.wake_by_ref();
            }
            state => {
                // Concurrent registration violates this type's contract. Ignoring the losing
                // registration preserves memory safety and lets the winner provide notification.
                debug_assert!(state == REGISTERING || state == REGISTERING | WAKING);
            }
        }
    }

    /// Registers a waker after this thread has acquired the REGISTERING state.
    ///
    /// # Safety
    ///
    /// The caller must have changed `state` from WAITING to REGISTERING and must be the only
    /// thread accessing `waker`.
    #[inline]
    unsafe fn register_locked(&self, waker: &Waker) {
        // Avoid both cloning and dropping the common case where an executor polls the receiver
        // repeatedly with the same task waker.
        let needs_replacement = match unsafe { &*self.waker.get() } {
            Some(current) => !current.will_wake(waker),
            None => true,
        };

        let mut clone_panic = None;
        let old_waker = if needs_replacement {
            match catch_unwind(AssertUnwindSafe(|| waker.clone())) {
                Ok(new_waker) => unsafe { (*self.waker.get()).replace(new_waker) },
                Err(payload) => {
                    clone_panic = Some(payload);
                    None
                }
            }
        } else {
            None
        };

        let concurrent_wake =
            match self
                .state
                .compare_exchange(REGISTERING, WAITING, AcqRel, Acquire)
            {
                Ok(_) => None,
                Err(state) => {
                    debug_assert_eq!(state, REGISTERING | WAKING);

                    // SAFETY: REGISTERING remains set, so this thread still owns the waker slot.
                    let registered = unsafe { (*self.waker.get()).take() };
                    self.state.swap(WAITING, AcqRel);
                    registered
                }
            };

        if let Some(payload) = clone_panic {
            // Preserve the original clone panic while still completing a wake that raced with it.
            if let Some(waker) = concurrent_wake {
                let _ = catch_unwind(AssertUnwindSafe(|| waker.wake()));
            }
            resume_unwind(payload);
        }

        // User waker code runs only after the state machine is back in WAITING, so a panic cannot
        // leave the cell locked. If the wake raced with a replacement, notify both tasks: the
        // concurrent call may have targeted the old registration, while future progress relies on
        // the new one. A panic from the superseded waker must not prevent the new task from waking.
        if let Some(waker) = concurrent_wake {
            if let Some(old_waker) = old_waker {
                let _ = catch_unwind(AssertUnwindSafe(|| old_waker.wake()));
            }
            waker.wake();
        } else {
            // Drop a replaced waker only after releasing the state lock.
            drop(old_waker);
        }
    }

    /// Wakes and removes the most recently registered waker, if any.
    #[inline]
    pub(crate) fn wake(&self) {
        if let Some(waker) = self.take() {
            waker.wake();
        }
    }

    #[inline]
    fn take(&self) -> Option<Waker> {
        match self.state.fetch_or(WAKING, AcqRel) {
            WAITING => {
                // SAFETY: changing WAITING to WAKING grants this thread exclusive access to the
                // waker slot until the state is returned to WAITING.
                let waker = unsafe { (*self.waker.get()).take() };
                let old_state = self.state.swap(WAITING, Release);
                debug_assert_eq!(old_state, WAKING);
                waker
            }
            state => {
                // The thread registering a waker observes WAKING and completes this notification,
                // or another waking thread has already taken responsibility for it.
                debug_assert!(
                    state == REGISTERING || state == REGISTERING | WAKING || state == WAKING
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::panic;
    use std::ptr;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Wake;

    use super::*;

    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn wake_notifies_once() {
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let atomic_waker = AtomicWaker::new();

        atomic_waker.register(&waker);
        atomic_waker.wake();
        atomic_waker.wake();

        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn reregistering_same_task_does_not_clone_waker() {
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let atomic_waker = AtomicWaker::new();

        atomic_waker.register(&waker);
        let registered_refs = Arc::strong_count(&counter);
        atomic_waker.register(&waker);

        assert_eq!(Arc::strong_count(&counter), registered_refs);
    }

    #[test]
    fn wake_before_register_is_not_remembered() {
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let atomic_waker = AtomicWaker::new();

        atomic_waker.wake();
        atomic_waker.register(&waker);

        assert_eq!(counter.0.load(Ordering::Relaxed), 0);
        atomic_waker.wake();
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn wake_during_replacement_notifies_old_and_new_tasks() {
        let old_counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let old_waker = Waker::from(old_counter.clone());
        let new_counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let new_waker = Waker::from(new_counter.clone());
        let atomic_waker = AtomicWaker::new();
        atomic_waker.register(&old_waker);

        assert_eq!(
            atomic_waker.state.compare_exchange(
                WAITING,
                REGISTERING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ),
            Ok(WAITING)
        );
        std::thread::scope(|scope| scope.spawn(|| atomic_waker.wake()).join().unwrap());

        // SAFETY: this test acquired REGISTERING above and the waking thread has finished touching
        // the slot. Calling the helper completes the interrupted registration.
        unsafe { atomic_waker.register_locked(&new_waker) };

        assert_eq!(old_counter.0.load(Ordering::Relaxed), 1);
        assert_eq!(new_counter.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn failed_wake_synchronizes_with_next_registration() {
        for _ in 0..1_000 {
            let did_publish = AtomicBool::new(false);
            let atomic_waker = AtomicWaker::new();
            atomic_waker.register(Waker::noop());

            std::thread::scope(|scope| {
                let wake = scope.spawn(|| {
                    did_publish.store(true, Ordering::Relaxed);
                    atomic_waker.take()
                });

                let local_waker = atomic_waker.take();
                atomic_waker.register(Waker::noop());

                let publication_is_visible = did_publish.load(Ordering::Relaxed);
                let concurrent_thread_took_waker = wake.join().unwrap().is_some();
                assert!(publication_is_visible || concurrent_thread_took_waker);
                drop(local_waker);
            });
        }
    }

    #[cfg(panic = "unwind")]
    #[test]
    fn clone_panic_does_not_poison_state() {
        static PANICKING_VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| panic!("clone failed"),
            |_| unreachable!(),
            |_| unreachable!(),
            |_| {},
        );

        let panicking = unsafe { Waker::from_raw(RawWaker::new(ptr::null(), &PANICKING_VTABLE)) };
        let atomic_waker = AtomicWaker::new();

        assert!(
            panic::catch_unwind(AssertUnwindSafe(|| {
                atomic_waker.register(&panicking);
            }))
            .is_err()
        );

        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        atomic_waker.register(&Waker::from(counter.clone()));
        atomic_waker.wake();
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    }
}
