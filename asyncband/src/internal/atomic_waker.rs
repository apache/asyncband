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

// Portions derived from futures-rs 0.3.34, with panic recovery informed by Tokio 1.53.1:
// https://github.com/rust-lang/futures-rs/blob/705e6b5c0f06535b1aac1cb1989a172b3d45be8c/futures-core/src/task/__internal/atomic_waker.rs
// https://github.com/tokio-rs/tokio/blob/75fef53d0a8590c2d1dbb63672aa7b7d1ef51155/tokio/src/sync/task/atomic_waker.rs

use std::cell::UnsafeCell;
use std::panic::AssertUnwindSafe;
use std::panic::RefUnwindSafe;
use std::panic::UnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Waker;

const WAITING: usize = 0;
const REGISTERING: usize = 0b01;
const WAKING: usize = 0b10;

/// A single-registerer, multi-notifier cell for task wake-up.
///
/// The atomic state both grants exclusive access to `waker` and records one coalesced wake request.
/// The operation that moves the state out of `WAITING` remains the only slot owner until it returns
/// the state to `WAITING`.
///
/// * `WAITING`: the slot is unlocked and may contain a registered waker.
/// * `REGISTERING`: `register` exclusively owns the slot and no concurrent wake is pending.
/// * `WAKING`: `wake` exclusively owns the slot. A racing `register` self-wakes without touching
///   the slot.
/// * `REGISTERING | WAKING`: `register` still owns the slot and must complete a concurrent wake
///   before returning to `WAITING`.
///
/// Valid state transitions are:
///
/// ```text
/// register: WAITING ----------------Acquire CAS---------------> REGISTERING
///           REGISTERING ------------AcqRel CAS----------------> WAITING
///
/// wake:     WAITING ----------------AcqRel fetch_or-----------> WAKING
///           WAKING -----------------Release swap--------------> WAITING
///
/// race:     REGISTERING ------------AcqRel fetch_or-----------> REGISTERING | WAKING
///           REGISTERING | WAKING ---AcqRel swap---------------> WAITING
/// ```
///
/// Additional calls to `wake` while `WAKING` is set are coalesced. A wake completed before a
/// registration starts is not remembered, so callers must register before rechecking the condition
/// that determines whether to return `Pending`.
///
/// Every transition that acquires slot ownership has an Acquire operation paired with the previous
/// owner's Release transition to `WAITING`. The Release half of `wake` also publishes the caller's
/// preceding condition update; a racing `register` acquires that publication before it returns.
pub struct AtomicWaker {
    state: AtomicUsize,
    waker: UnsafeCell<Option<Waker>>,
}

// SAFETY: `state` grants exclusive access to `waker`, and losing concurrent registrations do not
// touch the slot. `Waker` itself is `Send + Sync`.
unsafe impl Sync for AtomicWaker {}

// `Waker` callbacks may unwind, but no panic leaves a state bit owned by the unwinding operation. A
// failed clone leaves the old slot intact and completes any raced wake, while wake and drop
// callbacks run after that operation's critical section has been released.
impl RefUnwindSafe for AtomicWaker {}
impl UnwindSafe for AtomicWaker {}

impl AtomicWaker {
    #[inline]
    pub const fn new() -> Self {
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
    pub fn register(&self, waker: &Waker) {
        // ORDERING: On success, Acquire pairs with the Release operation that last returned the
        // state to WAITING and transfers exclusive ownership of the waker slot to this thread. On
        // failure, Acquire matters when this reads WAKING from a notifier's AcqRel fetch_or: it
        // receives the condition update that preceded that wake before this method returns.
        match self
            .state
            .compare_exchange(WAITING, REGISTERING, Ordering::Acquire, Ordering::Acquire)
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

        // ORDERING: Release publishes a newly registered waker when the CAS succeeds. If it fails,
        // Acquire receives the concurrent notifier's Release publication before the wake is
        // completed below. AcqRel is the weakest success ordering that permits an Acquire failure
        // ordering, although its Acquire half is not otherwise relied upon on the success path.
        let concurrent_wake = match self.state.compare_exchange(
            REGISTERING,
            WAITING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => None,
            Err(state) => {
                debug_assert_eq!(state, REGISTERING | WAKING);

                // SAFETY: REGISTERING remains set, so this thread still owns the waker slot.
                let registered = unsafe { (*self.waker.get()).take() };

                // ORDERING: Acquire receives all coalesced wake publications. Release publishes
                // the empty slot and makes it available to the next register or wake operation.
                self.state.swap(WAITING, Ordering::AcqRel);
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
    pub fn wake(&self) {
        if let Some(waker) = self.take() {
            waker.wake();
        }
    }

    #[inline]
    fn take(&self) -> Option<Waker> {
        // ORDERING: When this reads WAITING, Acquire receives the registered waker published by the
        // previous owner. Release publishes the condition update that the caller performed before
        // calling wake, including when a registering thread already owns the slot.
        match self.state.fetch_or(WAKING, Ordering::AcqRel) {
            WAITING => {
                // SAFETY: changing WAITING to WAKING grants this thread exclusive access to the
                // waker slot until the state is returned to WAITING.
                let waker = unsafe { (*self.waker.get()).take() };

                // ORDERING: Release publishes the emptied slot before another operation acquires
                // it. The fetch_or above already performed the required Acquire operation.
                let old_state = self.state.swap(WAITING, Ordering::Release);
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

    #[cfg(panic = "unwind")]
    fn clone_panicking_waker() -> Waker {
        static VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| panic!("clone failed"),
            |_| unreachable!(),
            |_| unreachable!(),
            |_| {},
        );

        unsafe { Waker::from_raw(RawWaker::new(ptr::null(), &VTABLE)) }
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
        let atomic_waker = AtomicWaker::new();

        assert!(
            catch_unwind(|| {
                atomic_waker.register(&clone_panicking_waker());
            })
            .is_err()
        );

        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        atomic_waker.register(&Waker::from(counter.clone()));
        atomic_waker.wake();
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    }

    #[cfg(panic = "unwind")]
    #[test]
    fn clone_panic_completes_concurrent_wake() {
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let atomic_waker = AtomicWaker::new();
        atomic_waker.register(&Waker::from(counter.clone()));

        assert_eq!(
            atomic_waker.state.compare_exchange(
                WAITING,
                REGISTERING,
                Ordering::Acquire,
                Ordering::Acquire,
            ),
            Ok(WAITING)
        );
        std::thread::scope(|scope| scope.spawn(|| atomic_waker.wake()).join().unwrap());

        // SAFETY: this test acquired REGISTERING above and the waking thread has finished touching
        // the state. Calling the helper completes the interrupted registration.
        assert!(
            catch_unwind(|| unsafe {
                atomic_waker.register_locked(&clone_panicking_waker());
            })
            .is_err()
        );

        assert_eq!(counter.0.load(Ordering::Relaxed), 1);

        let next_counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        atomic_waker.register(&Waker::from(next_counter.clone()));
        atomic_waker.wake();
        assert_eq!(next_counter.0.load(Ordering::Relaxed), 1);
    }

    #[cfg(panic = "unwind")]
    #[test]
    fn drop_panic_does_not_poison_state() {
        unsafe fn clone_drop_panicker(data: *const ()) -> RawWaker {
            RawWaker::new(data, &DROP_PANICKING_VTABLE)
        }

        unsafe fn wake_drop_panicker(_: *const ()) {}

        unsafe fn drop_drop_panicker(data: *const ()) {
            // SAFETY: the test keeps the pointed-to AtomicBool alive until every derived waker has
            // been dropped.
            let should_panic = unsafe { &*data.cast::<AtomicBool>() };
            if should_panic.swap(false, Ordering::Relaxed) {
                panic!("drop failed");
            }
        }

        static DROP_PANICKING_VTABLE: RawWakerVTable = RawWakerVTable::new(
            clone_drop_panicker,
            wake_drop_panicker,
            wake_drop_panicker,
            drop_drop_panicker,
        );

        let should_panic = AtomicBool::new(true);
        let old_waker = unsafe {
            Waker::from_raw(RawWaker::new(
                ptr::from_ref(&should_panic).cast(),
                &DROP_PANICKING_VTABLE,
            ))
        };
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let new_waker = Waker::from(counter.clone());
        let atomic_waker = AtomicWaker::new();
        atomic_waker.register(&old_waker);

        assert!(catch_unwind(AssertUnwindSafe(|| atomic_waker.register(&new_waker))).is_err());

        atomic_waker.wake();
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    }
}
