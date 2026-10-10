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

use std::future::Future;
use std::pin::Pin;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Wake;
use std::task::Waker;
use std::thread;

use asyncband::blocking::FutureExt;
use asyncband::event::ManualResetEvent;
use tests_integration::WakeCounter;
use tests_integration::assert_completes_without_deadlock;
use tests_integration::poll_once;

#[test]
fn set_is_sticky_and_reset_blocks_new_waiters() {
    let event = ManualResetEvent::new();
    assert!(!event.try_wait());
    event.set();
    assert!(event.try_wait());
    assert!(event.try_wait());

    let mut ready = pin!(event.wait());
    assert!(poll_once(ready.as_mut()).is_ready());
    assert!(event.is_set());

    event.reset();
    assert!(!event.try_wait());
    let mut pending = pin!(event.wait());
    assert!(poll_once(pending.as_mut()).is_pending());
}

// Registration happens on the first poll, not when `wait` builds the future.
#[test]
fn an_unpolled_wait_is_not_a_waiter_of_a_preceding_set() {
    let event = ManualResetEvent::new();
    let mut unpolled = pin!(event.wait());

    event.set();
    event.reset();

    assert!(poll_once(unpolled.as_mut()).is_pending());
}

#[test]
fn polling_with_a_new_waker_replaces_the_registration() {
    let event = ManualResetEvent::new();
    let first = Arc::new(WakeCounter::default());
    let second = Arc::new(WakeCounter::default());
    let first_waker = Waker::from(first.clone());
    let second_waker = Waker::from(second.clone());
    let baseline = Arc::strong_count(&first);
    let mut wait = pin!(event.wait());

    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    assert_eq!(Arc::strong_count(&first), baseline + 1);

    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending()
    );
    assert_eq!(Arc::strong_count(&first), baseline);

    event.set();
    assert_eq!(first.count(), 0);
    assert_eq!(second.count(), 1);
}

#[test]
fn cancelling_a_committed_waiter_leaves_the_others_committed() {
    let event = ManualResetEvent::new();
    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker.clone());
    let mut context = Context::from_waker(&waker);
    let mut cancelled = Box::pin(event.wait());
    let mut survivor = Box::pin(event.wait());

    assert!(cancelled.as_mut().poll(&mut context).is_pending());
    assert!(survivor.as_mut().poll(&mut context).is_pending());

    event.set();
    event.reset();
    assert_eq!(tracker.count(), 2);

    // A committed waiter holds no consumable permit, so dropping it hands nothing on.
    drop(cancelled);
    assert!(survivor.as_mut().poll(&mut context).is_ready());
    assert!(!event.is_set());
}

// A waker that re-enters the event it belongs to, both when woken and when dropped. The internal
// lock is non-reentrant, so waking or dropping this waker inside the critical section deadlocks.
struct ReentrantWaker(Arc<ManualResetEvent>);

impl Wake for ReentrantWaker {
    fn wake(self: Arc<Self>) {
        self.0.reset();
    }
}

impl Drop for ReentrantWaker {
    fn drop(&mut self) {
        self.0.reset();
    }
}

// Wakers must be invoked and dropped after the internal lock is released.
#[test]
fn wakers_are_woken_and_dropped_outside_the_internal_lock() {
    assert_completes_without_deadlock(|| {
        let event = Arc::new(ManualResetEvent::new());

        // `set` wakes the waiter, and the waker re-enters `reset`.
        let mut woken = Box::pin(event.clone().wait_owned());
        {
            let waker = Waker::from(Arc::new(ReentrantWaker(event.clone())));
            assert!(
                woken
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }
        event.set();
        assert!(!event.is_set(), "the waker's reset did not land");
        assert!(
            woken
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );

        // Polling again with a different waker drops the one it replaces. The registration holds
        // the last reference to the first waker, so its `Drop` runs during that poll.
        let mut replaced = Box::pin(event.clone().wait_owned());
        {
            let first = Waker::from(Arc::new(ReentrantWaker(event.clone())));
            assert!(
                replaced
                    .as_mut()
                    .poll(&mut Context::from_waker(&first))
                    .is_pending()
            );
        }
        {
            let second = Waker::from(Arc::new(ReentrantWaker(event.clone())));
            assert!(
                replaced
                    .as_mut()
                    .poll(&mut Context::from_waker(&second))
                    .is_pending()
            );
        }

        // Cancelling a pending wait drops the registered waker, which re-enters `reset`. The
        // registration holds the last reference, so the drop runs here.
        drop(replaced);
    });
}

// A waker that resets the event and registers a fresh waiter from inside the wake callback.
struct ResetAndRegister {
    event: Arc<ManualResetEvent>,
    fresh: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    fresh_was_pending: AtomicBool,
}

impl Wake for ResetAndRegister {
    fn wake(self: Arc<Self>) {
        self.event.reset();
        let mut wait = Box::pin(self.event.clone().wait_owned());
        let pending = wait
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending();
        self.fresh_was_pending.store(pending, Ordering::Relaxed);
        *self.fresh.lock().unwrap() = Some(wait);
    }
}

// `set` detaches the whole registered cohort before any waker runs, so a waiter registered from a
// wake callback belongs to the period that callback's `reset` opened, not to the `set` in progress.
// This guards the cohort boundary across future drain or atomic slow-path refactors; a naive
// incremental wake-as-you-drain would lose it.
#[test]
fn a_wait_registered_from_a_wake_callback_belongs_to_the_next_period() {
    let event = Arc::new(ManualResetEvent::new());
    let hook = Arc::new(ResetAndRegister {
        event: event.clone(),
        fresh: Mutex::new(None),
        fresh_was_pending: AtomicBool::new(false),
    });
    let waker = Waker::from(hook.clone());
    let mut released = Box::pin(event.clone().wait_owned());

    assert!(
        released
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    event.set();

    assert!(!event.is_set(), "the callback's reset did not land");
    assert!(
        hook.fresh_was_pending.load(Ordering::Relaxed),
        "a wait registered after the callback's reset was committed by the outer set"
    );
    assert!(
        released
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready(),
        "the cohort registered before the set stays committed"
    );

    let mut fresh = hook
        .fresh
        .lock()
        .unwrap()
        .take()
        .expect("the callback registered a waiter");
    assert!(
        fresh
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );

    event.set();
    assert!(
        fresh
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
}

#[test]
fn set_then_reset_commits_registered_waiters() {
    let event = ManualResetEvent::new();
    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker.clone());
    let mut context = Context::from_waker(&waker);
    let mut wait = pin!(event.wait());

    assert!(wait.as_mut().poll(&mut context).is_pending());
    event.set();
    event.reset();

    assert!(!event.is_set());
    assert!(!event.try_wait());
    assert_eq!(tracker.count(), 1);
    assert!(wait.as_mut().poll(&mut context).is_ready());

    let mut next_generation = pin!(event.wait());
    assert!(next_generation.as_mut().poll(&mut context).is_pending());
}

#[test]
fn repeated_set_is_coalesced() {
    let event = ManualResetEvent::new();
    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker.clone());
    let mut context = Context::from_waker(&waker);
    let mut wait = pin!(event.wait());

    assert!(wait.as_mut().poll(&mut context).is_pending());
    event.set();
    event.set();

    assert_eq!(tracker.count(), 1);
    assert!(wait.as_mut().poll(&mut context).is_ready());
}

#[test]
fn cancelling_a_waiter_releases_its_waker() {
    let event = ManualResetEvent::new();
    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker.clone());
    let baseline = Arc::strong_count(&tracker);
    let mut context = Context::from_waker(&waker);
    let mut wait = Box::pin(event.wait());

    assert!(wait.as_mut().poll(&mut context).is_pending());
    assert_eq!(Arc::strong_count(&tracker), baseline + 1);

    drop(wait);
    assert_eq!(Arc::strong_count(&tracker), baseline);
    event.set();
    assert_eq!(tracker.count(), 0);
}

#[test]
fn cancelling_an_owned_waiter_releases_its_waker_and_event_handle() {
    let event = Arc::new(ManualResetEvent::new());
    let tracker = Arc::new(WakeCounter::default());
    let waker = Waker::from(tracker.clone());
    let baseline = Arc::strong_count(&tracker);
    let mut context = Context::from_waker(&waker);
    let mut wait = Box::pin(event.clone().wait_owned());

    assert!(wait.as_mut().poll(&mut context).is_pending());
    assert_eq!(Arc::strong_count(&tracker), baseline + 1);

    drop(wait);
    assert_eq!(Arc::strong_count(&tracker), baseline);
    assert_eq!(Arc::strong_count(&event), 1);

    event.set();
    assert_eq!(tracker.count(), 0);
}

// Unlike Wake's Arc clone, this exercises the callback used to clone a raw executor waker.
fn waker_on_clone(callback: impl Fn() + Send + Sync + 'static) -> Waker {
    struct Hook(Box<dyn Fn() + Send + Sync>);

    unsafe fn clone(data: *const ()) -> RawWaker {
        // SAFETY: Every raw waker owns an Arc<Hook>; the source remains alive during clone.
        let hook = unsafe { &*data.cast::<Hook>() };
        (hook.0)();
        // SAFETY: The source waker keeps the allocation alive, and the returned waker owns
        // exactly the additional strong reference created here.
        unsafe { Arc::increment_strong_count(data.cast::<Hook>()) };
        RawWaker::new(data, &VTABLE)
    }

    unsafe fn drop_ref(data: *const ()) {
        // SAFETY: A consuming wake or drop releases exactly its own raw Arc reference.
        drop(unsafe { Arc::from_raw(data.cast::<Hook>()) });
    }

    unsafe fn wake_by_ref(_: *const ()) {}

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, drop_ref, wake_by_ref, drop_ref);
    let data = Arc::into_raw(Arc::new(Hook(Box::new(callback)))).cast();
    // SAFETY: VTABLE maintains the Arc ownership rules, and Hook is Send + Sync.
    unsafe { Waker::from_raw(RawWaker::new(data, &VTABLE)) }
}

#[test]
fn registration_rechecks_a_set_during_waker_preparation() {
    assert_completes_without_deadlock(|| {
        for reset in [false, true] {
            let event = Arc::new(ManualResetEvent::new());
            let on_clone = event.clone();
            let waker = waker_on_clone(move || {
                on_clone.set();
                if reset {
                    on_clone.reset();
                }
            });
            let mut wait = Box::pin(event.wait());
            let result = wait.as_mut().poll(&mut Context::from_waker(&waker));
            if reset {
                // This wait was not registered during the pulse. It belongs to the new period.
                assert!(result.is_pending());
                event.set();
                assert!(poll_once(wait.as_mut()).is_ready());
            } else {
                // A false fast probe cannot cause registration after an unretracted set.
                assert!(result.is_ready());
            }
        }
    });
}

#[test]
fn waker_replacement_rechecks_commitment_after_set_and_reset() {
    assert_completes_without_deadlock(|| {
        let event = Arc::new(ManualResetEvent::new());
        let mut wait = Box::pin(event.wait());
        assert!(poll_once(wait.as_mut()).is_pending());

        let on_clone = event.clone();
        let replacement = waker_on_clone(move || {
            on_clone.set();
            on_clone.reset();
        });
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&replacement))
                .is_ready()
        );
        assert!(!event.is_set());

        let mut next = Box::pin(event.wait());
        assert!(poll_once(next.as_mut()).is_pending());
        event.set();
        assert!(poll_once(next.as_mut()).is_ready());
    });
}

#[test]
fn ready_and_unchanged_pending_waits_do_not_clone_wakers() {
    let clones = Arc::new(AtomicUsize::new(0));
    let on_clone = clones.clone();
    let waker = waker_on_clone(move || {
        on_clone.fetch_add(1, Ordering::Relaxed);
    });
    let mut context = Context::from_waker(&waker);
    let event = ManualResetEvent::with_state(true);
    assert!(pin!(event.wait()).as_mut().poll(&mut context).is_ready());
    assert_eq!(clones.load(Ordering::Relaxed), 0);

    event.reset();
    let mut wait = pin!(event.wait());
    assert!(wait.as_mut().poll(&mut context).is_pending());
    assert!(wait.as_mut().poll(&mut context).is_pending());
    assert_eq!(clones.load(Ordering::Relaxed), 1);
    event.set();
    assert!(wait.as_mut().poll(&mut context).is_ready());
    assert_eq!(clones.load(Ordering::Relaxed), 1);
}

#[test]
fn concurrent_sets_and_waits_publish_state_across_reset_cycles() {
    assert_completes_without_deadlock(|| {
        let event = ManualResetEvent::new();
        let round = Barrier::new(2);
        let value = AtomicUsize::new(0);
        thread::scope(|scope| {
            scope.spawn(|| {
                for expected in 1..=300 {
                    round.wait();
                    value.store(expected, Ordering::Relaxed);
                    event.set();
                    round.wait();
                }
            });
            for expected in 1..=300 {
                // The opening barrier precedes publication; only the event publishes value.
                round.wait();
                match expected % 3 {
                    0 => event.wait().block_on(),
                    1 => {
                        while !event.try_wait() {
                            thread::yield_now();
                        }
                    }
                    _ => {
                        while !event.is_set() {
                            thread::yield_now();
                        }
                    }
                }
                assert_eq!(value.load(Ordering::Relaxed), expected);
                round.wait();
                event.reset();
            }
        });
    });
}
