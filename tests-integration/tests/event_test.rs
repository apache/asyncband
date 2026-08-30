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
use std::panic;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Context;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;

use asyncband::event::ManualResetEvent;
use asyncband::event::OwnedManualResetEventWait;
use tests_integration::poll_once;

struct TrackWake(AtomicUsize);

impl Wake for TrackWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn constructors_and_state_queries() {
    let unset = ManualResetEvent::new();
    let set = ManualResetEvent::with_state(true);

    assert!(!unset.is_set());
    assert!(set.is_set());
    assert!(!ManualResetEvent::default().is_set());
}

#[test]
fn set_is_sticky_and_reset_blocks_new_waiters() {
    let event = ManualResetEvent::new();
    event.set();

    let mut ready = pin!(event.wait());
    assert!(poll_once(ready.as_mut()).is_ready());

    event.reset();
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
    let first = Arc::new(TrackWake(AtomicUsize::new(0)));
    let second = Arc::new(TrackWake(AtomicUsize::new(0)));
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
    assert_eq!(first.0.load(Ordering::Relaxed), 0);
    assert_eq!(second.0.load(Ordering::Relaxed), 1);
}

#[test]
fn cancelling_a_committed_waiter_leaves_the_others_committed() {
    let event = ManualResetEvent::new();
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let mut context = Context::from_waker(&waker);
    let mut cancelled = Box::pin(event.wait());
    let mut survivor = Box::pin(event.wait());

    assert!(cancelled.as_mut().poll(&mut context).is_pending());
    assert!(survivor.as_mut().poll(&mut context).is_pending());

    event.set();
    event.reset();
    assert_eq!(tracker.0.load(Ordering::Relaxed), 2);

    // A committed waiter holds no consumable permit, so dropping it hands nothing on.
    drop(cancelled);
    assert!(survivor.as_mut().poll(&mut context).is_ready());
    assert!(!event.is_set());
}

struct PanicOnWake;

impl Wake for PanicOnWake {
    fn wake(self: Arc<Self>) {
        panic!("waker panicked");
    }
}

// `set` releases every waiter registered for the current unset period, so one broken waker must
// not strand the waiters queued behind it.
#[test]
fn a_panicking_waker_still_releases_the_remaining_waiters() {
    let event = ManualResetEvent::new();
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let tracking_waker = Waker::from(tracker.clone());
    let panicking_waker = Waker::from(Arc::new(PanicOnWake));

    let mut first = Box::pin(event.wait());
    let mut exploding = Box::pin(event.wait());
    let mut last = Box::pin(event.wait());
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&tracking_waker))
            .is_pending()
    );
    assert!(
        exploding
            .as_mut()
            .poll(&mut Context::from_waker(&panicking_waker))
            .is_pending()
    );
    assert!(
        last.as_mut()
            .poll(&mut Context::from_waker(&tracking_waker))
            .is_pending()
    );

    let panicked = panic::catch_unwind(AssertUnwindSafe(|| event.set()));

    assert!(panicked.is_err(), "the waker panic must reach the caller");
    assert_eq!(
        tracker.0.load(Ordering::Relaxed),
        2,
        "the waiter queued behind the panicking waker was never woken"
    );
    assert!(event.is_set());
    assert!(
        last.as_mut()
            .poll(&mut Context::from_waker(&tracking_waker))
            .is_ready()
    );
}

/// A waker that re-enters the event it belongs to, both when woken and when dropped.
///
/// The internal lock is a non-reentrant `std::sync::Mutex`, so waking or dropping this waker inside
/// the critical section blocks forever instead of returning.
struct ReentrantWaker(Arc<ManualResetEvent>);

impl Wake for ReentrantWaker {
    fn wake(self: Arc<Self>) {
        self.0.reset();
    }
}

impl Drop for ReentrantWaker {
    fn drop(&mut self) {
        self.0.is_set();
    }
}

// Wakers must be invoked and dropped after the internal lock is released.
//
// The scenario runs on a worker thread so that a regression surfaces as a bounded failure here
// rather than hanging until the harness times out.
#[test]
fn wakers_are_woken_and_dropped_outside_the_internal_lock() {
    let (done, finished) = mpsc::channel();

    thread::spawn(move || {
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

        // Cancelling a pending wait drops the registered waker, which re-enters `is_set`. The
        // registration holds the last reference, so the drop runs here.
        drop(replaced);

        done.send(()).unwrap();
    });

    finished
        .recv_timeout(Duration::from_secs(10))
        .expect("a waker was invoked or dropped while the internal lock was held");
}

// A waker that resets the event and registers a fresh waiter from inside the wake callback.
struct ResetAndRegister {
    event: Arc<ManualResetEvent>,
    fresh: Mutex<Option<OwnedManualResetEventWait>>,
    fresh_was_pending: AtomicBool,
}

impl Wake for ResetAndRegister {
    fn wake(self: Arc<Self>) {
        self.event.reset();
        let mut wait = self.event.clone().wait_owned();
        let pending = Pin::new(&mut wait)
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
        Pin::new(&mut fresh)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );

    event.set();
    assert!(
        Pin::new(&mut fresh)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
}

#[test]
fn set_then_reset_commits_registered_waiters() {
    let event = ManualResetEvent::new();
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let mut context = Context::from_waker(&waker);
    let mut wait = pin!(event.wait());

    assert!(wait.as_mut().poll(&mut context).is_pending());
    event.set();
    event.reset();

    assert_eq!(tracker.0.load(Ordering::Relaxed), 1);
    assert!(wait.as_mut().poll(&mut context).is_ready());

    let mut next_generation = pin!(event.wait());
    assert!(next_generation.as_mut().poll(&mut context).is_pending());
}

#[test]
fn repeated_set_is_coalesced() {
    let event = ManualResetEvent::new();
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let mut context = Context::from_waker(&waker);
    let mut wait = pin!(event.wait());

    assert!(wait.as_mut().poll(&mut context).is_pending());
    event.set();
    event.set();

    assert_eq!(tracker.0.load(Ordering::Relaxed), 1);
    assert!(wait.as_mut().poll(&mut context).is_ready());
}

#[test]
fn cancelling_a_waiter_releases_its_waker() {
    let event = ManualResetEvent::new();
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let baseline = Arc::strong_count(&tracker);
    let mut context = Context::from_waker(&waker);
    let mut wait = Box::pin(event.wait());

    assert!(wait.as_mut().poll(&mut context).is_pending());
    assert_eq!(Arc::strong_count(&tracker), baseline + 1);

    drop(wait);
    assert_eq!(Arc::strong_count(&tracker), baseline);
    event.set();
    assert_eq!(tracker.0.load(Ordering::Relaxed), 0);
}

#[test]
fn cancelling_an_owned_waiter_releases_its_waker_and_event_handle() {
    let event = Arc::new(ManualResetEvent::new());
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
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
    assert_eq!(tracker.0.load(Ordering::Relaxed), 0);
}

// Registrations must not be lost when they race a concurrent `set`.
//
// Each round only resets after every waiter has completed, so a waiter that registers late
// observes the set state instead of blocking. A lost wake-up therefore hangs the join below.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_racing_registration_wakes_every_waiter() {
    const ROUNDS: usize = 128;
    const WAITERS: usize = 8;

    let event = Arc::new(ManualResetEvent::new());
    for _ in 0..ROUNDS {
        let waiters = (0..WAITERS)
            .map(|_| tokio::spawn(event.clone().wait_owned()))
            .collect::<Vec<_>>();

        event.set();
        for waiter in waiters {
            waiter.await.unwrap();
        }

        event.reset();
    }

    assert!(!event.is_set());
    assert!(poll_once(pin!(event.wait())).is_pending());
}

// A `set` immediately followed by a `reset` on another thread still commits every waiter that
// registered before the transition.
#[test]
fn cross_thread_set_then_reset_commits_registered_waiters() {
    const ROUNDS: usize = 128;
    const WAITERS: usize = 8;

    for _ in 0..ROUNDS {
        let event = Arc::new(ManualResetEvent::new());
        let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
        let waker = Waker::from(tracker.clone());
        let mut context = Context::from_waker(&waker);
        let mut waits = (0..WAITERS)
            .map(|_| Box::pin(event.clone().wait_owned()))
            .collect::<Vec<_>>();
        for wait in &mut waits {
            assert!(wait.as_mut().poll(&mut context).is_pending());
        }

        let signaller = thread::spawn({
            let event = event.clone();
            move || {
                event.set();
                event.reset();
            }
        });
        signaller.join().unwrap();

        assert!(!event.is_set());
        assert_eq!(tracker.0.load(Ordering::Relaxed), WAITERS);
        for wait in &mut waits {
            assert!(wait.as_mut().poll(&mut context).is_ready());
        }
    }
}

// Cancelling one waiter while another thread sets the event must reclaim that waiter's waker
// under either outcome of the race, and must not disturb the remaining waiter.
#[test]
fn cancellation_racing_set_reclaims_the_waker() {
    const ROUNDS: usize = 256;

    for _ in 0..ROUNDS {
        let event = Arc::new(ManualResetEvent::new());
        let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
        let waker = Waker::from(tracker.clone());
        let baseline = Arc::strong_count(&tracker);
        let mut context = Context::from_waker(&waker);
        let mut cancelled = Box::pin(event.clone().wait_owned());
        let mut survivor = Box::pin(event.clone().wait_owned());

        assert!(cancelled.as_mut().poll(&mut context).is_pending());
        assert!(survivor.as_mut().poll(&mut context).is_pending());

        let start = Arc::new(Barrier::new(2));
        let signaller = thread::spawn({
            let event = event.clone();
            let start = start.clone();
            move || {
                start.wait();
                event.set();
            }
        });

        start.wait();
        drop(cancelled);
        signaller.join().unwrap();

        assert!(survivor.as_mut().poll(&mut context).is_ready());
        drop(survivor);
        assert_eq!(Arc::strong_count(&tracker), baseline);
        assert_eq!(Arc::strong_count(&event), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_releases_all_current_and_future_waiters() {
    let event = Arc::new(ManualResetEvent::new());
    let mut tasks = Vec::new();
    for _ in 0..16 {
        tasks.push(tokio::spawn(event.clone().wait_owned()));
    }

    tokio::task::yield_now().await;
    event.set();

    for task in tasks {
        task.await.unwrap();
    }
    event.clone().wait_owned().await;
}
