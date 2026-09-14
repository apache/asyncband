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
use std::pin::pin;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Wake;
use std::task::Waker;
use std::thread;

use asyncband::blocking::FutureExt;
use asyncband::event::AutoResetEvent;
use tests_integration::PanicWake;
use tests_integration::WakeCounter;
use tests_integration::assert_completes_without_deadlock;
use tests_integration::poll_once;

#[test]
fn unpolled_waits_do_not_reserve_stored_signals() {
    let event = AutoResetEvent::new();
    assert!(!event.is_set());
    let mut first = pin!(event.wait());
    let mut second = pin!(event.wait());

    event.set();
    event.set();
    assert!(event.is_set());
    assert!(poll_once(second.as_mut()).is_ready());
    assert!(!event.is_set());
    assert!(!event.try_wait());
    assert!(poll_once(first.as_mut()).is_pending());

    event.set();
    assert!(poll_once(first.as_mut()).is_ready());
    assert!(!event.try_wait());
}

#[test]
fn reset_discards_only_unassigned_signals() {
    let event = AutoResetEvent::new();
    let mut selected = pin!(event.wait());
    assert!(poll_once(selected.as_mut()).is_pending());
    event.set();
    assert!(!event.is_set()); // The signal belongs to the selected wait.

    let mut unpolled = pin!(event.wait());
    event.set(); // One signal is assigned and another is stored.
    assert!(event.is_set());
    event.reset();
    assert!(!event.is_set());
    assert!(!event.try_wait());
    assert!(poll_once(unpolled.as_mut()).is_pending());
    assert!(poll_once(selected.as_mut()).is_ready());

    event.reset();
    event.set();
    assert!(poll_once(unpolled.as_mut()).is_ready());
    assert!(!event.try_wait());
}

#[test]
fn reset_preserves_cancellation_handoff() {
    let event = AutoResetEvent::new();
    let mut selected = Box::pin(event.wait());
    assert!(poll_once(selected.as_mut()).is_pending());
    event.set();
    event.reset();

    let mut remaining = Box::pin(event.wait());
    assert!(poll_once(remaining.as_mut()).is_pending());
    drop(selected);
    assert!(!event.is_set());
    assert!(!event.try_wait());

    // The transferred signal also survives reset and can be restored by cancellation.
    event.reset();
    drop(remaining);
    assert!(event.is_set());
    assert!(event.try_wait());
    assert!(!event.is_set());
    assert!(!event.try_wait());
}

#[test]
fn assigned_signals_cannot_be_stolen() {
    let event = AutoResetEvent::new();
    let first_wake = Arc::new(WakeCounter::default());
    let second_wake = Arc::new(WakeCounter::default());
    let first_waker = Waker::from(first_wake.clone());
    let second_waker = Waker::from(second_wake.clone());
    let mut first = pin!(event.wait());
    let mut second = pin!(event.wait());
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    assert!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending()
    );

    event.set();
    assert_eq!(first_wake.count() + second_wake.count(), 1);
    assert!(!event.try_wait());
    let (unselected, waker) = if first_wake.count() == 0 {
        (first.as_mut(), &first_waker)
    } else {
        (second.as_mut(), &second_waker)
    };
    assert!(
        unselected
            .poll(&mut Context::from_waker(waker))
            .is_pending()
    );

    // Another set serves the remaining waiter before the selected wait is polled again.
    event.set();
    assert_eq!(first_wake.count(), 1);
    assert_eq!(second_wake.count(), 1);
    let mut newcomer = pin!(event.wait());
    assert!(poll_once(newcomer.as_mut()).is_pending());
    assert!(!event.try_wait());
    assert!(poll_once(second.as_mut()).is_ready());
    assert!(poll_once(first.as_mut()).is_ready());
    event.set();
    assert!(poll_once(newcomer.as_mut()).is_ready());
    assert!(!event.try_wait());
}

#[test]
fn cancelling_selected_waits_transfers_then_restores_the_signal() {
    let event = AutoResetEvent::new();
    let mut first = Box::pin(event.wait());
    let mut second = Box::pin(event.wait());
    let mut third = Box::pin(event.wait());
    assert!(poll_once(first.as_mut()).is_pending());
    event.set();
    assert!(poll_once(second.as_mut()).is_pending());
    drop(first);
    assert!(!event.try_wait());
    assert!(poll_once(third.as_mut()).is_pending());
    drop(second);
    assert!(!event.try_wait());
    drop(third);
    assert!(event.try_wait());
    assert!(!event.try_wait());
}

#[test]
fn cancelling_unselected_waits_does_not_add_a_signal() {
    let event = AutoResetEvent::new();
    let mut cancelled = Box::pin(event.wait());
    let mut remaining = Box::pin(event.wait());
    assert!(poll_once(cancelled.as_mut()).is_pending());
    assert!(poll_once(remaining.as_mut()).is_pending());

    drop(cancelled);
    assert!(!event.try_wait());
    assert!(poll_once(remaining.as_mut()).is_pending());
    event.set();
    assert!(poll_once(remaining.as_mut()).is_ready());
    drop(remaining);
    assert!(!event.try_wait());
}

#[test]
fn returned_signals_coalesce_with_an_already_stored_signal() {
    let event = AutoResetEvent::new();
    let mut first = Box::pin(event.wait());
    let mut second = Box::pin(event.wait());
    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());
    event.set();
    event.set();
    event.set();

    drop(first);
    drop(second);
    assert!(event.try_wait());
    assert!(!event.try_wait());
}

#[test]
fn completed_owned_waits_do_not_return_their_signal() {
    let event = Arc::new(AutoResetEvent::with_state(true));
    let weak = Arc::downgrade(&event);
    let mut completed = Box::pin(event.clone().wait_owned());
    assert!(poll_once(completed.as_mut()).is_ready());
    drop(completed);
    assert!(!event.try_wait());

    let mut pending = Box::pin(event.clone().wait_owned());
    assert!(poll_once(pending.as_mut()).is_pending());
    event.set();
    drop(pending);
    assert!(event.try_wait());
    drop(event);
    assert!(weak.upgrade().is_none());
}

#[test]
fn waker_replacement_and_cancellation_release_registrations() {
    let event = AutoResetEvent::new();
    let first = Arc::new(WakeCounter::default());
    let second = Arc::new(WakeCounter::default());
    let first_waker = Waker::from(first.clone());
    let second_waker = Waker::from(second.clone());
    let mut wait = Box::pin(event.wait());
    for _ in 0..2 {
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&first_waker))
                .is_pending()
        );
        assert_eq!(Arc::strong_count(&first), 3);
    }
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending()
    );
    assert_eq!(Arc::strong_count(&first), 2);
    assert_eq!(Arc::strong_count(&second), 3);
    drop(wait);
    assert_eq!(Arc::strong_count(&second), 2);
    event.set();
    assert_eq!(first.count(), 0);
    assert_eq!(second.count(), 0);
    assert!(event.try_wait());
}

struct ReentrantWaker(Arc<AutoResetEvent>);

impl Wake for ReentrantWaker {
    fn wake(self: Arc<Self>) {
        self.0.try_wait();
    }
}

impl Drop for ReentrantWaker {
    fn drop(&mut self) {
        self.0.try_wait();
    }
}

#[test]
fn wake_and_waker_destruction_happen_outside_the_event_lock() {
    assert_completes_without_deadlock(|| {
        let event = Arc::new(AutoResetEvent::new());
        let mut first = Box::pin(event.wait());
        let mut second = Box::pin(event.wait());
        assert!(poll_once(first.as_mut()).is_pending());
        event.set();
        {
            let waker = Waker::from(Arc::new(ReentrantWaker(event.clone())));
            assert!(
                second
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }
        drop(first); // The cancellation handoff wakes the second waiter.
        assert!(poll_once(second.as_mut()).is_ready());

        let mut wait = Box::pin(event.wait());
        {
            let waker = Waker::from(Arc::new(ReentrantWaker(event.clone())));
            assert!(
                wait.as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }
        event.set();
        assert!(poll_once(wait.as_mut()).is_ready());

        let mut cancelled = Box::pin(event.wait());
        for _ in 0..2 {
            let waker = Waker::from(Arc::new(ReentrantWaker(event.clone())));
            assert!(
                cancelled
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }
        drop(cancelled); // Both replaced and cancelled registrations release their last waker.
    });
}

#[test]
fn a_panicking_wake_leaves_its_signal_available_for_cancellation_handoff() {
    let event = AutoResetEvent::new();
    let waker = Waker::from(Arc::new(PanicWake));
    let mut selected = Box::pin(event.wait());
    let mut next = Box::pin(event.wait());
    assert!(
        selected
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(panic::catch_unwind(AssertUnwindSafe(|| event.set())).is_err());
    assert!(!event.try_wait());
    assert!(poll_once(next.as_mut()).is_pending());
    drop(selected);
    assert!(poll_once(next.as_mut()).is_ready());
    assert!(!event.try_wait());
}

#[test]
fn concurrent_sets_and_waits_publish_state_without_losing_signals() {
    assert_completes_without_deadlock(|| {
        let event = AutoResetEvent::new();
        let round = Barrier::new(2);
        let value = AtomicUsize::new(0);
        thread::scope(|scope| {
            scope.spawn(|| {
                for expected in 1..=100 {
                    round.wait();
                    value.store(expected, Ordering::Relaxed);
                    event.set();
                    round.wait();
                }
            });
            for expected in 1..=100 {
                // Registration races with set; the second barrier prevents the next set from
                // coalescing before this round's signal has been consumed.
                round.wait();
                event.wait().block_on();
                assert_eq!(value.load(Ordering::Relaxed), expected);
                round.wait();
            }
        });
    });
}
