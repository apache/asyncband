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
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use tokio_test::assert_pending;
use tokio_test::assert_ready;
use tokio_test::task::spawn;

use super::Phaser;

struct CountWake(AtomicUsize);

impl Wake for CountWake {
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

#[test]
fn register_many_joins_one_observed_phase() {
    let phaser = Arc::new(Phaser::new());
    let participants = phaser.register_many(3);

    assert_eq!(participants.len(), 3);
    assert_eq!(phaser.registered_parties(), 3);
    assert_eq!(phaser.unarrived_parties(), 3);
}

#[test]
fn registering_zero_parties_is_a_noop() {
    let phaser = Arc::new(Phaser::new());
    let phase = phaser.phase();

    assert!(phaser.register_many(0).is_empty());
    assert_eq!(phaser.phase(), phase);
    assert_eq!(phaser.registered_parties(), 0);
    assert_eq!(phaser.unarrived_parties(), 0);
}

#[test]
fn participants_advance_across_repeated_phases() {
    let phaser = Arc::new(Phaser::new());
    let phase0 = phaser.phase();
    let mut first = phaser.register();
    let mut second = phaser.register();

    assert_eq!(first.arrive(), phase0);
    assert_eq!(phaser.arrived_parties(), 1);
    assert_eq!(second.arrive(), phase0);
    let phase1 = phaser.phase();
    assert_ne!(phase1, phase0);
    assert_eq!(phaser.arrived_parties(), 0);

    assert_eq!(first.arrive(), phase1);
    assert_eq!(second.arrive(), phase1);
    assert_ne!(phaser.phase(), phase1);
}

#[test]
fn unpolled_arrive_and_wait_future_does_not_arrive() {
    let phaser = Arc::new(Phaser::new());
    let mut participant = phaser.register();

    let wait = participant.arrive_and_wait();

    assert_eq!(phaser.arrived_parties(), 0);
    drop(wait);
    assert_eq!(phaser.arrived_parties(), 0);
}

#[test]
fn cancelled_arrive_and_wait_retry_waits_for_original_phase_after_advance() {
    let phaser = Arc::new(Phaser::new());
    let phase0 = phaser.phase();
    let mut first = phaser.register();
    let mut second = phaser.register();

    {
        let mut cancelled = spawn(first.arrive_and_wait());
        assert_pending!(cancelled.poll());
    }

    assert_eq!(phaser.arrived_parties(), 1);
    assert_eq!(second.arrive(), phase0);
    let phase1 = phaser.phase();
    assert_ne!(phase1, phase0);
    assert_eq!(phaser.arrived_parties(), 0);

    let mut retry = spawn(first.arrive_and_wait());
    assert_eq!(assert_ready!(retry.poll()), phase1);
    assert_eq!(phaser.arrived_parties(), 0);
}

#[test]
fn cancelled_arrive_and_wait_retry_before_advance_does_not_arrive_twice() {
    let phaser = Arc::new(Phaser::new());
    let mut first = phaser.register();
    let mut second = phaser.register();

    {
        let mut cancelled = spawn(first.arrive_and_wait());
        assert_pending!(cancelled.poll());
    }

    let mut retry = spawn(first.arrive_and_wait());
    assert_pending!(retry.poll());
    assert_eq!(phaser.arrived_parties(), 1);

    second.arrive();
    assert_ready!(retry.poll());
}

#[test]
fn dropping_last_participant_advances_once_and_dormant_phaser_can_be_reused() {
    let phaser = Arc::new(Phaser::new());
    let phase0 = phaser.phase();
    let participant = phaser.register();

    drop(participant);
    let phase1 = phaser.phase();
    assert_ne!(phase1, phase0);
    assert_eq!(phaser.registered_parties(), 0);
    assert_eq!(phaser.arrived_parties(), 0);

    let mut participant = phaser.register();
    assert_eq!(participant.arrive(), phase1);
    assert_ne!(phaser.phase(), phase1);
}

#[test]
fn dropping_an_arrived_participant_only_removes_its_next_phase_registration() {
    let phaser = Arc::new(Phaser::new());
    let phase0 = phaser.phase();
    let mut first = phaser.register();
    let mut second = phaser.register();

    first.arrive();
    drop(first);
    assert_eq!(phaser.phase(), phase0);
    assert_eq!(phaser.registered_parties(), 1);
    assert_eq!(phaser.unarrived_parties(), 1);

    second.arrive();
    assert_ne!(phaser.phase(), phase0);
}

#[test]
fn registration_before_last_arrival_joins_and_delays_current_phase() {
    let phaser = Arc::new(Phaser::new());
    let phase = phaser.phase();
    let mut first = phaser.register();
    let mut second = phaser.register();

    first.arrive();
    let mut joining = phaser.register();
    second.arrive();

    assert_eq!(phaser.phase(), phase);
    assert_eq!(phaser.unarrived_parties(), 1);
    joining.arrive();
    assert_ne!(phaser.phase(), phase);
}

#[test]
fn registration_after_last_arrival_joins_the_advanced_phase() {
    let phaser = Arc::new(Phaser::new());
    let phase0 = phaser.phase();
    let mut first = phaser.register();

    first.arrive();
    let phase1 = phaser.phase();
    assert_ne!(phase1, phase0);

    let mut joining = phaser.register();
    assert_eq!(phaser.registered_parties(), 2);
    assert_eq!(phaser.unarrived_parties(), 2);
    assert_eq!(joining.arrive(), phase1);
    assert_eq!(phaser.phase(), phase1);
}

#[test]
fn registration_before_last_participant_drop_joins_the_current_phase() {
    let phaser = Arc::new(Phaser::new());
    let phase0 = phaser.phase();
    let participant = phaser.register();
    let joining = phaser.register();

    drop(participant);

    assert_eq!(phaser.phase(), phase0);
    assert_eq!(phaser.registered_parties(), 1);
    assert_eq!(phaser.unarrived_parties(), 1);
    assert_eq!(joining.arrive_and_deregister(), phase0);
    assert_ne!(phaser.phase(), phase0);
}

#[test]
fn registration_after_last_participant_drop_joins_the_advanced_phase() {
    let phaser = Arc::new(Phaser::new());
    let phase0 = phaser.phase();
    let participant = phaser.register();

    drop(participant);
    let phase1 = phaser.phase();
    let joining = phaser.register();

    assert_ne!(phase1, phase0);
    assert_eq!(phaser.registered_parties(), 1);
    assert_eq!(phaser.unarrived_parties(), 1);
    assert_eq!(joining.arrive_and_deregister(), phase1);
    assert_ne!(phaser.phase(), phase1);
}

#[test]
fn wait_for_advance_is_a_cancel_safe_non_participant_observer() {
    let phaser = Arc::new(Phaser::new());
    let phase = phaser.phase();
    let counter = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&counter));
    let mut context = Context::from_waker(&waker);

    {
        let mut wait = Box::pin(phaser.wait_for_advance(phase));
        assert_eq!(Future::poll(wait.as_mut(), &mut context), Poll::Pending);
        assert_eq!(phaser.registered_parties(), 0);
    }

    assert_eq!(phaser.registered_parties(), 0);
    let participant = phaser.register();
    drop(participant);
    assert_eq!(counter.0.load(Ordering::Relaxed), 0);
}

#[test]
fn advancing_a_phase_wakes_every_registered_waiter_once() {
    let phaser = Arc::new(Phaser::new());
    let observed = phaser.phase();
    let participant = phaser.register();
    let first_counter = Arc::new(CountWake(AtomicUsize::new(0)));
    let second_counter = Arc::new(CountWake(AtomicUsize::new(0)));
    let first_waker = Waker::from(Arc::clone(&first_counter));
    let second_waker = Waker::from(Arc::clone(&second_counter));
    let mut first_context = Context::from_waker(&first_waker);
    let mut second_context = Context::from_waker(&second_waker);
    let mut first_wait = Box::pin(phaser.wait_for_advance(observed));
    let mut second_wait = Box::pin(phaser.wait_for_advance(observed));

    assert_eq!(
        Future::poll(first_wait.as_mut(), &mut first_context),
        Poll::Pending
    );
    assert_eq!(
        Future::poll(second_wait.as_mut(), &mut second_context),
        Poll::Pending
    );

    drop(participant);
    assert_eq!(first_counter.0.load(Ordering::Relaxed), 1);
    assert_eq!(second_counter.0.load(Ordering::Relaxed), 1);
    assert!(matches!(
        Future::poll(first_wait.as_mut(), &mut first_context),
        Poll::Ready(_)
    ));
    assert!(matches!(
        Future::poll(second_wait.as_mut(), &mut second_context),
        Poll::Ready(_)
    ));
}

#[test]
fn cancelling_a_woken_waiter_does_not_unregister_a_next_phase_waiter() {
    let phaser = Arc::new(Phaser::new());
    let phase0 = phaser.phase();
    let participant = phaser.register();
    let stale_counter = Arc::new(CountWake(AtomicUsize::new(0)));
    let stale_waker = Waker::from(Arc::clone(&stale_counter));
    let mut stale_context = Context::from_waker(&stale_waker);
    let mut stale_wait = Box::pin(phaser.wait_for_advance(phase0));

    assert_eq!(
        Future::poll(stale_wait.as_mut(), &mut stale_context),
        Poll::Pending
    );
    drop(participant);
    let phase1 = phaser.phase();
    assert_ne!(phase1, phase0);
    assert_eq!(stale_counter.0.load(Ordering::Relaxed), 1);

    let participant = phaser.register();
    let current_counter = Arc::new(CountWake(AtomicUsize::new(0)));
    let current_waker = Waker::from(Arc::clone(&current_counter));
    let mut current_context = Context::from_waker(&current_waker);
    let mut current_wait = Box::pin(phaser.wait_for_advance(phase1));
    assert_eq!(
        Future::poll(current_wait.as_mut(), &mut current_context),
        Poll::Pending
    );

    drop(stale_wait);
    drop(participant);
    assert_eq!(current_counter.0.load(Ordering::Relaxed), 1);
    assert!(matches!(
        Future::poll(current_wait.as_mut(), &mut current_context),
        Poll::Ready(_)
    ));
}

#[test]
fn panicking_waker_does_not_lose_an_arrive_and_wait_phase() {
    let phaser = Arc::new(Phaser::new());
    let phase0 = phaser.phase();
    let mut first = phaser.register();
    let mut second = phaser.register();
    let panic_waker = Waker::from(Arc::new(PanicWake));
    let mut panic_context = Context::from_waker(&panic_waker);
    let mut observer = Box::pin(phaser.wait_for_advance(phase0));

    assert_eq!(
        Future::poll(observer.as_mut(), &mut panic_context),
        Poll::Pending
    );
    assert_eq!(first.arrive(), phase0);

    let polling_waker = Waker::from(Arc::new(CountWake(AtomicUsize::new(0))));
    let mut polling_context = Context::from_waker(&polling_waker);
    let mut wait = Box::pin(second.arrive_and_wait());
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        Future::poll(wait.as_mut(), &mut polling_context)
    }));

    assert!(result.is_err());
    drop(wait);
    drop(observer);

    let phase1 = phaser.phase();
    assert_ne!(phase1, phase0);
    assert_eq!(phaser.arrived_parties(), 0);

    let mut retry = spawn(second.arrive_and_wait());
    assert_eq!(assert_ready!(retry.poll()), phase1);
    assert_eq!(phaser.arrived_parties(), 0);
}

#[test]
fn a_late_waiter_for_a_completed_phase_is_immediately_ready() {
    let phaser = Arc::new(Phaser::new());
    let observed = phaser.phase();
    let participant = phaser.register();
    drop(participant);

    let mut wait = spawn(phaser.wait_for_advance(observed));
    assert_eq!(assert_ready!(wait.poll()), phaser.phase());
}

#[test]
fn phase_identity_wraps_without_an_ordering_contract() {
    let phaser = Arc::new(Phaser::new());
    phaser.state.lock().phase = super::Phase(u64::MAX);
    let observed = phaser.phase();
    let mut participant = phaser.register();

    assert_eq!(participant.arrive(), observed);
    assert_eq!(phaser.phase().get(), 0);
    assert_ne!(phaser.phase(), observed);
}

#[test]
fn registration_overflow_panics_without_partially_updating_state() {
    let phaser = Arc::new(Phaser::new());
    {
        let mut state = phaser.state.lock();
        state.registered = u32::MAX;
        state.unarrived = u32::MAX;
    }

    assert!(panic::catch_unwind(|| phaser.register()).is_err());
    assert_eq!(phaser.registered_parties(), u32::MAX);
    assert_eq!(phaser.unarrived_parties(), u32::MAX);
}
