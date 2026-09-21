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

use std::panic;
use std::sync::Arc;
use std::task::Poll;
use std::task::Waker;

use asyncband::blocking::FutureExt;
use asyncband::phaser::Phaser;
use tests_integration::PanicWake;
use tests_integration::WakeCounter;
use tests_integration::poll_once;
use tests_integration::poll_with;
use tests_integration::waker_on_drop;

#[test]
fn batch_registration_joins_one_observed_phase() {
    let phaser = Phaser::new();
    let mut participants = phaser.register(3).unwrap();

    assert_eq!(participants.len(), 3);
    assert_eq!(phaser.registered_parties(), 3);
    assert_eq!(phaser.unarrived_parties(), 3);

    let mut first = participants.next().unwrap();
    let observed = first.arrive().unwrap();
    let mut second = participants.next().unwrap();
    second.arrive().unwrap();
    assert_eq!(participants.len(), 1);
    assert_eq!(phaser.phase(), observed);

    let (waker, counter) = WakeCounter::new();
    let mut wait = Box::pin(first.wait());
    assert!(poll_with(wait.as_mut(), &waker).is_pending());
    drop(participants);
    assert_eq!(counter.count(), 1);
    assert_eq!(poll_once(wait.as_mut()), Poll::Ready(Ok(phaser.phase())));
    assert_ne!(phaser.phase(), observed);
    assert_eq!(phaser.registered_parties(), 2);
    assert_eq!(phaser.unarrived_parties(), 2);
}

#[test]
fn collecting_a_batch_can_unwind_without_leaking_registrations() {
    let phaser = Phaser::new();
    let mut coordinator = phaser.register_one().unwrap();
    let observed = phaser.phase();

    assert!(
        panic::catch_unwind(|| {
            let _: Vec<_> = phaser
                .register(3)
                .unwrap()
                .enumerate()
                .map(|(index, participant)| {
                    assert_ne!(index, 1, "task setup failed");
                    participant
                })
                .collect();
        })
        .is_err()
    );
    assert_eq!(phaser.phase(), observed);
    assert_eq!(phaser.registered_parties(), 1);
    assert_eq!(phaser.unarrived_parties(), 1);
    coordinator.arrive().unwrap();
    assert_ne!(phaser.phase(), observed);
}

#[test]
fn exhausted_batch_does_not_advance_a_dormant_phaser() {
    let phaser = Phaser::new();
    let mut participants = phaser.register(1).unwrap();
    drop(participants.next().unwrap());
    let completed = phaser.phase();

    assert_eq!(participants.len(), 0);
    assert!(participants.next().is_none());
    drop(participants);
    assert_eq!(phaser.phase(), completed);
    assert_eq!(phaser.registered_parties(), 0);
}

#[test]
fn an_existing_batch_can_be_iterated_and_withdrawn_after_close() {
    let phaser = Phaser::new();
    let mut participants = phaser.register(3).unwrap();
    let observed = phaser.phase();
    phaser.close();

    let mut participant = participants.next().unwrap();
    drop(participants);
    assert_eq!(phaser.registered_parties(), 1);
    assert_eq!(phaser.unarrived_parties(), 1);
    assert!(participant.arrive().is_err());
    drop(participant);
    assert_eq!(phaser.phase(), observed);
    assert_eq!(phaser.registered_parties(), 0);
    assert_eq!(phaser.unarrived_parties(), 0);
}

#[test]
fn registering_zero_parties_is_a_noop() {
    let phaser = Phaser::new();
    let phase = phaser.phase();

    assert_eq!(phaser.register(0).unwrap().len(), 0);
    assert_eq!(phaser.phase(), phase);
    assert_eq!(phaser.registered_parties(), 0);
    assert_eq!(phaser.unarrived_parties(), 0);
}

#[test]
fn participants_advance_across_repeated_phases() {
    let phaser = Phaser::new();
    let phase0 = phaser.phase();
    let mut first = phaser.register_one().unwrap();
    let mut second = phaser.register_one().unwrap();

    assert_eq!(first.arrive().unwrap(), phase0);
    assert_eq!(phaser.arrived_parties(), 1);
    assert_eq!(second.arrive().unwrap(), phase0);
    let phase1 = phaser.phase();
    assert_ne!(phase1, phase0);
    assert_eq!(phaser.arrived_parties(), 0);

    assert_eq!(first.arrive().unwrap(), phase1);
    assert_eq!(second.arrive().unwrap(), phase1);
    assert_ne!(phaser.phase(), phase1);
}

#[test]
fn unpolled_wait_future_does_not_arrive() {
    let phaser = Phaser::new();
    let mut participant = phaser.register_one().unwrap();

    let wait = participant.wait();

    assert_eq!(phaser.arrived_parties(), 0);
    drop(wait);
    assert_eq!(phaser.arrived_parties(), 0);
}

#[test]
fn cancelled_wait_retry_waits_for_original_phase_after_advance() {
    let phaser = Phaser::new();
    let phase0 = phaser.phase();
    let mut first = phaser.register_one().unwrap();
    let mut second = phaser.register_one().unwrap();

    {
        let mut cancelled = Box::pin(first.wait());
        assert!(poll_once(cancelled.as_mut()).is_pending());
    }

    assert_eq!(phaser.arrived_parties(), 1);
    assert_eq!(second.arrive().unwrap(), phase0);
    let phase1 = phaser.phase();
    assert_ne!(phase1, phase0);
    assert_eq!(phaser.arrived_parties(), 0);

    let mut retry = Box::pin(first.wait());
    assert_eq!(poll_once(retry.as_mut()), Poll::Ready(Ok(phase1)));
    assert_eq!(phaser.arrived_parties(), 0);
}

#[test]
fn cancelled_wait_retry_before_advance_does_not_arrive_twice() {
    let phaser = Phaser::new();
    let mut first = phaser.register_one().unwrap();
    let mut second = phaser.register_one().unwrap();

    {
        let mut cancelled = Box::pin(first.wait());
        assert!(poll_once(cancelled.as_mut()).is_pending());
    }

    let mut retry = Box::pin(first.wait());
    assert!(poll_once(retry.as_mut()).is_pending());
    assert_eq!(phaser.arrived_parties(), 1);

    second.arrive().unwrap();
    assert!(poll_once(retry.as_mut()).is_ready());
}

#[test]
fn dropping_last_participant_advances_once_and_dormant_phaser_can_be_reused() {
    let phaser = Phaser::new();
    let phase0 = phaser.phase();
    let participant = phaser.register_one().unwrap();

    drop(participant);
    let phase1 = phaser.phase();
    assert_ne!(phase1, phase0);
    assert_eq!(phaser.registered_parties(), 0);
    assert_eq!(phaser.arrived_parties(), 0);

    let mut participant = phaser.register_one().unwrap();
    assert_eq!(participant.arrive().unwrap(), phase1);
    assert_ne!(phaser.phase(), phase1);
}

#[test]
fn dropping_an_arrived_participant_only_removes_its_next_phase_registration() {
    let phaser = Phaser::new();
    let phase0 = phaser.phase();
    let mut first = phaser.register_one().unwrap();
    let mut second = phaser.register_one().unwrap();

    first.arrive().unwrap();
    drop(first);
    assert_eq!(phaser.phase(), phase0);
    assert_eq!(phaser.registered_parties(), 1);
    assert_eq!(phaser.unarrived_parties(), 1);

    second.arrive().unwrap();
    assert_ne!(phaser.phase(), phase0);
}

#[test]
fn registration_before_last_arrival_joins_and_delays_current_phase() {
    let phaser = Phaser::new();
    let phase = phaser.phase();
    let mut first = phaser.register_one().unwrap();
    let mut second = phaser.register_one().unwrap();

    first.arrive().unwrap();
    let mut joining = phaser.register_one().unwrap();
    second.arrive().unwrap();

    assert_eq!(phaser.phase(), phase);
    assert_eq!(phaser.unarrived_parties(), 1);
    joining.arrive().unwrap();
    assert_ne!(phaser.phase(), phase);
}

#[test]
fn registration_after_last_arrival_joins_the_advanced_phase() {
    let phaser = Phaser::new();
    let phase0 = phaser.phase();
    let mut first = phaser.register_one().unwrap();

    first.arrive().unwrap();
    let phase1 = phaser.phase();
    assert_ne!(phase1, phase0);

    let mut joining = phaser.register_one().unwrap();
    assert_eq!(phaser.registered_parties(), 2);
    assert_eq!(phaser.unarrived_parties(), 2);
    assert_eq!(joining.arrive().unwrap(), phase1);
    assert_eq!(phaser.phase(), phase1);
}

#[test]
fn registration_before_last_participant_drop_joins_the_current_phase() {
    let phaser = Phaser::new();
    let phase0 = phaser.phase();
    let participant = phaser.register_one().unwrap();
    let joining = phaser.register_one().unwrap();

    drop(participant);

    assert_eq!(phaser.phase(), phase0);
    assert_eq!(phaser.registered_parties(), 1);
    assert_eq!(phaser.unarrived_parties(), 1);
    assert_eq!(joining.deregister().unwrap(), phase0);
    assert_ne!(phaser.phase(), phase0);
}

#[test]
fn registration_after_last_participant_drop_joins_the_advanced_phase() {
    let phaser = Phaser::new();
    let phase0 = phaser.phase();
    let participant = phaser.register_one().unwrap();

    drop(participant);
    let phase1 = phaser.phase();
    let joining = phaser.register_one().unwrap();

    assert_ne!(phase1, phase0);
    assert_eq!(phaser.registered_parties(), 1);
    assert_eq!(phaser.unarrived_parties(), 1);
    assert_eq!(joining.deregister().unwrap(), phase1);
    assert_ne!(phaser.phase(), phase1);
}

#[test]
fn observer_wait_is_cancel_safe_and_does_not_participate() {
    let phaser = Phaser::new();
    let phase = phaser.phase();
    let (waker, counter) = WakeCounter::new();

    {
        let mut wait = Box::pin(phaser.wait(phase));
        assert_eq!(poll_with(wait.as_mut(), &waker), Poll::Pending);
        assert_eq!(phaser.registered_parties(), 0);
    }

    assert_eq!(phaser.registered_parties(), 0);
    let participant = phaser.register_one().unwrap();
    drop(participant);
    assert_eq!(counter.count(), 0);
}

#[test]
fn advancing_a_phase_wakes_every_registered_waiter_once() {
    let phaser = Phaser::new();
    let observed = phaser.phase();
    let participant = phaser.register_one().unwrap();
    let (first_waker, first_counter) = WakeCounter::new();
    let (second_waker, second_counter) = WakeCounter::new();
    let mut first_wait = Box::pin(phaser.wait(observed));
    let mut second_wait = Box::pin(phaser.wait(observed));

    assert_eq!(poll_with(first_wait.as_mut(), &first_waker), Poll::Pending);
    assert_eq!(
        poll_with(second_wait.as_mut(), &second_waker),
        Poll::Pending
    );

    drop(participant);
    assert_eq!(first_counter.count(), 1);
    assert_eq!(second_counter.count(), 1);
    assert!(matches!(
        poll_with(first_wait.as_mut(), &first_waker),
        Poll::Ready(_)
    ));
    assert!(matches!(
        poll_with(second_wait.as_mut(), &second_waker),
        Poll::Ready(_)
    ));
}

#[test]
fn repolling_updates_the_task_that_will_be_notified() {
    let phaser = Phaser::new();
    let participant = phaser.register_one().unwrap();
    let (first_waker, first) = WakeCounter::new();
    let (second_waker, second) = WakeCounter::new();
    let mut wait = Box::pin(phaser.wait(phaser.phase()));

    for waker in [&first_waker, &first_waker, &second_waker, &second_waker] {
        assert!(poll_with(wait.as_mut(), waker).is_pending());
    }
    drop(participant);

    assert_eq!(first.count(), 0);
    assert_eq!(second.count(), 1);
    assert_eq!(poll_once(wait.as_mut()), Poll::Ready(Ok(phaser.phase())));
}

#[test]
fn cancelling_a_woken_waiter_does_not_unregister_a_next_phase_waiter() {
    let phaser = Phaser::new();
    let phase0 = phaser.phase();
    let participant = phaser.register_one().unwrap();
    let (stale_waker, stale_counter) = WakeCounter::new();
    let mut stale_wait = Box::pin(phaser.wait(phase0));

    assert_eq!(poll_with(stale_wait.as_mut(), &stale_waker), Poll::Pending);
    drop(participant);
    let phase1 = phaser.phase();
    assert_ne!(phase1, phase0);
    assert_eq!(stale_counter.count(), 1);

    let participant = phaser.register_one().unwrap();
    let (current_waker, current_counter) = WakeCounter::new();
    let mut current_wait = Box::pin(phaser.wait(phase1));
    assert_eq!(
        poll_with(current_wait.as_mut(), &current_waker),
        Poll::Pending
    );

    drop(stale_wait);
    drop(participant);
    assert_eq!(current_counter.count(), 1);
    assert!(matches!(
        poll_with(current_wait.as_mut(), &current_waker),
        Poll::Ready(_)
    ));
}

#[test]
fn panicking_waker_does_not_lose_a_pending_phase() {
    let phaser = Phaser::new();
    let phase0 = phaser.phase();
    let mut first = phaser.register_one().unwrap();
    let mut second = phaser.register_one().unwrap();
    let panic_waker = Waker::from(Arc::new(PanicWake));
    let mut observer = Box::pin(phaser.wait(phase0));

    assert_eq!(poll_with(observer.as_mut(), &panic_waker), Poll::Pending);
    assert_eq!(first.arrive().unwrap(), phase0);

    let (polling_waker, _) = WakeCounter::new();
    let mut wait = Box::pin(second.wait());
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        poll_with(wait.as_mut(), &polling_waker)
    }));

    assert!(result.is_err());
    drop(wait);
    drop(observer);

    let phase1 = phaser.phase();
    assert_ne!(phase1, phase0);
    assert_eq!(phaser.arrived_parties(), 0);

    let mut retry = Box::pin(second.wait());
    assert_eq!(poll_once(retry.as_mut()), Poll::Ready(Ok(phase1)));
    assert_eq!(phaser.arrived_parties(), 0);
}

#[test]
fn a_late_waiter_for_a_completed_phase_is_immediately_ready() {
    let phaser = Phaser::new();
    let observed = phaser.phase();
    let participant = phaser.register_one().unwrap();
    drop(participant);

    let mut wait = Box::pin(phaser.wait(observed));
    assert_eq!(poll_once(wait.as_mut()), Poll::Ready(Ok(phaser.phase())));
}

#[test]
fn registration_overflow_panics_without_partially_updating_state() {
    let phaser = Phaser::new();
    let participants = phaser.register(usize::MAX).unwrap();

    assert!(panic::catch_unwind(|| phaser.register_one().unwrap()).is_err());
    assert!(panic::catch_unwind(|| phaser.register(2).unwrap()).is_err());
    assert_eq!(phaser.registered_parties(), usize::MAX);
    assert_eq!(phaser.unarrived_parties(), usize::MAX);
    drop(participants);
    assert_eq!(phaser.registered_parties(), 0);
    assert_eq!(phaser.unarrived_parties(), 0);
}

#[test]
fn explicit_arrival_and_wait_observe_the_same_completed_phase() {
    let phaser = Phaser::new();
    let mut first = phaser.register_one().unwrap();
    let mut second = phaser.register_one().unwrap();
    let observed = first.arrive().unwrap();
    second.arrive().unwrap();
    let next = phaser.phase();

    assert_ne!(observed, next);
    assert_eq!(
        poll_once(Box::pin(first.wait()).as_mut()),
        Poll::Ready(Ok(next))
    );
    assert_eq!(
        poll_once(Box::pin(second.wait()).as_mut()),
        Poll::Ready(Ok(next))
    );
    assert_eq!(phaser.arrived_parties(), 0);

    let mut wait = Box::pin(first.wait());
    assert!(poll_once(wait.as_mut()).is_pending());
    second.arrive().unwrap();
    assert_eq!(poll_once(wait.as_mut()), Poll::Ready(Ok(phaser.phase())));
}

#[test]
fn explicit_arrival_replaces_a_cancelled_pending_observation() {
    let phaser = Phaser::new();
    let mut first = phaser.register_one().unwrap();
    let mut second = phaser.register_one().unwrap();
    assert!(poll_once(Box::pin(first.wait()).as_mut()).is_pending());
    second.arrive().unwrap();
    let next = first.arrive().unwrap();
    assert_eq!(next, phaser.phase());

    let mut wait = Box::pin(first.wait());
    assert!(poll_once(wait.as_mut()).is_pending());
    second.arrive().unwrap();
    assert_eq!(poll_once(wait.as_mut()), Poll::Ready(Ok(phaser.phase())));
}

#[test]
fn cloned_handles_observe_without_registering_and_participants_own_the_state() {
    let phaser = Phaser::new();
    let observer = phaser.clone();
    let mut participant = phaser.register_one().unwrap();
    drop(phaser);
    assert_eq!(observer.registered_parties(), 1);
    let observed = observer.phase();
    participant.arrive().unwrap();
    assert_eq!(
        poll_once(Box::pin(observer.wait(observed)).as_mut()),
        Poll::Ready(Ok(observer.phase()))
    );
}

#[test]
fn closing_wakes_all_waiters_once_and_rejects_new_obligations() {
    let phaser = Phaser::new();
    let mut first = phaser.register_one().unwrap();
    let second = phaser.register_one().unwrap();
    let observed = first.arrive().unwrap();
    let (waker, counter) = WakeCounter::new();
    let mut observer = Box::pin(phaser.wait(observed));
    let mut wait = Box::pin(first.wait());
    assert!(poll_with(observer.as_mut(), &waker).is_pending());
    assert!(poll_with(wait.as_mut(), &waker).is_pending());

    phaser.clone().close();
    phaser.close();
    assert!(phaser.is_closed());
    assert_eq!(counter.count(), 2);
    assert!(matches!(poll_once(observer.as_mut()), Poll::Ready(Err(_))));
    assert!(matches!(poll_once(wait.as_mut()), Poll::Ready(Err(_))));
    drop(wait);
    assert!(first.arrive().is_err());
    assert!(phaser.register_one().is_err());
    assert!(phaser.register(2).is_err());
    assert!(phaser.register(0).is_err());
    assert!(first.deregister().is_err());
    drop(second);
    assert_eq!(phaser.registered_parties(), 0);
    assert_eq!(phaser.unarrived_parties(), 0);
    assert_eq!(phaser.phase(), observed);
}

#[test]
fn completed_arrival_remains_successful_after_close_but_cannot_start_another_round() {
    let phaser = Phaser::new();
    let mut first = phaser.register_one().unwrap();
    let mut second = phaser.register_one().unwrap();
    let observed = first.arrive().unwrap();
    let mut observer = Box::pin(phaser.wait(observed));
    assert!(poll_once(observer.as_mut()).is_pending());
    second.arrive().unwrap();
    let completed = phaser.phase();
    phaser.close();

    assert_eq!(poll_once(observer.as_mut()), Poll::Ready(Ok(completed)));
    assert_eq!(
        poll_once(Box::pin(first.wait()).as_mut()),
        Poll::Ready(Ok(completed))
    );
    assert!(matches!(
        poll_once(Box::pin(first.wait()).as_mut()),
        Poll::Ready(Err(_))
    ));
    drop(first);
    drop(second);
    assert_eq!(phaser.phase(), completed);
}

#[test]
fn close_survives_a_panicking_waker_and_notifies_other_waiters() {
    let phaser = Phaser::new();
    let observed = phaser.phase();
    let panic_waker = Waker::from(Arc::new(PanicWake));
    let (count_waker, counter) = WakeCounter::new();
    let mut first = Box::pin(phaser.wait(observed));
    let mut second = Box::pin(phaser.wait(observed));
    assert!(poll_with(first.as_mut(), &panic_waker).is_pending());
    assert!(poll_with(second.as_mut(), &count_waker).is_pending());

    assert!(panic::catch_unwind(|| phaser.close()).is_err());
    assert!(phaser.is_closed());
    assert_eq!(counter.count(), 1);
    assert!(matches!(poll_once(first.as_mut()), Poll::Ready(Err(_))));
    assert!(matches!(poll_once(second.as_mut()), Poll::Ready(Err(_))));
}

#[test]
fn replacing_a_waiter_waker_can_close_the_phaser_from_its_destructor() {
    let phaser = Phaser::new();
    let waker = waker_on_drop({
        let phaser = phaser.clone();
        move || phaser.close()
    });
    let mut wait = Box::pin(phaser.wait(phaser.phase()));
    assert!(poll_with(wait.as_mut(), &waker).is_pending());
    drop(waker);
    assert!(!phaser.is_closed());

    let (replacement, counter) = WakeCounter::new();
    assert!(poll_with(wait.as_mut(), &replacement).is_pending());
    assert!(phaser.is_closed());
    assert_eq!(counter.count(), 1);
    assert!(matches!(poll_once(wait.as_mut()), Poll::Ready(Err(_))));
}

#[test]
fn cancelling_a_waiter_can_close_the_phaser_from_its_waker_destructor() {
    let phaser = Phaser::new();
    let waker = waker_on_drop({
        let phaser = phaser.clone();
        move || phaser.close()
    });
    let mut wait = Box::pin(phaser.wait(phaser.phase()));
    assert!(poll_with(wait.as_mut(), &waker).is_pending());
    drop(waker);
    assert!(!phaser.is_closed());

    drop(wait);
    assert!(phaser.is_closed());
}

#[tokio::test]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn participant_can_wait_from_a_spawned_task() {
    let phaser = Phaser::new();
    let mut first = phaser.register_one().unwrap();
    let mut second = phaser.register_one().unwrap();

    let first_wait = tokio::spawn(async move { first.wait().await });

    tokio::task::yield_now().await;
    let phase = second.arrive().unwrap();
    assert_eq!(first_wait.await.unwrap().unwrap(), phaser.phase());
    assert_ne!(phase, phaser.phase());
}

#[tokio::test]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn observer_waits_without_becoming_a_party() {
    let phaser = Phaser::new();
    let observed = phaser.phase();
    let mut first = phaser.register_one().unwrap();
    let second = phaser.register_one().unwrap();
    let observer_phaser = phaser.clone();
    let observer = tokio::spawn(async move { observer_phaser.wait(observed).await });

    tokio::task::yield_now().await;
    assert_eq!(phaser.registered_parties(), 2);
    first.arrive().unwrap();
    second.deregister().unwrap();

    assert_eq!(observer.await.unwrap().unwrap(), phaser.phase());
    assert_eq!(phaser.registered_parties(), 1);
}

#[test]
fn arrivals_publish_each_workers_writes_across_threads() {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    let phaser = Phaser::new();
    let values = std::array::from_fn::<_, 4, _>(|_| AtomicUsize::new(0));
    let participants = phaser.register(values.len()).unwrap();
    std::thread::scope(|scope| {
        for (id, mut participant) in participants.enumerate() {
            let values = &values;
            scope.spawn(move || {
                FutureExt::block_on(async {
                    for round in 1..=16 {
                        values[id].store(round, Ordering::Relaxed);
                        participant.wait().await.unwrap();
                        assert!(
                            values
                                .iter()
                                .all(|value| value.load(Ordering::Relaxed) == round)
                        );
                        // Keep the next round's writers behind this read-side rendezvous.
                        participant.wait().await.unwrap();
                    }
                });
            });
        }
    });
}

#[tokio::test]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn a_failed_task_can_close_the_group_without_reporting_phase_completion() {
    let phaser = Phaser::new();
    let mut worker = phaser.register_one().unwrap();
    let failing = phaser.register_one().unwrap();
    let observed = phaser.phase();
    let (arrived, arrival) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        worker.arrive().unwrap();
        arrived.send(()).unwrap();
        worker.wait().await
    });
    arrival.await.unwrap();
    failing.phaser().close();
    drop(failing);
    assert!(task.await.unwrap().is_err());
    assert_eq!(phaser.phase(), observed);
    assert_eq!(phaser.registered_parties(), 0);
}
