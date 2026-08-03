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

use std::future::Future;
use std::future::pending;
use std::future::ready;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use super::*;

fn poll<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    let waker = Waker::noop();
    poll_with_waker(future, waker)
}

fn poll_with_waker<T>(future: Pin<&mut impl Future<Output = T>>, waker: &Waker) -> Poll<T> {
    let mut cx = Context::from_waker(waker);
    future.poll(&mut cx)
}

fn budget() -> TurnBudget {
    TurnBudget::new(
        NonZeroUsize::new(64).unwrap(),
        NonZeroUsize::new(64).unwrap(),
    )
}

fn tiny_budget() -> TurnBudget {
    TurnBudget::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(1).unwrap())
}

fn drive(driver: &mut TimerDriver, now: Instant) {
    let _ = driver.turn(now, budget());
}

fn drive_with_tiny_budget(driver: &mut TimerDriver, now: Instant) {
    let _ = driver.turn(now, tiny_budget());
}

fn assert_schedule_waits_for_initial_delay(
    driver: &mut TimerDriver,
    genesis: Instant,
    mut schedule: Pin<&mut impl Future<Output = Result<(), TimerClosed>>>,
    count: &AtomicUsize,
) {
    assert!(poll(schedule.as_mut()).is_pending());
    assert_eq!(count.load(Ordering::Relaxed), 0);
    drive(driver, genesis);
    drive(driver, genesis + Duration::from_millis(4));
    assert!(poll(schedule.as_mut()).is_pending());
    assert_eq!(count.load(Ordering::Relaxed), 0);
    drive(driver, genesis + Duration::from_millis(5));
    assert!(poll(schedule.as_mut()).is_pending());
    assert_eq!(count.load(Ordering::Relaxed), 1);
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn default_turn_budget_matches_documented_limits() {
    let budget = TurnBudget::default();
    assert_eq!(budget.max_operations().get(), 1_024);
    assert_eq!(budget.max_timer_entries().get(), 4_096);
}

#[test]
fn system_clock_context_uses_wall_clock_and_published_observation() {
    let (mut driver, timer) = TimerDriver::new();
    let duration = Duration::from_millis(10);

    let before = Instant::now();
    let wall_clock_delay = timer.delay(duration);
    assert!(
        wall_clock_delay.deadline.as_instant().unwrap() >= before.checked_add(duration).unwrap()
    );

    let published = Instant::now()
        .checked_add(Duration::from_secs(86_400))
        .unwrap();
    drive(&mut driver, published);
    let published_delay = timer.delay(duration);
    assert_eq!(
        published_delay.deadline,
        Deadline::At(published.checked_add(duration).unwrap())
    );
}

#[test]
fn next_poll_at_reports_pending_operations_and_the_earliest_deadline() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let later_deadline = genesis + Duration::from_millis(20);
    let earlier_deadline = genesis + Duration::from_millis(10);

    let mut later = Box::pin(timer.delay_until(later_deadline));
    assert!(poll(later.as_mut()).is_pending());
    assert_eq!(driver.next_poll_at(), Some(genesis));
    drive(&mut driver, genesis);
    assert_eq!(driver.next_poll_at(), Some(later_deadline));

    let mut earlier = Box::pin(timer.delay_until(earlier_deadline));
    assert!(poll(earlier.as_mut()).is_pending());
    assert_eq!(driver.next_poll_at(), Some(genesis));
    drive(&mut driver, genesis);
    assert_eq!(driver.next_poll_at(), Some(earlier_deadline));
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "timer driver cannot move backwards")]
fn turn_rejects_a_backwards_clock_in_debug_builds() {
    let genesis = Instant::now();
    let (mut driver, _timer) = TimerDriver::new_at(genesis);
    drive(&mut driver, genesis + Duration::from_millis(1));
    drive(&mut driver, genesis);
}

#[test]
fn dropping_driver_releases_its_registered_reactor_waker() {
    let genesis = Instant::now();
    let (driver, timer) = TimerDriver::new_at(genesis);
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());

    assert!(driver.register_wake(&waker));
    drop(waker);
    assert_eq!(Arc::strong_count(&counter), 2);

    drop(driver);
    assert_eq!(Arc::strong_count(&counter), 1);
    drop(timer);
}

#[test]
fn completed_delay_releases_its_registered_waker() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());
    let mut delay = Box::pin(timer.delay(Duration::from_millis(1)));

    assert!(poll_with_waker(delay.as_mut(), &waker).is_pending());
    drive(&mut driver, genesis);
    drive(&mut driver, genesis + Duration::from_millis(1));
    assert_eq!(poll_with_waker(delay.as_mut(), &waker), Poll::Ready(Ok(())));
    assert!(delay.as_ref().get_ref().state.is_none());

    drop(waker);
    assert_eq!(Arc::strong_count(&counter), 1);
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn cancelled_delays_release_wakers_before_the_driver_drains() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);

    let submitted_counter = Arc::new(WakeCount::default());
    let submitted_waker = Waker::from(submitted_counter.clone());
    let mut submitted = Box::pin(timer.delay(Duration::from_secs(1)));
    assert!(poll_with_waker(submitted.as_mut(), &submitted_waker).is_pending());
    drop(submitted_waker);
    drop(submitted);
    assert_eq!(Arc::strong_count(&submitted_counter), 1);

    drive(&mut driver, genesis);

    let registered_counter = Arc::new(WakeCount::default());
    let registered_waker = Waker::from(registered_counter.clone());
    let mut registered = Box::pin(timer.delay(Duration::from_secs(1)));
    assert!(poll_with_waker(registered.as_mut(), &registered_waker).is_pending());
    drive(&mut driver, genesis);
    drop(registered_waker);
    drop(registered);
    assert_eq!(Arc::strong_count(&registered_counter), 1);

    drive(&mut driver, genesis);
    assert_eq!(driver.wheel.len(), 0);
}

#[test]
fn delay_is_lazy_and_uses_driven_time() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let mut delay = std::pin::pin!(timer.delay(Duration::from_millis(10)));

    assert_eq!(driver.wheel.len(), 0);
    assert!(poll(delay.as_mut()).is_pending());
    assert_eq!(driver.wheel.len(), 0);

    assert!(!driver.turn(genesis, budget()).has_more_work());
    assert_eq!(driver.wheel.len(), 1);
    assert!(poll(delay.as_mut()).is_pending());

    drive(&mut driver, genesis + Duration::from_millis(9));
    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut driver, genesis + Duration::from_millis(10));
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn dropping_driver_closes_registered_delay() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let mut delay = std::pin::pin!(timer.delay(Duration::from_secs(1)));

    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut driver, genesis);
    drop(driver);
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
    assert!(delay.as_ref().get_ref().state.is_none());
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
}

#[test]
fn dropping_driver_closes_queued_delay() {
    let genesis = Instant::now();
    let (driver, timer) = TimerDriver::new_at(genesis);
    let mut delay = std::pin::pin!(timer.delay(Duration::from_secs(1)));

    assert!(poll(delay.as_mut()).is_pending());
    drop(driver);
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
    assert!(delay.as_ref().get_ref().state.is_none());
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
}

#[test]
fn failed_registration_send_returns_closed_without_self_wake() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    drop(driver.receiver.take());
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());
    let mut delay = Box::pin(timer.delay(Duration::from_secs(1)));

    assert_eq!(
        poll_with_waker(delay.as_mut(), &waker),
        Poll::Ready(Err(TimerClosed))
    );
    assert_eq!(counter.0.load(Ordering::Relaxed), 0);
    assert!(delay.as_ref().get_ref().state.is_none());
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
}

#[test]
fn fresh_delay_after_close_is_synchronously_closed_and_fused() {
    let genesis = Instant::now();
    let (driver, timer) = TimerDriver::new_at(genesis);
    drop(driver);
    let mut delay = std::pin::pin!(timer.delay_until(genesis + Duration::from_secs(1)));

    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
}

#[test]
fn elapsed_deadline_wins_over_closed_driver() {
    let genesis = Instant::now();
    let (driver, timer) = TimerDriver::new_at(genesis);
    drop(driver);
    let mut delay = std::pin::pin!(timer.delay_until(genesis));

    assert_eq!(poll(delay.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn cancellation_before_and_after_registration_reclaims_entries() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);

    let mut submitted = Box::pin(timer.delay(Duration::from_secs(1)));
    assert!(poll(submitted.as_mut()).is_pending());
    drop(submitted);
    drive(&mut driver, genesis);
    assert_eq!(driver.wheel.len(), 0);

    let mut registered = Box::pin(timer.delay(Duration::from_secs(1)));
    assert!(poll(registered.as_mut()).is_pending());
    drive(&mut driver, genesis);
    assert_eq!(driver.wheel.len(), 1);
    drop(registered);
    drive(&mut driver, genesis);
    assert_eq!(driver.wheel.len(), 0);
}

#[test]
fn operation_and_entry_budgets_bound_each_turn() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let mut delays: Vec<_> = (0..5)
        .map(|_| Box::pin(timer.delay_until(genesis + Duration::from_millis(1))))
        .collect();
    for delay in &mut delays {
        assert!(poll(delay.as_mut()).is_pending());
    }

    assert!(driver.turn(genesis, tiny_budget()).has_more_work());
    assert_eq!(driver.wheel.len(), 2);
    assert!(driver.turn(genesis, tiny_budget()).has_more_work());
    assert_eq!(driver.wheel.len(), 4);
    drive_with_tiny_budget(&mut driver, genesis);
    assert_eq!(driver.wheel.len(), 5);

    let due = genesis + Duration::from_millis(1);
    for expected in 1..=5 {
        let result = driver.turn(due, tiny_budget());
        let mut now_completed = 0;
        for delay in &mut delays {
            if poll(delay.as_mut()).is_ready() {
                now_completed += 1;
            }
        }
        assert_eq!(now_completed, expected);
        assert_eq!(result.has_more_work(), expected != 5);
    }
}

#[test]
fn immediate_and_wheel_timers_both_progress_under_continuous_registrations() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let mut existing = Box::pin(timer.delay_until(genesis + Duration::from_millis(1)));
    assert!(poll(existing.as_mut()).is_pending());
    drive(&mut driver, genesis);

    let mut immediate = Vec::new();
    for millis in 1..=2 {
        let now = genesis + Duration::from_millis(millis);
        for _ in 0..2 {
            let mut delay = Box::pin(timer.delay_until(now));
            assert!(poll(delay.as_mut()).is_pending());
            immediate.push(delay);
        }
        let _ = driver.turn(now, tiny_budget());
    }

    assert_eq!(poll(existing.as_mut()), Poll::Ready(Ok(())));
    assert!(
        immediate
            .iter_mut()
            .any(|delay| poll(delay.as_mut()).is_ready())
    );
}

#[test]
fn immediate_and_wheel_timers_share_the_entry_budget() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let due = genesis + Duration::from_millis(1);
    let mut wheel: Vec<_> = (0..8).map(|_| Box::pin(timer.delay_until(due))).collect();
    for delay in &mut wheel {
        assert!(poll(delay.as_mut()).is_pending());
    }
    drive(&mut driver, genesis);

    let mut immediate: Vec<_> = (0..8).map(|_| Box::pin(timer.delay_until(due))).collect();
    for delay in &mut immediate {
        assert!(poll(delay.as_mut()).is_pending());
    }

    let budget = TurnBudget::new(
        NonZeroUsize::new(16).unwrap(),
        NonZeroUsize::new(6).unwrap(),
    );
    assert!(driver.turn(due, budget).has_more_work());
    assert_eq!(
        wheel
            .iter_mut()
            .map(|delay| poll(delay.as_mut()).is_ready())
            .filter(|ready| *ready)
            .count(),
        3
    );
    assert_eq!(
        immediate
            .iter_mut()
            .map(|delay| poll(delay.as_mut()).is_ready())
            .filter(|ready| *ready)
            .count(),
        3
    );
}

#[test]
fn operation_wake_is_coalesced_and_renewable() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    drive(&mut driver, genesis);
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());
    assert!(driver.register_wake(&waker));

    let mut first = Box::pin(timer.delay(Duration::from_secs(1)));
    let mut second = Box::pin(timer.delay(Duration::from_secs(2)));
    assert!(poll(first.as_mut()).is_pending());
    assert!(poll(second.as_mut()).is_pending());
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    assert!(!driver.register_wake(&waker));

    drive(&mut driver, genesis);
    assert!(driver.register_wake(&waker));
    drop(first);
    assert_eq!(counter.0.load(Ordering::Relaxed), 2);
    drive(&mut driver, genesis);
    assert!(driver.register_wake(&waker));
}

#[test]
fn submillisecond_deadline_never_fires_early() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let deadline = genesis + Duration::from_micros(500);
    let mut delay = Box::pin(timer.delay_until(deadline));

    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut driver, genesis);
    drive(&mut driver, genesis + Duration::from_micros(999));
    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut driver, genesis + Duration::from_millis(1));
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn driven_relative_delay_keeps_submillisecond_observation() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    drive(&mut driver, genesis + Duration::from_micros(500));
    let mut delay = Box::pin(timer.delay(Duration::from_millis(1)));

    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut driver, genesis + Duration::from_millis(1));
    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut driver, genesis + Duration::from_millis(2));
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn timeout_prefers_guarded_future_on_tie() {
    let genesis = Instant::now();
    let (_driver, timer) = TimerDriver::new_at(genesis);
    assert_eq!(
        pollster::block_on(timeout_at(&timer, genesis, ready(7))),
        Ok(7)
    );
}

#[test]
fn timeout_distinguishes_elapsed_and_closed() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let mut elapsed = Box::pin(timeout_at(
        &timer,
        genesis + Duration::from_millis(1),
        pending::<()>(),
    ));
    assert!(poll(elapsed.as_mut()).is_pending());
    drive(&mut driver, genesis + Duration::from_millis(1));
    assert_eq!(
        poll(elapsed.as_mut()),
        Poll::Ready(Err(TimeoutError::Elapsed))
    );
    drop(elapsed);

    let mut closed = Box::pin(timeout(&timer, Duration::from_secs(1), pending::<()>()));
    assert!(poll(closed.as_mut()).is_pending());
    drop(driver);
    assert_eq!(
        poll(closed.as_mut()),
        Poll::Ready(Err(TimeoutError::Closed))
    );
}

#[test]
fn never_deadline_only_completes_on_close() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let mut delay = Box::pin(timer.delay(Duration::MAX));

    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut driver, genesis + Duration::from_secs(10));
    assert!(poll(delay.as_mut()).is_pending());
    assert_eq!(driver.next_poll_at(), None);
    drop(driver);
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
}

#[test]
fn interval_missed_tick_behaviors_have_distinct_schedules() {
    let genesis = Instant::now();
    for (behavior, expected) in [
        (MissedTickBehavior::Burst, 20),
        (MissedTickBehavior::Delay, 45),
        (MissedTickBehavior::Skip, 40),
    ] {
        let (mut driver, timer) = TimerDriver::new_at(genesis);
        let mut interval = interval_at(
            &timer,
            genesis + Duration::from_millis(10),
            Duration::from_millis(10),
        );
        interval.set_missed_tick_behavior(behavior);
        assert_eq!(interval.missed_tick_behavior(), behavior);
        let mut tick = Box::pin(interval.tick());
        assert!(poll(tick.as_mut()).is_pending());
        drive(&mut driver, genesis + Duration::from_millis(35));
        assert_eq!(
            poll(tick.as_mut()),
            Poll::Ready(Ok(genesis + Duration::from_millis(10)))
        );
        drop(tick);
        assert_eq!(
            interval.deadline,
            Deadline::At(genesis + Duration::from_millis(expected))
        );
    }
}

#[test]
fn interval_has_an_immediate_first_tick() {
    let genesis = Instant::now();
    let (_driver, timer) = TimerDriver::new_at(genesis);
    let mut interval = interval(&timer, Duration::from_millis(10));

    assert_eq!(interval.missed_tick_behavior(), MissedTickBehavior::Burst);
    assert_eq!(pollster::block_on(interval.tick()), Ok(genesis));
}

#[test]
#[should_panic(expected = "interval period must be non-zero")]
fn interval_rejects_a_zero_period() {
    let genesis = Instant::now();
    let (_driver, timer) = TimerDriver::new_at(genesis);
    let _ = interval(&timer, Duration::ZERO);
}

#[test]
#[should_panic(expected = "interval period must be non-zero")]
fn interval_at_rejects_a_zero_period() {
    let genesis = Instant::now();
    let (_driver, timer) = TimerDriver::new_at(genesis);
    let _ = interval_at(&timer, genesis, Duration::ZERO);
}

#[test]
fn skip_does_not_discard_a_future_grid_point_within_the_same_tick() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let mut interval = interval_at(
        &timer,
        genesis + Duration::from_millis(10),
        Duration::from_millis(10),
    );
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut tick = Box::pin(interval.tick());
    assert!(poll(tick.as_mut()).is_pending());

    drive(&mut driver, genesis + Duration::from_micros(39_900));
    assert_eq!(
        poll(tick.as_mut()),
        Poll::Ready(Ok(genesis + Duration::from_millis(10)))
    );
    drop(tick);
    assert_eq!(
        interval.deadline,
        Deadline::At(genesis + Duration::from_millis(40))
    );
}

#[test]
fn interval_tick_is_cancel_safe() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let mut interval = interval_at(
        &timer,
        genesis + Duration::from_millis(10),
        Duration::from_millis(10),
    );

    let mut first_attempt = Box::pin(interval.tick());
    assert!(poll(first_attempt.as_mut()).is_pending());
    drop(first_attempt);
    drive(&mut driver, genesis + Duration::from_millis(10));
    assert_eq!(
        pollster::block_on(interval.tick()),
        Ok(genesis + Duration::from_millis(10))
    );
}

#[test]
fn scheduling_futures_do_not_spawn_and_propagate_close() {
    let genesis = Instant::now();

    let (driver, timer) = TimerDriver::new_at(genesis);
    let fixed_delay_count = Arc::new(AtomicUsize::new(0));
    let count = fixed_delay_count.clone();
    let mut fixed_delay = Box::pin(schedule_with_fixed_delay(
        &timer,
        None,
        Duration::from_millis(1),
        async move || {
            count.fetch_add(1, Ordering::Relaxed);
        },
    ));
    assert!(poll(fixed_delay.as_mut()).is_pending());
    assert_eq!(fixed_delay_count.load(Ordering::Relaxed), 1);
    drop(driver);
    assert_eq!(poll(fixed_delay.as_mut()), Poll::Ready(Err(TimerClosed)));

    let (driver, timer) = TimerDriver::new_at(genesis);
    let fixed_rate_count = Arc::new(AtomicUsize::new(0));
    let count = fixed_rate_count.clone();
    let mut fixed_rate = Box::pin(schedule_at_fixed_rate(
        &timer,
        None,
        Duration::from_millis(1),
        async move || {
            count.fetch_add(1, Ordering::Relaxed);
        },
    ));
    assert!(poll(fixed_rate.as_mut()).is_pending());
    assert_eq!(fixed_rate_count.load(Ordering::Relaxed), 1);
    drop(driver);
    assert_eq!(poll(fixed_rate.as_mut()), Poll::Ready(Err(TimerClosed)));

    let (driver, timer) = TimerDriver::new_at(genesis);
    let arbitrary_count = Arc::new(AtomicUsize::new(0));
    let count = arbitrary_count.clone();
    let mut arbitrary = Box::pin(schedule_with_arbitrary_delay(
        &timer,
        None,
        async move || {
            count.fetch_add(1, Ordering::Relaxed);
            genesis + Duration::from_millis(1)
        },
    ));
    assert!(poll(arbitrary.as_mut()).is_pending());
    assert_eq!(arbitrary_count.load(Ordering::Relaxed), 1);
    drop(driver);
    assert_eq!(poll(arbitrary.as_mut()), Poll::Ready(Err(TimerClosed)));
}

#[test]
fn scheduling_futures_honor_their_initial_delay() {
    let genesis = Instant::now();
    let initial_delay = Some(Duration::from_millis(5));

    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let mut schedule = Box::pin(schedule_with_fixed_delay(
        &timer,
        initial_delay,
        Duration::from_millis(1),
        async move || {
            observed.fetch_add(1, Ordering::Relaxed);
        },
    ));
    assert_schedule_waits_for_initial_delay(&mut driver, genesis, schedule.as_mut(), &count);

    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let mut schedule = Box::pin(schedule_at_fixed_rate(
        &timer,
        initial_delay,
        Duration::from_millis(1),
        async move || {
            observed.fetch_add(1, Ordering::Relaxed);
        },
    ));
    assert_schedule_waits_for_initial_delay(&mut driver, genesis, schedule.as_mut(), &count);

    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let mut schedule = Box::pin(schedule_with_arbitrary_delay(
        &timer,
        initial_delay,
        async move || {
            observed.fetch_add(1, Ordering::Relaxed);
            genesis + Duration::from_millis(6)
        },
    ));
    assert_schedule_waits_for_initial_delay(&mut driver, genesis, schedule.as_mut(), &count);
}

#[test]
#[should_panic(expected = "fixed delay must be non-zero")]
fn fixed_delay_schedule_rejects_a_zero_delay() {
    let genesis = Instant::now();
    let (_driver, timer) = TimerDriver::new_at(genesis);
    let _ = pollster::block_on(schedule_with_fixed_delay(
        &timer,
        None,
        Duration::ZERO,
        async || {},
    ));
}

#[test]
#[should_panic(expected = "fixed rate must be non-zero")]
fn fixed_rate_schedule_rejects_a_zero_period() {
    let genesis = Instant::now();
    let (_driver, timer) = TimerDriver::new_at(genesis);
    let _ = pollster::block_on(schedule_at_fixed_rate(
        &timer,
        None,
        Duration::ZERO,
        async || {},
    ));
}

#[test]
fn concurrent_arm_and_first_operation_never_both_miss() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);

    for _ in 0..1_000 {
        drive(&mut driver, genesis);
        let barrier = Arc::new(Barrier::new(2));
        let sender_barrier = barrier.clone();
        let sender_timer = timer.clone();
        let sender = std::thread::spawn(move || {
            let mut delay = Box::pin(sender_timer.delay(Duration::from_secs(1)));
            sender_barrier.wait();
            assert!(poll(delay.as_mut()).is_pending());
            delay
        });

        let counter = Arc::new(WakeCount::default());
        let waker = Waker::from(counter.clone());
        barrier.wait();
        let armed = driver.register_wake(&waker);
        let delay = sender.join().unwrap();
        assert!(
            !armed || counter.0.load(Ordering::Relaxed) > 0,
            "driver armed successfully but the first producer missed its wake slot"
        );

        drive(&mut driver, genesis);
        drop(delay);
        drive(&mut driver, genesis);
        assert_eq!(driver.wheel.len(), 0);
    }
}

#[test]
fn concurrent_delay_registration_and_fire_never_lose_wake() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let mut now = genesis;

    for _ in 0..1_000 {
        let deadline = now + Duration::from_millis(1);
        let mut delay = Box::pin(timer.delay_until(deadline));
        assert!(poll(delay.as_mut()).is_pending());
        drive(&mut driver, now);

        let barrier = Arc::new(Barrier::new(2));
        let poll_barrier = barrier.clone();
        let counter = Arc::new(WakeCount::default());
        let poll_counter = counter.clone();
        let polling = std::thread::spawn(move || {
            let waker = Waker::from(poll_counter.clone());
            poll_barrier.wait();
            let result = poll_with_waker(delay.as_mut(), &waker);
            (delay, result, poll_counter)
        });

        barrier.wait();
        drive(&mut driver, deadline);
        let (mut delay, result, counter) = polling.join().unwrap();
        if result.is_pending() {
            assert!(
                counter.0.load(Ordering::Relaxed) > 0,
                "Delay returned Pending while the terminal publisher missed its waker"
            );
        }
        assert_eq!(poll(delay.as_mut()), Poll::Ready(Ok(())));
        now = deadline;
    }
}

#[test]
fn concurrent_cancel_and_fire_reclaim_exactly_once() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let mut now = genesis;

    for _ in 0..1_000 {
        let deadline = now + Duration::from_millis(1);
        let mut delay = Box::pin(timer.delay_until(deadline));
        assert!(poll(delay.as_mut()).is_pending());
        drive(&mut driver, now);

        let barrier = Arc::new(Barrier::new(2));
        let drop_barrier = barrier.clone();
        let dropping = std::thread::spawn(move || {
            drop_barrier.wait();
            drop(delay);
        });
        barrier.wait();
        drive(&mut driver, deadline);
        dropping.join().unwrap();
        drive(&mut driver, deadline);
        assert_eq!(driver.wheel.len(), 0);
        now = deadline;
    }
}

#[test]
fn fixed_rate_task_longer_than_period_stays_on_the_grid() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let starts = Arc::new(Mutex::new(Vec::new()));
    let observed = starts.clone();
    let clock = timer.clone();

    // Each invocation occupies 35ms of the timeline, so it misses three 10ms grid points. The
    // task has to consume that time itself: advancing the clock only between polls would let the
    // scheduler observe the pre-task instant and would not model an overrun at all.
    let mut schedule = Box::pin(schedule_at_fixed_rate(
        &timer,
        None,
        Duration::from_millis(10),
        async move || {
            observed.lock().unwrap().push(clock.now() - genesis);
            let _ = clock.delay(Duration::from_millis(35)).await;
        },
    ));

    assert!(poll(schedule.as_mut()).is_pending());
    for millis in [35, 40, 75, 80] {
        drive(&mut driver, genesis + Duration::from_millis(millis));
        assert!(poll(schedule.as_mut()).is_pending());
    }

    let starts = starts.lock().unwrap().clone();
    assert_eq!(
        starts,
        vec![
            Duration::ZERO,
            Duration::from_millis(40),
            Duration::from_millis(80),
        ],
        "an overrunning task must skip missed grid points, not run back to back"
    );
}

// These Loom cases are protocol litmus tests, not instrumentation of the production types. Their
// sequences mirror `DriverWake::{notify_after_send, mark_empty, arm}` and
// `DelayWakeSlot::{register_and_load, take_after_publish}` and must stay aligned with them.
#[test]
fn model_driver_clear_and_producer_publish_cannot_both_miss() {
    loom::model(|| {
        use loom::sync::Arc;
        use loom::sync::atomic::AtomicBool;
        use loom::sync::atomic::Ordering;
        use loom::sync::atomic::fence;
        use loom::thread;

        // `queued` stands in for the operation channel: the producer publishes an operation and
        // only then sets `pending`. `pending` starts set, which is the interleaving where the
        // producer takes the non-first path and never touches the wake slot.
        let queued = Arc::new(AtomicBool::new(false));
        let pending = Arc::new(AtomicBool::new(true));

        let producer_queued = queued.clone();
        let producer_pending = pending.clone();
        let producer = thread::spawn(move || {
            producer_queued.store(true, Ordering::Release);
            // Isolate the leading clear/recheck fence; the trailing fence has its own model below.
            fence(Ordering::SeqCst);
            producer_pending.swap(true, Ordering::AcqRel);
        });

        let driver_queued = queued.clone();
        let driver_pending = pending.clone();
        let driver = thread::spawn(move || {
            driver_pending.store(false, Ordering::Release);
            fence(Ordering::SeqCst);
            driver_queued.load(Ordering::Acquire)
        });

        let saw_queued = driver.join().unwrap();
        producer.join().unwrap();
        assert!(
            saw_queued || pending.load(Ordering::SeqCst),
            "the driver cleared `pending` and missed the queued operation"
        );
    });
}

#[test]
fn concurrent_drain_and_send_never_park_with_queued_operations() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);

    // Exercises the clear/recheck handshake against the real channel rather than a model of it.
    for _ in 0..1_000 {
        let barrier = Arc::new(Barrier::new(2));
        let sender_barrier = barrier.clone();
        let sender_timer = timer.clone();
        let sender = std::thread::spawn(move || {
            let mut delay = Box::pin(sender_timer.delay(Duration::from_secs(1)));
            sender_barrier.wait();
            assert!(poll(delay.as_mut()).is_pending());
            delay
        });

        barrier.wait();
        let result = driver.turn(genesis, budget());
        let delay = sender.join().unwrap();

        let counter = Arc::new(WakeCount::default());
        let waker = Waker::from(counter.clone());
        let parked = !result.has_more_work() && driver.register_wake(&waker);
        assert!(
            !parked || driver.turn(genesis, budget()).has_more_work() || driver.wheel.len() == 1,
            "driver parked while a registration was still queued"
        );

        drive(&mut driver, genesis);
        assert_eq!(driver.wheel.len(), 1);
        drop(delay);
        drive(&mut driver, genesis);
        assert_eq!(driver.wheel.len(), 0);
    }
}

#[test]
fn repeated_polls_reuse_the_registered_waker() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());
    let mut delay = Box::pin(timer.delay(Duration::from_millis(1)));

    assert!(poll_with_waker(delay.as_mut(), &waker).is_pending());
    drive(&mut driver, genesis);
    for _ in 0..16 {
        assert!(poll_with_waker(delay.as_mut(), &waker).is_pending());
    }

    // A repoll with the same waker must not disarm the slot the driver is going to take.
    drive(&mut driver, genesis + Duration::from_millis(1));
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    assert_eq!(poll_with_waker(delay.as_mut(), &waker), Poll::Ready(Ok(())));
}

#[test]
fn a_changed_waker_is_republished() {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
    let first = Arc::new(WakeCount::default());
    let second = Arc::new(WakeCount::default());
    let first_waker = Waker::from(first.clone());
    let second_waker = Waker::from(second.clone());
    let mut delay = Box::pin(timer.delay(Duration::from_millis(1)));

    assert!(poll_with_waker(delay.as_mut(), &first_waker).is_pending());
    drive(&mut driver, genesis);
    assert!(poll_with_waker(delay.as_mut(), &second_waker).is_pending());

    drive(&mut driver, genesis + Duration::from_millis(1));
    assert_eq!(first.0.load(Ordering::Relaxed), 0);
    assert_eq!(second.0.load(Ordering::Relaxed), 1);
}

#[test]
fn model_driver_arm_and_publish_cannot_both_miss() {
    loom::model(|| {
        use loom::sync::Arc;
        use loom::sync::atomic::AtomicBool;
        use loom::sync::atomic::Ordering;
        use loom::sync::atomic::fence;
        use loom::thread;

        let pending = Arc::new(AtomicBool::new(false));
        let wake_slot = Arc::new(AtomicBool::new(false));

        let driver_pending = pending.clone();
        let driver_slot = wake_slot.clone();
        let driver = thread::spawn(move || {
            driver_slot.swap(true, Ordering::AcqRel);
            fence(Ordering::SeqCst);
            let saw_pending = driver_pending.load(Ordering::Acquire);
            if saw_pending {
                driver_slot.swap(false, Ordering::Acquire);
            }
            saw_pending
        });

        let producer_pending = pending.clone();
        let producer_slot = wake_slot.clone();
        let producer = thread::spawn(move || {
            // The leading clear/recheck fence is isolated in the preceding model.
            let first = !producer_pending.swap(true, Ordering::AcqRel);
            fence(Ordering::SeqCst);
            if first {
                producer_slot.swap(false, Ordering::Acquire)
            } else {
                false
            }
        });

        let saw_pending = driver.join().unwrap();
        let took_waker = producer.join().unwrap();
        assert!(saw_pending || took_waker);
    });
}

#[test]
fn model_delay_registration_and_terminal_publish_cannot_both_miss() {
    loom::model(|| {
        use loom::sync::Arc;
        use loom::sync::atomic::AtomicBool;
        use loom::sync::atomic::AtomicUsize;
        use loom::sync::atomic::Ordering;
        use loom::sync::atomic::fence;
        use loom::thread;

        let lifecycle = Arc::new(AtomicUsize::new(STATE_REGISTERED as usize));
        let wake_slot = Arc::new(AtomicBool::new(false));

        let poll_lifecycle = lifecycle.clone();
        let poll_slot = wake_slot.clone();
        let polling = thread::spawn(move || {
            poll_slot.swap(true, Ordering::AcqRel);
            fence(Ordering::SeqCst);
            poll_lifecycle.load(Ordering::Acquire) == STATE_FIRED as usize
        });

        let publish_lifecycle = lifecycle.clone();
        let publish_slot = wake_slot.clone();
        let publishing = thread::spawn(move || {
            publish_lifecycle
                .compare_exchange(
                    STATE_REGISTERED as usize,
                    STATE_FIRED as usize,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .unwrap();
            fence(Ordering::SeqCst);
            publish_slot.swap(false, Ordering::Acquire)
        });

        let saw_terminal = polling.join().unwrap();
        let took_waker = publishing.join().unwrap();
        assert!(saw_terminal || took_waker);
    });
}
