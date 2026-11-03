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

use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Waker;

use asyncband::waitgroup::WaitGroup;
use tests_integration::WakeCounter;
use tests_integration::poll_once;

#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waits_for_all_worker_handles() {
    let wg = WaitGroup::new();
    let mut tasks = vec![];
    for _ in 0..100 {
        let worker = wg.clone();
        tasks.push(tokio::spawn(async move {
            drop(worker);
        }));
    }

    wg.await;
    for task in tasks {
        task.await.unwrap();
    }
}

#[test]
fn wait_is_pending_until_the_last_handle_drops() {
    let wg = WaitGroup::new();
    let worker = wg.clone();
    let mut wait = pin!(wg.into_future());

    assert!(poll_once(wait.as_mut()).is_pending());
    drop(worker);
    assert!(poll_once(wait.as_mut()).is_ready());
}

#[test]
fn cancelling_one_wait_does_not_cancel_another() {
    let wg = WaitGroup::new();
    let worker = wg.clone();
    let first = wg.into_future();
    let mut second = pin!(first.clone());
    let mut first = Box::pin(first);

    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());
    drop(first);

    drop(worker);
    assert!(poll_once(second.as_mut()).is_ready());
}

#[test]
fn multiple_handles_can_wait_for_the_same_completion() {
    let wg = WaitGroup::new();
    let worker = wg.clone();
    let mut first = pin!(wg.clone().into_future());
    let mut second = pin!(wg.into_future());

    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());

    drop(worker);
    assert!(poll_once(first.as_mut()).is_ready());
    assert!(poll_once(second.as_mut()).is_ready());
}

#[test]
fn handle_clones_register_nested_work() {
    let wg = WaitGroup::new();
    let worker = wg.clone();
    let nested = worker.clone();
    let mut wait = pin!(wg.into_future());

    drop(worker);
    assert!(poll_once(wait.as_mut()).is_pending());
    drop(nested);
    assert!(poll_once(wait.as_mut()).is_ready());
}

#[test]
fn a_consumed_initial_handle_is_immediately_ready() {
    let mut wait = pin!(WaitGroup::new().into_future());

    assert!(poll_once(wait.as_mut()).is_ready());
}

#[test]
fn a_completed_wait_can_be_cancelled_before_it_is_polled_again() {
    let wg = WaitGroup::new();
    let worker = wg.clone();
    let mut wait = Box::pin(wg.into_future());

    assert!(poll_once(wait.as_mut()).is_pending());
    drop(worker);
    drop(wait);
}

#[test]
fn repolling_replaces_the_registered_waker() {
    let wg = WaitGroup::new();
    let worker = wg.clone();
    let mut wait = pin!(wg.into_future());
    let first = Arc::new(WakeCounter::default());
    let second = Arc::new(WakeCounter::default());
    let first_waker = Waker::from(first.clone());
    let second_waker = Waker::from(second.clone());

    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending()
    );
    drop(worker);

    assert_eq!(first.count(), 0);
    assert_eq!(second.count(), 1);
}

#[test]
fn cancelled_slots_can_be_reused_while_other_observers_are_registered() {
    let wg = WaitGroup::new();
    let worker = wg.clone();
    let observer = wg.into_future();
    let mut first = Box::pin(observer.clone());
    let mut extra = Box::pin(observer.clone());
    let mut survivor = Box::pin(observer.clone());
    let (old, old_count) = WakeCounter::new();
    let (live, live_count) = WakeCounter::new();
    let (replaced, replaced_count) = WakeCounter::new();

    assert!(tests_integration::poll_with(first.as_mut(), &old).is_pending());
    assert!(tests_integration::poll_with(extra.as_mut(), &old).is_pending());
    assert!(tests_integration::poll_with(survivor.as_mut(), &replaced).is_pending());
    drop(first);
    drop(extra);

    let mut first = Box::pin(observer.clone());
    let mut extra = Box::pin(observer);
    for waiter in [&mut first, &mut extra, &mut survivor] {
        assert!(tests_integration::poll_with(waiter.as_mut(), &live).is_pending());
    }
    drop(worker);
    assert_eq!(old_count.count(), 0);
    assert_eq!(replaced_count.count(), 0);
    assert_eq!(live_count.count(), 3);
    for waiter in [&mut first, &mut extra, &mut survivor] {
        assert!(poll_once(waiter.as_mut()).is_ready());
    }
}

#[test]
fn concurrent_registration_acquires_every_participants_work() {
    use std::sync::Barrier;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::thread;

    let group = WaitGroup::new();
    let start = Barrier::new(6);
    let writes = [const { AtomicBool::new(false) }; 3];
    let (waker, _) = WakeCounter::new();
    thread::scope(|scope| {
        for write in &writes {
            let worker = group.clone();
            let start = &start;
            scope.spawn(move || {
                start.wait();
                write.store(true, Ordering::Relaxed);
                drop(worker);
            });
        }
        let observer = group.into_future();
        for _ in 0..2 {
            let mut wait = Box::pin(observer.clone());
            let start = &start;
            let writes = &writes;
            let waker = &waker;
            scope.spawn(move || {
                start.wait();
                while tests_integration::poll_with(wait.as_mut(), waker).is_pending() {
                    thread::yield_now();
                }
                assert!(writes.iter().all(|v| v.load(Ordering::Relaxed)));
            });
        }
        let mut wait = Box::pin(observer);
        assert!(tests_integration::poll_with(wait.as_mut(), &waker).is_pending());
        start.wait();
        drop(wait);
    });
}

#[test]
fn racing_completion_notifies_a_poll_that_returns_pending() {
    use std::sync::Barrier;
    use std::thread;

    let group = WaitGroup::new();
    let worker = group.clone();
    let mut wait = Box::pin(group.into_future());
    let start = Barrier::new(2);
    let (waker, counter) = WakeCounter::new();
    let result = thread::scope(|scope| {
        scope.spawn(|| {
            start.wait();
            drop(worker);
        });
        start.wait();
        tests_integration::poll_with(wait.as_mut(), &waker)
    });
    if result.is_pending() {
        assert_eq!(counter.count(), 1);
    }
    assert!(poll_once(wait.as_mut()).is_ready());
}

#[test]
fn wait_group_and_observers_preserve_unwind_safety() {
    fn assert_traits<T: std::panic::UnwindSafe + std::panic::RefUnwindSafe>() {}
    assert_traits::<WaitGroup>();
    assert_traits::<asyncband::waitgroup::Wait>();
}

#[test]
fn retiring_a_registration_can_complete_the_group() {
    for replace in [false, true] {
        for use_overflow in [false, true] {
            tests_integration::assert_completes_without_deadlock(move || {
                let group = WaitGroup::new();
                let worker = group.clone();
                let observer = group.into_future();
                let mut other = pin!(observer.clone());
                if use_overflow {
                    assert!(poll_once(other.as_mut()).is_pending());
                }
                let mut wait = Box::pin(observer);
                let retired = tests_integration::waker_on_drop(move || drop(worker));
                assert!(tests_integration::poll_with(wait.as_mut(), &retired).is_pending());
                drop(retired);
                if replace {
                    let (next, counter) = WakeCounter::new();
                    assert!(tests_integration::poll_with(wait.as_mut(), &next).is_pending());
                    assert_eq!(counter.count(), 1);
                }
                drop(wait);
                assert!(poll_once(other.as_mut()).is_ready());
            });
        }
    }
}

#[test]
fn final_worker_and_cancelled_observer_release_their_registration() {
    use std::sync::Barrier;
    use std::thread;

    let group = WaitGroup::new();
    let worker = group.clone();
    let mut wait = Box::pin(group.into_future());
    let (waker, counter) = WakeCounter::new();
    assert!(tests_integration::poll_with(wait.as_mut(), &waker).is_pending());
    drop(waker);

    let start = Barrier::new(2);
    thread::scope(|scope| {
        scope.spawn(|| {
            start.wait();
            drop(worker);
        });
        start.wait();
        drop(wait);
    });
    assert!(counter.count() <= 1);
    assert_eq!(Arc::strong_count(&counter), 1);
}
