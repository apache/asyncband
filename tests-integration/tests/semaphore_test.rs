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
use std::sync::Barrier;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;

use asyncband::blocking::FutureExt;
use asyncband::semaphore::Semaphore;
use tests_integration::PanicWake;
use tests_integration::WakeCounter;
use tests_integration::expect_ready;
use tests_integration::poll_with;

#[test]
fn no_permits() {
    // this should not panic
    Semaphore::new(0);
}

#[test]
fn try_acquire() {
    let sem = Semaphore::new(1);
    {
        let p1 = sem.try_acquire(1);
        assert!(p1.is_some());
        let p2 = sem.try_acquire(1);
        assert!(p2.is_none());
    }
    let p3 = sem.try_acquire(1);
    assert!(p3.is_some());
}

#[test]
fn released_permit_wakes_a_pending_acquire() {
    let sem = Semaphore::new(1);
    let held = sem.try_acquire(1).unwrap();
    let mut acquire = pin!(sem.acquire(1));
    let (waker, wakes) = WakeCounter::new();
    assert!(poll_with(acquire.as_mut(), &waker).is_pending());

    drop(held);
    assert_eq!(wakes.count(), 1);
    let permit = expect_ready(poll_with(acquire.as_mut(), &waker));
    assert_eq!(sem.available_permits(), 0);
    drop(permit);
    assert_eq!(sem.available_permits(), 1);
}

#[test]
fn forget() {
    let sem = Arc::new(Semaphore::new(1));
    {
        let p = sem.try_acquire(1).unwrap();
        assert_eq!(sem.available_permits(), 0);
        p.forget();
        assert_eq!(sem.available_permits(), 0);
    }
    assert_eq!(sem.available_permits(), 0);
    assert!(sem.try_acquire(1).is_none());
}

#[test]
fn add_max_amount_permits() {
    let s = Semaphore::new(0);
    s.release(usize::MAX);
    assert_eq!(s.available_permits(), usize::MAX);
}

#[test]
fn release_overflow_preserves_permits() {
    let s = Semaphore::new(usize::MAX);
    let result = std::panic::catch_unwind(|| s.release(1));

    assert!(result.is_err());
    assert_eq!(s.available_permits(), usize::MAX);

    let permit = s.try_acquire(1).unwrap();
    assert_eq!(s.available_permits(), usize::MAX - 1);
    drop(permit);
    assert_eq!(s.available_permits(), usize::MAX);
}

#[test]
fn merge_overflow_panics_without_losing_borrowed_permits() {
    let s = Semaphore::new(usize::MAX);
    let mut first = s.try_acquire(usize::MAX).unwrap();
    s.release(1);
    let second = s.try_acquire(1).unwrap();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        first.merge(second);
    }));

    assert!(result.is_err());
    assert_eq!(first.permits(), usize::MAX);
    assert_eq!(s.available_permits(), 1);
    first.forget();
}

#[test]
fn merge_overflow_panics_without_losing_owned_permits() {
    let s = Arc::new(Semaphore::new(usize::MAX));
    let mut first = s.clone().try_acquire_owned(usize::MAX).unwrap();
    s.release(1);
    let second = s.clone().try_acquire_owned(1).unwrap();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        first.merge(second);
    }));

    assert!(result.is_err());
    assert_eq!(first.permits(), usize::MAX);
    assert_eq!(s.available_permits(), 1);
    first.forget();
}

#[test]
fn no_panic_at_max_permits() {
    let _ = Semaphore::new(usize::MAX);
    let s = Semaphore::new(usize::MAX - 1);
    s.release(1);
}

#[test]
fn acquire_then_drop() {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    let s = Semaphore::new(1);
    let p1 = s.try_acquire(1).unwrap();
    {
        let p2 = s.acquire(1);
        let poll = pin!(p2).poll(&mut context);
        assert!(poll.is_pending());
    }
    drop(p1);
    assert_eq!(s.available_permits(), 1);
}

#[test]
fn wake_then_drop() {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    let s = Semaphore::new(2);
    let p1 = s.try_acquire(2).unwrap();
    {
        let p2 = s.acquire(1);
        let p2 = pin!(p2);
        assert!(p2.poll(&mut context).is_pending());
        {
            let p3 = s.acquire(1);
            let p3 = pin!(p3);
            assert!(p3.poll(&mut context).is_pending());
            drop(p1);
        }
    }
    assert_eq!(s.available_permits(), 2);
}

#[test]
fn release_attempts_every_waker_after_one_panics() {
    let semaphore = Semaphore::new(0);
    let mut panicking = pin!(semaphore.acquire(1));
    let mut tracked = pin!(semaphore.acquire(1));
    let panic_waker = Waker::from(Arc::new(PanicWake));
    let wake_count = Arc::new(WakeCounter::default());
    let tracked_waker = Waker::from(wake_count.clone());

    assert!(
        panicking
            .as_mut()
            .poll(&mut Context::from_waker(&panic_waker))
            .is_pending()
    );
    assert!(
        tracked
            .as_mut()
            .poll(&mut Context::from_waker(&tracked_waker))
            .is_pending()
    );

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| semaphore.release(2)));

    assert!(result.is_err());
    assert_eq!(wake_count.count(), 1);
    assert!(
        panicking
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    assert!(
        tracked
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
}

#[test]
fn cancellation_restores_permits_before_dropping_waker() {
    struct AssertPermitsOnDrop {
        semaphore: Arc<Semaphore>,
    }

    impl Wake for AssertPermitsOnDrop {
        fn wake(self: Arc<Self>) {
            panic!("cancellation must not wake the removed waiter");
        }
    }

    impl Drop for AssertPermitsOnDrop {
        fn drop(&mut self) {
            assert_eq!(self.semaphore.available_permits(), 1);
        }
    }

    let semaphore = Arc::new(Semaphore::new(1));
    let waker = Waker::from(Arc::new(AssertPermitsOnDrop {
        semaphore: semaphore.clone(),
    }));
    let mut acquire = Box::pin(semaphore.acquire(2));

    assert!(
        acquire
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    drop(acquire);
    assert_eq!(Arc::strong_count(&semaphore), 1);
}

#[test]
fn reduce_permits_takes_priority_over_pending_acquires() {
    let s = Semaphore::new(0);
    let mut acquire = pin!(s.acquire(1));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    assert!(acquire.as_mut().poll(&mut context).is_pending());

    s.reduce_permits(1);
    s.release(1);
    assert!(acquire.as_mut().poll(&mut context).is_pending());

    s.release(1);
    let Poll::Ready(permit) = acquire.as_mut().poll(&mut context) else {
        panic!("acquire should complete after the reduction debt is repaid");
    };
    assert_eq!(s.available_permits(), 0);

    drop(permit);
    assert_eq!(s.available_permits(), 1);
}

/// Releases race acquisitions that drain the balance to zero and link waiters. The permit count
/// must be conserved, and no more permits than the capacity may ever be held at once.
#[test]
fn concurrent_releases_conserve_permits() {
    const PERMITS: usize = 3;
    const THREADS: usize = 8;
    const ITERATIONS: usize = 4_000;

    fn next(state: &mut u64) -> usize {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (*state >> 33) as usize
    }

    let semaphore = Arc::new(Semaphore::new(PERMITS));
    let held = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(Barrier::new(THREADS));
    let workers = (0..THREADS)
        .map(|seed| {
            let semaphore = semaphore.clone();
            let held = held.clone();
            let start = start.clone();
            thread::spawn(move || {
                let mut state = seed as u64 + 1;
                start.wait();
                let hold = |n: usize, state: &mut u64| {
                    let now = held.fetch_add(n, Ordering::AcqRel) + n;
                    assert!(now <= PERMITS, "{now} permits held at once");
                    for _ in 0..next(state) % 8 {
                        std::hint::spin_loop();
                    }
                    held.fetch_sub(n, Ordering::AcqRel);
                };
                for _ in 0..ITERATIONS {
                    let n = next(&mut state) % PERMITS + 1;
                    match next(&mut state) % 5 {
                        0 => {
                            if let Some(permit) = semaphore.try_acquire(n) {
                                hold(n, &mut state);
                                drop(permit);
                            }
                        }
                        1 => {
                            let permit = FutureExt::block_on(semaphore.acquire(n));
                            hold(n, &mut state);
                            permit.forget();
                            semaphore.release(n);
                        }
                        2 => {
                            semaphore.reduce_permits(n);
                            semaphore.release(n);
                        }
                        3 => {
                            let mut acquire = pin!(semaphore.acquire(n));
                            let poll = poll_with(acquire.as_mut(), Waker::noop());
                            if let Poll::Ready(permit) = poll {
                                hold(n, &mut state);
                                drop(permit);
                            }
                        }
                        _ => {
                            let permit = FutureExt::block_on(semaphore.acquire(n));
                            hold(n, &mut state);
                            drop(permit);
                        }
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(semaphore.available_permits(), PERMITS);
    assert!(semaphore.try_acquire(PERMITS).is_some());
}

/// Two releases race on a balance one below `usize::MAX`: exactly one may succeed, and the other
/// must panic before adding anything, whichever path each of them takes.
#[test]
fn concurrent_releases_at_the_limit_panic_exactly_once() {
    let semaphore = Arc::new(Semaphore::new(usize::MAX - 1));
    let start = Arc::new(Barrier::new(2));
    let workers = (0..2)
        .map(|_| {
            let semaphore = semaphore.clone();
            let start = start.clone();
            thread::spawn(move || {
                start.wait();
                std::panic::catch_unwind(|| semaphore.release(1)).is_ok()
            })
        })
        .collect::<Vec<_>>();
    let succeeded = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .filter(|succeeded| *succeeded)
        .count();
    assert_eq!(succeeded, 1);
    assert_eq!(semaphore.available_permits(), usize::MAX);
}
