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
use std::mem::ManuallyDrop;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Wake;
use std::task::Waker;
use std::thread;

use super::CapacityLimiter;

struct Callback(Box<dyn Fn() + Send + Sync>);

impl Wake for Callback {
    fn wake(self: Arc<Self>) {
        (self.0)();
    }
}

#[test]
fn committed_grants_remain_pending_until_delivery() {
    let limiter = CapacityLimiter::new(0);
    let mut acquire = Box::pin(limiter.acquire(1));
    assert!(
        acquire
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    limiter.set_total(1);
    assert_eq!(limiter.borrowed(), 0);
    assert_eq!(limiter.waiting(), 1);
    assert_eq!(limiter.available(), 0);
    limiter.set_total(0);
    let Poll::Ready(Ok(permit)) = acquire
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("shrink must not revoke the committed grant");
    };
    assert_eq!(limiter.borrowed(), 1);
    assert_eq!(limiter.waiting(), 0);
    drop(permit);
    assert_eq!(limiter.available(), 0);
}

#[test]
fn large_resizes_and_deficit_do_not_loop_over_capacity() {
    let limiter = CapacityLimiter::new(0);
    limiter.set_total(usize::MAX);
    let permit = limiter.try_acquire(1).unwrap();
    limiter.set_total(0);
    limiter.set_total(usize::MAX);
    assert_eq!(limiter.available(), usize::MAX - 1);
    drop(permit);
    assert_eq!(limiter.available(), usize::MAX);
}

#[test]
fn wake_panic_still_attempts_all_notifications() {
    let limiter = CapacityLimiter::new(0);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut pending = Vec::new();
    for id in 0..64 {
        let calls = calls.clone();
        let waker = Waker::from(Arc::new(Callback(Box::new(move || {
            calls.fetch_add(1, Ordering::Relaxed);
            assert!(id != 0, "first wake panics");
        }))));
        let mut future = Box::pin(limiter.acquire(id));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        pending.push(future);
    }
    assert!(catch_unwind(AssertUnwindSafe(|| limiter.set_total(64))).is_err());
    assert_eq!(calls.load(Ordering::Relaxed), 64);
    assert_eq!(limiter.waiting(), 64);
    for mut future in pending {
        assert!(matches!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(_))
        ));
    }
    assert_eq!(limiter.available(), 64);
    assert_eq!(limiter.waiting(), 0);
}

// Arc's Wake adapter has no clone callback. This fixture exercises the full RawWaker contract.
// Each raw pointer owns one Arc, and all callbacks are Send + Sync.
struct CloneCallback(Box<dyn Fn() + Send + Sync>);

unsafe fn clone_raw(data: *const ()) -> RawWaker {
    // SAFETY: The pointer came from Arc::into_raw for CloneCallback. Cloning borrows that Arc.
    let callback = ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<CloneCallback>()) });
    (callback.0)();
    let cloned = Arc::clone(&callback);
    RawWaker::new(Arc::into_raw(cloned).cast(), &VTABLE)
}

unsafe fn drop_raw(data: *const ()) {
    // SAFETY: Consuming wake or drop releases exactly this pointer's owned Arc.
    drop(unsafe { Arc::from_raw(data.cast::<CloneCallback>()) });
}

unsafe fn wake_by_ref_raw(_: *const ()) {}

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_raw, drop_raw, wake_by_ref_raw, drop_raw);

fn clone_callback(callback: impl Fn() + Send + Sync + 'static) -> Waker {
    let data = Arc::into_raw(Arc::new(CloneCallback(Box::new(callback)))).cast();
    // SAFETY: The vtable preserves Arc ownership and its callbacks are thread safe.
    unsafe { Waker::from_raw(RawWaker::new(data, &VTABLE)) }
}

#[test]
fn clone_can_grant_before_registration_finishes() {
    let limiter = Arc::new(CapacityLimiter::new(0));
    let observer = limiter.clone();
    let waker = clone_callback(move || {
        assert_eq!(observer.waiting(), 1);
        observer.set_total(1);
    });
    let mut future = Box::pin(limiter.acquire(1));
    assert!(matches!(
        future.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Ready(Ok(_))
    ));
    assert_eq!(limiter.available(), 1);
    assert_eq!(limiter.waiting(), 0);
}

#[test]
fn clone_panic_can_be_cancelled_without_leaking_identity() {
    let limiter = CapacityLimiter::new(0);
    let waker = clone_callback(|| panic!("clone panic"));
    let mut future = Box::pin(limiter.acquire(1));
    assert!(
        catch_unwind(AssertUnwindSafe(|| future
            .as_mut()
            .poll(&mut Context::from_waker(&waker))))
        .is_err()
    );
    drop(future);
    assert_eq!(limiter.waiting(), 0);
    limiter.set_total(1);
    assert!(limiter.try_acquire(1).is_ok());
}

struct DropCallback(Box<dyn Fn() + Send + Sync>);

// This fixture needs an owned destructor; Waker::noop() cannot exercise drop callbacks.
#[allow(clippy::manual_noop_waker)]
impl Wake for DropCallback {
    fn wake(self: Arc<Self>) {}
}

impl Drop for DropCallback {
    fn drop(&mut self) {
        (self.0)();
    }
}

#[test]
fn replacing_a_waker_allows_its_destructor_to_grant() {
    let limiter = Arc::new(CapacityLimiter::new(0));
    let observer = limiter.clone();
    let old = Waker::from(Arc::new(DropCallback(Box::new(move || {
        observer.set_total(1)
    }))));
    let mut future = Box::pin(limiter.acquire(1));
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(&old))
            .is_pending()
    );
    drop(old);
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(matches!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(_))
    ));
    assert_eq!(limiter.available(), 1);
}

#[test]
fn cancellation_removes_identity_before_dropping_waker() {
    let limiter = Arc::new(CapacityLimiter::new(0));
    let observer = limiter.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let old = Waker::from(Arc::new(DropCallback(Box::new(move || {
        assert_eq!(observer.waiting(), 0);
        observer.set_total(1);
        assert!(observer.try_acquire(1).is_ok());
        counter.fetch_add(1, Ordering::Relaxed);
    }))));
    let mut future = Box::pin(limiter.acquire(1));
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(&old))
            .is_pending()
    );
    drop(old);
    drop(future);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[test]
fn concurrent_extreme_resizes_preserve_capacity() {
    let limiter = CapacityLimiter::<usize>::new(usize::MAX);
    thread::scope(|scope| {
        for offset in 0..2 {
            let limiter = &limiter;
            scope.spawn(move || {
                for round in 0..5000 {
                    let target = if (round + offset) % 2 == 0 {
                        0
                    } else {
                        usize::MAX
                    };
                    limiter.set_total(target);
                }
            });
        }
    });
    assert_eq!(limiter.available(), limiter.total());
    limiter.set_total(0);
    assert_eq!(limiter.available(), 0);
}

#[test]
fn wake_callback_can_resize_without_revoking_its_grant() {
    let limiter = Arc::new(CapacityLimiter::new(0));
    let observer = limiter.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let waker = Waker::from(Arc::new(Callback(Box::new(move || {
        observer.set_total(0);
        counter.fetch_add(1, Ordering::Relaxed);
    }))));
    let mut acquire = Box::pin(limiter.acquire(1));
    assert!(
        acquire
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    limiter.set_total(1);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(limiter.total(), 0);
    assert!(matches!(
        acquire
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(_))
    ));
    assert_eq!(limiter.available(), 0);
    assert_eq!(limiter.waiting(), 0);
}

#[test]
fn cancelling_a_grant_removes_identity_before_waking_the_next_borrower() {
    let limiter = Arc::new(CapacityLimiter::new(0));
    let mut first = Box::pin(limiter.acquire("x"));
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );

    let observer = limiter.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let waker = Waker::from(Arc::new(Callback(Box::new(move || {
        assert_eq!(observer.waiting(), 1);
        assert_eq!(observer.borrowed(), 0);
        // x is no longer registered; y owns the committed capacity. This must not be a duplicate.
        assert_eq!(
            observer.try_acquire("x").unwrap_err(),
            super::TryAcquireError::NoCapacity
        );
        counter.fetch_add(1, Ordering::Relaxed);
    }))));
    let mut second = Box::pin(limiter.acquire("y"));
    assert!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    limiter.set_total(1); // Only x receives a grant; y must be woken by cancellation below.
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    drop(first);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    let Poll::Ready(Ok(permit)) = second
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("cancelling x must pass its grant to y");
    };
    assert_eq!(permit.borrower(), &"y");
    drop(permit);
    assert!(limiter.try_acquire("x").is_ok());
}

#[test]
fn debug_output_does_not_traverse_other_borrowers() {
    let limiter = CapacityLimiter::new(2);
    let first = limiter.try_acquire("private-first-key").unwrap();
    let second = limiter.try_acquire("private-second-key").unwrap();
    let mut pending = Box::pin(limiter.acquire("private-waiting-key"));
    assert!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );

    let limiter_debug = format!("{limiter:?}");
    assert!(!limiter_debug.contains("private-"));
    let permit_debug = format!("{first:?}");
    assert!(permit_debug.contains("private-first-key"));
    assert!(!permit_debug.contains("private-second-key"));
    assert!(!permit_debug.contains("private-waiting-key"));
    let acquire_debug = format!("{pending:?}");
    assert!(acquire_debug.contains("private-waiting-key"));
    assert!(!acquire_debug.contains("private-first-key"));
    assert!(!acquire_debug.contains("private-second-key"));
    drop(second);
}
