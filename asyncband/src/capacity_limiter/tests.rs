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

use super::*;
use crate::capacity_limiter::TryAcquireError;

// FIFO handoff, cancellation, and deficit accounting share the same state.

fn poll_acquire<'a, B: Eq + Hash + Clone>(
    acquire: Pin<&mut Acquire<'a, B>>,
) -> Poll<Result<Permit<'a, B>, AlreadyBorrowed>> {
    acquire.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn capacity_bounds_concurrent_borrowers() {
    let limiter = CapacityLimiter::new(2);
    let a = limiter.try_acquire("a").unwrap();
    let b = limiter.try_acquire("b").unwrap();

    assert_eq!(limiter.borrowed(), 2);
    assert_eq!(
        limiter.try_acquire("c").unwrap_err(),
        TryAcquireError::NoCapacity
    );

    drop(a);
    drop(b);
    assert_eq!(limiter.available(), 2);
    assert_eq!(limiter.borrowed(), 0);
}

#[test]
fn one_token_per_borrower() {
    let limiter = CapacityLimiter::new(4);
    let held = limiter.try_acquire("a").unwrap();

    assert!(limiter.available() > 0);
    assert_eq!(
        limiter.try_acquire("a").unwrap_err(),
        TryAcquireError::AlreadyBorrowed
    );

    drop(held);
    assert!(limiter.try_acquire("a").is_ok());
}

#[test]
fn queued_borrowers_are_granted_in_order() {
    let limiter = CapacityLimiter::new(0);

    let mut first = pin!(limiter.acquire("a"));
    assert!(poll_acquire(first.as_mut()).is_pending());
    let mut second = pin!(limiter.acquire("b"));
    assert!(poll_acquire(second.as_mut()).is_pending());
    assert_eq!(limiter.waiting(), 2);

    limiter.set_total(1);

    // The token goes to the borrower that queued first.
    assert!(poll_acquire(second.as_mut()).is_pending());
    let permit = match poll_acquire(first.as_mut()) {
        Poll::Ready(result) => result.unwrap(),
        Poll::Pending => panic!("the first waiter must be granted the token"),
    };
    assert_eq!(permit.borrower(), &"a");

    drop(permit);
    assert!(matches!(poll_acquire(second.as_mut()), Poll::Ready(Ok(_))));
}

#[test]
fn duplicate_acquire_is_rejected_on_first_poll() {
    let limiter = CapacityLimiter::new(1);
    let _held = limiter.try_acquire("a").unwrap();

    let mut duplicate = pin!(limiter.acquire("a"));
    assert!(matches!(
        poll_acquire(duplicate.as_mut()),
        Poll::Ready(Err(AlreadyBorrowed))
    ));

    // The rejected acquire must not have disturbed the live registration.
    assert_eq!(limiter.borrowed(), 1);
    assert_eq!(limiter.waiting(), 0);
}

#[test]
fn cancelled_acquire_releases_the_identity() {
    let limiter = CapacityLimiter::new(0);

    {
        let mut queued = pin!(limiter.acquire("a"));
        assert!(poll_acquire(queued.as_mut()).is_pending());
        assert_eq!(limiter.waiting(), 1);
    }

    assert_eq!(limiter.waiting(), 0);
    limiter.set_total(1);
    assert!(limiter.try_acquire("a").is_ok());
}

#[test]
fn cancelling_after_a_grant_passes_the_token_on() {
    let limiter = CapacityLimiter::new(1);
    let held = limiter.try_acquire("a").unwrap();
    let mut next = pin!(limiter.acquire("c"));

    {
        // Queued ahead of "c", so the released token is offered here first.
        let mut queued = pin!(limiter.acquire("b"));
        assert!(poll_acquire(queued.as_mut()).is_pending());
        assert!(poll_acquire(next.as_mut()).is_pending());

        // "b" is handed the token, then cancelled at the end of this scope before taking it.
        drop(held);
    }

    // The token must reach "c" rather than vanish with the cancelled future. The permit has to
    // be bound: dropping it as a temporary would return the token before it can be observed.
    let permit = match poll_acquire(next.as_mut()) {
        Poll::Ready(result) => result.unwrap(),
        Poll::Pending => panic!("the cancelled grant must be passed to the next waiter"),
    };
    assert_eq!(permit.borrower(), &"c");
    assert_eq!(limiter.borrowed(), 1);
    assert_eq!(limiter.waiting(), 0);
}

#[test]
fn cancelling_a_lone_grant_returns_the_token() {
    let limiter = CapacityLimiter::new(1);
    let held = limiter.try_acquire("a").unwrap();

    {
        let mut queued = pin!(limiter.acquire("b"));
        assert!(poll_acquire(queued.as_mut()).is_pending());
        drop(held);
    }

    assert_eq!(limiter.available(), 1);
    assert_eq!(limiter.borrowed(), 0);
    assert_eq!(limiter.waiting(), 0);
}

#[test]
fn shrinking_never_revokes_borrowed_tokens() {
    let limiter = CapacityLimiter::new(3);
    let a = limiter.try_acquire("a").unwrap();
    let b = limiter.try_acquire("b").unwrap();
    let c = limiter.try_acquire("c").unwrap();

    limiter.set_total(1);
    assert_eq!(limiter.total(), 1);
    assert_eq!(limiter.borrowed(), 3);

    drop(a);
    assert_eq!(limiter.available(), 0);
    drop(b);
    assert_eq!(limiter.available(), 0);

    drop(c);
    assert_eq!(limiter.available(), 1);
}

#[test]
fn a_deficit_is_repaid_before_queued_borrowers() {
    let limiter = CapacityLimiter::new(1);
    let held = limiter.try_acquire("a").unwrap();

    let mut queued = pin!(limiter.acquire("b"));
    assert!(poll_acquire(queued.as_mut()).is_pending());

    limiter.set_total(0);
    drop(held);

    // The released token repays the shrink instead of admitting the waiter.
    assert!(poll_acquire(queued.as_mut()).is_pending());
    assert_eq!(limiter.available(), 0);

    limiter.set_total(1);
    assert!(matches!(poll_acquire(queued.as_mut()), Poll::Ready(Ok(_))));
}
#[test]
fn cancelled_acquire_leaves_no_registration() {
    let limiter = CapacityLimiter::new(0);

    {
        let mut acquire = pin!(limiter.acquire("a"));
        let mut context = Context::from_waker(Waker::noop());
        assert!(acquire.as_mut().poll(&mut context).is_pending());
        assert_eq!(limiter.state.lock().borrowers.len(), 1);
    }

    assert!(limiter.state.lock().borrowers.is_empty());
    assert_eq!(limiter.state.lock().borrowed, 0);
}

#[test]
fn rejected_try_acquire_leaves_no_registration() {
    let limiter = CapacityLimiter::new(1);
    let held = limiter.try_acquire("a").unwrap();

    assert!(limiter.try_acquire("b").is_err());
    assert_eq!(limiter.state.lock().borrowers.len(), 1);

    drop(held);
    assert!(limiter.state.lock().borrowers.is_empty());
}

#[test]
fn granted_permit_registers_exactly_once() {
    let limiter = CapacityLimiter::new(2);
    let first = limiter.try_acquire("a").unwrap();
    let second = limiter.try_acquire("b").unwrap();

    assert_eq!(limiter.state.lock().borrowers.len(), 2);
    assert_eq!(limiter.state.lock().borrowed, 2);

    drop(first);
    drop(second);

    assert!(limiter.state.lock().borrowers.is_empty());
    assert_eq!(limiter.state.lock().borrowed, 0);
}
