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

use std::future;
use std::future::Future;
use std::marker::PhantomPinned;
use std::mem;
use std::panic;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use super::Registrar;
use super::TaskGroup;
use crate::test_support::poll_once;

struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct PinnedReady {
    output: usize,
    _pin: PhantomPinned,
}

struct DropFlagFuture(Arc<AtomicBool>);

struct ReadyDropFlagFuture(Arc<AtomicBool>);

impl Future for DropFlagFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for DropFlagFuture {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl Future for ReadyDropFlagFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(())
    }
}

impl Drop for ReadyDropFlagFuture {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

struct AssertDroppedWake {
    dropped: Arc<AtomicBool>,
    wakes: AtomicUsize,
}

impl Wake for AssertDroppedWake {
    fn wake(self: Arc<Self>) {
        assert!(self.dropped.load(Ordering::Relaxed));
        self.wakes.fetch_add(1, Ordering::Relaxed);
    }
}

struct PanicOnDropFuture;

struct PanicOnDropOutput;

impl Future for PanicOnDropFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PanicOnDropFuture {
    fn drop(&mut self) {
        panic!("future drop failed");
    }
}

impl Drop for PanicOnDropOutput {
    fn drop(&mut self) {
        panic!("output drop failed");
    }
}

impl Future for PinnedReady {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(self.output)
    }
}

#[test]
fn task_group_and_registrar_default_to_unit_outputs() {
    let (group, registrar): (TaskGroup, Registrar) = TaskGroup::new();

    assert!(!group.is_closed());
    assert!(!registrar.is_closed());
}

#[test]
fn outputs_follow_completion_order() {
    let (mut group, registrar) = TaskGroup::new();
    let mut first = Box::pin(registrar.track(future::ready("first")).unwrap());
    let mut second = Box::pin(registrar.track(future::ready("second")).unwrap());

    assert_eq!(poll_once(second.as_mut()), Poll::Ready(()));
    assert_eq!(poll_once(first.as_mut()), Poll::Ready(()));
    group.close();

    let mut next = Box::pin(group.join_next());
    assert_eq!(poll_once(next.as_mut()), Poll::Ready(Some("second")));
    drop(next);
    let mut next = Box::pin(group.join_next());
    assert_eq!(poll_once(next.as_mut()), Poll::Ready(Some("first")));
    drop(next);
    let mut next = Box::pin(group.join_next());
    assert_eq!(poll_once(next.as_mut()), Poll::Pending);
    drop(second);
    drop(first);
    assert_eq!(poll_once(next.as_mut()), Poll::Ready(None));
}

#[test]
fn join_collects_queued_and_later_outputs() {
    let (mut group, registrar) = TaskGroup::new();
    let mut first = Box::pin(registrar.track(future::ready(1)).unwrap());
    let mut second = Box::pin(registrar.track(future::ready(2)).unwrap());
    let mut third = Box::pin(registrar.track(future::ready(3)).unwrap());
    assert_eq!(poll_once(first.as_mut()), Poll::Ready(()));
    group.close();

    let mut join = Box::pin(group.join());
    assert!(poll_once(join.as_mut()).is_pending());
    assert_eq!(poll_once(second.as_mut()), Poll::Ready(()));
    assert_eq!(poll_once(third.as_mut()), Poll::Ready(()));
    drop(first);
    drop(second);
    drop(third);

    let Poll::Ready(outputs) = poll_once(join.as_mut()) else {
        panic!("the last task completion must finish the join");
    };
    assert_eq!(outputs, [1, 2, 3]);
    assert!(outputs.capacity() >= 3);
}

#[test]
fn dropping_the_last_tracked_future_finishes_a_closed_group() {
    let (mut group, registrar) = TaskGroup::<()>::new();
    let tracked = registrar.track(future::pending()).unwrap();
    group.close();

    let counter = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    let mut next = Box::pin(group.join_next());
    assert!(
        next.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(tracked);
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    assert_eq!(poll_once(next.as_mut()), Poll::Ready(None));
}

#[test]
fn completed_future_is_dropped_before_wait_finishes() {
    let (mut group, registrar) = TaskGroup::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let mut tracked = Box::pin(
        registrar
            .track(ReadyDropFlagFuture(dropped.clone()))
            .unwrap(),
    );
    group.close();

    assert_eq!(poll_once(tracked.as_mut()), Poll::Ready(()));

    let counter = Arc::new(AssertDroppedWake {
        dropped: dropped.clone(),
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(counter.clone());
    let mut wait = Box::pin(group.wait());
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    drop(tracked);
    assert_eq!(counter.wakes.load(Ordering::Relaxed), 1);
    assert_eq!(poll_once(wait.as_mut()), Poll::Ready(()));
}

#[test]
fn cancellation_destroys_the_inner_future_before_notifying_the_group() {
    let (mut group, registrar) = TaskGroup::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let tracked = registrar.track(DropFlagFuture(dropped.clone())).unwrap();
    group.close();

    let counter = Arc::new(AssertDroppedWake {
        dropped,
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(counter.clone());
    let mut join = Box::pin(group.join_next());
    assert!(
        join.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    drop(tracked);
    assert_eq!(counter.wakes.load(Ordering::Relaxed), 1);
    assert_eq!(poll_once(join.as_mut()), Poll::Ready(None));
}

#[test]
fn a_panicking_future_destructor_still_releases_the_registration() {
    let (mut group, registrar) = TaskGroup::new();
    let tracked = registrar.track(PanicOnDropFuture).unwrap();
    group.close();

    assert!(panic::catch_unwind(|| drop(tracked)).is_err());
    let mut join = Box::pin(group.join_next());
    assert_eq!(poll_once(join.as_mut()), Poll::Ready(None));
}

#[test]
fn cancelling_join_next_unregisters_its_waker() {
    let (mut group, _registrar) = TaskGroup::<()>::new();
    let mut next = Box::pin(group.join_next());
    assert!(poll_once(next.as_mut()).is_pending());
    drop(next);

    assert!(group.shared.state.lock().waiter.is_none());
}

#[test]
fn wait_discards_outputs_without_intermediate_wakes() {
    let (mut group, registrar) = TaskGroup::new();
    let mut first = Box::pin(registrar.track(future::ready(1)).unwrap());
    let mut second = Box::pin(registrar.track(future::ready(2)).unwrap());
    let shared = group.shared.clone();
    group.close();

    let counter = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    let mut wait = Box::pin(group.wait());
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(shared.state.lock().discard_outputs);

    assert_eq!(poll_once(first.as_mut()), Poll::Ready(()));
    assert_eq!(counter.0.load(Ordering::Relaxed), 0);
    assert!(shared.state.lock().outputs.is_empty());

    assert_eq!(poll_once(second.as_mut()), Poll::Ready(()));
    assert_eq!(counter.0.load(Ordering::Relaxed), 0);
    drop(first);
    assert_eq!(counter.0.load(Ordering::Relaxed), 0);
    drop(second);
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    assert_eq!(poll_once(wait.as_mut()), Poll::Ready(()));
    assert!(!shared.state.lock().discard_outputs);
}

#[test]
fn cancelling_wait_restores_result_collection() {
    let (mut group, registrar) = TaskGroup::new();
    let mut discarded = Box::pin(registrar.track(future::ready(1)).unwrap());
    let mut retained = Box::pin(registrar.track(future::ready(2)).unwrap());
    let shared = group.shared.clone();

    let mut wait = Box::pin(group.wait());
    assert!(poll_once(wait.as_mut()).is_pending());
    assert_eq!(poll_once(discarded.as_mut()), Poll::Ready(()));
    drop(wait);
    {
        let state = shared.state.lock();
        assert!(!state.discard_outputs);
        assert!(state.waiter.is_none());
        assert!(state.outputs.is_empty());
    }

    assert_eq!(poll_once(retained.as_mut()), Poll::Ready(()));
    drop(discarded);
    drop(retained);
    group.close();
    let mut next = Box::pin(group.join_next());
    assert_eq!(poll_once(next.as_mut()), Poll::Ready(Some(2)));
    drop(next);
    let mut next = Box::pin(group.join_next());
    assert_eq!(poll_once(next.as_mut()), Poll::Ready(None));
}

#[test]
#[cfg_attr(miri, ignore = "intentionally leaks a polled future")]
fn forgetting_wait_does_not_prevent_another_wait() {
    let (mut group, registrar) = TaskGroup::new();
    let mut tracked = Box::pin(registrar.track(future::ready(())).unwrap());
    let shared = group.shared.clone();
    group.close();

    let first_counter = Arc::new(CountWake(AtomicUsize::new(0)));
    let first_waker = Waker::from(first_counter.clone());
    let mut first_wait = Box::pin(group.wait());
    assert!(
        first_wait
            .as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    mem::forget(first_wait);

    let second_counter = Arc::new(CountWake(AtomicUsize::new(0)));
    let second_waker = Waker::from(second_counter.clone());
    let mut second_wait = Box::pin(group.wait());
    assert!(
        second_wait
            .as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending()
    );

    assert_eq!(poll_once(tracked.as_mut()), Poll::Ready(()));
    assert_eq!(first_counter.0.load(Ordering::Relaxed), 0);
    assert_eq!(second_counter.0.load(Ordering::Relaxed), 0);
    drop(tracked);
    assert_eq!(first_counter.0.load(Ordering::Relaxed), 0);
    assert_eq!(second_counter.0.load(Ordering::Relaxed), 1);
    assert_eq!(poll_once(second_wait.as_mut()), Poll::Ready(()));
    assert!(!shared.state.lock().discard_outputs);
}

#[test]
fn panicking_discarded_output_does_not_prevent_the_final_wake() {
    let (mut group, registrar) = TaskGroup::new();
    let mut tracked = Box::pin(registrar.track(future::ready(PanicOnDropOutput)).unwrap());
    group.close();

    let counter = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    let mut wait = Box::pin(group.wait());
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    assert!(panic::catch_unwind(panic::AssertUnwindSafe(|| poll_once(tracked.as_mut()))).is_err());
    assert_eq!(counter.0.load(Ordering::Relaxed), 0);
    drop(tracked);
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    assert_eq!(poll_once(wait.as_mut()), Poll::Ready(()));
}

#[test]
fn close_rejects_registration_and_returns_the_future() {
    let (group, registrar) = TaskGroup::new();
    group.close();

    let future = future::ready(42);
    let error = registrar.track(future).unwrap_err();
    assert_eq!(error.into_inner().into_inner(), 42);
    assert!(group.is_closed());
    assert!(registrar.is_closed());
}

#[test]
fn dropping_the_owner_rejects_registration_without_stopping_tracked_futures() {
    let (group, registrar) = TaskGroup::new();
    let mut tracked = Box::pin(registrar.track(future::ready(42)).unwrap());
    drop(group);

    assert!(registrar.track(future::ready(7)).is_err());
    assert_eq!(poll_once(tracked.as_mut()), Poll::Ready(()));
}

#[test]
fn tracked_supports_non_unpin_futures() {
    let (mut group, registrar) = TaskGroup::new();
    let mut tracked = Box::pin(
        registrar
            .track(PinnedReady {
                output: 42,
                _pin: PhantomPinned,
            })
            .unwrap(),
    );
    group.close();

    assert_eq!(poll_once(tracked.as_mut()), Poll::Ready(()));
    let mut next = Box::pin(group.join_next());
    assert_eq!(poll_once(next.as_mut()), Poll::Ready(Some(42)));
}
