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

use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::BuildHasherDefault;
use std::pin::Pin;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::FairShare;

fn poll_once<F>(future: Pin<&mut F>) -> Poll<F::Output>
where
    F: Future,
{
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
#[should_panic(expected = "FairShare requires a non-zero capacity")]
fn zero_capacity_panics() {
    FairShare::<usize>::new(0);
}

#[test]
fn tracks_capacity_and_key_usage() {
    let admission = FairShare::new(2);
    assert_eq!(admission.capacity(), 2);
    assert_eq!(admission.available_permits(), 2);
    assert_eq!(admission.queue_len(), 0);
    assert!(admission.is_idle());

    let permit_a0 = admission.try_acquire("a").unwrap();
    let permit_a1 = admission.try_acquire("a").unwrap();
    assert_eq!(permit_a0.key(), &"a");
    assert_eq!(admission.available_permits(), 0);
    assert_eq!(admission.in_flight(&"a"), 2);
    assert!(!admission.is_idle());
    assert!(admission.try_acquire("b").is_none());

    drop(permit_a0);
    assert_eq!(admission.available_permits(), 1);
    assert_eq!(admission.in_flight(&"a"), 1);

    drop(permit_a1);
    assert_eq!(admission.available_permits(), 2);
    assert_eq!(admission.in_flight(&"a"), 0);
    assert!(admission.is_idle());
}

#[test]
fn uses_spare_capacity_without_reservations() {
    let admission = FairShare::new(3);
    let permits = [
        admission.try_acquire("a").unwrap(),
        admission.try_acquire("a").unwrap(),
        admission.try_acquire("a").unwrap(),
    ];

    assert_eq!(admission.in_flight(&"a"), 3);
    assert_eq!(admission.available_permits(), 0);

    drop(permits);
    assert!(admission.is_idle());
}

#[test]
fn admits_the_key_with_the_smallest_share() {
    let admission = FairShare::new(2);
    let permit_a0 = admission.try_acquire("a").unwrap();
    let permit_a1 = admission.try_acquire("a").unwrap();

    let acquire_a = admission.acquire("a");
    let mut acquire_a = pin!(acquire_a);
    assert!(poll_once(acquire_a.as_mut()).is_pending());

    let acquire_b = admission.acquire("b");
    let mut acquire_b = pin!(acquire_b);
    assert!(poll_once(acquire_b.as_mut()).is_pending());
    assert_eq!(admission.queue_len(), 2);

    drop(permit_a0);
    assert!(poll_once(acquire_a.as_mut()).is_pending());
    let permit_b = match poll_once(acquire_b.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("key b should receive the released permit"),
    };
    assert_eq!(permit_b.key(), &"b");

    drop(permit_a1);
    let permit_a = match poll_once(acquire_a.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("key a should receive the next permit"),
    };
    assert_eq!(permit_a.key(), &"a");
}

#[test]
fn equalizes_capacity_across_contending_keys() {
    let admission = FairShare::new(3);
    let mut held_by_a = vec![
        admission.try_acquire("a").unwrap(),
        admission.try_acquire("a").unwrap(),
        admission.try_acquire("a").unwrap(),
    ];

    let acquire_a = admission.acquire("a");
    let mut acquire_a = pin!(acquire_a);
    assert!(poll_once(acquire_a.as_mut()).is_pending());

    let acquire_b = admission.acquire("b");
    let mut acquire_b = pin!(acquire_b);
    assert!(poll_once(acquire_b.as_mut()).is_pending());

    let acquire_c = admission.acquire("c");
    let mut acquire_c = pin!(acquire_c);
    assert!(poll_once(acquire_c.as_mut()).is_pending());

    drop(held_by_a.pop().unwrap());
    let permit_b = match poll_once(acquire_b.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("key b should receive the first released permit"),
    };

    drop(held_by_a.pop().unwrap());
    let permit_c = match poll_once(acquire_c.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("key c should receive the second released permit"),
    };

    drop(held_by_a.pop().unwrap());
    let permit_a = match poll_once(acquire_a.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("key a should receive the third released permit"),
    };

    assert_eq!(admission.in_flight(&"a"), 1);
    assert_eq!(admission.in_flight(&"b"), 1);
    assert_eq!(admission.in_flight(&"c"), 1);
    drop((permit_a, permit_b, permit_c));
    assert!(admission.is_idle());
}

#[test]
fn breaks_equal_share_ties_by_queue_order() {
    let admission = FairShare::new(1);
    let held = admission.try_acquire("held").unwrap();

    let acquire_b = admission.acquire("b");
    let mut acquire_b = pin!(acquire_b);
    assert!(poll_once(acquire_b.as_mut()).is_pending());

    let acquire_a = admission.acquire("a");
    let mut acquire_a = pin!(acquire_a);
    assert!(poll_once(acquire_a.as_mut()).is_pending());

    drop(held);
    let permit_b = match poll_once(acquire_b.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("the first queued acquisition should win an equal-share tie"),
    };
    assert!(poll_once(acquire_a.as_mut()).is_pending());

    drop(permit_b);
    let permit_a = match poll_once(acquire_a.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("the second queued acquisition should be admitted next"),
    };
    assert_eq!(permit_a.key(), &"a");
}

#[test]
fn preserves_queue_order_within_a_key() {
    let admission = FairShare::new(1);
    let held = admission.try_acquire(7usize).unwrap();

    let first = admission.acquire(7usize);
    let mut first = pin!(first);
    assert!(poll_once(first.as_mut()).is_pending());

    let second = admission.acquire(7usize);
    let mut second = pin!(second);
    assert!(poll_once(second.as_mut()).is_pending());

    drop(held);
    let first_permit = match poll_once(first.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("the first acquisition should be admitted first"),
    };
    assert!(poll_once(second.as_mut()).is_pending());

    drop(first_permit);
    let second_permit = match poll_once(second.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("the second acquisition should be admitted second"),
    };
    assert_eq!(second_permit.key(), &7);
}

#[test]
fn cancelling_a_pending_acquire_removes_it() {
    let admission = FairShare::new(1);
    let held = admission.try_acquire(1usize).unwrap();

    {
        let acquire = admission.acquire(2usize);
        let mut acquire = pin!(acquire);
        assert!(poll_once(acquire.as_mut()).is_pending());
        assert_eq!(admission.queue_len(), 1);
    }

    assert_eq!(admission.queue_len(), 0);
    drop(held);
    assert!(admission.is_idle());

    let permit = admission.try_acquire(3usize).unwrap();
    assert_eq!(permit.key(), &3);
}

#[test]
fn cancelling_an_admitted_acquire_reassigns_its_permit() {
    let admission = FairShare::new(1);
    let held = admission.try_acquire("held").unwrap();

    let mut first = Box::pin(admission.acquire("first"));
    assert!(poll_once(first.as_mut()).is_pending());

    let mut second = Box::pin(admission.acquire("second"));
    assert!(poll_once(second.as_mut()).is_pending());

    drop(held);
    assert_eq!(admission.in_flight(&"first"), 1);
    assert_eq!(admission.queue_len(), 1);

    drop(first);
    assert_eq!(admission.in_flight(&"first"), 0);
    assert_eq!(admission.in_flight(&"second"), 1);
    assert_eq!(admission.queue_len(), 0);

    let permit = match poll_once(second.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("cancellation should reassign the granted permit"),
    };
    assert_eq!(permit.key(), &"second");
}

#[test]
fn cancelling_within_a_key_preserves_its_queue() {
    let admission = FairShare::new(1);
    let held = admission.try_acquire("held").unwrap();

    let mut first = Box::pin(admission.acquire("tenant"));
    assert!(poll_once(first.as_mut()).is_pending());

    let mut second = Box::pin(admission.acquire("tenant"));
    assert!(poll_once(second.as_mut()).is_pending());

    drop(first);
    assert_eq!(admission.queue_len(), 1);

    drop(held);
    let permit = match poll_once(second.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("cancelling one acquisition must not detach the next"),
    };
    assert_eq!(permit.key(), &"tenant");
}

#[test]
fn supports_a_custom_hash_builder() {
    let admission = FairShare::<String, BuildHasherDefault<DefaultHasher>>::with_hasher(
        1,
        BuildHasherDefault::default(),
    );
    let permit = admission.try_acquire("tenant".to_owned()).unwrap();
    assert_eq!(permit.key(), "tenant");
}

#[test]
fn owned_permit_keeps_the_admission_controller_alive() {
    let admission = Arc::new(FairShare::new(1));
    let permit = admission
        .clone()
        .try_acquire_owned("tenant")
        .expect("capacity should be available");

    drop(admission);
    assert_eq!(permit.key(), &"tenant");
    drop(permit);
}

#[test]
fn acquire_futures_are_send() {
    fn assert_send<T: Send>(_: T) {}

    let admission = FairShare::<String>::new(1);
    assert_send(admission.acquire("tenant".to_owned()));

    let admission = Arc::new(FairShare::<String>::new(1));
    assert_send(admission.acquire_owned("tenant".to_owned()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_test_preserves_capacity() {
    let admission = Arc::new(FairShare::new(3));
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for key in 0..5usize {
        for _ in 0..32usize {
            let admission = admission.clone();
            let active = active.clone();
            let max_active = max_active.clone();
            handles.push(tokio::spawn(async move {
                let _permit = admission.acquire_owned(key).await;
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_active.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::SeqCst);
            }));
        }
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(max_active.load(Ordering::SeqCst) <= 3);
    assert_eq!(admission.available_permits(), 3);
    assert_eq!(admission.queue_len(), 0);
    assert!(admission.is_idle());
}
