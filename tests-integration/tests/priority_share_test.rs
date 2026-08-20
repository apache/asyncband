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

use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;

use asyncband::admission::FairShare;
use asyncband::admission::PriorityShare;
use tests_integration::poll_once;

#[test]
#[should_panic(expected = "PriorityShare requires at least one priority")]
fn empty_capacity_increments_panic() {
    PriorityShare::<usize>::new([]);
}

#[test]
#[should_panic(expected = "PriorityShare requires at least one permit")]
fn zero_total_capacity_panics() {
    PriorityShare::<usize>::new([0, 0]);
}

#[test]
#[should_panic(expected = "PriorityShare capacity overflow")]
fn capacity_overflow_panics() {
    PriorityShare::<usize>::new([usize::MAX, 1]);
}

#[test]
fn returns_priority_bound_handles_that_share_state() {
    let priorities = PriorityShare::new([1, 0, 2]);

    assert_eq!(priorities.len(), 3);
    for (priority, admission) in priorities.iter().enumerate() {
        assert_eq!(admission.priority(), priority);
    }

    let high = priorities[2].clone();
    let permits = ["high-0", "high-1", "high-2"].map(|owner| high.try_acquire(owner).unwrap());
    assert!(priorities[0].try_acquire("low").is_none());
    assert!(priorities[1].try_acquire("middle").is_none());
    assert!(high.try_acquire("high-3").is_none());

    drop(permits);
    let low_permit = priorities[0].try_acquire("low").unwrap();
    assert_eq!(low_permit.priority(), 0);
    assert!(priorities[1].try_acquire("middle").is_none());
    assert!(high.try_acquire("high").is_some());
}

#[test]
fn available_permits_are_scoped_to_each_priority() {
    let priorities = PriorityShare::new([4, 1]);
    let low = &priorities[0];
    let high = &priorities[1];

    assert_eq!(low.available_permits(), 4);
    assert_eq!(high.available_permits(), 5);
    let permits = ["high-0", "high-1", "high-2"].map(|owner| high.try_acquire(owner).unwrap());
    assert_eq!(low.available_permits(), 1);
    assert_eq!(high.available_permits(), 2);

    drop(permits);
    assert_eq!(low.available_permits(), 4);
    assert_eq!(high.available_permits(), 5);
}

#[test]
fn all_priorities_count_toward_shared_admission_thresholds() {
    let priorities = PriorityShare::new([4, 1]);
    let low = &priorities[0];
    let high = &priorities[1];
    let low0 = low.try_acquire("low-0").unwrap();
    let low1 = low.try_acquire("low-1").unwrap();
    let low2 = low.try_acquire("low-2").unwrap();
    let high0 = high.try_acquire("high-0").unwrap();

    assert!(low.try_acquire("low-3").is_none());

    let high1 = high.try_acquire("high-1").unwrap();
    assert_eq!(high1.priority(), 1);
    assert!(high.try_acquire("high-2").is_none());

    drop((low0, low1, low2, high0, high1));
    assert!(low.try_acquire("low-after-release").is_some());
}

#[test]
fn higher_priority_can_use_entire_shared_capacity() {
    let priorities = PriorityShare::new([2, 1]);
    let high = &priorities[1];
    let permits = [
        high.try_acquire("high-0").unwrap(),
        high.try_acquire("high-1").unwrap(),
        high.try_acquire("high-2").unwrap(),
    ];

    assert!(high.try_acquire("high-3").is_none());

    drop(permits);
    assert!(high.try_acquire("high-after-release").is_some());
}

#[test]
fn higher_priority_bypasses_queued_lower_priority() {
    let priorities = PriorityShare::new([1, 1]);
    let low = &priorities[0];
    let high = &priorities[1];
    let low_held = low.try_acquire("low-held").unwrap();

    let low_waiter = low.acquire("low-waiter");
    let mut low_waiter = pin!(low_waiter);
    assert!(poll_once(low_waiter.as_mut()).is_pending());

    let high_permit = high.try_acquire("high").unwrap();
    assert_eq!(high_permit.key(), &"high");
    assert_eq!(high_permit.priority(), 1);

    drop(low_held);
    assert!(poll_once(low_waiter.as_mut()).is_pending());

    drop(high_permit);
    let low_permit = match poll_once(low_waiter.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => {
            panic!("the lower-priority waiter should enter below its shared threshold")
        }
    };

    drop(low_permit);
}

#[test]
fn released_capacity_goes_to_the_highest_priority_waiter() {
    let priorities = PriorityShare::new([1, 0]);
    let low = &priorities[0];
    let high = &priorities[1];
    let held = low.try_acquire("held").unwrap();

    let low_waiter = low.acquire("low");
    let mut low_waiter = pin!(low_waiter);
    assert!(poll_once(low_waiter.as_mut()).is_pending());

    let high_waiter = high.acquire("high");
    let mut high_waiter = pin!(high_waiter);
    assert!(poll_once(high_waiter.as_mut()).is_pending());

    drop(held);
    assert!(poll_once(low_waiter.as_mut()).is_pending());
    let high_permit = match poll_once(high_waiter.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("the higher-priority waiter should be admitted first"),
    };

    drop(high_permit);
    let low_permit = match poll_once(low_waiter.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("the lower-priority waiter should eventually be admitted"),
    };
    drop(low_permit);
}

#[test]
fn assigned_permit_is_not_revoked_for_a_higher_priority_waiter() {
    let priorities = PriorityShare::new([1, 0]);
    let low = &priorities[0];
    let high = &priorities[1];
    let held = low.try_acquire("held").unwrap();

    let low_waiter = low.acquire("low");
    let mut low_waiter = pin!(low_waiter);
    assert!(poll_once(low_waiter.as_mut()).is_pending());
    drop(held);

    let high_waiter = high.acquire("high");
    let mut high_waiter = pin!(high_waiter);
    assert!(poll_once(high_waiter.as_mut()).is_pending());

    let low_permit = match poll_once(low_waiter.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("an assigned lower-priority permit must not be revoked"),
    };
    assert!(poll_once(high_waiter.as_mut()).is_pending());

    drop(low_permit);
    let high_permit = match poll_once(high_waiter.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("the higher-priority waiter should receive the released permit"),
    };
    drop(high_permit);
}

#[test]
fn cancelling_an_assigned_permit_reassigns_it_by_priority() {
    let priorities = PriorityShare::new([1, 0]);
    let low = &priorities[0];
    let high = &priorities[1];
    let held = low.try_acquire("held").unwrap();

    let mut low_waiter = Box::pin(low.acquire("low"));
    assert!(poll_once(low_waiter.as_mut()).is_pending());
    drop(held);

    let high_waiter = high.acquire("high");
    let mut high_waiter = pin!(high_waiter);
    assert!(poll_once(high_waiter.as_mut()).is_pending());

    drop(low_waiter);
    let high_permit = match poll_once(high_waiter.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("cancellation should reassign the permit by priority"),
    };
    drop(high_permit);
}

#[test]
fn fair_shares_within_one_priority() {
    let priorities = PriorityShare::new([0, 3]);
    let high = &priorities[1];
    let mut held_by_a = vec![
        high.try_acquire("a").unwrap(),
        high.try_acquire("a").unwrap(),
        high.try_acquire("a").unwrap(),
    ];

    let acquire_a = high.acquire("a");
    let mut acquire_a = pin!(acquire_a);
    assert!(poll_once(acquire_a.as_mut()).is_pending());

    let acquire_b = high.acquire("b");
    let mut acquire_b = pin!(acquire_b);
    assert!(poll_once(acquire_b.as_mut()).is_pending());

    let acquire_c = high.acquire("c");
    let mut acquire_c = pin!(acquire_c);
    assert!(poll_once(acquire_c.as_mut()).is_pending());

    drop(held_by_a.pop().unwrap());
    let permit_b = match poll_once(acquire_b.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("owner b should receive the first released permit"),
    };

    drop(held_by_a.pop().unwrap());
    let permit_c = match poll_once(acquire_c.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("owner c should receive the second released permit"),
    };

    drop(held_by_a.pop().unwrap());
    let permit_a = match poll_once(acquire_a.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("owner a should receive the third released permit"),
    };
    drop((permit_a, permit_b, permit_c));
}

#[test]
fn same_owner_can_acquire_multiple_priorities() {
    let priorities = PriorityShare::new([1, 1]);
    let low = &priorities[0];
    let high = &priorities[1];

    let low_permit = low.try_acquire("owner").unwrap();
    let high_permit = high.try_acquire("owner").unwrap();

    assert_eq!(low_permit.priority(), 0);
    assert_eq!(high_permit.priority(), 1);

    drop((low_permit, high_permit));
    assert!(low.try_acquire("owner-after-release").is_some());
}

#[test]
fn fairness_counts_an_owners_permits_across_priorities() {
    let priorities = PriorityShare::new([2, 0]);
    let low = &priorities[0];
    let high = &priorities[1];
    let held_a0 = low.try_acquire("a").unwrap();
    let held_a1 = low.try_acquire("a").unwrap();

    let acquire_a = high.acquire("a");
    let mut acquire_a = pin!(acquire_a);
    assert!(poll_once(acquire_a.as_mut()).is_pending());

    let acquire_b = high.acquire("b");
    let mut acquire_b = pin!(acquire_b);
    assert!(poll_once(acquire_b.as_mut()).is_pending());

    drop(held_a0);
    assert!(poll_once(acquire_a.as_mut()).is_pending());
    let permit_b = match poll_once(acquire_b.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("owner b has the smaller global share"),
    };

    drop(permit_b);
    let permit_a = match poll_once(acquire_a.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("owner a should receive the next permit"),
    };
    drop((held_a1, permit_a));
}

#[test]
fn same_owner_can_wait_at_multiple_priorities() {
    let priorities = PriorityShare::new([1, 0]);
    let low = &priorities[0];
    let high = &priorities[1];
    let held = low.try_acquire("held").unwrap();

    let low_waiter = low.acquire("owner");
    let mut low_waiter = pin!(low_waiter);
    assert!(poll_once(low_waiter.as_mut()).is_pending());

    let high_waiter = high.acquire("owner");
    let mut high_waiter = pin!(high_waiter);
    assert!(poll_once(high_waiter.as_mut()).is_pending());

    drop(held);
    assert!(poll_once(low_waiter.as_mut()).is_pending());
    let high_permit = match poll_once(high_waiter.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("the owner's higher-priority queue should be selected"),
    };

    drop(high_permit);
    let low_permit = match poll_once(low_waiter.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("the owner's lower-priority queue should remain attached"),
    };
    drop(low_permit);
}

#[test]
fn cancelling_one_priority_preserves_the_owners_other_queue() {
    let priorities = PriorityShare::new([1, 0]);
    let low = &priorities[0];
    let high = &priorities[1];
    let held = low.try_acquire("held").unwrap();

    let low_waiter = low.acquire("owner");
    let mut low_waiter = pin!(low_waiter);
    assert!(poll_once(low_waiter.as_mut()).is_pending());

    let mut high_waiter = Box::pin(high.acquire("owner"));
    assert!(poll_once(high_waiter.as_mut()).is_pending());
    drop(high_waiter);

    drop(held);
    let low_permit = match poll_once(low_waiter.as_mut()) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("cancelling one queue must not detach the other"),
    };
    drop(low_permit);
}

#[test]
fn single_priority_matches_fair_share() {
    let fair = FairShare::new(2);
    let mut priorities = PriorityShare::new([2]);
    let priority = priorities.pop().unwrap();

    let fair_a0 = fair.try_acquire("a").unwrap();
    let fair_a1 = fair.try_acquire("a").unwrap();
    let priority_a0 = priority.try_acquire("a").unwrap();
    let priority_a1 = priority.try_acquire("a").unwrap();

    let fair_a = fair.acquire("a");
    let mut fair_a = pin!(fair_a);
    let priority_a = priority.acquire("a");
    let mut priority_a = pin!(priority_a);
    assert_eq!(
        poll_once(fair_a.as_mut()).is_pending(),
        poll_once(priority_a.as_mut()).is_pending()
    );

    let fair_b = fair.acquire("b");
    let mut fair_b = pin!(fair_b);
    let priority_b = priority.acquire("b");
    let mut priority_b = pin!(priority_b);
    assert_eq!(
        poll_once(fair_b.as_mut()).is_pending(),
        poll_once(priority_b.as_mut()).is_pending()
    );

    drop((fair_a0, priority_a0));
    assert!(poll_once(fair_a.as_mut()).is_pending());
    assert!(poll_once(priority_a.as_mut()).is_pending());
    let fair_b_permit = poll_once(fair_b.as_mut());
    let priority_b_permit = poll_once(priority_b.as_mut());
    assert!(fair_b_permit.is_ready());
    assert!(priority_b_permit.is_ready());

    drop((fair_b_permit, priority_b_permit));
    drop((fair_a1, priority_a1));
    assert!(poll_once(fair_a.as_mut()).is_ready());
    assert!(poll_once(priority_a.as_mut()).is_ready());
}

#[test]
fn supports_a_custom_hash_builder() {
    let mut priorities = PriorityShare::<String, BuildHasherDefault<DefaultHasher>>::with_hasher(
        [1],
        BuildHasherDefault::default(),
    );
    let admission = priorities.pop().unwrap();
    let permit = admission.try_acquire("tenant".to_owned()).unwrap();
    assert_eq!(permit.key(), "tenant");
}

#[test]
fn owned_permit_keeps_the_priority_handle_alive() {
    let mut priorities = PriorityShare::new([1]);
    let admission = Arc::new(priorities.pop().unwrap());
    let permit = admission
        .clone()
        .try_acquire_owned("tenant")
        .expect("a permit should be available");

    drop(admission);
    assert_eq!(permit.key(), &"tenant");
    assert_eq!(permit.priority(), 0);
    drop(permit);
}

#[test]
fn acquire_futures_are_send() {
    fn assert_send<T: Send>(_: T) {}

    let mut priorities = PriorityShare::<String>::new([1]);
    let admission = priorities.pop().unwrap();
    assert_send(admission.acquire("tenant".to_owned()));

    let admission = Arc::new(admission);
    assert_send(admission.acquire_owned("tenant".to_owned()));
}

#[test]
fn deterministic_events_match_capacity_model() {
    for capacity_increments in [&[2, 1, 1][..], &[0, 2, 0, 1], &[1, 0, 2, 0]] {
        let priorities = PriorityShare::new(capacity_increments);
        let mut held = Vec::new();
        let mut seed = 0x4d59_5df4_d0f3_3173u64;

        for owner in 0..2_000usize {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);

            if !held.is_empty() && seed % 3 == 0 {
                let index = seed as usize % held.len();
                drop(held.swap_remove(index));
            } else {
                let priority = seed as usize % capacity_increments.len();
                let expected = can_admit(capacity_increments, held.len(), priority);
                let permit = priorities[priority].try_acquire(owner);
                assert_eq!(permit.is_some(), expected);
                if let Some(permit) = permit {
                    held.push(permit);
                }
            }
        }
    }
}

fn can_admit(capacity_increments: &[usize], held: usize, priority: usize) -> bool {
    held < capacity_increments[..=priority].iter().sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_test_preserves_shared_capacity_limit() {
    const CAPACITY_INCREMENTS: [usize; 3] = [2, 2, 1];

    let priorities: Vec<_> = PriorityShare::<usize>::new(CAPACITY_INCREMENTS)
        .into_iter()
        .map(Arc::new)
        .collect();
    let total_capacity = CAPACITY_INCREMENTS.iter().sum::<usize>();
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for admission in &priorities {
        for owner in 0..8usize {
            for _ in 0..16usize {
                let admission = admission.clone();
                let active = active.clone();
                let max_active = max_active.clone();
                handles.push(tokio::spawn(async move {
                    let _permit = admission.acquire_owned(owner).await;
                    let active_count = active.fetch_add(1, Ordering::SeqCst) + 1;
                    assert!(active_count <= total_capacity);
                    max_active.fetch_max(active_count, Ordering::SeqCst);

                    tokio::task::yield_now().await;
                    active.fetch_sub(1, Ordering::SeqCst);
                }));
            }
        }
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(max_active.load(Ordering::SeqCst) <= total_capacity);
    let high = &priorities[2];
    let permits: Vec<_> = (0..total_capacity)
        .map(|owner| high.try_acquire(owner).unwrap())
        .collect();
    assert!(high.try_acquire(total_capacity).is_none());
    drop(permits);
}
