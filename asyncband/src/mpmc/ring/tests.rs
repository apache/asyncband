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
use std::pin::pin;
use std::sync::Arc;
use std::task::Wake;
use std::thread;

use super::*;

struct CountWakes(AtomicUsize);

impl Wake for CountWakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn wake_counter() -> (Waker, Arc<CountWakes>) {
    let wakes = Arc::new(CountWakes(AtomicUsize::new(0)));
    (Waker::from(wakes.clone()), wakes)
}

fn reservation_state<T>(shared: &Shared<T>) -> (usize, bool) {
    let producer = shared.producer.lock();
    (producer.reserved, !producer.waiters.is_empty())
}

struct CountDrops(Arc<AtomicUsize>);

impl Drop for CountDrops {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn positions_wrap_across_laps_in_fifo_order() {
    let shared = Shared::new(3);
    let mut next = 0;
    for lap in 0..20 {
        for _ in 0..lap % 3 + 1 {
            shared.try_send(next).unwrap();
            next += 1;
        }
        for expected in next - (lap % 3 + 1)..next {
            assert_eq!(shared.try_recv(), Ok(expected));
        }
    }
    for value in 0..3 {
        shared.try_send(value).unwrap();
    }
    assert_eq!(shared.try_send(3), Err(TrySendError::Full(3)));
}

#[test]
fn storage_selection_does_not_require_allocating_large_queues() {
    assert!(!Shared::<()>::supports(usize::MAX));
    assert!(!Shared::<[u8; 32769]>::supports(2));
    assert!(Shared::<usize>::supports(3));
}

#[test]
fn reservations_and_messages_survive_complete_cursor_wraps() {
    let shared = Shared::new(3);
    for round in 0..32 {
        let permit = shared.try_reserve().unwrap();
        shared.try_send(round * 3).unwrap();
        shared.try_send(round * 3 + 1).unwrap();
        assert_eq!(shared.try_send(999), Err(TrySendError::Full(999)));
        permit.send(round * 3 + 2).unwrap();
        for value in round * 3..round * 3 + 3 {
            assert_eq!(shared.try_recv(), Ok(value));
        }
        assert_eq!(shared.try_recv(), Err(TryRecvError::Empty));
        assert_eq!(reservation_state(&shared), (0, false));
        // These are complete cursor-value wraps, not just slot-index wraps.
        let expected = if round % 2 == 0 { 3 } else { 0 };
        assert_eq!(shared.published.load(Ordering::Relaxed), expected);
        assert_eq!(shared.reclaimed.load(Ordering::Relaxed), expected);
    }
}

#[test]
fn held_capacity_survives_many_cursor_wraps() {
    let shared = Shared::new(2);
    let permit = shared.try_reserve().unwrap();
    for value in 0..128 {
        shared.try_send(value).unwrap();
        assert_eq!(shared.try_send(999), Err(TrySendError::Full(999)));
        assert_eq!(shared.try_recv(), Ok(value));
        assert_eq!(reservation_state(&shared), (1, false));
    }
    permit.send(128).unwrap();
    assert_eq!(shared.try_recv(), Ok(128));
    assert_eq!(reservation_state(&shared), (0, false));
}

#[test]
fn ready_payload_transfers_do_not_acquire_the_opposite_lock() {
    let shared = Shared::new(2);
    {
        let _consumer = shared.consumer.lock();
        shared.try_send(1).unwrap();
    }
    {
        let _producer = shared.producer.lock();
        assert_eq!(shared.try_recv(), Ok(1));
    }
}

#[test]
fn last_receiver_drops_buffered_values_across_the_wrap() {
    let drops = Arc::new(AtomicUsize::new(0));
    let shared = Shared::new(2);
    shared.try_send(CountDrops(drops.clone())).unwrap();
    shared.try_send(CountDrops(drops.clone())).unwrap();
    drop(shared.try_recv().unwrap());
    shared.try_send(CountDrops(drops.clone())).unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    shared.drop_receiver();
    assert_eq!(drops.load(Ordering::Relaxed), 3);
    assert!(matches!(
        shared.try_send(CountDrops(drops.clone())),
        Err(TrySendError::Disconnected(_))
    ));
    assert_eq!(drops.load(Ordering::Relaxed), 4);
}

#[test]
fn discard_continues_after_a_destructor_panics() {
    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("value destructor panicked");
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let shared = Shared::new(3);
    // Wraps the remaining values, so they are dropped in two parts.
    for _ in 0..2 {
        shared.try_send((None, None)).unwrap();
        shared.try_recv().unwrap();
    }
    shared.try_send((None, Some(PanicOnDrop))).unwrap();
    shared
        .try_send((Some(CountDrops(drops.clone())), None))
        .unwrap();
    shared
        .try_send((Some(CountDrops(drops.clone())), None))
        .unwrap();
    assert!(panic::catch_unwind(|| shared.drop_receiver()).is_err());
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}

#[test]
fn receiver_that_takes_a_silently_published_value_leaves_the_wait_queue() {
    let shared = Shared::new(2);
    shared.clone_receiver();
    let (first_waker, first_wakes) = wake_counter();
    let (second_waker, second_wakes) = wake_counter();
    let mut first = Recv {
        shared: &shared,
        waiter: None,
    };
    let mut second = Recv {
        shared: &shared,
        waiter: None,
    };
    assert!(
        first
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    assert!(
        second
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending()
    );
    // Delay notification after publication; a spurious poll must remove its registration.
    shared.publish(&mut shared.producer.lock(), 1);
    assert_eq!(
        first.poll(&mut Context::from_waker(&first_waker)),
        Poll::Ready(Ok(1))
    );
    shared.try_send(2).unwrap();
    assert_eq!(first_wakes.0.load(Ordering::Relaxed), 0);
    assert_eq!(second_wakes.0.load(Ordering::Relaxed), 1);
    assert_eq!(
        second.poll(&mut Context::from_waker(&second_waker)),
        Poll::Ready(Ok(2))
    );
}

#[test]
fn sender_close_wakes_linked_and_notified_receivers_and_preserves_the_final_value() {
    let shared = Shared::new(2);
    let (first_waker, first_wakes) = wake_counter();
    let (second_waker, second_wakes) = wake_counter();
    let mut first = pin!(shared.recv());
    let mut second = pin!(shared.recv());
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    assert!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending()
    );
    shared.try_send(7).unwrap();
    shared.drop_sender();
    assert_eq!(first_wakes.0.load(Ordering::Relaxed), 1);
    assert_eq!(second_wakes.0.load(Ordering::Relaxed), 1);
    assert_eq!(
        first.as_mut().poll(&mut Context::from_waker(&first_waker)),
        Poll::Ready(Ok(7))
    );
    assert_eq!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(&second_waker)),
        Poll::Ready(Err(RecvError::Disconnected))
    );
}

#[test]
fn cancelled_reservation_keeps_capacity_exact() {
    let shared = Shared::new(1);
    let permit = shared.try_reserve().unwrap();
    let (waker, wakes) = wake_counter();
    let mut reserve = pin!(shared.reserve());
    assert!(
        reserve
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(permit);
    assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
    let Poll::Ready(Ok(granted)) = reserve.as_mut().poll(&mut Context::from_waker(&waker)) else {
        panic!("released capacity must be granted");
    };
    assert!(matches!(shared.try_send(1), Err(TrySendError::Full(1))));
    granted.send(2).unwrap();
    assert_eq!(shared.try_recv(), Ok(2));
    shared.try_send(3).unwrap();
    assert_eq!(shared.try_recv(), Ok(3));
}

#[test]
fn reservations_and_grants_keep_capacity_exact_across_laps() {
    let shared = Shared::new(2);
    let (waker, wakes) = wake_counter();
    for round in 0..6 {
        let permit = shared.try_reserve().unwrap();
        shared.try_send(round).unwrap();
        assert!(matches!(shared.try_send(9), Err(TrySendError::Full(9))));
        let mut reserve = pin!(shared.reserve());
        assert!(
            reserve
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(shared.try_recv(), Ok(round));
        assert!(matches!(shared.try_send(9), Err(TrySendError::Full(9))));
        let Poll::Ready(Ok(granted)) = reserve.as_mut().poll(&mut Context::from_waker(&waker))
        else {
            panic!("freed capacity must be granted");
        };
        drop(permit);
        granted.send(round + 10).unwrap();
        assert_eq!(reservation_state(&shared), (0, false));
        assert_eq!(shared.try_recv(), Ok(round + 10));
    }
    assert_eq!(wakes.0.load(Ordering::Relaxed), 6);
}

#[test]
fn receive_and_unused_permit_release_grant_all_available_capacity() {
    let shared = Shared::new(2);
    let held = shared.try_reserve().unwrap();
    shared.try_send(1).unwrap();
    let (first_waker, first_wakes) = wake_counter();
    let (second_waker, second_wakes) = wake_counter();
    let mut first = pin!(shared.reserve());
    let mut second = pin!(shared.reserve());
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    assert!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending()
    );
    assert_eq!(shared.try_recv(), Ok(1));
    assert_eq!(
        (
            first_wakes.0.load(Ordering::Relaxed),
            second_wakes.0.load(Ordering::Relaxed)
        ),
        (1, 0)
    );
    drop(held);
    assert_eq!(
        (
            first_wakes.0.load(Ordering::Relaxed),
            second_wakes.0.load(Ordering::Relaxed)
        ),
        (1, 1)
    );
    assert_eq!(reservation_state(&shared), (2, false));
    let Poll::Ready(Ok(first)) = first.as_mut().poll(&mut Context::from_waker(&first_waker)) else {
        panic!("first grant missing");
    };
    let Poll::Ready(Ok(second)) = second
        .as_mut()
        .poll(&mut Context::from_waker(&second_waker))
    else {
        panic!("second grant missing");
    };
    first.send(2).unwrap();
    second.send(3).unwrap();
    assert_eq!(shared.try_recv(), Ok(2));
    assert_eq!(shared.try_recv(), Ok(3));
}

#[test]
fn threads_mixing_reservations_and_direct_sends_deliver_every_value_once() {
    const THREADS: usize = 3;
    const VALUES: usize = if cfg!(miri) { 16 } else { 20_000 };
    let shared = Shared::new(2);
    let mut received = thread::scope(|scope| {
        for producer in 0..THREADS {
            let shared = &shared;
            scope.spawn(move || {
                for value in producer * VALUES..(producer + 1) * VALUES {
                    if value % 2 == 0 {
                        let mut value = value;
                        while let Err(error) = shared.try_send(value) {
                            let TrySendError::Full(returned) = error else {
                                unreachable!("receivers outlive producers");
                            };
                            value = returned;
                            thread::yield_now();
                        }
                    } else {
                        let permit = loop {
                            match shared.try_reserve() {
                                Ok(permit) => break permit,
                                Err(TrySendError::Full(())) => thread::yield_now(),
                                Err(TrySendError::Disconnected(())) => unreachable!(),
                            }
                        };
                        permit.send(value).unwrap();
                    }
                }
            });
        }
        let consumers = (0..THREADS)
            .map(|_| {
                let shared = &shared;
                scope.spawn(move || {
                    let mut values = Vec::new();
                    while values.len() < VALUES {
                        match shared.try_recv() {
                            Ok(value) => values.push(value),
                            Err(TryRecvError::Empty) => thread::yield_now(),
                            Err(TryRecvError::Disconnected) => unreachable!(),
                        }
                    }
                    values
                })
            })
            .collect::<Vec<_>>();
        consumers
            .into_iter()
            .flat_map(|consumer| consumer.join().unwrap())
            .collect::<Vec<_>>()
    });
    received.sort_unstable();
    assert_eq!(received, (0..THREADS * VALUES).collect::<Vec<_>>());
    assert_eq!(reservation_state(&shared), (0, false));
}
