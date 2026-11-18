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

use std::cell::Cell;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::thread;

use asyncband::spmc;
use asyncband::spmc::TryRecvError;
use asyncband::spmc::TrySendError;
use tests_integration::WakeCounter;
use tests_integration::assert_completes_without_deadlock;
use tests_integration::poll_once;
use tests_integration::poll_with;
use tests_integration::waker_on_drop;

#[test]
fn arbitrary_capacities_preserve_fifo_across_many_wraps() {
    for capacity in [1, 2, 3, 7, 64] {
        let (mut sender, receiver) = spmc::bounded(capacity);
        for lap in 0..16 {
            for offset in 0..capacity {
                sender.try_send(lap * capacity + offset).unwrap();
            }
            assert_eq!(
                sender.try_send(usize::MAX),
                Err(TrySendError::Full(usize::MAX))
            );
            for offset in 0..capacity {
                assert_eq!(receiver.try_recv(), Ok(lap * capacity + offset));
            }
            assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
        }
    }
}

#[test]
fn partial_receives_and_new_publications_preserve_fifo_across_wraps() {
    for capacity in [1, 3, 7, 64] {
        let (mut sender, receiver) = spmc::bounded(capacity);
        for value in 0..capacity {
            sender.try_send(value).unwrap();
        }
        let receives = capacity * if cfg!(miri) { 4 } else { 64 };
        for expected in 0..receives {
            assert_eq!(receiver.try_recv(), Ok(expected));
            sender.try_send(expected + capacity).unwrap();
            assert_eq!(
                sender.try_send(usize::MAX),
                Err(TrySendError::Full(usize::MAX))
            );
        }
        drop(sender);
        for expected in receives..receives + capacity {
            assert_eq!(receiver.try_recv(), Ok(expected));
        }
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }
}

#[test]
fn large_logical_capacities_do_not_require_eager_allocation() {
    let (mut sender, receiver) = spmc::bounded(usize::MAX);
    sender.try_send(1usize).unwrap();
    sender.try_send(2).unwrap();
    drop(sender);
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Ok(2));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn send_only_values_are_delivered_once_to_competing_threads() {
    competing_threads_deliver_once::<1>();
}

#[test]
fn large_inline_values_are_delivered_once_to_competing_threads() {
    competing_threads_deliver_once::<128>();
}

fn competing_threads_deliver_once<const WIDTH: usize>() {
    for capacity in [1, 2, 3, 64] {
        let total = if cfg!(miri) {
            32.max(4 * capacity)
        } else {
            2048
        };
        let (mut sender, receiver) = spmc::bounded(capacity);
        let mut received = thread::scope(|scope| {
            let consumers: Vec<_> = (0..3)
                .map(|_| {
                    let receiver = receiver.clone();
                    scope.spawn(move || {
                        let mut values = vec![];
                        loop {
                            match receiver.try_recv() {
                                Ok(value) => {
                                    let words: [usize; WIDTH] = Cell::into_inner(value);
                                    assert_eq!(words, [words[0]; WIDTH]);
                                    values.push(words[0]);
                                }
                                Err(TryRecvError::Empty) => thread::yield_now(),
                                Err(TryRecvError::Disconnected) => return values,
                            }
                        }
                    })
                })
                .collect();
            drop(receiver);
            let producer = scope.spawn(move || {
                for sequence in 0..total {
                    let mut value = Cell::new([sequence; WIDTH]);
                    loop {
                        match sender.try_send(value) {
                            Ok(()) => break,
                            Err(TrySendError::Full(unsent)) => {
                                value = unsent;
                                thread::yield_now();
                            }
                            Err(TrySendError::Disconnected(_)) => panic!("receivers are alive"),
                        }
                    }
                }
            });
            producer.join().unwrap();
            consumers
                .into_iter()
                .flat_map(|consumer| consumer.join().unwrap())
                .collect::<Vec<_>>()
        });
        received.sort_unstable();
        assert_eq!(received, (0..total).collect::<Vec<_>>());
    }
}

#[test]
fn publication_racing_receive_registration_cannot_lose_a_wakeup() {
    for capacity in [1, 3]
        .into_iter()
        .cycle()
        .take(if cfg!(miri) { 8 } else { 128 })
    {
        let (mut sender, receiver) = spmc::bounded(capacity);
        let start = Barrier::new(2);
        let (waker, wakes) = WakeCounter::new();
        let mut receive = pin!(receiver.recv());
        let outcome = thread::scope(|scope| {
            let producer = scope.spawn(|| {
                start.wait();
                sender.try_send(7).unwrap();
            });
            start.wait();
            let outcome = poll_with(receive.as_mut(), &waker);
            producer.join().unwrap();
            outcome
        });
        if outcome.is_pending() {
            assert!(wakes.count() > 0);
            assert_eq!(poll_once(receive.as_mut()), Poll::Ready(Ok(7)));
        } else {
            assert_eq!(outcome, Poll::Ready(Ok(7)));
        }
    }
}

#[test]
fn reclamation_racing_send_registration_cannot_lose_a_wakeup() {
    for capacity in [1, 3]
        .into_iter()
        .cycle()
        .take(if cfg!(miri) { 8 } else { 128 })
    {
        let (mut sender, receiver) = spmc::bounded(capacity);
        for _ in 0..capacity {
            sender.try_send(7).unwrap();
        }
        let start = Barrier::new(2);
        let (waker, wakes) = WakeCounter::new();
        let mut send = pin!(sender.send(8));
        let outcome = thread::scope(|scope| {
            let consumer = scope.spawn(|| {
                start.wait();
                assert_eq!(receiver.try_recv(), Ok(7));
            });
            start.wait();
            let outcome = poll_with(send.as_mut(), &waker);
            consumer.join().unwrap();
            outcome
        });
        if outcome.is_pending() {
            assert!(wakes.count() > 0);
            assert_eq!(poll_once(send.as_mut()), Poll::Ready(Ok(())));
        } else {
            assert_eq!(outcome, Poll::Ready(Ok(())));
        }
        for _ in 1..capacity {
            assert_eq!(receiver.try_recv(), Ok(7));
        }
        assert_eq!(receiver.try_recv(), Ok(8));
    }
}

#[derive(Debug)]
struct Payload {
    drops: Arc<AtomicUsize>,
    panic: bool,
}

impl Drop for Payload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic, "payload destructor");
    }
}

#[test]
fn receiver_close_racing_publication_releases_values_while_sender_lives() {
    for _ in 0..if cfg!(miri) { 8 } else { 128 } {
        let drops = Arc::new(AtomicUsize::new(0));
        let (mut sender, receiver) = spmc::bounded(2);
        sender
            .try_send(Payload {
                drops: drops.clone(),
                panic: false,
            })
            .unwrap();
        let start = Barrier::new(2);
        thread::scope(|scope| {
            let close = scope.spawn(|| {
                start.wait();
                drop(receiver);
            });
            start.wait();
            drop(sender.try_send(Payload {
                drops: drops.clone(),
                panic: false,
            }));
            close.join().unwrap();
        });
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert!(matches!(
            sender.try_send(Payload {
                drops: drops.clone(),
                panic: false
            }),
            Err(TrySendError::Disconnected(_))
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 3);
    }
}

#[test]
fn receiver_close_finishes_cleanup_after_a_payload_panics() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (mut sender, receiver) = spmc::bounded(3);
    // Wraps the remaining payloads, so they are dropped in two parts.
    for _ in 0..2 {
        sender
            .try_send(Payload {
                drops: drops.clone(),
                panic: false,
            })
            .unwrap();
        drop(receiver.try_recv().unwrap());
    }
    for panic in [true, false, false] {
        sender
            .try_send(Payload {
                drops: drops.clone(),
                panic,
            })
            .unwrap();
    }
    assert!(catch_unwind(AssertUnwindSafe(|| drop(receiver))).is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 5);
}

#[test]
fn zero_sized_values_preserve_capacity_across_wraps() {
    for capacity in [1, 3, 64] {
        let (mut sender, receiver) = spmc::bounded(capacity);
        for _ in 0..16 {
            for _ in 0..capacity {
                sender.try_send(()).unwrap();
            }
            assert_eq!(sender.try_send(()), Err(TrySendError::Full(())));
            for _ in 0..capacity {
                assert_eq!(receiver.try_recv(), Ok(()));
            }
            assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
        }
        drop(sender);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }
}

#[test]
fn sender_close_racing_receive_registration_cannot_lose_a_wakeup() {
    for capacity in [1, 3]
        .into_iter()
        .cycle()
        .take(if cfg!(miri) { 8 } else { 128 })
    {
        let (sender, receiver) = spmc::bounded::<usize>(capacity);
        let start = Barrier::new(2);
        let (waker, wakes) = WakeCounter::new();
        let mut receive = pin!(receiver.recv());
        let outcome = thread::scope(|scope| {
            let close = scope.spawn(|| {
                start.wait();
                drop(sender);
            });
            start.wait();
            let outcome = poll_with(receive.as_mut(), &waker);
            close.join().unwrap();
            outcome
        });
        if outcome.is_pending() {
            assert!(wakes.count() > 0);
            assert_eq!(
                poll_once(receive.as_mut()),
                Poll::Ready(Err(spmc::RecvError::Disconnected))
            );
        } else {
            assert_eq!(outcome, Poll::Ready(Err(spmc::RecvError::Disconnected)));
        }
    }
}

#[test]
fn replaced_and_cancelled_wakers_can_reenter_endpoint_bookkeeping() {
    assert_completes_without_deadlock(|| {
        for (capacity, replace) in [(1, false), (1, true), (3, false), (3, true)] {
            let (mut sender, receiver) = spmc::bounded::<usize>(capacity);
            let waker = waker_on_drop({
                let witness = receiver.clone();
                move || {
                    let clone = witness.clone();
                    assert_eq!(clone.try_recv(), Err(TryRecvError::Empty));
                }
            });
            let mut recv = Box::pin(receiver.recv());
            assert!(poll_with(recv.as_mut(), &waker).is_pending());
            drop(waker);
            if replace {
                assert!(poll_once(recv.as_mut()).is_pending());
            }
            drop(recv);

            for _ in 0..capacity {
                sender.try_send(1).unwrap();
            }
            let waker = waker_on_drop({
                let witness = receiver.clone();
                move || drop(witness.clone())
            });
            let mut send = Box::pin(sender.send(2));
            assert!(poll_with(send.as_mut(), &waker).is_pending());
            drop(waker);
            if replace {
                assert!(poll_once(send.as_mut()).is_pending());
            }
            drop(send);
            for _ in 0..capacity {
                assert_eq!(receiver.try_recv(), Ok(1));
            }
        }
    });
}
