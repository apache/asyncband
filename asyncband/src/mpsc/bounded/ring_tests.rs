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

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::thread;

use super::Ring;
use super::TrySendError;

#[test]
fn bounded_queue_preserves_capacity_and_fifo_order() {
    let queue = Ring::new(3);
    for value in 0..3 {
        assert!(queue.try_push(value).is_ok());
    }
    assert!(matches!(queue.try_push(3), Err(TrySendError::Full(3))));
    for value in 0..3 {
        // SAFETY: This thread is the only consumer.
        assert_eq!(unsafe { queue.pop() }, Poll::Ready(Some(value)));
    }
    // SAFETY: This thread is the only consumer.
    assert_eq!(unsafe { queue.pop() }, Poll::Ready(None));

    for value in 3..12 {
        assert!(queue.try_push(value).is_ok());
        // SAFETY: This thread is the only consumer.
        assert_eq!(unsafe { queue.pop() }, Poll::Ready(Some(value)));
    }
}

#[test]
fn bounded_queue_does_not_report_empty_behind_an_unpublished_head() {
    let queue = Ring::new(2);

    // Pause a synthetic producer after reserving and initializing slot 0, before publishing
    // its stamp. Another producer can finish sending into slot 1 in the meantime.
    queue.tail.store(1, Ordering::SeqCst);
    let slot = &queue.slots[0];
    // SAFETY: advancing the tail reserved this initially empty slot for the synthetic producer.
    unsafe { (*slot.value.get()).write(1) };
    let later_send = queue.try_push(2);
    // SAFETY: This thread is the only consumer, even while a producer is unpublished.
    let receive = unsafe { queue.pop() };

    // Finish publication before asserting so even a failed assertion can safely drop the queue.
    slot.stamp.store(1, Ordering::Release);
    assert!(later_send.is_ok());
    assert_eq!(receive, Poll::Pending);
    // SAFETY: This thread is the only consumer.
    unsafe {
        assert_eq!(queue.pop(), Poll::Ready(Some(1)));
        assert_eq!(queue.pop(), Poll::Ready(Some(2)));
        assert_eq!(queue.pop(), Poll::Ready(None));
    }
}

#[test]
fn unpublished_reservations_count_toward_capacity() {
    let queue = Arc::new(Ring::new(1));
    queue.tail.store(queue.one_lap, Ordering::SeqCst);
    let (done, completed) = std::sync::mpsc::channel();
    let producer = {
        let queue = queue.clone();
        thread::spawn(move || done.send(queue.try_push(2)).unwrap())
    };
    #[cfg(not(miri))]
    let result = completed.recv_timeout(std::time::Duration::from_secs(10));
    // Miri reports a deadlock directly instead of relying on an interpretation-time deadline.
    #[cfg(miri)]
    let result = completed.recv();
    // Finish the synthetic reservation even if the other producer stalled. This lets the
    // worker and the ring's destructor finish before the failure is reported.
    let slot = &queue.slots[0];
    // SAFETY: Advancing the tail above exclusively reserved the initially empty slot.
    unsafe { (*slot.value.get()).write(1) };
    slot.stamp.store(1, Ordering::Release);
    producer.join().unwrap();
    assert!(matches!(result, Ok(Err(TrySendError::Full(2)))));
    // SAFETY: Both producers have finished and this thread is the only consumer.
    assert_eq!(unsafe { queue.pop() }, Poll::Ready(Some(1)));
}

#[test]
fn bounded_queue_coordinates_multiple_producers() {
    let queue = Arc::new(Ring::new(4));
    let producers: Vec<_> = (0..2)
        .map(|producer| {
            let queue = queue.clone();
            thread::spawn(move || {
                for offset in 0..32 {
                    let mut value = producer * 32 + offset;
                    loop {
                        match queue.try_push(value) {
                            Ok(()) => break,
                            Err(TrySendError::Full(returned)) => {
                                value = returned;
                                thread::yield_now();
                            }
                            Err(TrySendError::Disconnected(_)) => panic!("queue disconnected"),
                        }
                    }
                }
            })
        })
        .collect();

    let mut values = Vec::new();
    while values.len() < 64 {
        // SAFETY: Worker threads only push; this thread is the only consumer.
        if let Poll::Ready(Some(value)) = unsafe { queue.pop() } {
            values.push(value);
        } else {
            thread::yield_now();
        }
    }
    for producer in producers {
        producer.join().unwrap();
    }
    values.sort_unstable();
    assert_eq!(values, (0..64).collect::<Vec<_>>());
}

#[test]
fn bounded_queue_discards_wrapped_values_once_after_receiver_disconnect() {
    // This has no owning fields, so a buggy second drop remains observable as count == 2
    // instead of invalidating the tracker first.
    struct DropSpy<'a>(&'a AtomicUsize);

    impl<'a> Drop for DropSpy<'a> {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    // Declare this before `queue` so the counters outlive values held by the queue.
    let drops = [
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    ];
    let queue = Ring::new(3);

    // Positions: 0, 1, 2 (then tail wraps to 8).
    for counter in &drops[..3] {
        assert!(queue.try_push(DropSpy(counter)).is_ok());
    }

    // Free slot 0, then reuse it on the next lap at position 8.
    // SAFETY: This thread is the only consumer.
    let popped = unsafe { queue.pop() };
    assert!(matches!(popped, Poll::Ready(Some(_))));
    drop(popped);
    assert_eq!(drops[0].load(Ordering::Relaxed), 1);
    assert!(queue.try_push(DropSpy(&drops[3])).is_ok());

    // The pending range is positions 1 -> 2 -> 8 -> 9, not a contiguous integer range.
    assert_eq!(queue.head.load(Ordering::Relaxed), 1);
    assert_eq!(queue.tail.load(Ordering::Relaxed), queue.one_lap + 1);

    queue.close();
    // SAFETY: The queue is closed and this thread is the only consumer.
    unsafe { queue.drain() };

    // `discard_until` must dispose every value exactly once, including position 8.
    for (value, counter) in drops.iter().enumerate() {
        assert_eq!(
            counter.load(Ordering::Relaxed),
            1,
            "value {value} was dropped an unexpected number of times"
        );
    }

    // Queue Drop calls discard_until again; it must see head == tail and not redrop.
    drop(queue);
    for (value, counter) in drops.iter().enumerate() {
        assert_eq!(
            counter.load(Ordering::Relaxed),
            1,
            "value {value} was dropped more than once"
        );
    }
}
