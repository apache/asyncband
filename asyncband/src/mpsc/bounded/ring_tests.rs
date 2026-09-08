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

use std::sync::atomic::Ordering;
use std::task::Poll;

use super::Ring;

#[test]
fn receive_waits_for_the_first_claim_even_if_a_later_claim_is_published() {
    let queue = Ring::new(2);
    let mut head = 0;
    // SAFETY: The two claims fit in the initially empty ring. Both publish before teardown.
    let (first, second) = unsafe { (queue.claim().unwrap(), queue.claim().unwrap()) };
    second.publish(2);
    // SAFETY: This test owns the only consumer cursor.
    let pending = unsafe { queue.pop(&mut head) };
    first.publish(1);
    assert_eq!(pending, Poll::Pending);
    // SAFETY: No other consumer exists, and no slot will be reused by another producer.
    unsafe {
        assert_eq!(queue.pop(&mut head), Poll::Ready(Some(1)));
        assert_eq!(queue.pop(&mut head), Poll::Ready(Some(2)));
        assert_eq!(queue.pop(&mut head), Poll::Ready(None));
    }
}

#[test]
fn closed_ring_finishes_claimed_publications_and_rejects_new_claims() {
    let queue = Ring::new(2);
    let mut head = 0;
    // SAFETY: The initially empty ring has two available slots.
    let first = unsafe { queue.claim().unwrap() };
    let tail = queue.close();
    // SAFETY: The second capacity unit has not been claimed, even though close rejects it.
    let rejected = unsafe { queue.claim().is_err() };
    first.publish(String::from("claimed before close"));
    assert!(rejected);
    // SAFETY: This test owns the only consumer cursor, and the queue is closed.
    unsafe { queue.drain(&mut head, tail) };
    // SAFETY: Repeated draining must not revisit a consumed value.
    unsafe { queue.drain(&mut head, tail) };
}

#[test]
fn publication_stamps_survive_cursor_overflow_and_non_power_of_two_capacity() {
    for capacity in [1, 3, 4] {
        let queue = Ring::new(capacity);
        // Start on the final lap before usize overflow; the index and close bit are both zero.
        let mut head = usize::MAX - (2 * queue.slots.len() - 1);
        queue.tail.store(head, Ordering::Relaxed);
        for lap in 0..3 {
            for index in 0..capacity {
                // SAFETY: The previous lap was completely drained, so these claims fit.
                unsafe { queue.claim().unwrap() }.publish((lap, index));
            }
            for index in 0..capacity {
                // SAFETY: The only consumer owns this cursor; all reads finish before reuse.
                assert_eq!(
                    unsafe { queue.pop(&mut head) },
                    Poll::Ready(Some((lap, index)))
                );
            }
            // SAFETY: The only consumer owns this cursor.
            assert_eq!(unsafe { queue.pop(&mut head) }, Poll::Ready(None));
        }
    }
}
