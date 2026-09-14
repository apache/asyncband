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

// These run under Miri via `cargo x miri`, so they stay single-threaded and small. Behavior
// reachable from the public API is covered in `tests-integration/broadcast_mpmc_bounded_test.rs`.

use std::task::Waker;

use super::*;

#[test]
#[should_panic(expected = "broadcast bounded channel requires capacity > 0")]
fn bounded_panics_on_zero_capacity() {
    let _ = bounded::<()>(0);
}

#[test]
#[should_panic(expected = "broadcast channel version counter overflowed")]
fn send_panics_on_version_overflow() {
    // The receiver is dropped right away: the doctored counter would make its own drop overflow.
    let (tx, _) = bounded(1);
    tx.shared.inner.lock().log.set_tail(u64::MAX);
    let _ = tx.try_send(());
}

#[test]
fn buffer_is_preallocated_and_never_shrinks() {
    let capacity = 128;
    let (tx, mut rx) = bounded(capacity);
    let allocated = tx.shared.inner.lock().log.buffer_capacity();
    assert!(allocated >= capacity);

    // Fill to capacity, drain completely, and repeat with a much smaller cycle. An elastic backlog
    // would hand the allocation back after the small cycle; a fixed one must not.
    for i in 0..capacity {
        tx.try_send(i).unwrap();
    }
    for i in 0..capacity {
        assert_eq!(rx.try_recv(), Ok(i));
    }
    tx.try_send(0).unwrap();
    assert_eq!(rx.try_recv(), Ok(0));

    assert_eq!(tx.retained_message_count(), 0);
    assert_eq!(tx.shared.inner.lock().log.buffer_capacity(), allocated);
}

#[test]
fn capacity_reports_the_requested_value() {
    let (tx, _rx) = bounded::<i32>(3);
    assert_eq!(tx.capacity(), 3);
}

#[test]
fn a_large_reclaim_leaves_no_permit_slack() {
    // Dropping a lagging subscription frees the whole backlog in one step, far more slots than the
    // single parked producer can use. Permits beyond that producer would sit in the semaphore, and
    // the next send to block would burn each one on a publish attempt that cannot succeed.
    let capacity = 64;
    let (tx, mut fast) = bounded(capacity);
    let lagging = tx.subscribe();
    for value in 0..capacity {
        tx.try_send(value).unwrap();
    }
    for _ in 0..capacity {
        fast.try_recv().unwrap();
    }

    let mut cx = Context::from_waker(Waker::noop());
    let mut send = Box::pin(tx.send(capacity));
    assert!(send.as_mut().poll(&mut cx).is_pending());

    drop(lagging);
    assert!(send.as_mut().poll(&mut cx).is_ready());
    assert_eq!(tx.shared.tx_permits.available_permits(), 0);
}
