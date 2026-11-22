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
// reachable from the public API is covered in `tests-integration/tests/broadcast_test/bounded.rs`.

use std::cell::Cell;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::pin;
use std::rc::Rc;
use std::task::Wake;
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
    let (tx, _) = bounded(1);
    tx.shared.inner.lock().log.set_tail(u64::MAX);
    let _ = tx.try_send(());
}

#[test]
fn overflow_drops_payload_outside_the_channel_lock() {
    for has_receiver in [false, true] {
        let (tx, _) = bounded(1);
        tx.shared.inner.lock().log.set_tail(u64::MAX);
        let _receiver = has_receiver.then(|| tx.subscribe());
        let shared = tx.shared.clone();
        common::tests::assert_overflow_drops_unlocked(
            move || shared.inner.try_lock().map(|inner| inner.log.retained()),
            |value| {
                let _ = tx.try_send(value);
            },
        );
    }
}

#[test]
fn full_channel_rejects_the_value_at_the_last_version() {
    let (tx, _) = bounded(1);
    tx.shared.inner.lock().log.set_tail(u64::MAX - 1);
    let _receiver = tx.subscribe();
    tx.try_send(1).unwrap();
    assert!(matches!(tx.try_send(2), Err(TrySendError::Full(2))));
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
        assert_eq!(rx.try_recv().as_deref(), Ok(&i));
    }
    tx.try_send(0).unwrap();
    assert_eq!(rx.try_recv().as_deref(), Ok(&0));

    assert_eq!(tx.retained_message_count(), 0);
    assert_eq!(tx.shared.inner.lock().log.buffer_capacity(), allocated);
}

#[test]
fn a_large_reclaim_leaves_no_sender_slack() {
    // Dropping a lagging subscription frees the whole backlog in one step, far more slots than the
    // single parked producer can use. Once that producer publishes, no unconsumed sender nodes or
    // wakeups remain queued.
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
    let mut send = pin!(tx.send(capacity));
    assert!(send.as_mut().poll(&mut cx).is_pending());

    drop(lagging);
    assert!(send.as_mut().poll(&mut cx).is_ready());
    assert!(tx.shared.inner.lock().send_waiters.waiters.is_empty());
}

#[test]
fn permit_send_overflow_releases_slot_and_drops_payload_outside_lock() {
    #[derive(Default)]
    struct CountingWaker(AtomicUsize);

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct InspectOnDrop(Option<Box<dyn FnOnce()>>);

    impl Drop for InspectOnDrop {
        fn drop(&mut self) {
            if let Some(inspect) = self.0.take() {
                inspect();
            }
        }
    }

    for has_receiver in [false, true] {
        let (tx, rx) = bounded(1);
        drop(rx);
        tx.shared.inner.lock().log.set_tail(u64::MAX);
        let _receiver = has_receiver.then(|| tx.subscribe());

        let permit = tx.try_reserve().unwrap();
        let wakes = Arc::new(CountingWaker::default());
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        let mut waiting = pin!(tx.reserve());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());

        let shared = tx.shared.clone();
        let wakes_in_drop = wakes.clone();
        let observed = Rc::new(Cell::new(None));
        let observed_in_drop = observed.clone();
        let overflow = catch_unwind(AssertUnwindSafe(|| {
            permit.send(InspectOnDrop(Some(Box::new(move || {
                observed_in_drop.set(shared.inner.try_lock().map(|inner| {
                    (
                        inner.log.retained(),
                        inner.send_waiters.reserved,
                        wakes_in_drop.0.load(Ordering::Relaxed),
                    )
                }));
            }))));
        }));
        assert!(overflow.is_err());
        assert_eq!(observed.get(), Some((0, 0, 1)));
        assert!(waiting.as_mut().poll(&mut cx).is_ready());
    }
}
