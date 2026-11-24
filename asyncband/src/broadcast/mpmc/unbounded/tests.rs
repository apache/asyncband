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

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::pin;
use std::sync::atomic::AtomicUsize;
use std::task::Wake;
use std::task::Waker;
use std::thread;

use super::*;
use crate::broadcast::mpmc::unbounded::segment::HEAP_CAP;
use crate::broadcast::mpmc::unbounded::segment::INLINE_CAP;
use crate::broadcast::mpmc::unbounded::segment::MsgSlot;

#[test]
#[should_panic(expected = "broadcast channel version counter overflowed")]
fn send_panics_on_version_overflow() {
    let (tx, _) = unbounded();
    tx.shared.set_tail(u64::MAX);
    tx.send(());
}

#[test]
fn overflow_drops_payload_outside_the_channel_lock() {
    for has_receiver in [false, true] {
        let (tx, _) = unbounded();
        tx.shared.set_tail(u64::MAX);
        let _receiver = has_receiver.then(|| tx.subscribe());
        let tx_inspect = tx.clone();
        common::tests::assert_overflow_drops_unlocked(
            move || {
                tx_inspect
                    .shared
                    .is_unlocked_for_test()
                    .then(|| tx_inspect.retained_message_count())
            },
            |value| tx.send(value),
        );
    }
}

#[test]
fn one_off_burst_allocation_is_returned_once_it_is_behind_us() {
    let (tx, mut rx) = unbounded();

    let burst = HEAP_CAP * 8;
    for i in 0..burst {
        tx.send(i);
    }
    assert!(tx.shared.buffer_capacity() >= burst);

    for i in 0..burst {
        assert_eq!(rx.try_recv().as_deref(), Ok(&i));
    }

    // Draining evaluates the cycle that just peaked, so the burst allocation is still held.
    assert_eq!(tx.retained_message_count(), 0);
    assert!(tx.shared.buffer_capacity() >= burst);

    // The next cycle stays small, which is what releases the memory.
    tx.send(0);
    assert_eq!(rx.try_recv().as_deref(), Ok(&0));
    assert!(tx.shared.buffer_capacity() < burst);
}

#[test]
fn repeated_bursts_keep_their_allocation() {
    let (tx, mut rx) = unbounded();
    let burst = HEAP_CAP * 4;

    for _ in 0..4 {
        for i in 0..burst {
            tx.send(i);
        }
        for i in 0..burst {
            assert_eq!(rx.try_recv().as_deref(), Ok(&i));
        }
    }

    // Every cycle peaks at the same size, so the buffer must not rebuild its allocation each time.
    assert!(tx.shared.buffer_capacity() >= burst);
}

#[test]
fn dropping_all_receivers_reclaims_heap_segments_even_across_unwind() {
    struct Bomb(bool);

    impl Drop for Bomb {
        fn drop(&mut self) {
            if self.0 {
                panic!("payload destructor unwind during half-closed drop");
            }
        }
    }

    let (tx, rx) = unbounded();
    let burst = HEAP_CAP * 4;
    for i in 0..burst {
        tx.send(Bomb(i == burst / 2));
    }
    assert!(tx.shared.buffer_capacity() >= burst);

    let unwind = catch_unwind(AssertUnwindSafe(|| drop(rx)));
    assert!(unwind.is_err());

    // Idle senders must not retain heap segments once all receivers have closed.
    assert_eq!(tx.retained_message_count(), 0);
    assert_eq!(tx.shared.buffer_capacity(), INLINE_CAP);

    let mut fresh = tx.subscribe();
    tx.send(Bomb(false));
    assert!(!fresh.try_recv().unwrap().0);
}

#[test]
fn draining_after_sender_disconnect_reclaims_heap_segments_immediately() {
    for burst in [
        INLINE_CAP,
        INLINE_CAP + HEAP_CAP,
        HEAP_CAP * 4,
        INLINE_CAP + HEAP_CAP * 4,
    ] {
        let (tx, mut rx) = unbounded();
        for i in 0..burst {
            tx.send(i);
        }
        if burst > INLINE_CAP {
            assert!(rx.shared.buffer_capacity() >= burst);
        }

        drop(tx);
        for i in 0..burst {
            assert_eq!(rx.try_recv().as_deref(), Ok(&i));
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(rx.shared.buffer_capacity(), INLINE_CAP);
    }
}

#[test]
fn layout_holds_across_payload_size_table() {
    fn check_payload<T>() {
        assert_eq!(size_of::<MsgSlot<T>>(), 2 * size_of::<usize>());
        assert_eq!(size_of::<Subscription<T>>(), 2 * size_of::<usize>() + 16);
        assert!(size_of::<RecvShared<'_, T>>() <= 2 * size_of::<usize>());
        assert!(size_of::<RecvCloned<'_, T>>() <= 2 * size_of::<usize>());
        assert!(align_of::<Shared<T>>() <= 16);
        assert!(size_of::<Shared<T>>() <= 944);
    }

    check_payload::<()>();
    check_payload::<u8>();
    check_payload::<u64>();
    check_payload::<[u8; 64]>();
    check_payload::<[u8; 96]>();
    check_payload::<[u8; 128]>();
    check_payload::<[u8; 1024]>();
}

#[test]
fn poll_recv_never_blocks_when_publisher_is_preempted_mid_slot() {
    struct CountWake(AtomicUsize);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let (tx, mut rx) = unbounded();
    let (epoch, version, live_rcvs) = tx.shared.begin_inflight_publish_for_test();

    let wake1 = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker1 = Waker::from(wake1.clone());
    let mut cx1 = Context::from_waker(&waker1);

    let wake2 = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker2 = Waker::from(wake2.clone());
    let mut cx2 = Context::from_waker(&waker2);

    let mut fut = pin!(rx.recv());
    // Must return `Pending` immediately on the same thread instead of spinning on `slot.state`.
    assert!(fut.as_mut().poll(&mut cx1).is_pending());
    // Re-polling with a replacement waker updates the registration in place without spurious wake.
    assert!(fut.as_mut().poll(&mut cx2).is_pending());
    assert_eq!(wake1.0.load(Ordering::SeqCst), 0);
    assert_eq!(wake2.0.load(Ordering::SeqCst), 0);

    tx.shared
        .finish_inflight_publish_for_test(epoch, version, live_rcvs, 42usize);
    assert_eq!(wake1.0.load(Ordering::SeqCst), 0);
    assert_eq!(wake2.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        fut.as_mut().poll(&mut cx2).map(|res| res.map(|v| *v)),
        Poll::Ready(Ok(42))
    );
}

#[test]
fn try_recv_parks_without_consuming_caller_thread_token() {
    let (tx, mut rx) = unbounded();
    let (epoch, version, live_rcvs) = tx.shared.begin_inflight_publish_for_test();
    assert!(!rx.is_empty());

    let shared_producer = tx.shared.clone();
    let publisher = thread::spawn(move || {
        while !shared_producer.has_waiters_for_test() {
            thread::yield_now();
        }
        shared_producer.finish_inflight_publish_for_test(epoch, version, live_rcvs, 77usize);
    });

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let consumer = thread::spawn(move || {
        // Pre-arm the caller thread's unpark token and verify `try_recv` leaves it intact.
        thread::current().unpark();
        assert_eq!(rx.try_recv().as_deref(), Ok(&77));
        thread::park();
        done_tx.send(()).unwrap();
        rx
    });

    #[cfg(not(miri))]
    if done_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .is_err()
    {
        consumer.thread().unpark();
        let _ = consumer.join();
        panic!("try_recv consumed the caller thread's unpark token");
    }
    #[cfg(miri)]
    done_rx.recv().unwrap();

    let rx = consumer.join().unwrap();
    publisher.join().unwrap();
    drop((tx, rx));
}
