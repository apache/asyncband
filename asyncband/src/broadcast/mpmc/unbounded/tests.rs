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
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Wake;
use std::task::Waker;
use std::thread;
#[cfg(not(miri))]
use std::time::Duration;

use super::*;
use crate::broadcast::mpmc::unbounded::queue::MsgSlot;
use crate::broadcast::mpmc::unbounded::queue::SEG_CAP;

#[test]
#[should_panic(expected = "broadcast channel version counter overflowed")]
fn send_panics_on_version_overflow() {
    let (tx, rx) = unbounded();
    drop(rx);
    tx.shared.set_tail(u64::MAX);
    tx.send(());
}

#[test]
fn concurrent_version_overflow_saturates_without_wrapping() {
    let (tx, initial_rx) = unbounded::<usize>();
    drop(initial_rx);
    tx.shared.set_tail(u64::MAX - 2);
    let mut rx = tx.subscribe();

    let workers: Vec<_> = (0..6)
        .map(|i| {
            let tx = tx.clone();
            thread::spawn(move || catch_unwind(AssertUnwindSafe(|| tx.send(i))).is_ok())
        })
        .collect();

    let succeeded = workers
        .into_iter()
        .map(|h| h.join().unwrap())
        .filter(|&ok| ok)
        .count();
    assert_eq!(succeeded, 2);
    assert!(rx.try_recv().is_ok());
    assert!(rx.try_recv().is_ok());
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn overflow_drops_payload_outside_the_channel_lock() {
    for has_receiver in [false, true] {
        let (tx, rx) = unbounded();
        drop(rx);
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
fn partial_and_full_drain_reclaims_burst_segments_without_waiting_for_idle() {
    let (tx, mut rx) = unbounded();

    let burst = SEG_CAP * 8;
    for i in 0..burst {
        tx.send(i);
    }
    let peak_capacity = tx.shared.buffer_capacity();
    assert_eq!(peak_capacity, burst);

    // Draining half the segments reclaims retired prefix segments immediately even while the
    // backlog remains non-empty.
    let first_half = SEG_CAP * 4;
    for i in 0..first_half {
        assert_eq!(rx.try_recv().as_deref(), Ok(&i));
    }
    assert_eq!(tx.retained_message_count(), burst - first_half);
    assert!(tx.shared.buffer_capacity() <= peak_capacity * 3 / 4);

    // Draining the rest leaves at most the active tail segment and the next linked segment.
    for i in first_half..burst {
        assert_eq!(rx.try_recv().as_deref(), Ok(&i));
    }
    assert_eq!(tx.retained_message_count(), 0);
    assert!(tx.shared.buffer_capacity() <= 2 * SEG_CAP);
}

#[test]
fn multi_segment_bursts_bound_retained_segments_and_release_on_disconnect() {
    let (tx, mut rx) = unbounded();

    for _ in 0..4 {
        for i in 0..(SEG_CAP * 2) {
            tx.send(i);
        }
        for i in 0..(SEG_CAP * 2) {
            assert_eq!(rx.try_recv().as_deref(), Ok(&i));
        }
        assert!(tx.shared.buffer_capacity() <= 2 * SEG_CAP);
    }

    // Disconnecting after an already-drained burst releases the tail segment immediately.
    drop(tx);
    assert!(rx.shared.buffer_capacity() <= SEG_CAP);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    assert_eq!(rx.shared.buffer_capacity(), 0);
}

#[test]
fn dropping_all_receivers_reclaims_all_segments_even_across_unwind() {
    struct Bomb(bool);

    impl Drop for Bomb {
        fn drop(&mut self) {
            if self.0 {
                panic!("payload destructor unwind during half-closed drop");
            }
        }
    }

    let (tx, rx) = unbounded();
    let burst = SEG_CAP * 4;
    for i in 0..burst {
        tx.send(Bomb(i == burst / 2));
    }
    assert!(tx.shared.buffer_capacity() >= burst);

    let unwind = catch_unwind(AssertUnwindSafe(|| drop(rx)));
    assert!(unwind.is_err());

    // Idle senders retain zero segment allocations once all receivers have closed.
    assert_eq!(tx.retained_message_count(), 0);
    assert_eq!(tx.shared.buffer_capacity(), 0);

    let mut fresh = tx.subscribe();
    for _ in 0..8 {
        tx.send(Bomb(false));
        assert!(!fresh.try_recv().unwrap().0);
    }
    drop(fresh);
    assert_eq!(tx.shared.buffer_capacity(), 0);

    let fresh = tx.subscribe();
    for _ in 0..8 {
        tx.send(Bomb(false));
    }
    drop(fresh);
    assert_eq!(tx.shared.buffer_capacity(), 0);
}

#[test]
fn draining_after_sender_disconnect_reclaims_all_segments_immediately() {
    for burst in [8, SEG_CAP, SEG_CAP + 8, SEG_CAP * 4, SEG_CAP * 4 + 8] {
        let (tx, mut rx) = unbounded();
        for i in 0..burst {
            tx.send(i);
        }
        assert!(rx.shared.buffer_capacity() >= burst);

        drop(tx);
        for i in 0..burst {
            assert_eq!(rx.try_recv().as_deref(), Ok(&i));
        }
        assert_eq!(rx.shared.buffer_capacity(), 0);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(rx.shared.buffer_capacity(), 0);
    }
}

#[test]
fn layout_holds_across_payload_size_table() {
    fn check_payload<T>() {
        assert_eq!(size_of::<MsgSlot<T>>(), 2 * size_of::<usize>());
        assert_eq!(size_of::<UnboundedSender<T>>(), 2 * size_of::<usize>());
        assert_eq!(
            size_of::<UnboundedReceiver<T>>(),
            4 * size_of::<usize>() + 16
        );
        assert_eq!(size_of::<RecvShared<'_, T>>(), size_of::<usize>());
        assert_eq!(size_of::<RecvCloned<'_, T>>(), size_of::<usize>());
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
    let (version, live_rcvs) = tx.shared.begin_inflight_publish_for_test();

    let wake1 = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker1 = Waker::from(wake1.clone());
    let mut cx1 = Context::from_waker(&waker1);

    let wake2 = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker2 = Waker::from(wake2.clone());
    let mut cx2 = Context::from_waker(&waker2);

    let mut fut = pin!(rx.recv());
    assert!(fut.as_mut().poll(&mut cx1).is_pending());
    assert!(fut.as_mut().poll(&mut cx2).is_pending());
    assert_eq!(wake1.0.load(Ordering::SeqCst), 0);
    assert_eq!(wake2.0.load(Ordering::SeqCst), 0);

    tx.shared
        .finish_inflight_publish_for_test(version, live_rcvs, 42usize);
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
    let (version, live_rcvs) = tx.shared.begin_inflight_publish_for_test();
    assert!(!rx.is_empty());

    let shared_producer = tx.shared.clone();
    let publisher = thread::spawn(move || {
        while !shared_producer.wake.has_waiters() {
            thread::yield_now();
        }
        shared_producer.finish_inflight_publish_for_test(version, live_rcvs, 77usize);
    });

    let (done_tx, done_rx) = mpsc::channel();
    let consumer = thread::spawn(move || {
        thread::current().unpark();
        assert_eq!(rx.try_recv().as_deref(), Ok(&77));
        thread::park();
        done_tx.send(()).unwrap();
        rx
    });

    #[cfg(not(miri))]
    if done_rx.recv_timeout(Duration::from_secs(5)).is_err() {
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

#[test]
fn sync_inflight_waiters_finish_before_async_waker_callbacks() {
    struct LockingWake {
        try_shared_lock: Arc<Mutex<()>>,
        try_cloned_lock: Arc<Mutex<()>>,
        woke: AtomicUsize,
    }

    impl Wake for LockingWake {
        fn wake(self: Arc<Self>) {
            drop(self.try_shared_lock.lock().unwrap());
            drop(self.try_cloned_lock.lock().unwrap());
            self.woke.fetch_add(1, Ordering::SeqCst);
        }
    }

    let (tx, mut rx_async) = unbounded();
    let mut rx_try_shared = tx.subscribe();
    let mut rx_try_cloned = tx.subscribe();
    let mut rx_probe = tx.subscribe();

    let (version, live_rcvs) = tx.shared.begin_inflight_publish_for_test();

    let try_shared_lock = Arc::new(Mutex::new(()));
    let try_cloned_lock = Arc::new(Mutex::new(()));
    let wake_state = Arc::new(LockingWake {
        try_shared_lock: try_shared_lock.clone(),
        try_cloned_lock: try_cloned_lock.clone(),
        woke: AtomicUsize::new(0),
    });
    let waker = Waker::from(wake_state.clone());
    let mut cx = Context::from_waker(&waker);

    let mut fut = rx_async.recv();
    assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());

    let shared_lock_for_thread = try_shared_lock.clone();
    let try_shared_handle = thread::spawn(move || {
        let _guard = shared_lock_for_thread.lock().unwrap();
        assert_eq!(rx_try_shared.try_recv().as_deref(), Ok(&91usize));
        rx_try_shared
    });

    let cloned_lock_for_thread = try_cloned_lock.clone();
    let try_cloned_handle = thread::spawn(move || {
        let _guard = cloned_lock_for_thread.lock().unwrap();
        assert_eq!(rx_try_cloned.try_recv_cloned(), Ok(91usize));
        rx_try_cloned
    });

    while tx.shared.wake.sync_waiter_count() < 2 {
        thread::yield_now();
    }

    tx.shared
        .finish_inflight_publish_for_test(version, live_rcvs, 91usize);

    assert_eq!(wake_state.woke.load(Ordering::SeqCst), 1);
    assert_eq!(rx_probe.try_recv().as_deref(), Ok(&91));
    let rx_try_shared = try_shared_handle.join().unwrap();
    let rx_try_cloned = try_cloned_handle.join().unwrap();
    assert_eq!(
        Pin::new(&mut fut).poll(&mut cx).map(|res| res.map(|v| *v)),
        Poll::Ready(Ok(91))
    );
    drop(fut);
    assert_eq!(tx.retained_message_count(), 0);
    drop((tx, rx_async, rx_try_shared, rx_try_cloned, rx_probe));
}
