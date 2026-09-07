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

use std::future::Future;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Wake;
use std::task::Waker;
use std::thread;
#[cfg(not(miri))]
use std::time::Duration;

use asyncband::mpsc;
use asyncband::mpsc::RecvError;
use asyncband::mpsc::TryRecvError;
use asyncband::mpsc::TrySendError;
use tests_integration::poll_once;
use tests_integration::test_runtime;
use tokio_test::assert_ok;

fn expect_ready<T>(poll: Poll<T>) -> T {
    match poll {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("future should be ready"),
    }
}

struct HoldSender<S> {
    _sender: S,
}

// This waker must own the sender so its final drop can break the tested reference cycle.
#[allow(clippy::manual_noop_waker)]
impl<S: Send + Sync> Wake for HoldSender<S> {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn bounded_receiver_drop_releases_registered_waker() {
    let (tx, mut rx) = mpsc::bounded::<()>(1);
    let holder = Arc::new(HoldSender { _sender: tx });
    let retained = Arc::downgrade(&holder);
    let waker = Waker::from(holder);
    assert!(
        Box::pin(rx.recv())
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    drop(rx);
    assert!(retained.upgrade().is_none());
}

#[test]
fn unbounded_receiver_drop_releases_registered_waker() {
    let (tx, mut rx) = mpsc::unbounded::<()>();
    let holder = Arc::new(HoldSender { _sender: tx });
    let retained = Arc::downgrade(&holder);
    let waker = Waker::from(holder);
    assert!(
        Box::pin(rx.recv())
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    drop(rx);
    assert!(retained.upgrade().is_none());
}

fn assert_completes_without_deadlock(test: impl FnOnce() + Send + 'static) {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        test();
        finished_tx.send(()).unwrap();
    });
    #[cfg(not(miri))]
    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("waker callback did not finish");
    // Miri detects deadlock itself; its interpretation time must not determine test success.
    #[cfg(miri)]
    finished_rx.recv().expect("waker callback did not finish");
    worker.join().unwrap();
}

#[test]
fn bounded_send_rechecks_capacity_freed_by_waker_clone() {
    struct ReceiveOnClone {
        receiver: Mutex<mpsc::BoundedReceiver<usize>>,
        received: AtomicBool,
    }

    unsafe fn clone(data: *const ()) -> RawWaker {
        let pointer = data.cast::<ReceiveOnClone>();
        // SAFETY: Each raw waker owns one Arc reference, and this callback borrows that reference.
        let state = unsafe { &*pointer };
        if !state.received.swap(true, Ordering::Relaxed) {
            assert_eq!(state.receiver.lock().unwrap().try_recv(), Ok(1));
        }
        // SAFETY: The live reference owned by the input waker keeps the allocation alive.
        unsafe { Arc::increment_strong_count(pointer) };
        RawWaker::new(data, &VTABLE)
    }
    unsafe fn release(data: *const ()) {
        // SAFETY: Consumes exactly the Arc reference owned by this waker.
        drop(unsafe { Arc::from_raw(data.cast::<ReceiveOnClone>()) });
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, release, |_| {}, release);

    assert_completes_without_deadlock(|| {
        let (tx, rx) = mpsc::bounded(1);
        tx.try_send(1).unwrap();
        let state = Arc::new(ReceiveOnClone {
            receiver: Mutex::new(rx),
            received: AtomicBool::new(false),
        });
        let data = Arc::into_raw(state.clone()).cast();
        // SAFETY: The vtable owns one Arc per waker; the callback state is Send + Sync.
        let waker = unsafe { Waker::from_raw(RawWaker::new(data, &VTABLE)) };
        assert_eq!(
            Box::pin(tx.send(2))
                .as_mut()
                .poll(&mut Context::from_waker(&waker)),
            Poll::Ready(Ok(()))
        );
        assert_eq!(state.receiver.lock().unwrap().try_recv(), Ok(2));
    });
}

#[test]
fn bounded_receive_racing_with_send_registration_cannot_lose_wakeup() {
    struct Notified(AtomicBool);
    impl Wake for Notified {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    for _ in 0..128 {
        let (tx, mut rx) = mpsc::bounded(1);
        tx.try_send(1).unwrap();
        let start = Barrier::new(2);
        let notified = Arc::new(Notified(AtomicBool::new(false)));
        let waker = Waker::from(notified.clone());
        let mut send = Box::pin(tx.send(2));
        let poll = thread::scope(|scope| {
            let receive = scope.spawn(|| {
                start.wait();
                assert_eq!(rx.try_recv(), Ok(1));
            });
            start.wait();
            let poll = send.as_mut().poll(&mut Context::from_waker(&waker));
            receive.join().unwrap();
            poll
        });
        if poll.is_pending() {
            assert!(notified.0.load(Ordering::Relaxed));
            assert_eq!(poll_once(send.as_mut()), Poll::Ready(Ok(())));
        } else {
            assert_eq!(poll, Poll::Ready(Ok(())));
        }
        assert_eq!(rx.try_recv(), Ok(2));
    }
}

#[test]
fn unbounded_disconnect_drops_partial_and_queued_batches_outside_lock() {
    struct Value(Option<Arc<dyn Fn() + Send + Sync>>);
    impl Drop for Value {
        fn drop(&mut self) {
            if let Some(callback) = &self.0 {
                callback();
            }
        }
    }

    assert_completes_without_deadlock(|| {
        let (tx, mut rx) = mpsc::unbounded();
        let drops = Arc::new(AtomicUsize::new(0));
        let callback: Arc<dyn Fn() + Send + Sync> = {
            let tx = tx.clone();
            let drops = drops.clone();
            Arc::new(move || {
                // This exercises both a live receiver and disconnection. The marker has no
                // callback, so destroying an unsuccessful send cannot recursively send again.
                let _ = tx.send(Value(None));
                drops.fetch_add(1, Ordering::Relaxed);
            })
        };
        for _ in 0..8192 {
            assert!(tx.send(Value(Some(callback.clone()))).is_ok());
        }
        for _ in 0..17 {
            drop(rx.try_recv().unwrap());
        }
        drop(rx);
        assert_eq!(drops.load(Ordering::Relaxed), 8192);
    });
}

#[test]
fn unbounded_wake_callback_can_send() {
    struct SendOnWake(mpsc::UnboundedSender<usize>);

    impl Wake for SendOnWake {
        fn wake(self: Arc<Self>) {
            self.0.send(2).unwrap();
        }
    }

    assert_completes_without_deadlock(|| {
        let (tx, mut rx) = mpsc::unbounded();
        let waker = Waker::from(Arc::new(SendOnWake(tx.clone())));
        assert!(
            Box::pin(rx.recv())
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        tx.send(1).unwrap();
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
    });
}

#[test]
fn unbounded_replaced_and_disconnected_wakers_can_send() {
    struct SendOnDrop {
        sender: mpsc::UnboundedSender<usize>,
        disconnected: bool,
        drops: Arc<AtomicUsize>,
    }

    // The final waker drop must run a callback, even though waking itself does nothing.
    #[allow(clippy::manual_noop_waker)]
    impl Wake for SendOnDrop {
        fn wake(self: Arc<Self>) {}
    }

    impl Drop for SendOnDrop {
        fn drop(&mut self) {
            assert_eq!(self.sender.send(7).is_err(), self.disconnected);
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    assert_completes_without_deadlock(|| {
        for disconnected in [false, true] {
            let (tx, mut rx) = mpsc::unbounded();
            let drops = Arc::new(AtomicUsize::new(0));
            let waker = Waker::from(Arc::new(SendOnDrop {
                sender: tx,
                disconnected,
                drops: drops.clone(),
            }));
            assert!(
                Box::pin(rx.recv())
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            drop(waker);
            if disconnected {
                drop(rx);
            } else {
                // Replacing the waker can enqueue a message during this poll. Either immediate
                // completion or a notified Pending is valid, but the message must not be lost.
                let poll = poll_once(Box::pin(rx.recv()).as_mut());
                if poll.is_pending() {
                    assert_eq!(rx.try_recv(), Ok(7));
                } else {
                    assert_eq!(poll, Poll::Ready(Ok(7)));
                }
                assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
            }
            assert_eq!(drops.load(Ordering::Relaxed), 1);
        }
    });
}

#[test]
fn unbounded_waker_clone_rechecks_messages_sent_during_registration() {
    unsafe fn clone_sender(data: *const ()) -> RawWaker {
        let sender = data.cast::<mpsc::UnboundedSender<usize>>();
        // SAFETY: Each raw waker owns an Arc to this sender; cloning borrows the live sender and
        // then adds the strong reference owned by the returned waker.
        unsafe {
            (*sender).send(7).unwrap();
            Arc::increment_strong_count(sender);
        }
        RawWaker::new(data, &VTABLE)
    }

    unsafe fn drop_sender(data: *const ()) {
        // SAFETY: Consumes exactly the Arc reference owned by this raw waker.
        drop(unsafe { Arc::from_raw(data.cast::<mpsc::UnboundedSender<usize>>()) });
    }

    static VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_sender, drop_sender, |_| {}, drop_sender);

    assert_completes_without_deadlock(|| {
        let (tx, mut rx) = mpsc::unbounded::<usize>();
        let data = Arc::into_raw(Arc::new(tx)).cast();
        // SAFETY: The vtable manages one Arc reference per waker and the sender is Send + Sync.
        let waker = unsafe { Waker::from_raw(RawWaker::new(data, &VTABLE)) };
        assert_eq!(
            Box::pin(rx.recv())
                .as_mut()
                .poll(&mut Context::from_waker(&waker)),
            Poll::Ready(Ok(7))
        );
    });
}

#[test]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
fn unbounded_collects_from_multiple_producers() {
    let (tx, mut rx) = mpsc::unbounded();

    test_runtime().block_on(async move {
        for i in 0..8 {
            let tx = tx.clone();
            tokio::spawn(async move {
                tx.send(i).unwrap();
            });
        }
        drop(tx);

        let mut sum = 0;
        while let Ok(i) = rx.recv().await {
            sum += i;
        }
        assert_eq!(sum, 28);
    });
}

#[tokio::test]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn select_streams() {
    let (tx1, mut rx1) = mpsc::unbounded::<i32>();
    let (tx2, mut rx2) = mpsc::unbounded::<i32>();
    let (tx3, mut rx3) = mpsc::bounded(1);
    let (tx4, mut rx4) = mpsc::bounded(1);

    tokio::spawn(async move {
        assert_ok!(tx2.send(1));
        tokio::task::yield_now().await;

        assert_ok!(tx1.send(2));
        tokio::task::yield_now().await;

        assert_ok!(tx2.send(3));
        tokio::task::yield_now().await;

        assert_ok!(tx3.send(4).await);
        tokio::task::yield_now().await;

        assert_ok!(tx4.send(5).await);
        tokio::task::yield_now().await;

        assert_ok!(tx3.send(6).await);
        tokio::task::yield_now().await;

        drop((tx1, tx2));
    });

    let mut rem = true;
    let mut msgs = vec![];
    let mut rx1_disconnected = false;
    let mut rx2_disconnected = false;
    let mut rx3_disconnected = false;
    let mut rx4_disconnected = false;

    while rem {
        rem = !(rx1_disconnected && rx2_disconnected && rx3_disconnected && rx4_disconnected);

        tokio::select! {
            result = rx1.recv(), if !rx1_disconnected => {
                match result {
                    Ok(x) => msgs.push(x),
                    Err(RecvError::Disconnected) => rx1_disconnected = true,
                }
            }
            result = rx2.recv(), if !rx2_disconnected => {
                match result {
                    Ok(y) => msgs.push(y),
                    Err(RecvError::Disconnected) => rx2_disconnected = true,
                }
            }
            result = rx3.recv(), if !rx3_disconnected => {
                match result {
                    Ok(z) => msgs.push(z),
                    Err(RecvError::Disconnected) => rx3_disconnected = true,
                }
            }
            result = rx4.recv(), if !rx4_disconnected => {
                match result {
                    Ok(w) => msgs.push(w),
                    Err(RecvError::Disconnected) => rx4_disconnected = true,
                }
            }
            else => {
                rx1_disconnected = true;
                rx2_disconnected = true;
                rx3_disconnected = true;
                rx4_disconnected = true;
            }
        }
    }

    msgs.sort_unstable();
    assert_eq!(&msgs[..], &[1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn send_recv_unbounded() {
    let (tx, mut rx) = mpsc::unbounded::<i32>();

    // Using `try_send`
    assert_ok!(tx.send(1));
    assert_ok!(tx.send(2));

    assert_eq!(rx.recv().await, Ok(1));
    assert_eq!(rx.recv().await, Ok(2));

    drop(tx);

    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[tokio::test]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn async_send_recv_unbounded() {
    let (tx, mut rx) = mpsc::unbounded();

    tokio::spawn(async move {
        assert_ok!(tx.send(1));
        assert_ok!(tx.send(2));
    });

    assert_eq!(Ok(1), rx.recv().await);
    assert_eq!(Ok(2), rx.recv().await);
    assert_eq!(Err(RecvError::Disconnected), rx.recv().await);
}

#[test]
fn unbounded_try_recv_preserves_order_and_reports_state() {
    let (tx, mut rx) = mpsc::unbounded();

    for i in 0..4 {
        tx.send(i).unwrap();
    }

    for i in 0..4 {
        assert_eq!(rx.try_recv(), Ok(i));
    }
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    drop(tx);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn cancelled_receive_does_not_consume_a_later_message() {
    let (unbounded_tx, mut unbounded_rx) = mpsc::unbounded();
    {
        let mut receive = Box::pin(unbounded_rx.recv());
        assert!(poll_once(receive.as_mut()).is_pending());
    }
    unbounded_tx.send(1).unwrap();
    assert_eq!(unbounded_rx.try_recv(), Ok(1));

    let (bounded_tx, mut bounded_rx) = mpsc::bounded(1);
    {
        let mut receive = Box::pin(bounded_rx.recv());
        assert!(poll_once(receive.as_mut()).is_pending());
    }
    bounded_tx.try_send(2).unwrap();
    assert_eq!(bounded_rx.try_recv(), Ok(2));
}

#[test]
fn buffered_messages_are_drained_before_disconnection() {
    let (unbounded_tx, mut unbounded_rx) = mpsc::unbounded();
    unbounded_tx.send(1).unwrap();
    unbounded_tx.send(2).unwrap();
    drop(unbounded_tx);
    assert_eq!(unbounded_rx.try_recv(), Ok(1));
    assert_eq!(unbounded_rx.try_recv(), Ok(2));
    assert_eq!(unbounded_rx.try_recv(), Err(TryRecvError::Disconnected));

    let (bounded_tx, mut bounded_rx) = mpsc::bounded(2);
    bounded_tx.try_send(3).unwrap();
    bounded_tx.try_send(4).unwrap();
    drop(bounded_tx);
    assert_eq!(bounded_rx.try_recv(), Ok(3));
    assert_eq!(bounded_rx.try_recv(), Ok(4));
    assert_eq!(bounded_rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[tokio::test]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn send_recv_bounded() {
    let (tx, mut rx) = mpsc::bounded(1);

    tx.send(1).await.unwrap();
    assert_eq!(rx.recv().await, Ok(1));

    drop(tx);
    assert_eq!(rx.recv().await, Err(RecvError::Disconnected));
}

#[tokio::test]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn async_send_recv_bounded() {
    let (tx, mut rx) = mpsc::bounded(1);

    tx.send(1).await.unwrap();
    // This will block until the receiver is ready to receive.
    tokio::spawn(async move {
        tx.send(2).await.unwrap();
    });

    assert_eq!(Ok(1), rx.recv().await);
    assert_eq!(Ok(2), rx.recv().await);
    assert_eq!(Err(RecvError::Disconnected), rx.recv().await);
}

#[test]
fn bounded_try_send_respects_capacity_and_order() {
    for capacity in [1, 4, 16] {
        let (tx, mut rx) = mpsc::bounded(capacity);

        for i in 0..capacity {
            tx.try_send(i).unwrap();
        }

        assert_eq!(tx.try_send(capacity), Err(TrySendError::Full(capacity)));

        for i in 0..capacity {
            assert_eq!(rx.try_recv(), Ok(i));
        }

        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }
}

#[test]
fn bounded_try_recv_does_not_report_empty_after_completed_sends() {
    const PRODUCERS: usize = 4;
    const MESSAGES_PER_PRODUCER: usize = if cfg!(miri) { 64 } else { 16_384 };
    let (tx, mut rx) = mpsc::bounded(64);
    let completed = AtomicUsize::new(0);
    let mut premature_empty = 0;

    thread::scope(|scope| {
        for producer in 0..PRODUCERS {
            let tx = tx.clone();
            let completed = &completed;
            scope.spawn(move || {
                for sequence in 0..MESSAGES_PER_PRODUCER {
                    loop {
                        match tx.try_send((producer, sequence)) {
                            Ok(()) => break,
                            Err(TrySendError::Full(_)) => thread::yield_now(),
                            Err(TrySendError::Disconnected(_)) => panic!("receiver is still alive"),
                        }
                    }
                    completed.fetch_add(1, Ordering::Release);
                }
            });
        }

        let mut received = 0;
        while received < PRODUCERS * MESSAGES_PER_PRODUCER {
            // Once more sends have completed than messages received, Empty cannot be correct.
            let has_completed_send = completed.load(Ordering::Acquire) > received;
            match rx.try_recv() {
                Ok(_) => received += 1,
                Err(TryRecvError::Empty) => {
                    premature_empty += usize::from(has_completed_send);
                    thread::yield_now();
                }
                Err(TryRecvError::Disconnected) => panic!("original sender is still alive"),
            }
        }
    });

    // Drain and join before asserting so a failure cannot strand a producer on a full channel.
    assert_eq!(premature_empty, 0);
}

#[tokio::test]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn try_send_after_disconnection_bounded() {
    let (tx, rx) = mpsc::bounded(1);

    tx.try_send(1).unwrap();
    drop(rx);

    assert_eq!(tx.try_send(3), Err(TrySendError::Disconnected(3)));
}

#[tokio::test]
#[cfg_attr(miri, ignore = "requires an OS-backed Tokio runtime")]
async fn send_after_disconnection_bounded() {
    let (tx, mut rx) = mpsc::bounded(1);

    tx.send(1).await.unwrap();
    assert_eq!(rx.recv().await, Ok(1));

    drop(rx);
    let error = tx.send(2).await.unwrap_err();
    assert_eq!(error.into_inner(), 2);
}

#[test]
fn bounded_wakes_blocked_senders_one_at_a_time() {
    let (tx, mut rx) = mpsc::bounded(1);
    tx.try_send(0).unwrap();

    let first_tx = tx.clone();
    let second_tx = tx.clone();
    let mut first = Box::pin(first_tx.send(1));
    let mut second = Box::pin(second_tx.send(2));

    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());

    assert_eq!(rx.try_recv(), Ok(0));
    assert_eq!(expect_ready(poll_once(first.as_mut())), Ok(()));
    assert!(poll_once(second.as_mut()).is_pending());

    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(expect_ready(poll_once(second.as_mut())), Ok(()));
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn bounded_cancelled_notified_sender_passes_slot_to_next_sender() {
    let (tx, mut rx) = mpsc::bounded(1);
    tx.try_send(0).unwrap();

    let first_tx = tx.clone();
    let second_tx = tx.clone();
    let mut first = Box::pin(first_tx.send(1));
    let mut second = Box::pin(second_tx.send(2));

    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());

    assert_eq!(rx.try_recv(), Ok(0));
    drop(first);

    assert_eq!(expect_ready(poll_once(second.as_mut())), Ok(()));
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn bounded_receiver_drop_returns_values_to_all_blocked_senders() {
    let (tx, rx) = mpsc::bounded(1);
    tx.try_send(0).unwrap();

    let first_tx = tx.clone();
    let second_tx = tx.clone();
    let mut first = Box::pin(first_tx.send(1));
    let mut second = Box::pin(second_tx.send(2));

    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());

    drop(rx);

    let first_error = expect_ready(poll_once(first.as_mut())).unwrap_err();
    let second_error = expect_ready(poll_once(second.as_mut())).unwrap_err();
    assert_eq!(first_error.into_inner(), 1);
    assert_eq!(second_error.into_inner(), 2);
}

#[cfg(panic = "unwind")]
#[test]
fn bounded_disconnect_finishes_cleanup_when_a_callback_panics() {
    struct Value {
        id: usize,
        drops: Arc<[AtomicUsize; 4]>,
        panic_on_drop: bool,
        _sender: Option<mpsc::BoundedSender<Value>>,
    }
    impl Drop for Value {
        fn drop(&mut self) {
            self.drops[self.id].fetch_add(1, Ordering::Relaxed);
            assert!(!self.panic_on_drop, "payload destructor panicked");
        }
    }
    struct Notify {
        woken: AtomicBool,
        panic_on_wake: bool,
    }
    impl Wake for Notify {
        fn wake(self: Arc<Self>) {
            self.woken.store(true, Ordering::Relaxed);
            assert!(!self.panic_on_wake, "wake callback panicked");
        }
    }

    for panic_on_wake in [false, true] {
        let (tx, rx) = mpsc::bounded(3);
        let drops = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
        for id in 0..3 {
            assert!(
                tx.try_send(Value {
                    id,
                    drops: drops.clone(),
                    panic_on_drop: id == 0 && !panic_on_wake,
                    _sender: Some(tx.clone()),
                })
                .is_ok()
            );
        }
        let notify = Arc::new(Notify {
            woken: AtomicBool::new(false),
            panic_on_wake,
        });
        let waker = Waker::from(notify.clone());
        let mut send = Box::pin(tx.send(Value {
            id: 3,
            drops: drops.clone(),
            panic_on_drop: false,
            _sender: None,
        }));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(rx))).is_err());
        assert!(notify.woken.load(Ordering::Relaxed));
        for count in &drops[..3] {
            assert_eq!(count.load(Ordering::Relaxed), 1);
        }
        assert_eq!(drops[3].load(Ordering::Relaxed), 0);
        let error = match expect_ready(poll_once(send.as_mut())) {
            Err(error) => error,
            Ok(()) => panic!("the receiver is disconnected"),
        };
        assert_eq!(error.into_inner().id, 3);
        assert_eq!(drops[3].load(Ordering::Relaxed), 1);
    }
}
