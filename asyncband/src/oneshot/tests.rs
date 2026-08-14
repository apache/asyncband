// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;
use std::future::IntoFuture;
use std::mem;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use self::support::DropProbe;
use self::support::WakerProbe;
use self::support::spawn_named;
use self::support::spin_until;
use crate::oneshot;
use crate::oneshot::TryRecvError;

#[test]
fn send_before_await() {
    let (sender, receiver) = oneshot::channel();
    assert!(!receiver.has_message());
    assert!(sender.send(19i128).is_ok());
    assert!(receiver.has_message());
    assert_eq!(pollster::block_on(receiver), Ok(19i128));
}

#[test]
fn await_with_dropped_sender() {
    let (sender, receiver) = oneshot::channel::<u128>();
    assert!(!receiver.is_closed());
    drop(sender);
    assert!(receiver.is_closed());
    assert_eq!(
        pollster::block_on(receiver),
        Err(oneshot::RecvError::Disconnected)
    );
}

#[test]
fn try_recv_success_then_disconnected() {
    let (tx, rx) = oneshot::channel::<i32>();
    tx.send(10).unwrap();

    assert_eq!(rx.try_recv(), Ok(10));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    assert!(rx.is_closed());
    assert!(!rx.has_message());
    assert_eq!(
        pollster::block_on(rx.into_future()),
        Err(oneshot::RecvError::Disconnected)
    );
}

#[test]
fn try_recv_distinguishes_empty_from_disconnected() {
    let (tx, rx) = oneshot::channel::<()>();
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    drop(tx);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn send_error_preserves_message_until_consumed() {
    let (sender, receiver) = oneshot::channel();
    let (message, message_drop_count) = DropProbe::new(17u128);

    drop(receiver);
    assert!(sender.is_closed());

    let error = sender.send(message).unwrap_err();
    assert_eq!(message_drop_count.load(Ordering::Relaxed), 0);
    assert_eq!(*error.as_inner().value(), 17);

    let message = error.into_inner();
    assert_eq!(message_drop_count.load(Ordering::Relaxed), 0);
    drop(message);
    assert_eq!(message_drop_count.load(Ordering::Relaxed), 1);
}

#[test]
fn dropping_send_error_drops_message() {
    let (sender, receiver) = oneshot::channel();
    let (message, message_drop_count) = DropProbe::new(());

    drop(receiver);
    drop(sender.send(message).unwrap_err());

    assert_eq!(message_drop_count.load(Ordering::Relaxed), 1);
}

#[test]
fn dropping_receiver_after_send_drops_message() {
    let (sender, receiver) = oneshot::channel();
    let (message, message_drop_count) = DropProbe::new(());

    sender.send(message).unwrap();
    assert_eq!(message_drop_count.load(Ordering::Relaxed), 0);
    drop(receiver);

    assert_eq!(message_drop_count.load(Ordering::Relaxed), 1);
}

#[test]
fn dropping_unpolled_recv_closes_channel() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let receiver = receiver.into_future();

    drop(receiver);

    assert!(sender.is_closed());
    assert_eq!(sender.send(17).unwrap_err().into_inner(), 17);
}

#[test]
fn dropping_recv_after_send_drops_message() {
    let (sender, receiver) = oneshot::channel();
    let (message, message_drop_count) = DropProbe::new(());
    let receiver = receiver.into_future();

    sender.send(message).unwrap();
    assert_eq!(message_drop_count.load(Ordering::Relaxed), 0);
    drop(receiver);

    assert_eq!(message_drop_count.load(Ordering::Relaxed), 1);
}

#[test]
fn poll_then_send() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let mut receiver = receiver.into_future();

    let (waker, waker_probe) = WakerProbe::new();
    let mut context = Context::from_waker(&waker);

    assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Pending);
    assert_eq!(WakerProbe::live_waker_count(&waker_probe), 2);
    assert_eq!(waker_probe.wake_count(), 0);

    sender.send(1234).unwrap();
    assert_eq!(WakerProbe::live_waker_count(&waker_probe), 1);
    assert_eq!(waker_probe.wake_count(), 1);

    assert_eq!(
        Pin::new(&mut receiver).poll(&mut context),
        Poll::Ready(Ok(1234))
    );
    assert_eq!(WakerProbe::live_waker_count(&waker_probe), 1);
    assert_eq!(waker_probe.wake_count(), 1);
}

#[test]
fn poll_returns_while_sender_owns_waker() {
    let (sender, receiver) = oneshot::channel();
    let channel_ptr = sender.channel_ptr();
    mem::forget(sender);
    let mut receiver = receiver.into_future();

    let (stored_waker, stored_probe) = WakerProbe::new();
    let mut stored_context = Context::from_waker(&stored_waker);
    assert_eq!(
        Pin::new(&mut receiver).poll(&mut stored_context),
        Poll::Pending
    );

    let channel = unsafe { channel_ptr.as_ref() };
    unsafe { channel.write_message(1234u128) };
    // Pause the synthetic sender in AWAKING immediately after it takes the stored waker.
    assert_eq!(
        channel.state.fetch_add(1, Ordering::Release),
        super::RECEIVING
    );

    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    let poll_thread = spawn_named("receiver", move || {
        let (current_waker, current_probe) = WakerProbe::new();
        let mut current_context = Context::from_waker(&current_waker);
        started_tx.send(()).unwrap();
        let result = Pin::new(&mut receiver).poll(&mut current_context);
        result_tx.send((receiver, current_probe, result)).unwrap();
    });

    started_rx.recv().unwrap();
    let result = result_rx.recv_timeout(Duration::from_secs(5));
    let returned_before_publish = result.is_ok();

    // SAFETY: fetch_add observed RECEIVING and changed it to AWAKING after the message and waker
    // were initialized.
    let (sender_waker, receiver_owns_allocation) =
        unsafe { channel.finish_sender_awakening(super::MESSAGE) };
    assert!(receiver_owns_allocation);
    sender_waker.wake();
    assert_eq!(stored_probe.wake_count(), 1);

    let (mut receiver, current_probe, result) = match result {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("receiver remained blocked after the sender published MESSAGE"),
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("receiver thread exited unexpectedly"),
    };
    poll_thread.join().unwrap();
    assert!(
        returned_before_publish,
        "poll blocked while the sender owned the waker"
    );
    assert_eq!(result, Poll::Pending);
    assert_eq!(current_probe.wake_count(), 1);

    let (current_waker, _) = WakerProbe::new();
    let mut current_context = Context::from_waker(&current_waker);
    assert_eq!(
        Pin::new(&mut receiver).poll(&mut current_context),
        Poll::Ready(Ok(1234))
    );
}

#[test]
fn drop_transfers_cleanup_while_sender_owns_waker() {
    let (sender, receiver) = oneshot::channel();
    let channel_ptr = sender.channel_ptr();
    mem::forget(sender);
    let mut receiver = receiver.into_future();
    let (message, message_drop_count) = DropProbe::new(1234u128);

    let (stored_waker, stored_probe) = WakerProbe::new();
    let mut context = Context::from_waker(&stored_waker);
    assert!(matches!(
        Pin::new(&mut receiver).poll(&mut context),
        Poll::Pending
    ));

    let channel = unsafe { channel_ptr.as_ref() };
    unsafe { channel.write_message(message) };
    // Pause the synthetic sender in AWAKING immediately after it takes the stored waker.
    assert_eq!(
        channel.state.fetch_add(1, Ordering::Release),
        super::RECEIVING
    );

    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let drop_thread = spawn_named("receiver", move || {
        started_tx.send(()).unwrap();
        drop(receiver);
        done_tx.send(()).unwrap();
    });

    started_rx.recv().unwrap();
    let result = done_rx.recv_timeout(Duration::from_secs(5));
    let returned_before_publish = result.is_ok();

    // SAFETY: fetch_add observed RECEIVING and changed it to AWAKING after the message and waker
    // were initialized.
    let (sender_waker, receiver_owns_allocation) =
        unsafe { channel.finish_sender_awakening(super::MESSAGE) };
    if receiver_owns_allocation {
        sender_waker.wake();
    } else {
        // SAFETY: Receiver cancellation transferred allocation cleanup to the synthetic sender, so
        // the original pointer provenance may be reclaimed as a Box. The message is initialized and
        // the waker was moved out above.
        unsafe { super::drop_message_and_deallocate_channel(channel_ptr) };
        drop(sender_waker);
    }

    match result {
        Ok(()) => {}
        Err(mpsc::RecvTimeoutError::Timeout) => done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("receiver remained blocked after the sender published MESSAGE"),
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("receiver thread exited unexpectedly"),
    }
    drop_thread.join().unwrap();
    assert!(
        returned_before_publish,
        "drop blocked while the sender owned the waker"
    );
    assert!(!receiver_owns_allocation);
    assert_eq!(message_drop_count.load(Ordering::Relaxed), 1);
    assert_eq!(WakerProbe::live_waker_count(&stored_probe), 1);
}

#[test]
fn poll_then_drop_sender() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let mut receiver = receiver.into_future();

    let (waker, waker_probe) = WakerProbe::new();
    let mut context = Context::from_waker(&waker);

    assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Pending);
    assert_eq!(WakerProbe::live_waker_count(&waker_probe), 2);
    assert_eq!(waker_probe.wake_count(), 0);

    drop(sender);
    assert_eq!(WakerProbe::live_waker_count(&waker_probe), 1);
    assert_eq!(waker_probe.wake_count(), 1);

    assert_eq!(
        Pin::new(&mut receiver).poll(&mut context),
        Poll::Ready(Err(oneshot::RecvError::Disconnected))
    );
    assert_eq!(WakerProbe::live_waker_count(&waker_probe), 1);
    assert_eq!(waker_probe.wake_count(), 1);
}

#[test]
fn poll_with_different_wakers() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let mut receiver = receiver.into_future();

    let (waker1, waker_probe1) = WakerProbe::new();
    let mut context1 = Context::from_waker(&waker1);

    assert_eq!(Pin::new(&mut receiver).poll(&mut context1), Poll::Pending);
    assert_eq!(WakerProbe::live_waker_count(&waker_probe1), 2);
    assert_eq!(waker_probe1.wake_count(), 0);

    let (waker2, waker_probe2) = WakerProbe::new();
    let mut context2 = Context::from_waker(&waker2);

    assert_eq!(Pin::new(&mut receiver).poll(&mut context2), Poll::Pending);
    assert_eq!(WakerProbe::live_waker_count(&waker_probe1), 1);
    assert_eq!(waker_probe1.wake_count(), 0);

    assert_eq!(WakerProbe::live_waker_count(&waker_probe2), 2);
    assert_eq!(waker_probe2.wake_count(), 0);

    sender.send(1234).unwrap();
    assert_eq!(WakerProbe::live_waker_count(&waker_probe1), 1);
    assert_eq!(waker_probe1.wake_count(), 0);

    assert_eq!(WakerProbe::live_waker_count(&waker_probe2), 1);
    assert_eq!(waker_probe2.wake_count(), 1);
}

#[test]
fn poll_with_different_wakers_across_threads() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let mut receiver = receiver.into_future();

    let (waker1, waker_probe1) = WakerProbe::new();
    let mut context1 = Context::from_waker(&waker1);

    assert_eq!(Pin::new(&mut receiver).poll(&mut context1), Poll::Pending);
    assert_eq!(WakerProbe::live_waker_count(&waker_probe1), 2);
    assert_eq!(waker_probe1.wake_count(), 0);

    let receiver_thread = spawn_named("receiver", move || {
        let (waker2, waker_probe2) = WakerProbe::new();
        let mut context2 = Context::from_waker(&waker2);

        assert_eq!(Pin::new(&mut receiver).poll(&mut context2), Poll::Pending);
        assert_eq!(WakerProbe::live_waker_count(&waker_probe2), 2);
        assert_eq!(waker_probe2.wake_count(), 0);

        drop(receiver);
        assert_eq!(WakerProbe::live_waker_count(&waker_probe2), 1);
    });

    receiver_thread.join().unwrap();
    assert_eq!(WakerProbe::live_waker_count(&waker_probe1), 1);
    assert!(sender.is_closed());
}

#[test]
fn drop_pending_receiver_closes_channel_and_drops_waker() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let mut receiver = receiver.into_future();

    let (waker, waker_probe) = WakerProbe::new();
    let mut context = Context::from_waker(&waker);

    assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Pending);
    assert_eq!(WakerProbe::live_waker_count(&waker_probe), 2);
    assert_eq!(waker_probe.wake_count(), 0);

    drop(receiver);
    assert_eq!(WakerProbe::live_waker_count(&waker_probe), 1);
    assert_eq!(waker_probe.wake_count(), 0);
    assert!(sender.is_closed());

    let error = sender.send(1234).unwrap_err();
    assert_eq!(*error.as_inner(), 1234);
}

#[test]
fn poll_then_drop_receiver_during_send() {
    let (sender, receiver) = oneshot::channel();
    let (message, message_drop_count) = DropProbe::new(1234u128);
    let mut receiver = receiver.into_future();

    let (waker, _waker_probe) = WakerProbe::new();
    let mut context = Context::from_waker(&waker);

    assert!(matches!(
        Pin::new(&mut receiver).poll(&mut context),
        Poll::Pending
    ));

    let sender_thread = spawn_named("sender", move || sender.send(message));

    drop(receiver);

    // Whether send or receiver drop wins, exactly one side owns and drops the message.
    drop(sender_thread.join().unwrap());
    assert_eq!(message_drop_count.load(Ordering::Relaxed), 1);
}

#[test]
fn concurrent_send_and_try_recv_to_completion() {
    let (sender, receiver) = oneshot::channel::<i32>();

    let receiver_thread = spawn_named("receiver", move || {
        spin_until("message from sender", || match receiver.try_recv() {
            Ok(999) => true,
            Ok(value) => panic!("unexpected value: {value}"),
            Err(TryRecvError::Empty) => false,
            Err(TryRecvError::Disconnected) => panic!("unexpected disconnect"),
        });
    });

    let sender_thread = spawn_named("sender", move || {
        sender.send(999).unwrap();
    });

    receiver_thread.join().unwrap();
    sender_thread.join().unwrap();
}

#[test]
fn concurrent_drop_sender_and_try_recv_to_completion() {
    let (sender, receiver) = oneshot::channel::<i32>();

    let receiver_thread = spawn_named("receiver", move || {
        spin_until("sender disconnect", || match receiver.try_recv() {
            Ok(value) => panic!("unexpected value: {value}"),
            Err(TryRecvError::Empty) => false,
            Err(TryRecvError::Disconnected) => true,
        });
    });

    let sender_thread = spawn_named("sender", move || {
        drop(sender);
    });

    receiver_thread.join().unwrap();
    sender_thread.join().unwrap();
}

#[test]
fn concurrent_send_and_poll_to_completion() {
    let (sender, receiver) = oneshot::channel::<i32>();

    let receiver_thread = spawn_named("receiver", move || {
        let mut receiver = receiver.into_future();
        let (waker, _waker_probe) = WakerProbe::new();
        let mut context = Context::from_waker(&waker);

        spin_until("poll ready with message", || {
            match Pin::new(&mut receiver).poll(&mut context) {
                Poll::Ready(Ok(999)) => true,
                Poll::Ready(result) => panic!("unexpected result: {result:?}"),
                Poll::Pending => false,
            }
        });
    });

    let sender_thread = spawn_named("sender", move || {
        sender.send(999).unwrap();
    });

    receiver_thread.join().unwrap();
    sender_thread.join().unwrap();
}

#[test]
fn concurrent_drop_sender_and_poll_to_completion() {
    let (sender, receiver) = oneshot::channel::<i32>();

    let receiver_thread = spawn_named("receiver", move || {
        let mut receiver = receiver.into_future();
        let (waker, _waker_probe) = WakerProbe::new();
        let mut context = Context::from_waker(&waker);

        spin_until("poll ready with disconnect", || {
            match Pin::new(&mut receiver).poll(&mut context) {
                Poll::Ready(Err(oneshot::RecvError::Disconnected)) => true,
                Poll::Ready(result) => panic!("unexpected result: {result:?}"),
                Poll::Pending => false,
            }
        });
    });

    let sender_thread = spawn_named("sender", move || {
        drop(sender);
    });

    receiver_thread.join().unwrap();
    sender_thread.join().unwrap();
}

mod support {
    use std::hint::spin_loop;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Wake;
    use std::task::Waker;
    use std::time::Duration;
    use std::time::Instant;

    pub(super) struct DropProbe<T> {
        drop_count: Arc<AtomicUsize>,
        value: T,
    }

    impl<T> DropProbe<T> {
        pub(super) fn new(value: T) -> (Self, Arc<AtomicUsize>) {
            let drop_count = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    drop_count: drop_count.clone(),
                    value,
                },
                drop_count,
            )
        }

        pub(super) fn value(&self) -> &T {
            &self.value
        }
    }

    impl<T> Drop for DropProbe<T> {
        fn drop(&mut self) {
            self.drop_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[derive(Default)]
    pub(super) struct WakerProbe {
        wake_count: AtomicU32,
    }

    impl WakerProbe {
        pub(super) fn new() -> (Waker, Arc<Self>) {
            let probe = Arc::new(Self::default());
            (Waker::from(probe.clone()), probe)
        }

        pub(super) fn live_waker_count(this: &Arc<Self>) -> usize {
            // The returned probe owns one strong reference; every other reference belongs to a
            // live Waker created from it.
            Arc::strong_count(this) - 1
        }

        pub(super) fn wake_count(&self) -> u32 {
            self.wake_count.load(Ordering::Relaxed)
        }
    }

    impl Wake for WakerProbe {
        fn wake(self: Arc<Self>) {
            self.wake_count.fetch_add(1, Ordering::Relaxed);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wake_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn spawn_named<F, T>(name: &str, f: F) -> std::thread::JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        std::thread::Builder::new()
            .name(name.to_string())
            .spawn(f)
            .unwrap()
    }

    pub(super) fn spin_until<F>(label: &str, mut f: F)
    where
        F: FnMut() -> bool,
    {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut spins = 0usize;

        loop {
            if f() {
                break;
            }

            assert!(Instant::now() < deadline, "timed out waiting for {label}");

            if spins % 64 == 0 {
                std::thread::yield_now();
            } else {
                spin_loop();
            }

            spins += 1;
        }
    }
}
