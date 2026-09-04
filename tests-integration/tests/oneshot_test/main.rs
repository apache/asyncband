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

// Derived from the oneshot crate at commit 83fd0864:
// https://github.com/faern/oneshot/blob/83fd0864be7289067ce96cc79cd96c0928742979/src/lib.rs

use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use asyncband::oneshot;
use asyncband::oneshot::TryRecvError;

use self::support::DropProbe;
use self::support::WakerProbe;
use self::support::spawn_named;
use self::support::spin_until;

mod support;

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
    assert!(!receiver.is_disconnected());
    drop(sender);
    assert!(receiver.is_disconnected());
    assert_eq!(
        pollster::block_on(receiver),
        Err(oneshot::RecvError::Disconnected)
    );
}

#[test]
fn try_recv_success_then_disconnected() {
    let (tx, rx) = oneshot::channel::<i32>();
    tx.send(10).unwrap();

    assert!(!rx.is_disconnected());
    assert_eq!(rx.try_recv(), Ok(10));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    assert!(rx.is_disconnected());
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

    assert!(!sender.is_disconnected());
    drop(receiver);
    assert!(sender.is_disconnected());

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
fn dropping_unpolled_recv_disconnects_channel() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let receiver = receiver.into_future();

    drop(receiver);

    assert!(sender.is_disconnected());
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
    assert!(sender.is_disconnected());
}

#[test]
fn drop_pending_receiver_disconnects_channel_and_drops_waker() {
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
    assert!(sender.is_disconnected());

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
            Err(TryRecvError::Disconnected) => panic!("unexpected channel disconnection"),
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
        spin_until("channel disconnection", || match receiver.try_recv() {
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

        spin_until("poll ready with disconnection", || {
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
