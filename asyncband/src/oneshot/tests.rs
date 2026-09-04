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

// This file contains test support adapted from the oneshot crate.
// Asyncband uses the upstream crate's Apache-2.0 license option for that code.
// The incorporated code has been modified for use in Apache Asyncband.
// See the project LICENSE file for the exact upstream revision and source paths.

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
use crate::oneshot;

// These tests stay next to the implementation because they inspect private state.

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

mod support {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Wake;
    use std::task::Waker;

    pub(super) struct DropProbe<T> {
        drop_count: Arc<AtomicUsize>,
        _value: T,
    }

    impl<T> DropProbe<T> {
        pub(super) fn new(value: T) -> (Self, Arc<AtomicUsize>) {
            let drop_count = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    drop_count: drop_count.clone(),
                    _value: value,
                },
                drop_count,
            )
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
            .name(name.to_owned())
            .spawn(f)
            .unwrap()
    }
}
