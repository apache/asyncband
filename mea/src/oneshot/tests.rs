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
use std::hint::spin_loop;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use crate::oneshot;
use crate::oneshot::TryRecvError;

struct DropCounterHandle(Arc<AtomicUsize>);

impl DropCounterHandle {
    pub fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

struct DropCounter<T> {
    drop_count: Arc<AtomicUsize>,
    value: Option<T>,
}

impl<T> DropCounter<T> {
    fn new(value: T) -> (Self, DropCounterHandle) {
        let drop_count = Arc::new(AtomicUsize::new(0));
        (
            Self {
                drop_count: drop_count.clone(),
                value: Some(value),
            },
            DropCounterHandle(drop_count),
        )
    }

    fn value(&self) -> &T {
        self.value.as_ref().unwrap()
    }
}

impl<T> Drop for DropCounter<T> {
    fn drop(&mut self) {
        self.drop_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn send_before_await() {
    let (sender, receiver) = oneshot::channel();
    assert!(sender.send(19i128).is_ok());
    assert_eq!(receiver.await, Ok(19i128));
}

#[tokio::test]
async fn await_with_dropped_sender() {
    let (sender, receiver) = oneshot::channel::<u128>();
    drop(sender);
    receiver.await.unwrap_err();
}

#[tokio::test]
async fn await_before_send() {
    let (sender, receiver) = oneshot::channel();
    let (message, counter) = DropCounter::new(79u128);
    let t = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        sender.send(message)
    });
    let returned_message = receiver.await.unwrap();
    assert_eq!(counter.count(), 0);
    assert_eq!(*returned_message.value(), 79u128);
    drop(returned_message);
    assert_eq!(counter.count(), 1);
    t.await.unwrap().unwrap();
}

#[tokio::test]
async fn await_before_send_then_drop_sender() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let t = tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(sender);
    });
    assert!(receiver.await.is_err());
    t.await.unwrap();
}

#[tokio::test]
async fn poll_receiver_then_drop_it() {
    let (sender, receiver) = oneshot::channel::<()>();
    // This will poll the receiver and then give up after 100 ms.
    tokio::time::timeout(Duration::from_millis(100), receiver)
        .await
        .unwrap_err();
    // Make sure the receiver has been dropped by the runtime.
    assert!(sender.send(()).is_err());
}

#[tokio::test]
async fn recv_within_select() {
    let (tx, rx) = oneshot::channel::<&'static str>();
    let mut interval = tokio::time::interval(Duration::from_millis(10));

    let handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        tx.send("shut down").unwrap();
    });

    let mut recv = rx.into_future();
    loop {
        tokio::select! {
            _ = interval.tick() => println!("another 10ms"),
            msg = &mut recv => {
                println!("Got message: {}", msg.unwrap());
                break;
            }
        }
    }

    handle.await.unwrap();
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
fn try_recv_empty_with_live_sender() {
    let (_tx, rx) = oneshot::channel::<()>();
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn try_recv_disconnected_after_drop() {
    let (tx, rx) = oneshot::channel::<()>();
    drop(tx);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn send_error_preserves_message_until_consumed() {
    let (sender, receiver) = oneshot::channel();
    let (message, counter) = DropCounter::new(17u128);

    drop(receiver);
    assert!(sender.is_closed());

    let error = sender.send(message).unwrap_err();
    assert_eq!(counter.count(), 0);
    assert_eq!(*error.as_inner().value(), 17);

    let message = error.into_inner();
    assert_eq!(counter.count(), 0);
    drop(message);
    assert_eq!(counter.count(), 1);
}

#[test]
fn dropping_send_error_drops_message() {
    let (sender, receiver) = oneshot::channel();
    let (message, counter) = DropCounter::new(());

    drop(receiver);
    drop(sender.send(message).unwrap_err());

    assert_eq!(counter.count(), 1);
}

#[test]
fn dropping_receiver_after_send_drops_message() {
    let (sender, receiver) = oneshot::channel();
    let (message, counter) = DropCounter::new(());

    sender.send(message).unwrap();
    assert_eq!(counter.count(), 0);
    drop(receiver);

    assert_eq!(counter.count(), 1);
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
    let (message, counter) = DropCounter::new(());
    let receiver = receiver.into_future();

    sender.send(message).unwrap();
    assert_eq!(counter.count(), 0);
    drop(receiver);

    assert_eq!(counter.count(), 1);
}

#[derive(Default)]
pub struct WakerHandle {
    clone_count: AtomicU32,
    drop_count: AtomicU32,
    wake_count: AtomicU32,
}

impl WakerHandle {
    pub fn clone_count(&self) -> u32 {
        self.clone_count.load(Ordering::Relaxed)
    }

    pub fn drop_count(&self) -> u32 {
        self.drop_count.load(Ordering::Relaxed)
    }

    pub fn wake_count(&self) -> u32 {
        self.wake_count.load(Ordering::Relaxed)
    }
}

fn waker() -> (Waker, Arc<WakerHandle>) {
    let waker_handle = Arc::new(WakerHandle::default());
    let waker_handle_ptr = Arc::into_raw(waker_handle.clone());
    let raw_waker = RawWaker::new(waker_handle_ptr as *const _, waker_vtable());
    (unsafe { Waker::from_raw(raw_waker) }, waker_handle)
}

fn waker_vtable() -> &'static RawWakerVTable {
    &RawWakerVTable::new(clone_raw, wake_raw, wake_by_ref_raw, drop_raw)
}

unsafe fn clone_raw(data: *const ()) -> RawWaker {
    let handle: Arc<WakerHandle> = unsafe { Arc::from_raw(data as *const _) };
    handle.clone_count.fetch_add(1, Ordering::Relaxed);
    mem::forget(handle.clone());
    mem::forget(handle);
    RawWaker::new(data, waker_vtable())
}

unsafe fn wake_raw(data: *const ()) {
    let handle: Arc<WakerHandle> = unsafe { Arc::from_raw(data as *const _) };
    handle.wake_count.fetch_add(1, Ordering::Relaxed);
    handle.drop_count.fetch_add(1, Ordering::Relaxed);
}

unsafe fn wake_by_ref_raw(data: *const ()) {
    let handle: Arc<WakerHandle> = unsafe { Arc::from_raw(data as *const _) };
    handle.wake_count.fetch_add(1, Ordering::Relaxed);
    mem::forget(handle)
}

unsafe fn drop_raw(data: *const ()) {
    let handle: Arc<WakerHandle> = unsafe { Arc::from_raw(data as *const _) };
    handle.drop_count.fetch_add(1, Ordering::Relaxed);
    drop(handle)
}

#[test]
fn poll_then_send() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let mut receiver = receiver.into_future();

    let (waker, waker_handle) = waker();
    let mut context = Context::from_waker(&waker);

    assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Pending);
    assert_eq!(waker_handle.clone_count(), 1);
    assert_eq!(waker_handle.drop_count(), 0);
    assert_eq!(waker_handle.wake_count(), 0);

    sender.send(1234).unwrap();
    assert_eq!(waker_handle.clone_count(), 1);
    assert_eq!(waker_handle.drop_count(), 1);
    assert_eq!(waker_handle.wake_count(), 1);

    assert_eq!(
        Pin::new(&mut receiver).poll(&mut context),
        Poll::Ready(Ok(1234))
    );
    assert_eq!(waker_handle.clone_count(), 1);
    assert_eq!(waker_handle.drop_count(), 1);
    assert_eq!(waker_handle.wake_count(), 1);
}

#[test]
fn poll_returns_while_sender_owns_waker() {
    let (sender, receiver) = oneshot::channel();
    let channel_ptr = sender.channel_ptr();
    mem::forget(sender);
    let mut receiver = receiver.into_future();

    let (stored_waker, stored_handle) = waker();
    let mut stored_context = Context::from_waker(&stored_waker);
    assert_eq!(
        Pin::new(&mut receiver).poll(&mut stored_context),
        Poll::Pending
    );

    let channel = unsafe { channel_ptr.as_ref() };
    unsafe { channel.write_message(1234u128) };
    // Pause the synthetic sender in CLAIMED immediately after it claims the stored waker.
    assert_eq!(
        channel.state.fetch_add(1, Ordering::Release),
        super::REGISTERED
    );

    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    let poll_thread = spawn_named("receiver", move || {
        let (current_waker, current_handle) = waker();
        let mut current_context = Context::from_waker(&current_waker);
        started_tx.send(()).unwrap();
        let result = Pin::new(&mut receiver).poll(&mut current_context);
        result_tx.send((receiver, current_handle, result)).unwrap();
    });

    started_rx.recv().unwrap();
    let result = result_rx.recv_timeout(Duration::from_secs(5));
    let returned_before_publish = result.is_ok();

    fence(Ordering::Acquire);
    let (claimed_waker, receiver_owns_allocation) =
        super::take_waker_and_publish(channel, super::READY);
    assert!(receiver_owns_allocation);
    claimed_waker.wake();
    assert_eq!(stored_handle.wake_count(), 1);

    let (mut receiver, current_handle, result) = match result {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("receiver remained blocked after the sender published READY"),
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("receiver thread exited unexpectedly"),
    };
    poll_thread.join().unwrap();
    assert!(
        returned_before_publish,
        "poll blocked while the sender owned the waker"
    );
    assert_eq!(result, Poll::Pending);
    assert_eq!(current_handle.wake_count(), 1);

    let (current_waker, _) = waker();
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
    let (message, counter) = DropCounter::new(1234u128);

    let (stored_waker, stored_handle) = waker();
    let mut context = Context::from_waker(&stored_waker);
    assert!(matches!(
        Pin::new(&mut receiver).poll(&mut context),
        Poll::Pending
    ));

    let channel = unsafe { channel_ptr.as_ref() };
    unsafe { channel.write_message(message) };
    // Pause the synthetic sender in CLAIMED immediately after it claims the stored waker.
    assert_eq!(
        channel.state.fetch_add(1, Ordering::Release),
        super::REGISTERED
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

    fence(Ordering::Acquire);
    let (claimed_waker, receiver_owns_allocation) =
        super::take_waker_and_publish(channel, super::READY);
    if receiver_owns_allocation {
        claimed_waker.wake();
    } else {
        unsafe { super::drop_message_and_dealloc_channel(channel_ptr) };
        drop(claimed_waker);
    }

    match result {
        Ok(()) => {}
        Err(mpsc::RecvTimeoutError::Timeout) => done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("receiver remained blocked after the sender published READY"),
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("receiver thread exited unexpectedly"),
    }
    drop_thread.join().unwrap();
    assert!(
        returned_before_publish,
        "drop blocked while the sender owned the waker"
    );
    assert!(!receiver_owns_allocation);
    assert_eq!(counter.count(), 1);
    assert_eq!(stored_handle.drop_count(), 1);
}

#[test]
fn poll_then_drop_sender() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let mut receiver = receiver.into_future();

    let (waker, waker_handle) = waker();
    let mut context = Context::from_waker(&waker);

    assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Pending);
    assert_eq!(waker_handle.clone_count(), 1);
    assert_eq!(waker_handle.drop_count(), 0);
    assert_eq!(waker_handle.wake_count(), 0);

    drop(sender);
    assert_eq!(waker_handle.clone_count(), 1);
    assert_eq!(waker_handle.drop_count(), 1);
    assert_eq!(waker_handle.wake_count(), 1);

    assert_eq!(
        Pin::new(&mut receiver).poll(&mut context),
        Poll::Ready(Err(oneshot::RecvError::Disconnected))
    );
    assert_eq!(waker_handle.drop_count(), 1);
    assert_eq!(waker_handle.wake_count(), 1);
}

#[test]
fn poll_with_different_wakers() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let mut receiver = receiver.into_future();

    let (waker1, waker_handle1) = waker();
    let mut context1 = Context::from_waker(&waker1);

    assert_eq!(Pin::new(&mut receiver).poll(&mut context1), Poll::Pending);
    assert_eq!(waker_handle1.clone_count(), 1);
    assert_eq!(waker_handle1.drop_count(), 0);
    assert_eq!(waker_handle1.wake_count(), 0);

    let (waker2, waker_handle2) = waker();
    let mut context2 = Context::from_waker(&waker2);

    assert_eq!(Pin::new(&mut receiver).poll(&mut context2), Poll::Pending);
    assert_eq!(waker_handle1.clone_count(), 1);
    assert_eq!(waker_handle1.drop_count(), 1);
    assert_eq!(waker_handle1.wake_count(), 0);

    assert_eq!(waker_handle2.clone_count(), 1);
    assert_eq!(waker_handle2.drop_count(), 0);
    assert_eq!(waker_handle2.wake_count(), 0);

    // Sending should cause the waker from the latest poll to be woken up
    sender.send(1234).unwrap();
    assert_eq!(waker_handle1.clone_count(), 1);
    assert_eq!(waker_handle1.drop_count(), 1);
    assert_eq!(waker_handle1.wake_count(), 0);

    assert_eq!(waker_handle2.clone_count(), 1);
    assert_eq!(waker_handle2.drop_count(), 1);
    assert_eq!(waker_handle2.wake_count(), 1);
}

#[test]
fn poll_with_different_wakers_across_threads() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let mut receiver = receiver.into_future();

    let (waker1, waker_handle1) = waker();
    let mut context1 = Context::from_waker(&waker1);

    assert_eq!(Pin::new(&mut receiver).poll(&mut context1), Poll::Pending);
    assert_eq!(waker_handle1.clone_count(), 1);
    assert_eq!(waker_handle1.drop_count(), 0);
    assert_eq!(waker_handle1.wake_count(), 0);

    let receiver_thread = spawn_named("receiver", move || {
        let (waker2, waker_handle2) = waker();
        let mut context2 = Context::from_waker(&waker2);

        assert_eq!(Pin::new(&mut receiver).poll(&mut context2), Poll::Pending);
        assert_eq!(waker_handle2.clone_count(), 1);
        assert_eq!(waker_handle2.drop_count(), 0);
        assert_eq!(waker_handle2.wake_count(), 0);

        drop(receiver);
        assert_eq!(waker_handle2.drop_count(), 1);
    });

    receiver_thread.join().unwrap();
    assert_eq!(waker_handle1.drop_count(), 1);
    assert!(sender.is_closed());
}

#[test]
fn drop_pending_receiver_closes_channel_and_drops_waker() {
    let (sender, receiver) = oneshot::channel::<u128>();
    let mut receiver = receiver.into_future();

    let (waker, waker_handle) = waker();
    let mut context = Context::from_waker(&waker);

    assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Pending);
    assert_eq!(waker_handle.clone_count(), 1);
    assert_eq!(waker_handle.drop_count(), 0);
    assert_eq!(waker_handle.wake_count(), 0);

    drop(receiver);
    assert_eq!(waker_handle.drop_count(), 1);
    assert_eq!(waker_handle.wake_count(), 0);
    assert!(sender.is_closed());

    let error = sender.send(1234).unwrap_err();
    assert_eq!(*error.as_inner(), 1234);
}

#[test]
fn poll_then_drop_receiver_during_send() {
    let (sender, receiver) = oneshot::channel();
    let (message, counter) = DropCounter::new(1234u128);
    let mut receiver = receiver.into_future();

    let (waker, _waker_handle) = waker();
    let mut context = Context::from_waker(&waker);

    // Put the channel into the receiving state
    assert!(matches!(
        Pin::new(&mut receiver).poll(&mut context),
        Poll::Pending
    ));

    // Spawn a separate thread that sends in parallel
    let t = std::thread::spawn(move || sender.send(message));

    // Drop the receiver.
    drop(receiver);

    // Whether send or receiver drop wins, exactly one side owns and drops the message.
    drop(t.join().unwrap());
    assert_eq!(counter.count(), 1);
}

#[test]
fn dropping_sender_disconnects_async_receiver() {
    let (sender, receiver) = oneshot::channel::<()>();
    assert!(!sender.is_closed());
    assert!(!receiver.is_closed());
    drop(sender);
    assert!(receiver.is_closed());
}

#[test]
fn async_receiver_has_message() {
    let (sender, receiver) = oneshot::channel();
    assert!(!receiver.has_message());
    assert!(sender.send(19i128).is_ok());
    assert!(receiver.has_message());
}

#[test]
fn concurrent_send_and_try_recv_to_completion() {
    let (sender, receiver) = oneshot::channel::<i32>();

    let receiver_thread = spawn_named("receiver", move || {
        spin_until("message from sender", || match receiver.try_recv() {
            Ok(999) => Some(()),
            Ok(value) => panic!("unexpected value: {value}"),
            Err(TryRecvError::Empty) => None,
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
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => Some(()),
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
        let (waker, _waker_handle) = waker();
        let mut context = Context::from_waker(&waker);

        spin_until("poll ready with message", || {
            match Pin::new(&mut receiver).poll(&mut context) {
                Poll::Ready(Ok(999)) => Some(()),
                Poll::Ready(result) => panic!("unexpected result: {result:?}"),
                Poll::Pending => None,
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
        let (waker, _waker_handle) = waker();
        let mut context = Context::from_waker(&waker);

        spin_until("poll ready with disconnect", || {
            match Pin::new(&mut receiver).poll(&mut context) {
                Poll::Ready(Err(oneshot::RecvError::Disconnected)) => Some(()),
                Poll::Ready(result) => panic!("unexpected result: {result:?}"),
                Poll::Pending => None,
            }
        });
    });

    let sender_thread = spawn_named("sender", move || {
        drop(sender);
    });

    receiver_thread.join().unwrap();
    sender_thread.join().unwrap();
}

fn spawn_named<F>(name: &str, f: F) -> std::thread::JoinHandle<()>
where
    F: FnOnce() + Send + 'static,
{
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(f)
        .unwrap()
}

fn spin_until<F>(label: &str, mut f: F)
where
    F: FnMut() -> Option<()>,
{
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut spins = 0usize;

    loop {
        if f().is_some() {
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
