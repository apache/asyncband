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
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;
use std::thread;
use std::time::Duration;

use asyncband::barrier::Barrier;
use asyncband::broadcast::mpmc;
use asyncband::completion;
use asyncband::event::ManualResetEvent;
use asyncband::latch::Latch;
use asyncband::watch;

struct CloneCallback(Mutex<Option<Box<dyn FnOnce() + Send>>>);

unsafe fn clone_callback_waker(data: *const ()) -> RawWaker {
    // SAFETY: Every raw pointer using this vtable comes from `Arc::into_raw` below. `ManuallyDrop`
    // keeps the original waker's strong reference alive while the clone callback borrows it.
    let callback = ManuallyDrop::new(unsafe { Arc::<CloneCallback>::from_raw(data.cast()) });
    if let Some(callback) = callback.0.lock().unwrap().take() {
        callback();
    }
    RawWaker::new(
        Arc::into_raw(Arc::clone(&callback)).cast(),
        &CLONE_CALLBACK_VTABLE,
    )
}

unsafe fn wake_clone_callback_waker(data: *const ()) {
    // SAFETY: `wake` consumes the raw waker's strong reference exactly once.
    drop(unsafe { Arc::<CloneCallback>::from_raw(data.cast()) });
}

unsafe fn wake_clone_callback_waker_by_ref(_data: *const ()) {}

unsafe fn drop_clone_callback_waker(data: *const ()) {
    // SAFETY: `drop` consumes the raw waker's strong reference exactly once.
    drop(unsafe { Arc::<CloneCallback>::from_raw(data.cast()) });
}

static CLONE_CALLBACK_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_callback_waker,
    wake_clone_callback_waker,
    wake_clone_callback_waker_by_ref,
    drop_clone_callback_waker,
);

fn waker_with_clone_callback(callback: impl FnOnce() + Send + 'static) -> Waker {
    let callback = Arc::new(CloneCallback(Mutex::new(Some(Box::new(callback)))));
    let raw = RawWaker::new(Arc::into_raw(callback).cast(), &CLONE_CALLBACK_VTABLE);
    // SAFETY: The vtable preserves the Arc strong count and all callbacks are thread safe.
    unsafe { Waker::from_raw(raw) }
}

fn poll_with<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

fn assert_completes_without_deadlock(message: &'static str, test: impl FnOnce() + Send + 'static) {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(test));
        finished_tx.send(()).unwrap();
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });

    finished_rx
        .recv_timeout(Duration::from_secs(10))
        .expect(message);
    worker.join().unwrap();
}

#[test]
fn completion_clones_wakers_outside_its_state_lock() {
    assert_completes_without_deadlock(
        "waker clone callback deadlocked against the completion lock",
        || {
            let (completer, completion) = completion::new::<usize>();
            let waker = waker_with_clone_callback(move || drop(completer));
            let mut wait = Box::pin(completion.wait());

            assert!(matches!(
                poll_with(wait.as_mut(), &waker),
                Poll::Ready(Err(_))
            ));
        },
    );
}

#[test]
fn event_clones_wakers_outside_its_state_lock() {
    assert_completes_without_deadlock(
        "waker clone callback deadlocked against the event lock",
        || {
            let event = Arc::new(ManualResetEvent::new());
            let callback_event = event.clone();
            let waker = waker_with_clone_callback(move || callback_event.set());
            let mut first_wait = Box::pin(event.wait());

            assert_eq!(poll_with(first_wait.as_mut(), &waker), Poll::Ready(()));

            event.reset();
            let mut repolled_wait = Box::pin(event.wait());
            assert_eq!(
                poll_with(repolled_wait.as_mut(), Waker::noop()),
                Poll::Pending
            );

            let callback_event = event.clone();
            let replacement = waker_with_clone_callback(move || callback_event.set());
            assert_eq!(
                poll_with(repolled_wait.as_mut(), &replacement),
                Poll::Ready(())
            );
        },
    );
}

#[test]
fn latch_clones_wakers_outside_its_waiter_lock() {
    assert_completes_without_deadlock(
        "waker clone callback deadlocked against the latch lock",
        || {
            let latch = Arc::new(Latch::new(1));
            let callback_latch = latch.clone();
            let waker = waker_with_clone_callback(move || callback_latch.count_down());
            let mut wait = Box::pin(latch.wait());

            assert_eq!(poll_with(wait.as_mut(), &waker), Poll::Ready(()));
        },
    );
}

#[test]
fn barrier_clones_wakers_outside_its_state_lock() {
    assert_completes_without_deadlock(
        "waker clone callback deadlocked against the barrier lock",
        || {
            let barrier = Arc::new(Barrier::new(2));
            let callback_barrier = barrier.clone();
            let waker = waker_with_clone_callback(move || {
                let mut wait = Box::pin(callback_barrier.wait());
                let Poll::Ready(result) = poll_with(wait.as_mut(), Waker::noop()) else {
                    panic!("second barrier participant must complete the generation");
                };
                assert!(result.is_leader());
            });
            let mut wait = Box::pin(barrier.wait());

            let Poll::Ready(result) = poll_with(wait.as_mut(), &waker) else {
                panic!("first barrier participant must observe the completed generation");
            };
            assert!(!result.is_leader());
        },
    );
}

#[test]
fn watch_clones_wakers_outside_its_state_lock() {
    assert_completes_without_deadlock(
        "waker clone callback deadlocked against the watch lock",
        || {
            let (sender, mut receiver) = watch::channel(0);
            let callback_sender = sender.clone();
            let waker = waker_with_clone_callback(move || callback_sender.send(1).unwrap());
            let mut changed = Box::pin(receiver.changed());

            assert_eq!(
                poll_with(changed.as_mut(), &waker),
                Poll::Ready(Ok(Arc::new(1)))
            );
        },
    );
}

#[test]
fn broadcast_clones_wakers_outside_its_state_lock() {
    assert_completes_without_deadlock(
        "waker clone callback deadlocked against the broadcast lock",
        || {
            let (sender, mut receiver) = mpmc::unbounded();
            let callback_sender = sender.clone();
            let waker = waker_with_clone_callback(move || callback_sender.send(1));
            let mut recv = Box::pin(receiver.recv());

            assert_eq!(poll_with(recv.as_mut(), &waker), Poll::Ready(Ok(1)));
        },
    );
}

#[test]
fn bounded_broadcast_clones_wakers_outside_its_state_lock() {
    assert_completes_without_deadlock(
        "waker clone callback deadlocked against the bounded broadcast lock",
        || {
            let (sender, mut receiver) = mpmc::bounded(1);
            let callback_sender = sender.clone();
            let waker = waker_with_clone_callback(move || {
                callback_sender.try_send(1).expect("channel has room");
            });
            let mut recv = Box::pin(receiver.recv());

            assert_eq!(poll_with(recv.as_mut(), &waker), Poll::Ready(Ok(1)));
        },
    );
}
