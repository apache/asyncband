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
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;

use asyncband::condvar::Condvar;
use asyncband::event::ManualResetEvent;
use asyncband::mutex::Mutex as AsyncMutex;
use asyncband::semaphore::Semaphore;
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

struct DropCallback(Mutex<Option<Box<dyn FnOnce() + Send>>>);

impl Wake for DropCallback {
    fn wake(self: Arc<Self>) {
        drop(self);
    }
}

impl Drop for DropCallback {
    fn drop(&mut self) {
        if let Some(callback) = self.0.get_mut().unwrap().take() {
            callback();
        }
    }
}

fn waker_with_drop_callback(callback: impl FnOnce() + Send + 'static) -> Waker {
    Waker::from(Arc::new(DropCallback(Mutex::new(Some(Box::new(callback))))))
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
fn semaphore_clones_wakers_outside_its_waiter_lock() {
    assert_completes_without_deadlock(
        "waker clone callback deadlocked against the semaphore waiter lock",
        || {
            let semaphore = Arc::new(Semaphore::new(0));
            let callback_semaphore = semaphore.clone();
            let waker = waker_with_clone_callback(move || callback_semaphore.release(1));
            let mut acquire = Box::pin(semaphore.acquire(1));

            assert!(poll_with(acquire.as_mut(), &waker).is_ready());

            let semaphore = Arc::new(Semaphore::new(0));
            let mut acquire = Box::pin(semaphore.acquire(1));
            assert!(poll_with(acquire.as_mut(), Waker::noop()).is_pending());

            let callback_semaphore = semaphore.clone();
            let replacement = waker_with_clone_callback(move || callback_semaphore.release(1));
            assert!(poll_with(acquire.as_mut(), &replacement).is_ready());
        },
    );
}

#[test]
fn condvar_runs_waker_callbacks_outside_its_waiter_lock() {
    assert_completes_without_deadlock(
        "waker callback deadlocked against the condvar waiter lock",
        || {
            let mutex = AsyncMutex::new(());
            let condvar = Arc::new(Condvar::new());
            let callback_condvar = condvar.clone();
            let waker = waker_with_clone_callback(move || callback_condvar.notify_one());
            let mut wait = Box::pin(condvar.wait(mutex.try_lock().unwrap()));

            assert!(poll_with(wait.as_mut(), &waker).is_pending());
            condvar.notify_one();
            assert!(poll_with(wait.as_mut(), &waker).is_ready());

            let mutex = AsyncMutex::new(());
            let condvar = Arc::new(Condvar::new());
            let mut wait = Box::pin(condvar.wait(mutex.try_lock().unwrap()));
            assert!(poll_with(wait.as_mut(), Waker::noop()).is_pending());

            let callback_condvar = condvar.clone();
            let replacement = waker_with_clone_callback(move || callback_condvar.notify_one());
            assert!(poll_with(wait.as_mut(), &replacement).is_ready());

            let mutex = AsyncMutex::new(());
            let condvar = Arc::new(Condvar::new());
            let callback_condvar = condvar.clone();
            let waker = waker_with_drop_callback(move || callback_condvar.notify_one());
            let mut wait = Box::pin(condvar.wait(mutex.try_lock().unwrap()));
            assert!(poll_with(wait.as_mut(), &waker).is_pending());

            drop(waker);
            drop(wait);
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

            assert_eq!(poll_with(changed.as_mut(), &waker), Poll::Ready(Ok(())));
            drop(changed);
            assert_eq!(receiver.get(), 1);
        },
    );
}
