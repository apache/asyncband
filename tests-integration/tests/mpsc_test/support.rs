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
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Wake;
use std::task::Waker;
use std::thread;

pub fn expect_ready<T>(poll: Poll<T>) -> T {
    match poll {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("future should be ready"),
    }
}

#[derive(Default)]
pub struct WakeCounter(AtomicUsize);

impl WakeCounter {
    pub fn new() -> (Waker, Arc<Self>) {
        let counter = Arc::new(Self::default());
        (Waker::from(counter.clone()), counter)
    }

    pub fn count(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn poll_with<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

pub fn waker_on_drop(callback: impl Fn() + Send + Sync + 'static) -> Waker {
    struct OnDrop(Box<dyn Fn() + Send + Sync>);

    // Only destruction runs the callback; waking consumes the reference as usual.
    #[allow(clippy::manual_noop_waker)]
    impl Wake for OnDrop {
        fn wake(self: Arc<Self>) {}
    }

    impl Drop for OnDrop {
        fn drop(&mut self) {
            (self.0)();
        }
    }

    Waker::from(Arc::new(OnDrop(Box::new(callback))))
}

// RawWaker is needed only to exercise clone callbacks, which the safe Wake trait cannot override.
pub fn waker_on_clone(callback: impl Fn() + Send + Sync + 'static) -> Waker {
    struct OnClone(Box<dyn Fn() + Send + Sync>);

    unsafe fn clone(data: *const ()) -> RawWaker {
        let pointer = data.cast::<OnClone>();
        // SAFETY: The input waker owns a live Arc; the returned waker gains its own reference.
        unsafe {
            ((*pointer).0)();
            Arc::increment_strong_count(pointer);
        }
        RawWaker::new(data, &VTABLE)
    }

    unsafe fn release(data: *const ()) {
        // SAFETY: Consumes the one Arc reference owned by this waker.
        drop(unsafe { Arc::from_raw(data.cast::<OnClone>()) });
    }

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, release, |_| {}, release);
    let pointer = Arc::into_raw(Arc::new(OnClone(Box::new(callback)))).cast();
    // SAFETY: Each waker owns one Arc; its callback is Send + Sync and all vtable operations
    // preserve that ownership. wake_by_ref borrows the reference without changing it.
    unsafe { Waker::from_raw(RawWaker::new(pointer, &VTABLE)) }
}

pub fn assert_completes_without_deadlock(test: impl FnOnce() + Send + 'static) {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        test();
        finished_tx.send(()).unwrap();
    });
    #[cfg(not(miri))]
    finished_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("waker callback did not finish");
    // Miri detects deadlock itself; its interpretation time must not determine test success.
    #[cfg(miri)]
    finished_rx.recv().expect("waker callback did not finish");
    worker.join().unwrap();
}
