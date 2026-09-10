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
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use tokio::runtime::Runtime;

/// Polls a pinned future once with a no-op waker.
pub fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

/// Returns the runtime shared by synchronous integration tests.
pub fn test_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

pub fn expect_ready<T>(poll: Poll<T>) -> T {
    match poll {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("future should be ready"),
    }
}

pub fn poll_with<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
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

    pub fn take(&self) -> usize {
        self.0.swap(0, Ordering::Relaxed)
    }
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

pub struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("wake failed");
    }
}

pub fn waker_on_wake(callback: impl FnOnce() + Send + 'static) -> Waker {
    struct OnWake(Mutex<Option<Box<dyn FnOnce() + Send>>>);

    impl Wake for OnWake {
        fn wake(self: Arc<Self>) {
            let callback = self.0.lock().unwrap().take();
            if let Some(callback) = callback {
                callback();
            }
        }
    }

    Waker::from(Arc::new(OnWake(Mutex::new(Some(Box::new(callback))))))
}

pub fn waker_on_drop(callback: impl FnOnce() + Send + 'static) -> Waker {
    struct OnDrop(Mutex<Option<Box<dyn FnOnce() + Send>>>);

    // Only destruction runs the callback; waking consumes the reference as usual.
    #[allow(clippy::manual_noop_waker)]
    impl Wake for OnDrop {
        fn wake(self: Arc<Self>) {}
    }

    impl Drop for OnDrop {
        fn drop(&mut self) {
            if let Some(callback) = self.0.get_mut().unwrap().take() {
                callback();
            }
        }
    }

    Waker::from(Arc::new(OnDrop(Mutex::new(Some(Box::new(callback))))))
}

pub fn assert_completes_without_deadlock(test: impl FnOnce() + Send + 'static) {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(test));
        finished_tx.send(result).unwrap();
    });
    #[cfg(not(miri))]
    let result = finished_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("test did not finish");
    // Miri detects deadlock itself; its interpretation time must not determine test success.
    #[cfg(miri)]
    let result = finished_rx.recv().expect("test did not finish");
    worker.join().unwrap();
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}
