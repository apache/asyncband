// Portions adapted from parking 2.2.1.
// Copyright 2014-2020 The Rust Project Developers
// Licensed under Apache-2.0.
// Modified by the Apache Software Foundation.
// See the project LICENSE file for the exact upstream revision and source path.

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

const EMPTY: usize = 0;
const PARKED: usize = 1;
const NOTIFIED: usize = 2;

pub struct Parker {
    state: Arc<ParkerState>,
    // This marker keeps the single-waiter Parker !Sync while its waker state remains Sync.
    single_waiter: PhantomData<Cell<()>>,
}

struct ParkerState {
    state: AtomicUsize,
    lock: Mutex<()>,
    condvar: Condvar,
}

impl Parker {
    pub fn new() -> Self {
        Self {
            state: Arc::new(ParkerState {
                state: AtomicUsize::new(EMPTY),
                lock: Mutex::new(()),
                condvar: Condvar::new(),
            }),
            single_waiter: PhantomData,
        }
    }

    pub fn park(&self) {
        self.state.park(None);
    }

    pub fn park_timeout(&self, timeout: Duration) {
        self.state.park(Some(timeout));
    }

    pub fn waker(&self) -> Waker {
        Waker::from(self.state.clone())
    }
}

impl ParkerState {
    fn park(&self, timeout: Option<Duration>) {
        // Notifications are tokens. Consuming one is the common self-wake path and needs no lock.
        if self
            .state
            .compare_exchange(NOTIFIED, EMPTY, SeqCst, SeqCst)
            .is_ok()
        {
            return;
        }

        if timeout == Some(Duration::ZERO) {
            return;
        }

        // The mutex only closes the race between publishing PARKED and entering the condvar wait.
        let mut guard = self.lock.lock().unwrap();
        match self.state.compare_exchange(EMPTY, PARKED, SeqCst, SeqCst) {
            Ok(_) => {}
            Err(NOTIFIED) => {
                let previous = self.state.swap(EMPTY, SeqCst);
                assert_eq!(previous, NOTIFIED, "park state changed unexpectedly");
                return;
            }
            Err(state) => panic!("inconsistent park state: {state}"),
        }

        match timeout {
            None => loop {
                guard = self.condvar.wait(guard).unwrap();
                if self
                    .state
                    .compare_exchange(NOTIFIED, EMPTY, SeqCst, SeqCst)
                    .is_ok()
                {
                    return;
                }
            },
            Some(timeout) => {
                let (_guard, _wait_result) = self.condvar.wait_timeout(guard, timeout).unwrap();
                match self.state.swap(EMPTY, SeqCst) {
                    NOTIFIED | PARKED => {}
                    state => panic!("inconsistent timed park state: {state}"),
                }
            }
        }
    }

    fn unpark(&self) {
        match self.state.swap(NOTIFIED, SeqCst) {
            EMPTY | NOTIFIED => return,
            PARKED => {}
            state => panic!("inconsistent unpark state: {state}"),
        }

        // Waiting for this lock ensures the parker has entered Condvar::wait before notification.
        drop(self.lock.lock().unwrap());
        self.condvar.notify_one();
    }
}

impl Wake for ParkerState {
    #[inline]
    fn wake(self: Arc<Self>) {
        self.unpark();
    }

    #[inline]
    fn wake_by_ref(self: &Arc<Self>) {
        self.unpark();
    }
}
