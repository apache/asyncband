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

// Adapted from parking 2.2.1:
// https://github.com/smol-rs/parking/tree/v2.2.1

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
