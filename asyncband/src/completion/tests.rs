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

use std::pin::pin;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::task::Wake;
use std::task::Waker;

use super::*;

fn assert_wakes_outside_waiter_lock(publish_value: bool) {
    struct ProbeWaker {
        shared: Arc<Shared<usize>>,
        state: AtomicU8,
    }

    impl Wake for ProbeWaker {
        fn wake(self: Arc<Self>) {
            // Terminal waits are lock-free, so reentering wait() cannot detect a held lock.
            let unlocked = self.shared.waiters.try_lock().is_some();
            // Record the result without panicking inside the callback: 0 means no wake.
            self.state
                .store(if unlocked { 2 } else { 1 }, Ordering::Relaxed);
        }
    }

    let (completer, completion) = new::<usize>();
    let probe = Arc::new(ProbeWaker {
        shared: completion.shared.clone(),
        state: AtomicU8::new(0),
    });
    let waker = Waker::from(probe.clone());
    let mut cx = Context::from_waker(&waker);
    let mut wait = pin!(completion.wait());
    assert!(wait.as_mut().poll(&mut cx).is_pending());

    if publish_value {
        completer.complete(13).unwrap();
    } else {
        drop(completer);
    }

    assert_eq!(
        probe.state.load(Ordering::Relaxed),
        2,
        "wake callback must run after releasing the waiter lock"
    );
}

#[test]
fn complete_wakes_outside_waiter_lock() {
    assert_wakes_outside_waiter_lock(true);
}

#[test]
fn abandonment_wakes_outside_waiter_lock() {
    assert_wakes_outside_waiter_lock(false);
}
