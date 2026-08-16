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

use std::future::pending;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::block_on::FutureExt as _;
use crate::block_on::Timeout;
use crate::block_on::block_on;
use crate::block_on::block_on_timeout;
use crate::mutex::Mutex;
use crate::oneshot;

#[test]
fn ready_future_returns_immediately() {
    assert_eq!(block_on(async { 42 }), 42);
}

#[test]
fn suffix_extension_returns_immediately() {
    assert_eq!(async { 42 }.block_on(), 42);
}

#[test]
fn mutex_lock_acquires_when_uncontended() {
    let mutex = Mutex::new(1);
    let guard = block_on(mutex.lock());
    assert_eq!(*guard, 1);
}

#[test]
fn suffix_extension_acquires_mutex() {
    let mutex = Mutex::new(2);
    let guard = mutex.lock().block_on();
    assert_eq!(*guard, 2);
}

#[test]
fn wakes_thread_when_another_thread_completes_the_future() {
    let (sender, receiver) = oneshot::channel::<u8>();

    let producer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        sender.send(7).unwrap();
    });

    assert_eq!(block_on(receiver), Ok(7));
    producer.join().unwrap();
}

#[test]
fn timeout_returns_ready_future_before_deadline() {
    let (sender, receiver) = oneshot::channel::<u8>();

    let producer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        sender.send(7).unwrap();
    });

    assert_eq!(
        block_on_timeout(receiver, Duration::from_secs(1)),
        Ok(Ok(7))
    );
    producer.join().unwrap();
}

#[test]
fn suffix_timeout_returns_ready_future_before_deadline() {
    assert_eq!(
        async { 42 }.block_on_timeout(Duration::from_secs(1)),
        Ok(42)
    );
}

#[test]
fn timeout_rejects_a_future_that_never_becomes_ready() {
    let start = std::time::Instant::now();
    assert_eq!(
        block_on_timeout(pending::<()>(), Duration::from_millis(20)),
        Err(Timeout)
    );
    assert!(start.elapsed() >= Duration::from_millis(10));
}

#[test]
fn zero_timeout_returns_immediately() {
    assert_eq!(
        block_on_timeout(pending::<()>(), Duration::ZERO),
        Err(Timeout)
    );
}

#[test]
fn timed_out_mutex_lock_is_cancelled_and_released() {
    let mutex = Arc::new(Mutex::new(1));
    let contender = mutex.clone();

    let holder = block_on(mutex.lock());
    assert!(block_on_timeout(contender.lock(), Duration::from_millis(20)).is_err());

    drop(holder);
    let guard = block_on(mutex.lock());
    assert_eq!(*guard, 1);
}
