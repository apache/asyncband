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

//! An acquire synchronizes with the release whose release sequence it reads from, and every
//! write to the balance is a read-modify-write, so that sequence extends through later releases
//! and acquisitions. A plain store would end it, and Miri would report a data race here.
//!
//! The race shows only on schedules where the final acquire reads the latest balance rather than
//! a stale one, so the test runs under several seeds.

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;

use asyncband::semaphore::Semaphore;

struct Data(UnsafeCell<u64>);

// SAFETY: accesses are ordered only through the semaphore, which is the property under test.
unsafe impl Sync for Data {}

/// Sequences the steps in time without adding a happens-before edge.
fn wait_for(step: &AtomicUsize, value: usize) {
    while step.load(Ordering::Relaxed) < value {
        thread::yield_now();
    }
}

#[test]
fn acquire_synchronizes_with_every_earlier_release() {
    let semaphore = Arc::new(Semaphore::new(1));
    let data = Arc::new(Data(UnsafeCell::new(0)));
    let step = Arc::new(AtomicUsize::new(0));

    // Writes, then releases onto a positive balance.
    let writer = {
        let (semaphore, data, step) = (semaphore.clone(), data.clone(), step.clone());
        thread::spawn(move || {
            // SAFETY: the reader is ordered after this write by the semaphore.
            unsafe { *data.0.get() = 42 };
            semaphore.release(1);
            step.store(1, Ordering::Relaxed);
        })
    };

    // Takes every permit, so the balance is zero.
    let drainer = {
        let (semaphore, step) = (semaphore.clone(), step.clone());
        thread::spawn(move || {
            wait_for(&step, 1);
            loop {
                if let Some(permit) = semaphore.try_acquire(2) {
                    permit.forget();
                    break;
                }
                thread::yield_now();
            }
            step.store(2, Ordering::Relaxed);
        })
    };

    // Releases onto the zero balance without any synchronization with the writer.
    let releaser = {
        let (semaphore, step) = (semaphore.clone(), step.clone());
        thread::spawn(move || {
            wait_for(&step, 2);
            semaphore.release(1);
            step.store(3, Ordering::Relaxed);
        })
    };

    // Acquires the releaser's permit and reads the data.
    let reader = {
        let (semaphore, data, step) = (semaphore.clone(), data.clone(), step.clone());
        thread::spawn(move || {
            wait_for(&step, 3);
            let permit = loop {
                if let Some(permit) = semaphore.try_acquire(1) {
                    break permit;
                }
                thread::yield_now();
            };
            // SAFETY: every release before this acquire happens-before it.
            let value = unsafe { *data.0.get() };
            drop(permit);
            value
        })
    };

    writer.join().unwrap();
    drainer.join().unwrap();
    releaser.join().unwrap();
    assert_eq!(reader.join().unwrap(), 42);
}
