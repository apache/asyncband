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

use std::cell::Cell;
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::pin::pin;
use std::ptr;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use asyncband::mutex::MappedMutexGuard;
use asyncband::mutex::Mutex;
use asyncband::mutex::MutexGuard;
use asyncband::mutex::OwnedMappedMutexGuard;
use asyncband::mutex::OwnedMutexGuard;
use asyncband::once::LazyCell;
use asyncband::rwlock::MappedRwLockReadGuard;
use asyncband::rwlock::MappedRwLockWriteGuard;
use asyncband::rwlock::OwnedMappedRwLockReadGuard;
use asyncband::rwlock::OwnedMappedRwLockWriteGuard;
use asyncband::rwlock::OwnedRwLockReadGuard;
use asyncband::rwlock::OwnedRwLockWriteGuard;
use asyncband::rwlock::RwLock;
use asyncband::rwlock::RwLockReadGuard;
use asyncband::rwlock::RwLockWriteGuard;

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn mapped_mutex_guards_preserve_lock_ownership() {
    let mutex = Mutex::new((vec![1, 2], 3));
    let guard = mutex.try_lock().unwrap();
    let mapped = MutexGuard::map(guard, |value| &mut value.0);
    let mut mapped = MappedMutexGuard::map(mapped, |values| &mut values[1]);
    assert!(mutex.try_lock().is_none());
    *mapped = 4;
    drop(mapped);
    assert_eq!(mutex.into_inner(), (vec![1, 4], 3));

    let mutex = Arc::new(Mutex::new(Some(vec![5, 6])));
    let weak = Arc::downgrade(&mutex);
    let guard = mutex.clone().try_lock_owned().unwrap();
    let mapped = OwnedMutexGuard::filter_map(guard, Option::as_mut).unwrap();
    let mut mapped = OwnedMappedMutexGuard::map(mapped, |values| &mut values[0]);
    assert!(mutex.try_lock().is_none());
    *mapped = 7;
    drop(mapped);
    let guard = mutex.try_lock().unwrap();
    assert_eq!(guard.as_deref(), Some([7, 6].as_slice()));
    drop(guard);

    drop(mutex);
    assert!(weak.upgrade().is_none());
}

#[derive(Debug, Eq, PartialEq)]
struct Data {
    values: Vec<i32>,
    label: String,
}

#[test]
fn mapped_rwlock_guards_preserve_lock_ownership() {
    let lock = RwLock::new(Data {
        values: vec![1, 2],
        label: "borrowed".to_owned(),
    });
    let write = lock.try_write().unwrap();
    let mapped = RwLockWriteGuard::map(write, |data| &mut data.values);
    let mut mapped = MappedRwLockWriteGuard::map(mapped, |values| &mut values[1]);
    *mapped = 3;
    let mapped = mapped.downgrade();
    assert_eq!(*mapped, 3);
    assert!(lock.try_write().is_none());
    drop(mapped);

    let read = lock.try_read().unwrap();
    let mapped = RwLockReadGuard::map(read, |data| &data.label);
    let mapped =
        MappedRwLockReadGuard::filter_map(mapped, |label| label.strip_prefix("bor")).unwrap();
    assert_eq!(&*mapped, "rowed");
    drop(mapped);

    let lock = Arc::new(RwLock::new(Data {
        values: vec![4, 5],
        label: "owned".to_owned(),
    }));
    let weak = Arc::downgrade(&lock);
    let write = lock.clone().try_write_owned().unwrap();
    let mapped = OwnedRwLockWriteGuard::map(write, |data| &mut data.values);
    let mut mapped =
        OwnedMappedRwLockWriteGuard::filter_map(mapped, |values| values.first_mut()).unwrap();
    *mapped = 6;
    let mapped = mapped.downgrade();
    assert_eq!(*mapped, 6);
    drop(mapped);

    let read = lock.clone().try_read_owned().unwrap();
    let mapped = OwnedRwLockReadGuard::map(read, |data| &data.label);
    let mapped =
        OwnedMappedRwLockReadGuard::filter_map(mapped, |label| label.strip_suffix("ed")).unwrap();
    assert_eq!(&*mapped, "own");
    drop(mapped);

    drop(lock);
    assert!(weak.upgrade().is_none());
}

struct AddressSensitiveFuture {
    address: Cell<*const Self>,
    _pin: PhantomPinned,
}

impl AddressSensitiveFuture {
    fn new() -> Self {
        Self {
            address: Cell::new(ptr::null()),
            _pin: PhantomPinned,
        }
    }
}

impl Future for AddressSensitiveFuture {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_ref().get_ref();
        let current = ptr::from_ref(this);
        let first = this.address.get();
        if first.is_null() {
            this.address.set(current);
            Poll::Pending
        } else {
            assert!(ptr::eq(first, current));
            Poll::Ready(42)
        }
    }
}

#[test]
fn lazy_cell_resumes_a_pinned_attempt_in_place() {
    let lazy = LazyCell::from_future(AddressSensitiveFuture::new());
    let lazy = pin!(lazy);

    {
        let mut force = pin!(LazyCell::force_pin(lazy.as_ref()));
        assert!(poll_once(force.as_mut()).is_pending());
    }

    let mut force = pin!(LazyCell::force_pin(lazy.as_ref()));
    assert_eq!(poll_once(force.as_mut()), Poll::Ready(&42));
}
