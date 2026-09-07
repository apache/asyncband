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

#[test]
fn rwlock_projection_panics_release_access() {
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;

    let lock = Arc::new(RwLock::new(vec![1, 2]));

    // Exercise every guard representation. Some closures mutate before unwinding: the value
    // must remain accessible afterward, and the lock must neither leak access nor be poisoned.
    macro_rules! check {
        ($project:expr) => {
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    drop($project);
                }))
                .is_err()
            );
            assert!(lock.try_write().is_some());
            assert_eq!(Arc::strong_count(&lock), 1);
        };
    }

    check!(RwLockReadGuard::map::<(), _>(
        lock.try_read().unwrap(),
        |_| panic!("projection")
    ));
    check!(RwLockWriteGuard::filter_map::<(), _>(
        lock.try_write().unwrap(),
        |values| {
            values.push(3);
            panic!("projection");
        }
    ));
    check!(OwnedRwLockReadGuard::filter_map::<(), _>(
        lock.clone().try_read_owned().unwrap(),
        |_| panic!("projection")
    ));
    check!(OwnedRwLockWriteGuard::map::<(), _>(
        lock.clone().try_write_owned().unwrap(),
        |_| panic!("projection")
    ));

    let read = RwLockReadGuard::map(lock.try_read().unwrap(), Vec::as_slice);
    check!(MappedRwLockReadGuard::filter_map::<(), _>(
        read,
        |_| panic!("projection")
    ));
    let write = RwLockWriteGuard::map(lock.try_write().unwrap(), Vec::as_mut_slice);
    check!(MappedRwLockWriteGuard::map::<(), _>(write, |_| panic!(
        "projection"
    )));
    let read = OwnedRwLockReadGuard::map(lock.clone().try_read_owned().unwrap(), Vec::as_slice);
    check!(OwnedMappedRwLockReadGuard::map::<(), _>(read, |_| panic!(
        "projection"
    )));
    let write =
        OwnedRwLockWriteGuard::map(lock.clone().try_write_owned().unwrap(), Vec::as_mut_slice);
    check!(OwnedMappedRwLockWriteGuard::filter_map::<(), _>(
        write,
        |_| panic!("projection")
    ));

    assert_eq!(*lock.try_read().unwrap(), [1, 2, 3]);
}

#[test]
fn rwlock_failed_projection_keeps_access_and_mutations() {
    let lock = Arc::new(RwLock::new(vec![Some(1)]));
    let guard = lock.try_write().unwrap();
    let guard = RwLockWriteGuard::filter_map(guard, |values| {
        values.push(None);
        None::<&mut i32>
    })
    .unwrap_err();
    assert!(lock.try_read().is_none());
    let mut slot = RwLockWriteGuard::map(guard, |values| &mut values[1]);
    slot = MappedRwLockWriteGuard::filter_map(slot, Option::as_mut).unwrap_err();
    *slot = Some(2);
    let slot = slot.downgrade();
    let slot = MappedRwLockReadGuard::filter_map(slot, |_| None::<&i32>).unwrap_err();
    assert_eq!(*slot, Some(2));
    assert!(lock.try_write().is_none());
    drop(slot);

    let guard = lock.clone().try_write_owned().unwrap();
    let slot = OwnedRwLockWriteGuard::map(guard, |values| &mut values[1]);
    let mut slot = OwnedMappedRwLockWriteGuard::filter_map(slot, |_| None::<&mut i32>).unwrap_err();
    *slot = Some(3);
    let slot = slot.downgrade();
    let slot = OwnedMappedRwLockReadGuard::filter_map(slot, |_| None::<&i32>).unwrap_err();
    let weak = Arc::downgrade(&lock);
    drop(lock);
    assert_eq!(*slot, Some(3));
    drop(slot);
    assert!(weak.upgrade().is_none());
}

#[test]
fn rwlock_downgrade_unwind_releases_retained_access() {
    use std::num::NonZeroUsize;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::task::Wake;

    struct PanicOnWake;
    impl Wake for PanicOnWake {
        fn wake(self: Arc<Self>) {
            panic!("wake during downgrade");
        }
    }

    fn check(lock: &RwLock<(usize, usize)>, downgrade: impl FnOnce()) {
        let mut reader = Box::pin(lock.read());
        let waker = Waker::from(Arc::new(PanicOnWake));
        assert!(
            reader
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(catch_unwind(AssertUnwindSafe(downgrade)).is_err());
        // The queued reader received its permit before waking. After it releases that permit,
        // no read access from the failed downgrade may remain.
        let Poll::Ready(reader) = poll_once(reader.as_mut()) else {
            panic!("reader was not granted access");
        };
        drop(reader);
        assert!(lock.try_write().is_some());
    }

    for limit in [2, usize::MAX] {
        let lock = Arc::new(RwLock::with_max_readers(
            (1, 2),
            NonZeroUsize::new(limit).unwrap(),
        ));
        let guard = lock.try_write().unwrap();
        check(&lock, || {
            drop(guard.downgrade());
        });
        let guard = RwLockWriteGuard::map(lock.try_write().unwrap(), |value| &mut value.0);
        check(&lock, || {
            drop(guard.downgrade());
        });
        let guard = lock.clone().try_write_owned().unwrap();
        check(&lock, || {
            drop(guard.downgrade());
        });
        assert_eq!(Arc::strong_count(&lock), 1);
        let guard = OwnedRwLockWriteGuard::map(lock.clone().try_write_owned().unwrap(), |value| {
            &mut value.1
        });
        check(&lock, || {
            drop(guard.downgrade());
        });
        assert_eq!(Arc::strong_count(&lock), 1);
    }
}
