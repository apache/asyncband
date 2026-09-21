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

use std::num::NonZeroUsize;
use std::sync::Arc;

use asyncband::rwlock::*;
use tests_integration::poll_once;
use tokio_test::assert_pending;
use tokio_test::assert_ready;

#[test]
fn try_methods_respect_held_guards() {
    let rwlock = Arc::new(RwLock::new(42));

    let first_reader = rwlock.try_read().unwrap();
    let second_reader = rwlock.try_read().unwrap();
    assert_eq!(*first_reader, 42);
    assert_eq!(*second_reader, 42);

    assert!(rwlock.try_write().is_none());

    drop(first_reader);
    drop(second_reader);

    let writer = rwlock.try_write().unwrap();
    assert_eq!(*writer, 42);

    assert!(rwlock.try_read().is_none());
    assert!(rwlock.clone().try_read_owned().is_none());
}

#[test]
fn get_mut_and_into_inner_use_exclusive_access() {
    let mut rwlock = RwLock::new(100);

    *rwlock.get_mut() = 200;

    assert_eq!(*rwlock.get_mut(), 200);
    assert_eq!(rwlock.into_inner(), 200);
}

#[test]
fn max_readers_limits_concurrent_readers() {
    let rwlock = RwLock::with_max_readers(10, NonZeroUsize::new(2).unwrap());

    let first = rwlock.try_read().unwrap();
    let second = rwlock.try_read().unwrap();
    assert!(rwlock.try_read().is_none());
    assert!(rwlock.try_write().is_none());

    drop(first);
    let replacement = rwlock.try_read().unwrap();
    assert!(rwlock.try_read().is_none());

    drop(second);
    drop(replacement);
    assert!(rwlock.try_write().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_readers_and_writers_preserve_values() {
    const READERS: usize = 50;
    const WRITERS: usize = 10;

    let rwlock = Arc::new(RwLock::new(0));
    let start = Arc::new(tokio::sync::Barrier::new(READERS + WRITERS + 1));
    let mut readers = Vec::with_capacity(READERS);
    let mut writers = Vec::with_capacity(WRITERS);

    for _ in 0..READERS {
        let rwlock = rwlock.clone();
        let start = start.clone();
        readers.push(tokio::spawn(async move {
            start.wait().await;
            let value = *rwlock.read().await;
            tokio::task::yield_now().await;
            value
        }));
    }

    for _ in 0..WRITERS {
        let rwlock = rwlock.clone();
        let start = start.clone();
        writers.push(tokio::spawn(async move {
            start.wait().await;
            let mut guard = rwlock.write().await;
            *guard += 1;
            tokio::task::yield_now().await;
        }));
    }

    start.wait().await;
    for reader in readers {
        assert!((0..=WRITERS as i32).contains(&reader.await.unwrap()));
    }
    for writer in writers {
        writer.await.unwrap();
    }
    assert_eq!(*rwlock.read().await, WRITERS as i32);
}

#[tokio::test]
async fn zero_sized_values_follow_locking_rules() {
    let rwlock = Arc::new(RwLock::new(()));

    let borrowed = rwlock.read().await;
    let owned = rwlock.clone().read_owned().await;
    assert!(rwlock.try_write().is_none());

    drop(borrowed);
    drop(owned);
    assert!(rwlock.try_write().is_some());
}

#[tokio::test]
async fn owned_write_mappings_preserve_lock_ownership() {
    let rwlock = Arc::new(RwLock::new(Some(vec![1, 2, 3])));
    let weak = Arc::downgrade(&rwlock);

    let identity = OwnedRwLockWriteGuard::map(rwlock.clone().write_owned().await, |value| value);
    drop(identity);

    let failed = OwnedRwLockWriteGuard::filter_map(rwlock.clone().write_owned().await, |_| {
        None::<&mut Vec<i32>>
    });
    let original = match failed {
        Ok(_) => panic!("mapping should fail"),
        Err(original) => original,
    };
    drop(original);

    let value =
        OwnedRwLockWriteGuard::filter_map(rwlock.clone().write_owned().await, Option::as_mut)
            .unwrap();
    let value = OwnedMappedRwLockWriteGuard::map(value, Vec::as_mut_slice);
    let mut value =
        OwnedMappedRwLockWriteGuard::filter_map(value, |value| value.first_mut()).unwrap();
    *value = 100;
    let value = value.downgrade();

    drop(rwlock);
    assert!(weak.upgrade().is_some());
    assert_eq!(*value, 100);

    drop(value);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn owned_read_mappings_preserve_lock_ownership() {
    let rwlock = Arc::new(RwLock::new(Some(vec![1, 2, 3])));
    let weak = Arc::downgrade(&rwlock);

    let identity = OwnedRwLockReadGuard::map(rwlock.clone().read_owned().await, |value| value);
    drop(identity);

    let failed =
        OwnedRwLockReadGuard::filter_map(rwlock.clone().read_owned().await, |_| None::<&Vec<i32>>);
    let original = match failed {
        Ok(_) => panic!("mapping should fail"),
        Err(original) => original,
    };
    drop(original);

    let value = OwnedRwLockReadGuard::filter_map(rwlock.clone().read_owned().await, Option::as_ref)
        .unwrap();
    let value = OwnedMappedRwLockReadGuard::map(value, Vec::as_slice);
    let value = OwnedMappedRwLockReadGuard::filter_map(value, |value| value.first()).unwrap();

    drop(rwlock);
    assert!(weak.upgrade().is_some());
    assert_eq!(*value, 1);

    drop(value);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn every_write_guard_type_can_downgrade() {
    let rwlock = Arc::new(RwLock::new((42, "test".to_string())));

    {
        let mut write_guard = rwlock.write().await;
        write_guard.0 = 100;

        let read_guard = write_guard.downgrade();
        assert_eq!(read_guard.0, 100);

        assert!(rwlock.try_write().is_none());
        let concurrent_read = rwlock.try_read().unwrap();
        assert_eq!(concurrent_read.0, 100);
        drop(concurrent_read);
        drop(read_guard);
    }

    {
        let mut owned_write = rwlock.clone().write_owned().await;
        owned_write.1 = "updated".to_string();

        let owned_read = owned_write.downgrade();
        assert_eq!(owned_read.1, "updated");

        assert!(rwlock.try_write().is_none());
        drop(owned_read);
    }

    {
        let write_guard = rwlock.write().await;
        let mut mapped_write = RwLockWriteGuard::map(write_guard, |data| &mut data.0);
        *mapped_write = 200;

        let mapped_read = mapped_write.downgrade();
        assert_eq!(*mapped_read, 200);

        assert!(rwlock.try_write().is_none());
        drop(mapped_read);
    }

    {
        let owned_write = rwlock.clone().write_owned().await;
        let mut owned_mapped = OwnedRwLockWriteGuard::map(owned_write, |data| &mut data.1);
        *owned_mapped = "final".to_string();

        let owned_mapped_read = owned_mapped.downgrade();
        assert_eq!(*owned_mapped_read, "final");

        let moved_task = tokio::spawn(async move {
            assert_eq!(*owned_mapped_read, "final");
        });
        moved_task.await.unwrap();
    }
}

#[tokio::test]
async fn downgraded_guard_counts_against_max_readers() {
    let rwlock = Arc::new(RwLock::with_max_readers(0, NonZeroUsize::new(3).unwrap()));

    let mut write_guard = rwlock.write().await;
    *write_guard = 100;

    let read_guard = write_guard.downgrade();
    assert_eq!(*read_guard, 100);

    let read2 = rwlock.try_read().unwrap();
    let read3 = rwlock.try_read().unwrap();
    assert_eq!(*read2, 100);
    assert_eq!(*read3, 100);

    assert!(rwlock.try_read().is_none());

    assert!(rwlock.try_write().is_none());

    drop(read2);
    let read4 = rwlock.try_read().unwrap();
    assert_eq!(*read4, 100);

    drop(read_guard);
    drop(read3);
    drop(read4);

    let write_guard2 = rwlock.try_write().unwrap();
    assert_eq!(*write_guard2, 100);
}

#[tokio::test]
async fn queued_writer_precedes_a_later_reader() {
    let rwlock = RwLock::new(0);
    let first_reader = rwlock.read().await;
    let mut writer = Box::pin(rwlock.write());
    let mut later_reader = Box::pin(rwlock.read());

    assert_pending!(poll_once(writer.as_mut()));
    assert_pending!(poll_once(later_reader.as_mut()));

    drop(first_reader);
    let mut writer_guard = assert_ready!(poll_once(writer.as_mut()));
    assert_pending!(poll_once(later_reader.as_mut()));

    *writer_guard = 100;
    drop(writer_guard);
    let reader_guard = assert_ready!(poll_once(later_reader.as_mut()));
    assert_eq!(*reader_guard, 100);
}
