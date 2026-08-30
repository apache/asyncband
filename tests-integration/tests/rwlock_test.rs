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
use std::sync::Weak;

use asyncband::rwlock::*;
use tests_integration::poll_once;
use tokio_test::assert_pending;
use tokio_test::assert_ready;

#[test]
fn test_try_read_write_never_blocks() {
    // Test that try_read and try_write never block
    let rwlock = Arc::new(RwLock::new(42));

    let r1 = rwlock.try_read().unwrap();
    let _r2 = rwlock.try_read().unwrap();
    assert_eq!(*r1, 42);
    assert_eq!(*_r2, 42);

    assert!(rwlock.try_write().is_none());

    drop(r1);
    drop(_r2);

    let w = rwlock.try_write().unwrap();
    assert_eq!(*w, 42);

    assert!(rwlock.try_read().is_none());
    assert!(rwlock.clone().try_read_owned().is_none());
}

#[test]
fn test_get_mut_provides_exclusive_access() {
    // Test that get_mut provides exclusive access to the data
    let mut rwlock = RwLock::new(100);

    let data = rwlock.get_mut();
    *data = 200;

    assert_eq!(*rwlock.get_mut(), 200);

    let inner = rwlock.into_inner();
    assert_eq!(inner, 200);
}

#[test]
fn test_with_max_readers() {
    let rwlock = RwLock::with_max_readers(10, NonZeroUsize::new(2).unwrap());

    let r1 = rwlock.try_read().unwrap();
    let r2 = rwlock.try_read().unwrap();
    assert_eq!(*r1, 10);
    assert_eq!(*r2, 10);

    assert!(rwlock.try_read().is_none());

    assert!(rwlock.try_write().is_none());

    drop(r1);

    let r3 = rwlock.try_read().unwrap();
    assert_eq!(*r3, 10);

    assert!(rwlock.try_read().is_none());

    drop(r2);
    drop(r3);

    let mut w = rwlock.try_write().unwrap();
    *w = 20;
    drop(w);

    let r = rwlock.try_read().unwrap();
    assert_eq!(*r, 20);
}

#[tokio::test]
async fn test_stress_concurrent_readers_writers() {
    // Test concurrent readers and writers with RwLock
    let rwlock = Arc::new(RwLock::new(0i32));
    let mut reader_results = Vec::new();
    let mut writer_results = Vec::new();

    // Spawn reader tasks
    for i in 0..50 {
        let rwlock_clone = rwlock.clone();
        let handle = tokio::spawn(async move {
            let guard = rwlock_clone.read().await;
            let value = *guard;
            tokio::task::yield_now().await;
            (i, value)
        });
        reader_results.push(handle);
    }

    // Spawn writer tasks
    for i in 0..10 {
        let rwlock_clone = rwlock.clone();
        let handle = tokio::spawn(async move {
            let mut guard = rwlock_clone.write().await;
            let old_value = *guard;
            *guard += 1;
            tokio::task::yield_now().await;
            (i + 100, old_value, *guard)
        });
        writer_results.push(handle);
    }

    for handle in reader_results {
        let (reader_id, value) = handle.await.unwrap();
        assert!(
            (0..=10).contains(&value),
            "Reader {reader_id} saw invalid value: {value}"
        );
    }

    let mut writer_values = Vec::new();
    for handle in writer_results {
        let (writer_id, old_value, new_value) = handle.await.unwrap();
        assert_eq!(
            new_value,
            old_value + 1,
            "Writer {writer_id} increment failed: {old_value} -> {new_value}"
        );
        writer_values.push((old_value, new_value));
    }

    let final_guard = rwlock.read().await;
    assert_eq!(
        *final_guard, 10,
        "Final value should be 10 after 10 increments"
    );

    writer_values.sort_by_key(|(old, _)| *old);
    for (i, (old_value, new_value)) in writer_values.iter().enumerate() {
        assert_eq!(
            *old_value, i as i32,
            "Writer operations should be sequential"
        );
        assert_eq!(
            *new_value,
            (i + 1) as i32,
            "Each increment should be atomic"
        );
    }
}

#[tokio::test]
async fn test_memory_ordering_correctness() {
    // Test that rwlock provides proper memory ordering guarantees
    // When one task modifies data under rwlock protection,
    // another task should see the modification after acquiring the lock
    let rwlock = Arc::new(RwLock::new(vec![1, 2, 3]));
    let rwlock_clone = rwlock.clone();

    let handle = tokio::spawn(async move {
        let mut guard = rwlock_clone.write().await;
        guard.push(4);
        guard[0] = 100;
        // Lock is released when guard is dropped
    });

    handle.await.unwrap();

    let guard = rwlock.read().await;
    assert_eq!(*guard, vec![100, 2, 3, 4]);
}

#[tokio::test]
async fn test_rwlock_zst() {
    // Test that RwLock works correctly with Zero-Sized Types
    let rwlock = Arc::new(RwLock::new(()));

    let rwlock_clone = rwlock.clone();
    let handle = tokio::spawn(async move {
        let guard = rwlock_clone.read().await;
        *guard;
    });

    handle.await.unwrap();

    let guard1 = rwlock.read().await;
    let guard2 = rwlock.clone().read_owned().await;
    *guard1;
    *guard2;

    assert!(rwlock.try_write().is_none());

    drop(guard1);
    drop(guard2);

    let mut write_guard = rwlock.write().await;
    *write_guard = ();
    drop(write_guard);

    let try_write_guard = rwlock.try_write().unwrap();
    *try_write_guard;
    drop(try_write_guard);

    let guard = rwlock.try_read().unwrap();
    *guard;
}

#[tokio::test]
async fn test_owned_write_guard_map_memory_leak() {
    // Test OwnedRwLockWriteGuard::map memory management
    let rwlock = Arc::new(RwLock::new(29u32));
    let weak_ref: Weak<RwLock<u32>> = Arc::downgrade(&rwlock);

    {
        let write_guard = rwlock.clone().write_owned().await;
        let mut mapped_guard = OwnedRwLockWriteGuard::map(write_guard, |data| data);
        *mapped_guard = 100;
        assert_eq!(*mapped_guard, 100);
    }

    drop(rwlock);
    assert!(
        weak_ref.upgrade().is_none(),
        "expected Arc to be dropped (no strong refs)"
    );
}

#[tokio::test]
async fn test_owned_write_guard_filter_map_memory_leak() {
    // Test OwnedRwLockWriteGuard::filter_map memory management

    // Test success case
    {
        let rwlock = Arc::new(RwLock::new(Some(29u32)));
        let weak_ref: Weak<RwLock<Option<u32>>> = Arc::downgrade(&rwlock);

        {
            let write_guard = rwlock.clone().write_owned().await;
            let mut mapped_guard =
                OwnedRwLockWriteGuard::filter_map(write_guard, |data| data.as_mut())
                    .expect("Should succeed");
            *mapped_guard = 100;
            assert_eq!(*mapped_guard, 100);
        }

        drop(rwlock);
        assert!(
            weak_ref.upgrade().is_none(),
            "expected Arc to be dropped after filter_map success"
        );
    }

    // Test failure case
    {
        let rwlock = Arc::new(RwLock::new(None::<u32>));
        let weak_ref: Weak<RwLock<Option<u32>>> = Arc::downgrade(&rwlock);

        {
            let write_guard = rwlock.clone().write_owned().await;
            let result = OwnedRwLockWriteGuard::filter_map(write_guard, |data| data.as_mut());
            assert!(result.is_err(), "filter_map should have failed for None");
        }

        drop(rwlock);
        assert!(
            weak_ref.upgrade().is_none(),
            "expected Arc to be dropped after filter_map failure"
        );
    }
}

#[tokio::test]
async fn test_owned_mapped_write_guard_map_memory_leak() {
    // Test OwnedMappedRwLockWriteGuard::map memory management
    let rwlock = Arc::new(RwLock::new("test".to_string()));
    let weak_ref: Weak<RwLock<String>> = Arc::downgrade(&rwlock);

    {
        let write_guard = rwlock.clone().write_owned().await;
        let mapped_guard1 = OwnedRwLockWriteGuard::map(write_guard, |s| s);
        let mut mapped_guard2 = OwnedMappedRwLockWriteGuard::map(mapped_guard1, |s| s.as_mut_str());
        mapped_guard2.make_ascii_uppercase();
        assert_eq!(&*mapped_guard2, "TEST");
    }

    drop(rwlock);
    assert!(
        weak_ref.upgrade().is_none(),
        "expected Arc to be dropped (no strong refs)"
    );
}

#[tokio::test]
async fn test_owned_mapped_write_guard_filter_map_memory_leak() {
    // Test OwnedMappedRwLockWriteGuard::filter_map memory management

    // Test success case
    {
        let rwlock = Arc::new(RwLock::new(vec![1, 2, 3]));
        let weak_ref: Weak<RwLock<Vec<i32>>> = Arc::downgrade(&rwlock);

        {
            let write_guard = rwlock.clone().write_owned().await;
            let mapped_guard1 = OwnedRwLockWriteGuard::map(write_guard, |v| v);
            let mut mapped_guard2 = OwnedMappedRwLockWriteGuard::filter_map(mapped_guard1, |v| {
                if !v.is_empty() { Some(&mut v[0]) } else { None }
            })
            .expect("Should succeed");
            *mapped_guard2 = 100;
            assert_eq!(*mapped_guard2, 100);
        }

        drop(rwlock);
        assert!(
            weak_ref.upgrade().is_none(),
            "Memory leak detected on filter_map success"
        );
    }

    // Test failure case
    {
        let rwlock = Arc::new(RwLock::new(Vec::<i32>::new()));
        let weak_ref: Weak<RwLock<Vec<i32>>> = Arc::downgrade(&rwlock);

        {
            let write_guard = rwlock.clone().write_owned().await;
            let mapped_guard1 = OwnedRwLockWriteGuard::map(write_guard, |v| v);
            let result = OwnedMappedRwLockWriteGuard::filter_map(mapped_guard1, |v| {
                if !v.is_empty() { Some(&mut v[0]) } else { None }
            });

            assert!(
                result.is_err(),
                "filter_map should have failed for empty vector"
            );
            if let Err(original_guard) = result {
                assert!(original_guard.is_empty());
            }
        }

        drop(rwlock);
        assert!(
            weak_ref.upgrade().is_none(),
            "Memory leak detected on filter_map failure"
        );
    }
}

#[tokio::test]
async fn test_owned_read_guard_map_memory_leak() {
    // Test OwnedRwLockReadGuard::map memory management
    let rwlock = Arc::new(RwLock::new(29u32));
    let weak_ref: Weak<RwLock<u32>> = Arc::downgrade(&rwlock);

    {
        let read_guard = rwlock.clone().read_owned().await;
        let mapped_guard = OwnedRwLockReadGuard::map(read_guard, |data| data);
        assert_eq!(*mapped_guard, 29);
    }

    drop(rwlock);
    assert!(
        weak_ref.upgrade().is_none(),
        "expected Arc to be dropped (no strong ref; potential memory leaks)"
    );
}

#[tokio::test]
async fn test_owned_read_guard_filter_map_memory_leak() {
    // Test OwnedRwLockReadGuard::filter_map memory management

    // Test success case
    {
        let rwlock = Arc::new(RwLock::new(Some(29u32)));
        let weak_ref: Weak<RwLock<Option<u32>>> = Arc::downgrade(&rwlock);

        {
            let read_guard = rwlock.clone().read_owned().await;
            let mapped_guard = OwnedRwLockReadGuard::filter_map(read_guard, |data| data.as_ref())
                .expect("filter_map should succeed for Some(_) value");
            assert_eq!(*mapped_guard, 29);
        }

        drop(rwlock);
        assert!(
            weak_ref.upgrade().is_none(),
            "expected Arc to be dropped after filter_map success"
        );
    }

    // Test failure case
    {
        let rwlock = Arc::new(RwLock::new(None::<u32>));
        let weak_ref: Weak<RwLock<Option<u32>>> = Arc::downgrade(&rwlock);

        {
            let read_guard = rwlock.clone().read_owned().await;
            let result = OwnedRwLockReadGuard::filter_map(read_guard, |data| data.as_ref());
            assert!(result.is_err(), "filter_map should have failed for None");
        }

        drop(rwlock);
        assert!(
            weak_ref.upgrade().is_none(),
            "expected Arc to be dropped after filter_map failure"
        );
    }
}

#[tokio::test]
async fn test_owned_mapped_read_guard_map_memory_leak() {
    // Test OwnedMappedRwLockReadGuard::map memory management
    let rwlock = Arc::new(RwLock::new("test".to_string()));
    let weak_ref: Weak<RwLock<String>> = Arc::downgrade(&rwlock);

    {
        let read_guard = rwlock.clone().read_owned().await;
        let mapped_guard1 = OwnedRwLockReadGuard::map(read_guard, |s| s);
        let mapped_guard2 = OwnedMappedRwLockReadGuard::map(mapped_guard1, |s| s.as_str());
        assert_eq!(&*mapped_guard2, "test");
    }

    drop(rwlock);
    assert!(
        weak_ref.upgrade().is_none(),
        "Memory leak detected: Arc was not deallocated"
    );
}

#[tokio::test]
async fn test_owned_mapped_read_guard_filter_map_memory_leak() {
    // Test OwnedMappedRwLockReadGuard::filter_map memory management

    // Test success case
    {
        let rwlock = Arc::new(RwLock::new(Some(29u32)));
        let weak_ref: Weak<RwLock<Option<u32>>> = Arc::downgrade(&rwlock);

        {
            let read_guard = rwlock.clone().read_owned().await;
            let mapped_guard1 = OwnedRwLockReadGuard::map(read_guard, |data| data);
            let mapped_guard2 =
                OwnedMappedRwLockReadGuard::filter_map(mapped_guard1, |opt| opt.as_ref())
                    .expect("filter_map should succeed for Some(_) value");
            assert_eq!(*mapped_guard2, 29);
        }

        drop(rwlock);
        assert!(
            weak_ref.upgrade().is_none(),
            "expected Arc to be dropped after filter_map success"
        );
    }

    // Test failure case
    {
        let rwlock = Arc::new(RwLock::new(None::<u32>));
        let weak_ref: Weak<RwLock<Option<u32>>> = Arc::downgrade(&rwlock);

        {
            let read_guard = rwlock.clone().read_owned().await;
            let mapped_guard1 = OwnedRwLockReadGuard::map(read_guard, |data| data);
            let result = OwnedMappedRwLockReadGuard::filter_map(mapped_guard1, |opt| opt.as_ref());
            assert!(result.is_err(), "filter_map should have failed for None");
        }

        drop(rwlock);
        assert!(
            weak_ref.upgrade().is_none(),
            "Memory leak detected on filter_map failure"
        );
    }
}

#[tokio::test]
async fn test_downgrade_atomicity() {
    // Test atomic downgrade behavior for all guard types
    let rwlock = Arc::new(RwLock::new((42, "test".to_string())));

    // Test basic write guard downgrade
    {
        let mut write_guard = rwlock.write().await;
        write_guard.0 = 100;

        let read_guard = write_guard.downgrade();
        assert_eq!(read_guard.0, 100);

        // Writers blocked, readers allowed
        assert!(rwlock.try_write().is_none());
        let concurrent_read = rwlock.try_read().unwrap();
        assert_eq!(concurrent_read.0, 100);
        drop(concurrent_read);
        drop(read_guard);
    }

    // Test owned write guard downgrade
    {
        let mut owned_write = rwlock.clone().write_owned().await;
        owned_write.1 = "updated".to_string();

        let owned_read = owned_write.downgrade();
        assert_eq!(owned_read.1, "updated");

        assert!(rwlock.try_write().is_none());
        drop(owned_read);
    }

    // Test mapped write guard downgrade
    {
        let write_guard = rwlock.write().await;
        let mut mapped_write = RwLockWriteGuard::map(write_guard, |data| &mut data.0);
        *mapped_write = 200;

        let mapped_read = mapped_write.downgrade();
        assert_eq!(*mapped_read, 200);

        assert!(rwlock.try_write().is_none());
        drop(mapped_read);
    }

    // Test owned mapped write guard downgrade
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
async fn test_downgrade_with_max_readers() {
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

#[tokio::test]
async fn test_owned_write_guard_downgrade_no_memory_leak() {
    let rwlock = Arc::new(RwLock::new(42i32));
    let weak_ref = Arc::downgrade(&rwlock);

    {
        let write_guard = rwlock.clone().write_owned().await;
        let _read_guard = write_guard.downgrade();
    }

    drop(rwlock);

    assert_eq!(
        weak_ref.strong_count(),
        0,
        "Memory leak detected in OwnedRwLockWriteGuard downgrade!"
    );
}

#[tokio::test]
async fn test_owned_mapped_write_guard_downgrade_no_memory_leak() {
    #[derive(Debug)]
    struct Data {
        value: i32,
    }

    let rwlock = Arc::new(RwLock::new(Data { value: 42 }));
    let weak_ref = Arc::downgrade(&rwlock);

    {
        let write_guard = rwlock.clone().write_owned().await;
        let mapped_write_guard = OwnedRwLockWriteGuard::map(write_guard, |data| &mut data.value);
        let _mapped_read_guard = mapped_write_guard.downgrade();
    }

    drop(rwlock);

    assert_eq!(
        weak_ref.strong_count(),
        0,
        "Memory leak detected in OwnedMappedRwLockWriteGuard downgrade!"
    );
}
