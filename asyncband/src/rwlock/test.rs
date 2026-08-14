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

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Weak;

use super::*;
use crate::rwlock::OwnedMappedRwLockReadGuard;
use crate::rwlock::OwnedMappedRwLockWriteGuard;
use crate::rwlock::OwnedRwLockReadGuard;
use crate::rwlock::OwnedRwLockWriteGuard;

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
async fn test_guard_prevents_concurrent_access() {
    let rwlock = Arc::new(RwLock::new(0));
    let rwlock_clone = rwlock.clone();
    let writer_queued = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_queued_clone = writer_queued.clone();
    let writer_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_completed_clone = writer_completed.clone();

    let read_guard = rwlock.read().await;

    assert!(rwlock.try_write().is_none());

    let handle = tokio::spawn(async move {
        writer_queued_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        let mut write_guard = rwlock_clone.write().await;
        *write_guard = 123;
        writer_completed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        *write_guard
    });

    while !writer_queued.load(std::sync::atomic::Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }

    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    assert!(!writer_completed.load(std::sync::atomic::Ordering::SeqCst));
    assert!(rwlock.try_write().is_none());

    drop(read_guard);

    let result = handle.await.unwrap();
    assert_eq!(result, 123);
    assert!(writer_completed.load(std::sync::atomic::Ordering::SeqCst));

    // Verify the write took effect and lock is available
    let final_guard = rwlock.try_read().unwrap();
    assert_eq!(*final_guard, 123);
}

#[test]
fn test_lock_panic_safety() {
    // Test panic safety with synchronous locks
    use std::panic::AssertUnwindSafe;

    let rwlock = Arc::new(RwLock::new(0));
    let rwlock_clone = rwlock.clone();

    let result = std::panic::catch_unwind(AssertUnwindSafe(move || {
        let _guard = rwlock_clone.try_read().unwrap();
        panic!("test panic");
    }));

    assert!(result.is_err());
    // Lock should be released after panic
    assert!(rwlock.try_read().is_some());
}

#[tokio::test]
async fn test_async_lock_panic_safety() {
    // Test panic safety with async locks
    let rwlock = Arc::new(RwLock::new(0));
    let rwlock_clone = rwlock.clone();

    let handle = tokio::spawn(async move {
        let _guard = rwlock_clone.read().await;
        panic!("async test panic");
    });

    // panic
    assert!(handle.await.is_err());

    let guard = rwlock.try_read();
    assert!(guard.is_some());
}

#[tokio::test]
async fn test_owned_guard_panic_safety() {
    // Test panic safety with owned guards
    let rwlock = Arc::new(RwLock::new(0));
    let rwlock_clone = rwlock.clone();

    let handle = tokio::spawn(async move {
        let _guard = rwlock_clone.clone().read_owned().await;
        panic!("owned guard panic");
    });

    assert!(handle.await.is_err());

    // Lock should be available after the panicked task
    let guard = rwlock.try_read();
    assert!(guard.is_some());
}

#[tokio::test]
async fn test_mapped_guard_panic_safety() {
    // Test panic safety with mapped guards
    let rwlock = Arc::new(RwLock::new((66, vec![1, 2, 3])));
    let rwlock_clone = rwlock.clone();

    let handle = tokio::spawn(async move {
        let guard = rwlock_clone.read().await;
        let _mapped = RwLockReadGuard::map(guard, |data| &data.0);
        panic!("mapped guard panic");
    });

    assert!(handle.await.is_err());

    let guard = rwlock.try_read();
    assert!(guard.is_some());
}

#[tokio::test]
async fn test_mapped_write_guard_panic_safety() {
    // Test panic safety with mapped write guards
    let rwlock = Arc::new(RwLock::new((42, String::from("test"))));
    let rwlock_clone = rwlock.clone();

    let handle = tokio::spawn(async move {
        let guard = rwlock_clone.write().await;
        let _mapped = RwLockWriteGuard::map(guard, |data| &mut data.0);
        panic!("mapped write guard panic");
    });

    assert!(handle.await.is_err());

    let guard = rwlock.try_write();
    assert!(guard.is_some());
}

#[tokio::test]
async fn test_owned_mapped_read_guard_panic_safety() {
    // Test panic safety with owned mapped read guards
    let rwlock = Arc::new(RwLock::new((100, vec![4, 5, 6])));
    let rwlock_clone = rwlock.clone();

    let handle = tokio::spawn(async move {
        let guard = rwlock_clone.read_owned().await;
        let _mapped = OwnedRwLockReadGuard::map(guard, |data| &data.1);
        panic!("owned mapped read guard panic");
    });

    assert!(handle.await.is_err());

    let guard = rwlock.try_read();
    assert!(guard.is_some());
}

#[tokio::test]
async fn test_owned_mapped_write_guard_panic_safety() {
    // Test panic safety with owned mapped write guards
    let rwlock = Arc::new(RwLock::new((200, String::from("owned"))));
    let rwlock_clone = rwlock.clone();

    let handle = tokio::spawn(async move {
        let guard = rwlock_clone.write_owned().await;
        let _mapped = OwnedRwLockWriteGuard::map(guard, |data| &mut data.1);
        panic!("owned mapped write guard panic");
    });

    assert!(handle.await.is_err());

    let guard = rwlock.try_write();
    assert!(guard.is_some());
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
async fn test_rwlock_debug_when_locked() {
    let rwlock = Arc::new(RwLock::new(78));

    let rwlock_debug_unlocked = format!("{rwlock:?}");
    assert!(
        rwlock_debug_unlocked.contains("78"),
        "RwLock Debug should show value when unlocked, got: {rwlock_debug_unlocked}"
    );

    let write_guard = rwlock.write().await;
    let rwlock_debug_write = format!("{rwlock:?}");
    assert!(
        rwlock_debug_write.contains("<locked>"),
        "RwLock Debug should show <locked> when write lock is held, got: {rwlock_debug_write}"
    );
    drop(write_guard);

    let read_guard = rwlock.read().await;
    let rwlock_debug_read = format!("{rwlock:?}");

    let shows_value = rwlock_debug_read.contains("78");
    let shows_locked = rwlock_debug_read.contains("<locked>");
    assert!(
        shows_value || shows_locked,
        "RwLock Debug with read lock should show either value or <locked>, got: {rwlock_debug_read}"
    );

    drop(read_guard);

    let rwlock_debug_final = format!("{rwlock:?}");
    assert!(
        rwlock_debug_final.contains("78"),
        "RwLock Debug should show value when all locks are released, got: {rwlock_debug_final}"
    );
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
async fn test_downgrade_allows_concurrent_readers() {
    // Test that downgrading a write lock allows other readers to acquire the lock.
    let rwlock = Arc::new(RwLock::new(0i32));
    let writer_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let downgrade_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let writer_rwlock = rwlock.clone();
    let writer_started_clone = writer_started.clone();
    let downgrade_completed_clone = downgrade_completed.clone();

    let writer_handle = tokio::spawn(async move {
        let mut write_guard = writer_rwlock.write().await;
        *write_guard = 42;
        writer_started_clone.store(true, std::sync::atomic::Ordering::SeqCst);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let read_guard = write_guard.downgrade();
        downgrade_completed_clone.store(true, std::sync::atomic::Ordering::SeqCst);

        assert_eq!(*read_guard, 42);

        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        *read_guard
    });

    while !writer_started.load(std::sync::atomic::Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }

    let mut reader_handles = vec![];
    for i in 0..5 {
        let reader_rwlock = rwlock.clone();
        let downgrade_completed_clone = downgrade_completed.clone();

        let handle = tokio::spawn(async move {
            while !downgrade_completed_clone.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }

            let read_guard = reader_rwlock.read().await;
            assert_eq!(*read_guard, 42);
            (i, *read_guard)
        });
        reader_handles.push(handle);
    }

    // All tasks should complete successfully
    let writer_result = writer_handle.await.unwrap();
    assert_eq!(writer_result, 42);

    for handle in reader_handles {
        let (reader_id, value) = handle.await.unwrap();
        assert_eq!(value, 42, "Reader {reader_id} should see the written value");
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
async fn test_downgrade_panic_safety() {
    let rwlock = Arc::new(RwLock::new((0, "test".to_string())));

    {
        let rwlock_clone = rwlock.clone();
        let handle = tokio::spawn(async move {
            let mut write_guard = rwlock_clone.write().await;
            write_guard.0 = 42;
            let read_guard = write_guard.downgrade();
            assert_eq!(read_guard.0, 42);
            panic!("test panic after downgrade");
        });

        assert!(handle.await.is_err());

        let guard = rwlock.try_read().unwrap();
        assert_eq!(guard.0, 42);
        drop(guard);
    }

    {
        let rwlock_clone = rwlock.clone();
        let handle = tokio::spawn(async move {
            let owned_write = rwlock_clone.write_owned().await;
            let mut mapped_write = OwnedRwLockWriteGuard::map(owned_write, |data| &mut data.1);
            *mapped_write = "panic_test".to_string();
            let _mapped_read = mapped_write.downgrade();
            panic!("test panic with owned mapped downgrade");
        });

        assert!(handle.await.is_err());

        let guard = rwlock.try_read().unwrap();
        assert_eq!(guard.1, "panic_test");
    }
}

#[tokio::test]
async fn test_downgrade_prevents_deadlock() {
    // Test the classic deadlock prevention scenario
    // Demonstrates how downgrade enables safe lock ordering patterns

    let rwlock = Arc::new(RwLock::new(vec![1, 2, 3]));

    let rwlock1 = rwlock.clone();
    let task1 = tokio::spawn(async move {
        let mut write_guard = rwlock1.write().await;
        write_guard.push(4);
        let len_after_write = write_guard.len();

        let read_guard = write_guard.downgrade();

        assert_eq!(read_guard.len(), len_after_write);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let final_len = read_guard.len();
        drop(read_guard);
        final_len
    });

    let rwlock2 = rwlock.clone();
    let task2 = tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;

        let mut write_guard = rwlock2.write().await;
        write_guard.push(5);

        let read_guard = write_guard.downgrade();
        let final_len = read_guard.len();

        drop(read_guard);
        final_len
    });

    let rwlock3 = rwlock.clone();
    let task3 = tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(15)).await;

        let read_guard = rwlock3.read().await;
        read_guard.len()
    });

    let result1 = task1.await.unwrap();
    let result2 = task2.await.unwrap();
    let result3 = task3.await.unwrap();

    assert_eq!(result1, 4); // After first push
    assert_eq!(result2, 5); // After second push
    assert_eq!(result3, 5); // Final state

    let final_read = rwlock.read().await;
    assert_eq!(final_read.len(), 5);
    assert_eq!(*final_read, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_downgrade_with_waiting_writers() {
    let rwlock = Arc::new(RwLock::new(0i32));
    let writer_queued = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let downgrade_done = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let rwlock1 = rwlock.clone();
    let downgrade_done_clone = downgrade_done.clone();
    let downgrade_task = tokio::spawn(async move {
        let mut write_guard = rwlock1.write().await;
        *write_guard = 42;

        let read_guard = write_guard.downgrade();
        downgrade_done_clone.store(true, std::sync::atomic::Ordering::SeqCst);

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        assert_eq!(*read_guard, 42);

        drop(read_guard);
        42
    });

    while !downgrade_done.load(std::sync::atomic::Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }

    let rwlock2 = rwlock.clone();
    let writer_queued_clone = writer_queued.clone();
    let writer_task = tokio::spawn(async move {
        writer_queued_clone.store(true, std::sync::atomic::Ordering::SeqCst);

        let mut write_guard = rwlock2.write().await;

        assert_eq!(*write_guard, 42);
        *write_guard = 100;

        drop(write_guard);
        100
    });

    while !writer_queued.load(std::sync::atomic::Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let mut reader_tasks = vec![];
    for i in 0..3 {
        let rwlock_clone = rwlock.clone();

        let reader_task = tokio::spawn(async move {
            let read_guard = rwlock_clone.read().await;
            let value = *read_guard;
            drop(read_guard);
            (i, value)
        });
        reader_tasks.push(reader_task);
    }

    let downgrade_result = downgrade_task.await.unwrap();
    assert_eq!(downgrade_result, 42);

    let writer_result = writer_task.await.unwrap();
    assert_eq!(writer_result, 100);

    for reader_task in reader_tasks {
        let (reader_id, value) = reader_task.await.unwrap();
        // The readers might see either value depending on exact timing,
        // so let's not make strict assertions here
        println!("Reader {reader_id} saw value: {value}");
    }

    let final_read = rwlock.read().await;
    assert_eq!(*final_read, 100);
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
