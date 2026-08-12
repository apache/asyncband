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

use std::sync::Arc;

use super::*;

#[test]
fn test_try_lock_never_blocks() {
    // Test that try_lock and try_lock_owned never block, even under contention
    let mutex = Arc::new(Mutex::new(9));
    let _guard = mutex.try_lock().unwrap();

    let result = mutex.try_lock();
    assert!(result.is_none());

    let result = mutex.clone().try_lock_owned();
    assert!(result.is_none());
}

#[test]
fn test_get_mut_provides_exclusive_access() {
    // Test that get_mut provides direct access when we have exclusive ownership
    let mut mutex = Mutex::new(11);

    let data = mutex.get_mut();
    *data = 100;

    assert_eq!(*mutex.get_mut(), 100);

    let inner = mutex.into_inner();
    assert_eq!(inner, 100);
}

#[tokio::test]
async fn test_guard_map_preserves_lock() {
    let data = (99i32, vec![1, 2, 3]);
    let mutex = Mutex::new(data);

    let guard = mutex.lock().await;
    let mut mapped_guard = MutexGuard::map(guard, |data| &mut data.0);

    assert!(mutex.try_lock().is_none());

    *mapped_guard = 100;

    // After dropping, mutex should be available
    drop(mapped_guard);
    let guard = mutex.try_lock().unwrap();
    assert_eq!(guard.0, 100);
}

#[tokio::test]
async fn test_mapped_guard_holds_lock() {
    // Test that MappedMutexGuard properly holds the lock even after the original guard is moved
    let mutex = Arc::new(Mutex::new((10, 20)));

    let guard = mutex.lock().await;
    let mapped_guard = MutexGuard::map(guard, |data| &mut data.0);

    assert!(
        mutex.try_lock().is_none(),
        "Lock should be held by the mapped guard"
    );

    assert_eq!(*mapped_guard, 10);

    drop(mapped_guard);

    assert!(
        mutex.try_lock().is_some(),
        "Lock should be released after mapped guard is dropped"
    );
}

#[tokio::test]
async fn test_owned_mapped_guard_holds_lock() {
    // Test that mapped owned guard properly holds the lock
    let mutex = Arc::new(Mutex::new((30, 40)));

    let owned_guard = mutex.clone().lock_owned().await;
    let mapped_owned_guard = OwnedMutexGuard::map(owned_guard, |data| &mut data.1);

    assert!(
        mutex.try_lock().is_none(),
        "Lock should be held by the mapped owned guard"
    );

    assert_eq!(*mapped_owned_guard, 40);

    // When mapped owned guard is dropped, lock should be released
    drop(mapped_owned_guard);

    assert!(
        mutex.try_lock().is_some(),
        "Lock should be released after mapped owned guard is dropped"
    );
}

#[tokio::test]
async fn test_guard_filter_map_failure() {
    let data: Vec<i32> = vec![];
    let mutex = Mutex::new(data);
    let guard = mutex.lock().await;

    let result = MutexGuard::filter_map(guard, |vec| vec.get_mut(0));
    assert!(result.is_err());

    if let Err(mut original_guard) = result {
        original_guard.push(100);
        assert_eq!(*original_guard, vec![100]);
    } else {
        panic!("Expected Err, but got Ok");
    }
}

#[tokio::test]
async fn test_owned_guard_filter_map_failure() {
    let data: Vec<i32> = vec![];
    let mutex = Arc::new(Mutex::new(data));
    let guard = mutex.clone().lock_owned().await;

    let result = OwnedMutexGuard::filter_map(guard, |vec| vec.get_mut(0));
    assert!(result.is_err());

    if let Err(mut original_guard) = result {
        original_guard.push(200);
        assert_eq!(*original_guard, vec![200]);
    } else {
        panic!("Expected Err, but got Ok");
    }
}

#[tokio::test]
async fn test_multiple_map_operations() {
    // Test multiple consecutive map operations
    let data = vec![vec![1, 2], vec![3, 4]];
    let mutex = Mutex::new(data);

    let guard = mutex.lock().await;
    let first_vec = MutexGuard::map(guard, |data| &mut data[0]);
    let mut first_element = MappedMutexGuard::map(first_vec, |vec| &mut vec[0]);

    *first_element = 100;
    drop(first_element);

    let guard = mutex.lock().await;
    assert_eq!(guard[0][0], 100);
    assert_eq!(guard[0][1], 2);
    assert_eq!(guard[1][0], 3);
}

#[tokio::test]
async fn test_stress() {
    let mutex = Arc::new(Mutex::new(0));
    let mut handles = Vec::new();

    // Create many concurrent tasks
    for i in 0..1000 {
        let mutex = mutex.clone();
        handles.push(tokio::spawn(async move {
            let mut guard = mutex.lock().await;
            *guard += 1;
            if i % 10 == 0 {
                tokio::task::yield_now().await;
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let final_value = *mutex.lock().await;
    assert_eq!(final_value, 1000);
}

#[tokio::test]
async fn test_guard_prevents_concurrent_access() {
    // Test that holding a guard prevents other tasks from acquiring the lock
    let mutex = Arc::new(Mutex::new(0));
    let mutex_clone = mutex.clone();

    let guard = mutex.lock().await;

    assert!(
        mutex.try_lock().is_none(),
        "Lock should be held by the first guard"
    );

    let handle = tokio::spawn(async move {
        let _guard2 = mutex_clone.lock().await;
        123
    });

    tokio::task::yield_now().await;

    assert!(
        mutex.try_lock().is_none(),
        "Lock should still be held after yielding"
    );

    drop(guard);

    let result = handle.await.unwrap();
    assert_eq!(result, 123);

    assert!(
        mutex.try_lock().is_some(),
        "Lock should be available after all guards are dropped"
    );
}

#[test]
fn test_lock_panic_safety() {
    use std::panic::AssertUnwindSafe;

    let mutex = Arc::new(Mutex::new(0));
    let mutex_clone = mutex.clone();

    let result = std::panic::catch_unwind(AssertUnwindSafe(move || {
        let _guard = mutex_clone.try_lock().unwrap();
        panic!("test panic");
    }));

    assert!(result.is_err());
    // Lock should be released after panic
    assert!(mutex.try_lock().is_some());
}

#[tokio::test]
async fn test_async_lock_panic_safety() {
    // Test panic safety with async locks
    let mutex = Arc::new(Mutex::new(0));
    let mutex_clone = mutex.clone();

    let handle = tokio::spawn(async move {
        let _guard = mutex_clone.lock().await;
        panic!("async test panic");
    });

    // panic
    assert!(handle.await.is_err());

    let guard = mutex.try_lock();
    assert!(guard.is_some());
}

#[tokio::test]
async fn test_owned_guard_panic_safety() {
    let mutex = Arc::new(Mutex::new(0));
    let mutex_clone = mutex.clone();

    let handle = tokio::spawn(async move {
        let _guard = mutex_clone.clone().lock_owned().await;
        panic!("owned guard panic");
    });

    assert!(handle.await.is_err());

    // Lock should be available after the panicked task
    let guard = mutex.try_lock();
    assert!(guard.is_some());
}

#[tokio::test]
async fn test_mapped_guard_panic_safety() {
    // Test panic safety with mapped guards
    let mutex = Arc::new(Mutex::new((66, vec![1, 2, 3])));
    let mutex_clone = mutex.clone();

    let handle = tokio::spawn(async move {
        let guard = mutex_clone.lock().await;
        let _mapped = MutexGuard::map(guard, |data| &mut data.0);
        panic!("mapped guard panic");
    });

    assert!(handle.await.is_err());

    let guard = mutex.try_lock();
    assert!(guard.is_some());
}

#[tokio::test]
async fn test_memory_ordering_correctness() {
    // Test that mutex provides proper memory ordering guarantees
    // When one task modifies data under mutex protection,
    // another task should see the modification after acquiring the lock
    let mutex = Arc::new(Mutex::new(vec![1, 2, 3]));
    let mutex_clone = mutex.clone();

    let handle = tokio::spawn(async move {
        let mut guard = mutex_clone.lock().await;
        guard.push(4);
        guard[0] = 100;
        // Lock is released when guard is dropped
    });

    handle.await.unwrap();

    let guard = mutex.lock().await;
    assert_eq!(*guard, vec![100, 2, 3, 4]);
    // This test relies on mutex's acquire-release semantics to ensure
    // that modifications made in the critical section are visible
    // to subsequent lock acquisitions
}

#[tokio::test]
async fn test_mutex_zst() {
    // Test that Mutex works correctly with Zero-Sized Types
    let mutex = Arc::new(Mutex::new(()));

    let mutex_clone = mutex.clone();
    let handle = tokio::spawn(async move {
        let guard = mutex_clone.lock().await;
        *guard;
    });

    handle.await.unwrap();

    // try_lock and owned guard should also work with ZST
    let _owned_guard = mutex.clone().lock_owned().await;
    assert!(mutex.try_lock().is_none());

    drop(_owned_guard);
    let guard = mutex.try_lock().unwrap();
    *guard;
}

#[tokio::test]
async fn test_mapped_mutex_guard_send() {
    // Test that MappedMutexGuard can be sent across await points
    #[derive(Debug)]
    struct TestStruct {
        field1: i32,
        _field2: String,
    }

    let mutex = Arc::new(Mutex::new(TestStruct {
        field1: 2,
        _field2: "oh".to_owned(),
    }));

    let mutex_clone = mutex.clone();
    let handle = tokio::spawn(async move {
        let guard = mutex_clone.lock().await;
        let mapped_guard = MutexGuard::map(guard, |data| &mut data.field1);

        tokio::task::yield_now().await;
        *mapped_guard
    });

    let result = handle.await.unwrap();
    assert_eq!(result, 2);
}
