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

use std::sync::Arc;

use asyncband::mutex::MappedMutexGuard;
use asyncband::mutex::Mutex;
use asyncband::mutex::MutexGuard;
use asyncband::mutex::OwnedMappedMutexGuard;
use asyncband::mutex::OwnedMutexGuard;

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
