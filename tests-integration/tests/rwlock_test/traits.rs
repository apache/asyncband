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

use asyncband::rwlock::OwnedRwLockReadGuard;
use asyncband::rwlock::RwLock;
use asyncband::rwlock::RwLockReadGuard;
use asyncband::rwlock::RwLockWriteGuard;

#[test]
fn public_types_keep_their_auto_traits() {
    fn assert_send_and_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}
    fn assert_unpin<T: Unpin>() {}

    assert_send_and_sync::<RwLock<i64>>();
    assert_send_and_sync::<OwnedRwLockReadGuard<i64>>();
    assert_send_and_sync::<RwLockReadGuard<'_, i64>>();
    assert_send_and_sync::<RwLockWriteGuard<'_, i64>>();
    assert_unpin::<RwLock<i64>>();
    assert_unpin::<RwLockReadGuard<'_, i64>>();
    assert_unpin::<RwLockWriteGuard<'_, i64>>();
    assert_send::<RwLockReadGuard<'_, std::sync::MutexGuard<'static, ()>>>();
}
