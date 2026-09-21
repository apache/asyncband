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

use asyncband::pool;

use super::support::Manager;

#[test]
fn public_types_keep_their_auto_traits() {
    fn assert_send_and_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}
    fn assert_unpin<T: Unpin>() {}

    assert_send_and_sync::<pool::bounded::Pool<Manager>>();
    assert_send_and_sync::<pool::bounded::Object<Manager>>();
    assert_send_and_sync::<pool::unbounded::Pool<i64>>();
    assert_send_and_sync::<pool::unbounded::Object<i64>>();
    assert_send_and_sync::<pool::unbounded::Pool<Cell<u8>>>();
    assert_unpin::<pool::bounded::Pool<Manager>>();
    assert_unpin::<pool::bounded::Object<Manager>>();
    assert_unpin::<pool::unbounded::Pool<i64>>();
    assert_unpin::<pool::unbounded::Object<i64>>();
    assert_send::<pool::unbounded::Object<Cell<u8>>>();
}

#[test]
fn unbounded_manual_manager_traits_do_not_depend_on_the_object() {
    fn assert_copy<T: Copy>() {}
    fn assert_debug<T: std::fmt::Debug>() {}

    assert_copy::<pool::unbounded::NeverManageObject<String>>();
    assert_debug::<pool::unbounded::NeverManageObject<String>>();
}
