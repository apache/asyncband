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
use std::marker::PhantomPinned;
use std::panic::RefUnwindSafe;
use std::panic::UnwindSafe;

use asyncband::mpsc;

#[test]
fn public_types_keep_their_auto_traits() {
    fn assert_send_and_sync<T: Send + Sync>() {}
    fn assert_unpin<T: Unpin>() {}

    assert_send_and_sync::<mpsc::SendError<i64>>();
    assert_send_and_sync::<mpsc::UnboundedSender<i64>>();
    assert_send_and_sync::<mpsc::UnboundedReceiver<i64>>();
    assert_send_and_sync::<mpsc::BoundedSender<i64>>();
    assert_send_and_sync::<mpsc::BoundedReceiver<i64>>();
    assert_unpin::<mpsc::SendError<i64>>();
    assert_unpin::<mpsc::UnboundedSender<i64>>();
    assert_unpin::<mpsc::UnboundedReceiver<i64>>();
    assert_unpin::<mpsc::BoundedSender<i64>>();
    assert_unpin::<mpsc::BoundedReceiver<i64>>();
}

#[test]
fn mpsc_endpoints_keep_legacy_traits_regardless_of_payload() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    fn assert_unpin<T: Unpin>() {}
    fn assert_unwind_safe<T: UnwindSafe>() {}
    fn assert_ref_unwind_safe<T: RefUnwindSafe>() {}

    macro_rules! assert_endpoint_traits {
        ($endpoint:ident, $payload:ty) => {
            assert_send::<mpsc::$endpoint<$payload>>();
            assert_sync::<mpsc::$endpoint<$payload>>();
            assert_unpin::<mpsc::$endpoint<$payload>>();
            assert_unwind_safe::<mpsc::$endpoint<$payload>>();
            assert_ref_unwind_safe::<mpsc::$endpoint<$payload>>();
        };
    }

    macro_rules! assert_payload_traits {
        ($endpoint:ident) => {
            assert_endpoint_traits!($endpoint, i32);
            assert_endpoint_traits!($endpoint, Cell<u8>);
            assert_endpoint_traits!($endpoint, &'static mut i32);
            assert_endpoint_traits!($endpoint, PhantomPinned);
        };
    }

    // Four endpoint types × four payloads × five traits = 80 compile-time assertions.
    assert_payload_traits!(BoundedSender);
    assert_payload_traits!(BoundedReceiver);
    assert_payload_traits!(UnboundedSender);
    assert_payload_traits!(UnboundedReceiver);
}
