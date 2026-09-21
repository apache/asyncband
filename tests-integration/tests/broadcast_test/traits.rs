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

use asyncband::broadcast;

#[test]
fn public_types_keep_their_auto_traits() {
    fn assert_send_and_sync<T: Send + Sync>() {}
    fn assert_unpin<T: Unpin>() {}

    assert_send_and_sync::<broadcast::mpmc::UnboundedSender<i64>>();
    assert_send_and_sync::<broadcast::mpmc::UnboundedReceiver<i64>>();
    assert_send_and_sync::<broadcast::mpmc::BoundedSender<i64>>();
    assert_send_and_sync::<broadcast::mpmc::BoundedReceiver<i64>>();
    assert_send_and_sync::<broadcast::mpmc::RecvError>();
    assert_send_and_sync::<broadcast::mpmc::TryRecvError>();
    assert_send_and_sync::<broadcast::mpmc::TrySendError<i64>>();
    assert_unpin::<broadcast::mpmc::UnboundedSender<i64>>();
    assert_unpin::<broadcast::mpmc::UnboundedReceiver<i64>>();
    assert_unpin::<broadcast::mpmc::BoundedSender<i64>>();
    assert_unpin::<broadcast::mpmc::BoundedReceiver<i64>>();
    assert_unpin::<broadcast::mpmc::RecvError>();
    assert_unpin::<broadcast::mpmc::TryRecvError>();
    assert_unpin::<broadcast::mpmc::TrySendError<i64>>();
}
