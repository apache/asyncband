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

use asyncband::mpmc;

#[test]
fn public_types_keep_their_auto_traits() {
    fn assert_send_and_sync<T: Send + Sync>() {}
    fn assert_unpin<T: Unpin>() {}
    fn assert_send_value<T: Send>(_: T) {}

    assert_send_and_sync::<mpmc::SendError<i64>>();
    assert_send_and_sync::<mpmc::UnboundedSender<i64>>();
    assert_send_and_sync::<mpmc::UnboundedReceiver<i64>>();
    assert_send_and_sync::<mpmc::BoundedSender<i64>>();
    assert_send_and_sync::<mpmc::BoundedReceiver<i64>>();
    assert_send_and_sync::<mpmc::UnboundedSender<Cell<u8>>>();
    assert_send_and_sync::<mpmc::UnboundedReceiver<Cell<u8>>>();
    assert_send_and_sync::<mpmc::BoundedSender<Cell<u8>>>();
    assert_send_and_sync::<mpmc::BoundedReceiver<Cell<u8>>>();
    assert_unpin::<mpmc::SendError<i64>>();
    assert_unpin::<mpmc::UnboundedSender<i64>>();
    assert_unpin::<mpmc::UnboundedReceiver<i64>>();
    assert_unpin::<mpmc::BoundedSender<i64>>();
    assert_unpin::<mpmc::BoundedReceiver<i64>>();

    let (unbounded_sender, unbounded_receiver) = mpmc::unbounded::<Cell<u8>>();
    assert_send_value(unbounded_receiver.recv());
    drop(unbounded_sender);

    let (bounded_sender, bounded_receiver) = mpmc::bounded::<Cell<u8>>(1);
    assert_send_value(bounded_sender.send(Cell::new(0)));
    assert_send_value(bounded_receiver.recv());
}
