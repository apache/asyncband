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

use asyncband::watch;

#[test]
fn public_types_keep_their_auto_traits() {
    fn assert_send_and_sync<T: Send + Sync>() {}
    fn assert_unpin<T: Unpin>() {}
    fn assert_send_value<T: Send>(_: T) {}

    assert_send_and_sync::<watch::Sender<i64>>();
    assert_send_and_sync::<watch::Receiver<i64>>();
    assert_send_and_sync::<watch::SendError<i64>>();
    assert_send_and_sync::<watch::RecvError>();
    assert_unpin::<watch::Sender<i64>>();
    assert_unpin::<watch::Receiver<i64>>();
    assert_unpin::<watch::SendError<i64>>();
    assert_unpin::<watch::RecvError>();

    let (_tx, mut rx) = watch::channel(0);
    assert_send_value(rx.changed());
    assert_send_value(rx.recv());
}
