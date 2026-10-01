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

use asyncband::mpmc;
use tests_integration::WakeCounter;
use tests_integration::assert_completes_without_deadlock;
use tests_integration::expect_ready;
use tests_integration::poll_with;
use tests_integration::waker_on_drop;

#[test]
fn replacing_a_send_waker_allows_its_destructor_to_receive() {
    assert_completes_without_deadlock(|| {
        let (sender, receiver) = mpmc::bounded(1);
        sender.try_send(0).unwrap();
        let reentrant = receiver.clone();
        let first = waker_on_drop(move || assert_eq!(reentrant.try_recv(), Ok(0)));
        let (second, second_wakes) = WakeCounter::new();
        let mut send = Box::pin(sender.send(1));

        assert!(poll_with(send.as_mut(), &first).is_pending());
        drop(first);
        // Replacing the stored waker frees capacity and wakes the newly registered task.
        assert!(poll_with(send.as_mut(), &second).is_pending());
        assert_eq!(second_wakes.count(), 1);
        expect_ready(poll_with(send.as_mut(), &second)).unwrap();
        assert_eq!(receiver.try_recv(), Ok(1));
    });
}

#[test]
fn replacing_a_receive_waker_allows_its_destructor_to_send() {
    assert_completes_without_deadlock(|| {
        let (sender, receiver) = mpmc::unbounded();
        let reentrant = sender.clone();
        let first = waker_on_drop(move || reentrant.send(1).unwrap());
        let (second, second_wakes) = WakeCounter::new();
        let mut recv = Box::pin(receiver.recv());

        assert!(poll_with(recv.as_mut(), &first).is_pending());
        drop(first);
        // Replacing the stored waker releases its last reference, whose destructor sends.
        assert!(poll_with(recv.as_mut(), &second).is_pending());
        assert_eq!(second_wakes.count(), 1);
        assert_eq!(expect_ready(poll_with(recv.as_mut(), &second)), Ok(1));
        drop(sender);
    });
}
