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

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use asyncband::spmc;
use tests_integration::WakeCounter;
use tests_integration::assert_completes_without_deadlock;
use tests_integration::expect_ready;
use tests_integration::poll_once;
use tests_integration::poll_with;
use tests_integration::waker_on_drop;
use tests_integration::waker_on_wake;

#[test]
fn replacing_a_send_waker_allows_its_destructor_to_receive() {
    assert_completes_without_deadlock(|| {
        let (mut sender, receiver) = spmc::bounded(1);
        sender.try_send(0).unwrap();
        let reentrant = receiver.clone();
        let first = waker_on_drop(move || assert_eq!(reentrant.try_recv(), Ok(0)));
        let (second, second_wakes) = WakeCounter::new();
        let mut send = Box::pin(sender.send(1));

        assert!(poll_with(send.as_mut(), &first).is_pending());
        drop(first);
        // The retired waker frees capacity and wakes the replacement registration before this
        // poll returns. The next poll must still observe that capacity.
        assert!(poll_with(send.as_mut(), &second).is_pending());
        assert_eq!(second_wakes.count(), 1);
        expect_ready(poll_with(send.as_mut(), &second)).unwrap();
        assert_eq!(receiver.try_recv(), Ok(1));
    });
}

#[test]
fn cancelling_a_send_allows_its_waker_destructor_to_receive() {
    assert_completes_without_deadlock(|| {
        let (mut sender, receiver) = spmc::bounded(1);
        sender.try_send(0).unwrap();
        let reentrant = receiver.clone();
        let waker = waker_on_drop(move || assert_eq!(reentrant.try_recv(), Ok(0)));
        let mut send = Box::pin(sender.send(1));
        assert!(poll_with(send.as_mut(), &waker).is_pending());
        drop(waker);
        drop(send);

        sender.try_send(2).unwrap();
        assert_eq!(receiver.try_recv(), Ok(2));
    });
}

#[test]
fn waking_the_sender_allows_its_callback_to_receive() {
    assert_completes_without_deadlock(|| {
        let (mut sender, receiver) = spmc::bounded(2);
        sender.try_send(0).unwrap();
        sender.try_send(1).unwrap();
        let reentrant = receiver.clone();
        let waker = waker_on_wake(move || assert_eq!(reentrant.try_recv(), Ok(1)));
        let mut send = Box::pin(sender.send(2));
        assert!(poll_with(send.as_mut(), &waker).is_pending());

        assert_eq!(receiver.try_recv(), Ok(0));
        expect_ready(poll_once(send.as_mut())).unwrap();
        assert_eq!(receiver.try_recv(), Ok(2));
    });
}

#[derive(Debug)]
struct Payload {
    panic_on_drop: bool,
}

impl Drop for Payload {
    fn drop(&mut self) {
        assert!(!self.panic_on_drop, "payload destructor panicked");
    }
}

#[test]
fn cancellation_unregisters_before_a_payload_destructor_panics() {
    let (mut sender, receiver) = spmc::bounded(1);
    sender
        .try_send(Payload {
            panic_on_drop: false,
        })
        .unwrap();
    let (waker, wakes) = WakeCounter::new();
    let mut send = Box::pin(sender.send(Payload {
        panic_on_drop: true,
    }));
    assert!(poll_with(send.as_mut(), &waker).is_pending());

    assert!(catch_unwind(AssertUnwindSafe(|| drop(send))).is_err());
    drop(receiver.try_recv().unwrap());
    assert_eq!(wakes.count(), 0);
    sender
        .try_send(Payload {
            panic_on_drop: false,
        })
        .unwrap();
}

#[test]
fn disconnect_notifies_the_sender_before_a_buffered_payload_destructor_panics() {
    let (mut sender, receiver) = spmc::bounded(1);
    sender
        .try_send(Payload {
            panic_on_drop: true,
        })
        .unwrap();
    let (waker, wakes) = WakeCounter::new();
    let mut send = Box::pin(sender.send(Payload {
        panic_on_drop: false,
    }));
    assert!(poll_with(send.as_mut(), &waker).is_pending());

    assert!(catch_unwind(AssertUnwindSafe(|| drop(receiver))).is_err());
    assert_eq!(wakes.count(), 1);
    let rejected = expect_ready(poll_once(send.as_mut())).unwrap_err();
    assert!(!rejected.into_inner().panic_on_drop);
}
