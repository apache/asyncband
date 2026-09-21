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
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::pin::pin;
use std::ptr;
use std::task::Context;
use std::task::Poll;

use asyncband::once::LazyCell;
use tests_integration::poll_once;

struct AddressSensitiveFuture {
    address: Cell<*const Self>,
    _pin: PhantomPinned,
}

impl AddressSensitiveFuture {
    fn new() -> Self {
        Self {
            address: Cell::new(ptr::null()),
            _pin: PhantomPinned,
        }
    }
}

impl Future for AddressSensitiveFuture {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_ref().get_ref();
        let current = ptr::from_ref(this);
        let first = this.address.get();
        if first.is_null() {
            this.address.set(current);
            Poll::Pending
        } else {
            assert!(ptr::eq(first, current));
            Poll::Ready(42)
        }
    }
}

#[test]
fn lazy_cell_resumes_a_pinned_attempt_in_place() {
    let lazy = LazyCell::from_future(AddressSensitiveFuture::new());
    let lazy = pin!(lazy);

    {
        let mut force = pin!(LazyCell::force_pin(lazy.as_ref()));
        assert!(poll_once(force.as_mut()).is_pending());
    }

    let mut force = pin!(LazyCell::force_pin(lazy.as_ref()));
    assert_eq!(poll_once(force.as_mut()), Poll::Ready(&42));
}
