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
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;

// RawWaker is needed only to exercise clone callbacks, which the safe Wake trait cannot override.
pub fn waker_on_clone(callback: impl Fn() + Send + Sync + 'static) -> Waker {
    struct OnClone(Box<dyn Fn() + Send + Sync>);

    unsafe fn clone(data: *const ()) -> RawWaker {
        let pointer = data.cast::<OnClone>();
        // SAFETY: The input waker owns a live Arc; the returned waker gains its own reference.
        unsafe {
            ((*pointer).0)();
            Arc::increment_strong_count(pointer);
        }
        RawWaker::new(data, &VTABLE)
    }

    unsafe fn release(data: *const ()) {
        // SAFETY: Consumes the one Arc reference owned by this waker.
        drop(unsafe { Arc::from_raw(data.cast::<OnClone>()) });
    }

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, release, |_| {}, release);
    let pointer = Arc::into_raw(Arc::new(OnClone(Box::new(callback)))).cast();
    // SAFETY: Each waker owns one Arc; its callback is Send + Sync and all vtable operations
    // preserve that ownership. wake_by_ref borrows the reference without changing it.
    unsafe { Waker::from_raw(RawWaker::new(pointer, &VTABLE)) }
}
