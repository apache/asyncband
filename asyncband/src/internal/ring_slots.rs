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

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;

/// Drops the values between two ring cursors in place. Cursors wrap at twice the slot count.
///
/// As in `VecDeque`, the wrapped part is still dropped if a destructor in the first part unwinds.
///
/// # Safety
///
/// The range must hold initialized values that nothing else accesses during the call. Its slots
/// are uninitialized afterwards.
pub unsafe fn drop_range<T>(slots: &[UnsafeCell<MaybeUninit<T>>], head: usize, tail: usize) {
    struct Dropper<T>(*mut [T]);

    impl<T> Drop for Dropper<T> {
        fn drop(&mut self) {
            // SAFETY: The caller of drop_range owns these initialized values.
            unsafe { ptr::drop_in_place(self.0) };
        }
    }

    let capacity = slots.len();
    let len = if tail >= head {
        tail - head
    } else {
        tail + 2 * capacity - head
    };
    let start = if head >= capacity {
        head - capacity
    } else {
        head
    };
    let front = len.min(capacity - start);
    let values = UnsafeCell::raw_get(slots.as_ptr()).cast::<T>();
    // SAFETY: Both parts lie within the slots and hold the caller's initialized values.
    unsafe {
        let _back = Dropper(ptr::slice_from_raw_parts_mut(values, len - front));
        ptr::drop_in_place(ptr::slice_from_raw_parts_mut(values.add(start), front));
    }
}
