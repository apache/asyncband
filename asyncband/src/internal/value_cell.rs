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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

/// Storage for a value published exactly once.
///
/// Synchronizing initialization is deliberately left to the containing primitive.
pub struct ValueCell<T> {
    initialized: AtomicBool,
    value: UnsafeCell<MaybeUninit<T>>,
}

// SAFETY: Shared access only exposes `&T`, and publication is synchronized by the initialized
// flag. The containing primitive is responsible for serializing writes.
unsafe impl<T: Sync + Send> Sync for ValueCell<T> {}

// SAFETY: Ownership of the cell and its value may be transferred when `T` is `Send`.
unsafe impl<T: Send> Send for ValueCell<T> {}

impl<T> ValueCell<T> {
    pub const fn new() -> Self {
        Self {
            initialized: AtomicBool::new(false),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    pub const fn from_value(value: T) -> Self {
        Self {
            initialized: AtomicBool::new(true),
            value: UnsafeCell::new(MaybeUninit::new(value)),
        }
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::Acquire)
    }

    pub fn is_initialized_mut(&mut self) -> bool {
        *self.initialized.get_mut()
    }

    pub fn get(&self) -> Option<&T> {
        if self.is_initialized() {
            // SAFETY: The acquire load above observed publication of the value.
            Some(unsafe { self.get_unchecked() })
        } else {
            None
        }
    }

    pub fn get_mut(&mut self) -> Option<&mut T> {
        if self.is_initialized_mut() {
            // SAFETY: Exclusive access rules out concurrent reads or writes.
            Some(unsafe { self.get_unchecked_mut() })
        } else {
            None
        }
    }

    pub fn into_inner(mut self) -> Option<T> {
        self.take()
    }

    pub fn take(&mut self) -> Option<T> {
        if self.is_initialized_mut() {
            *self.initialized.get_mut() = false;
            // SAFETY: The value was initialized and the flag was cleared so it will not be
            // dropped a second time.
            Some(unsafe { self.value.get_mut().assume_init_read() })
        } else {
            None
        }
    }

    /// Publishes a value after the containing primitive has won initialization.
    ///
    /// # Safety
    ///
    /// The cell must be uninitialized, and no other initialization may read or write it.
    pub unsafe fn set(&self, value: T) -> &T {
        debug_assert!(!self.is_initialized());
        let value_ptr = self.value.get();
        unsafe { value_ptr.write(MaybeUninit::new(value)) };

        // Publish the initialized value to readers performing an acquire load.
        self.initialized.store(true, Ordering::Release);

        // SAFETY: The value was initialized and published above.
        unsafe { self.get_unchecked() }
    }

    pub fn set_mut(&mut self, value: T) -> &mut T {
        debug_assert!(!self.is_initialized_mut());
        let value = self.value.get_mut().write(value);
        *self.initialized.get_mut() = true;
        value
    }

    /// # Safety
    ///
    /// The cell must be initialized.
    unsafe fn get_unchecked(&self) -> &T {
        debug_assert!(self.is_initialized());
        unsafe { (&*self.value.get()).assume_init_ref() }
    }

    /// # Safety
    ///
    /// The cell must be initialized and exclusively borrowed.
    unsafe fn get_unchecked_mut(&mut self) -> &mut T {
        debug_assert!(self.is_initialized_mut());
        unsafe { (&mut *self.value.get()).assume_init_mut() }
    }
}

impl<T> Drop for ValueCell<T> {
    fn drop(&mut self) {
        if self.is_initialized_mut() {
            // SAFETY: The value is initialized and exclusive access rules out other users.
            unsafe { self.value.get_mut().assume_init_drop() };
        }
    }
}
