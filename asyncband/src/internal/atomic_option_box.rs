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

// This is derived from https://github.com/jorendorff/atomicbox/blob/07756444/src/atomic_option_box.rs.

use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;

/// Internal owning atomic pointer used to store an optional receiver waker.
pub(crate) struct AtomicOptionBox<T> {
    /// Pointer to a `T` value in the heap, representing `Some(t)`;
    /// or a null pointer for `None`.
    ptr: AtomicPtr<T>,

    /// This effectively makes `AtomicOptionBox<T>` non-`Send` and non-`Sync`
    /// if `T` is non-`Send`.
    phantom: PhantomData<Box<T>>,
}

/// Mark `AtomicOptionBox<T>` as safe to share across threads.
///
/// This is safe because shared access to an `AtomicOptionBox<T>` does not
/// provide shared access to any `T` value. However, it does provide the
/// ability to get a `Box<T>` from another thread, so `T: Send` is required.
unsafe impl<T> Sync for AtomicOptionBox<T> where T: Send {}

fn into_ptr<T>(value: Option<Box<T>>) -> *mut T {
    match value {
        Some(box_value) => Box::into_raw(box_value),
        None => ptr::null_mut(),
    }
}

// SAFETY: The caller must ensure that `ptr` was obtained from `Box::into_raw` or is null.
unsafe fn from_ptr<T>(ptr: *mut T) -> Option<Box<T>> {
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { Box::from_raw(ptr) })
    }
}

impl<T> AtomicOptionBox<T> {
    /// Creates a new `AtomicOptionBox` with no value.
    pub(crate) const fn none() -> Self {
        Self {
            ptr: AtomicPtr::new(ptr::null_mut()),
            phantom: PhantomData,
        }
    }

    fn swap(&self, other: Option<Box<T>>) -> Option<Box<T>> {
        let order = match other {
            Some(_) => Ordering::AcqRel,
            None => Ordering::Acquire,
        };
        let new_ptr = into_ptr(other);
        let old_ptr = self.ptr.swap(new_ptr, order);
        unsafe { from_ptr(old_ptr) }
    }

    /// Stores `other` and drops the previous value.
    pub(crate) fn store(&self, other: Option<Box<T>>) {
        drop(self.swap(other));
    }

    /// Replaces the value with `None` and returns the previous value.
    pub(crate) fn take(&self) -> Option<Box<T>> {
        self.swap(None)
    }
}

impl<T> Drop for AtomicOptionBox<T> {
    /// Dropping an `AtomicOptionBox<T>` drops the final `Box<T>` value (if any) stored in it.
    fn drop(&mut self) {
        let ptr = *self.ptr.get_mut();
        unsafe { drop(from_ptr(ptr)) }
    }
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use super::*;

    #[test]
    fn atomic_option_box_swap_works() {
        let b = AtomicOptionBox::none();
        let bis = Box::new("bis");
        assert_eq!(b.swap(Some(bis)), None);
        assert_eq!(b.swap(None), Some(Box::new("bis")));
    }

    #[test]
    fn atomic_option_box_store_works() {
        let b = AtomicOptionBox::none();
        let bis = Box::new("bis");
        b.store(Some(bis));
        assert_eq!(b.take(), Some(Box::new("bis")));
        assert_eq!(b.take(), None);
    }

    #[test]
    fn atomic_option_box_pointer_identity() {
        let box1 = Box::new(1);
        let p1 = &*box1 as *const i32;
        let atom = AtomicOptionBox::none();
        atom.store(Some(box1));

        let box2 = Box::new(2);
        let p2 = &*box2 as *const i32;
        assert_ne!(p2, p1);

        let box3 = atom.swap(Some(box2)).unwrap(); // box1 out, box2 in
        let p3 = &*box3 as *const i32;
        assert_eq!(p3, p1); // box3 is box1

        let box4 = atom.swap(None).unwrap(); // box2 out, None in
        let p4 = &*box4 as *const i32;
        assert_eq!(p4, p2); // box4 is box2
    }

    #[test]
    fn stored_values_are_dropped() {
        struct K(Arc<AtomicUsize>, usize);

        impl Drop for K {
            fn drop(&mut self) {
                self.0.fetch_add(self.1, Ordering::Relaxed);
            }
        }

        let n = Arc::new(AtomicUsize::new(0));
        {
            let ab = AtomicOptionBox::none();
            ab.store(Some(Box::new(K(n.clone(), 5))));
            assert_eq!(n.load(Ordering::Relaxed), 0);
            let first = ab.swap(None);
            assert_eq!(n.load(Ordering::Relaxed), 0);
            drop(first);
            assert_eq!(n.load(Ordering::Relaxed), 5);
            let second = ab.swap(Some(Box::new(K(n.clone(), 13))));
            assert!(second.is_none());
            assert_eq!(n.load(Ordering::Relaxed), 5);
        }
        assert_eq!(n.load(Ordering::Relaxed), 5 + 13);
    }
}
