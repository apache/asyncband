// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This is derived from https://github.com/jorendorff/atomicbox/blob/07756444/src/atomic_option_box.rs.

use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;

/// A type that holds a single `Option<Box<T>>` value and can be safely shared
/// between threads.
pub struct AtomicOptionBox<T> {
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
    /// Creates a new `AtomicOptionBox` with the given value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use asyncband::atomicbox::AtomicOptionBox;
    ///
    /// let atomic_box = AtomicOptionBox::new(Some(Box::new(0)));
    /// ```
    pub fn new(value: Option<Box<T>>) -> AtomicOptionBox<T> {
        AtomicOptionBox {
            ptr: AtomicPtr::new(into_ptr(value)),
            phantom: PhantomData,
        }
    }

    /// Creates a new `AtomicOptionBox` with no value.
    ///
    /// Equivalent to `AtomicOptionBox::new(None)`, but can be used in `const` context.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use asyncband::atomicbox::AtomicOptionBox;
    ///
    /// static GLOBAL_BOX: AtomicOptionBox<u32> = AtomicOptionBox::none();
    /// ```
    pub const fn none() -> Self {
        Self {
            ptr: AtomicPtr::new(ptr::null_mut()),
            phantom: PhantomData,
        }
    }

    /// Atomically set this `AtomicOptionBox` to `other` and return the previous value.
    ///
    /// This does not allocate or free memory, and it neither clones nor drops any values. `other`
    /// is moved into `self`; the value previously in `self` is returned.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use asyncband::atomicbox::AtomicOptionBox;
    ///
    /// let atom = AtomicOptionBox::new(None);
    /// let prev_value = atom.swap(Some(Box::new("ok")));
    /// assert_eq!(prev_value, None);
    /// ```
    pub fn swap(&self, other: Option<Box<T>>) -> Option<Box<T>> {
        let order = match other {
            Some(_) => Ordering::AcqRel,
            None => Ordering::Acquire,
        };
        let new_ptr = into_ptr(other);
        let old_ptr = self.ptr.swap(new_ptr, order);
        unsafe { from_ptr(old_ptr) }
    }

    /// Atomically set this `AtomicOptionBox` to `other` and drop the previous value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use asyncband::atomicbox::AtomicOptionBox;
    ///
    /// let atom = AtomicOptionBox::new(None);
    /// atom.store(Some(Box::new("ok")));
    /// assert_eq!(atom.into_inner(), Some(Box::new("ok")));
    /// ```
    pub fn store(&self, other: Option<Box<T>>) {
        self.swap(other);
    }

    /// Atomically set this `AtomicOptionBox` to `None` and return the previous value.
    ///
    /// This does not allocate or free memory, and it neither clones nor drops any values. It is
    /// equivalent to calling `self.swap(None)`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use asyncband::atomicbox::AtomicOptionBox;
    ///
    /// let atom = AtomicOptionBox::new(Some(Box::new("ok")));
    /// let prev_value = atom.take();
    /// assert!(prev_value.is_some());
    /// let prev_value = atom.take();
    /// assert!(prev_value.is_none());
    /// ```
    pub fn take(&self) -> Option<Box<T>> {
        self.swap(None)
    }

    /// Atomically swaps the contents of this `AtomicOptionBox` with the contents of `other`.
    ///
    /// This does not allocate or free memory, and it neither clones nor drops any values. The
    /// pointers in `*other` and `self` are simply exchanged.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use asyncband::atomicbox::AtomicOptionBox;
    ///
    /// let atom = AtomicOptionBox::new(None);
    /// let mut boxed = Some(Box::new("ok"));
    /// let prev_value = atom.swap_mut(&mut boxed);
    /// assert_eq!(boxed, None);
    /// ```
    pub fn swap_mut(&self, other: &mut Option<Box<T>>) {
        let previous = self.swap(other.take());
        *other = previous;
    }

    /// Consume this `AtomicOptionBox`, returning the last option value it
    /// contained.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use asyncband::atomicbox::AtomicOptionBox;
    ///
    /// let atom = AtomicOptionBox::new(Some(Box::new("hello")));
    /// assert_eq!(atom.into_inner(), Some(Box::new("hello")));
    /// ```
    pub fn into_inner(mut self) -> Option<Box<T>> {
        let result = unsafe { from_ptr(*self.ptr.get_mut()) };
        mem::forget(self);
        result
    }

    /// Returns a mutable reference to the contained value.
    ///
    /// This is safe because it borrows the `AtomicOptionBox` mutably, which
    /// ensures that no other threads can concurrently access either the atomic
    /// pointer field or the boxed data it points to.
    pub fn get_mut(&mut self) -> Option<&mut T> {
        unsafe { self.ptr.get_mut().as_mut() }
    }
}

impl<T> Drop for AtomicOptionBox<T> {
    /// Dropping an `AtomicOptionBox<T>` drops the final `Box<T>` value (if any) stored in it.
    fn drop(&mut self) {
        let ptr = *self.ptr.get_mut();
        unsafe { drop(from_ptr(ptr)) }
    }
}

impl<T> Default for AtomicOptionBox<T> {
    /// The default `AtomicOptionBox<T>` value is `AtomicBox::new(None)`.
    fn default() -> AtomicOptionBox<T> {
        AtomicOptionBox::new(None)
    }
}

impl<T> fmt::Debug for AtomicOptionBox<T> {
    /// The `{:?}` format of an `AtomicOptionBox<T>` looks like
    /// `"AtomicOptionBox(0x12341234)"` or `"AtomicOptionBox(None)"`.
    ///
    /// The address is the address of the `Box` allocation, if any, not the
    /// address of the `AtomicOptionBox`.
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let p = self.ptr.load(Ordering::Relaxed);
        f.write_str("AtomicOptionBox(")?;
        if p.is_null() {
            f.write_str("None")?;
        } else {
            fmt::Pointer::fmt(&p, f)?;
        }
        f.write_str(")")?;
        Ok(())
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
        let b = AtomicOptionBox::new(Some(Box::new("hello world")));
        let bis = Box::new("bis");
        assert_eq!(b.swap(None), Some(Box::new("hello world")));
        assert_eq!(b.swap(Some(bis)), None);
        assert_eq!(b.swap(None), Some(Box::new("bis")));
    }

    #[test]
    fn atomic_option_box_store_works() {
        let b = AtomicOptionBox::new(Some(Box::new("hello world")));
        b.store(None);
        assert_eq!(b.into_inner(), None);

        let b = AtomicOptionBox::new(Some(Box::new("hello world")));
        let bis = Box::new("bis");
        b.store(Some(bis));
        assert_eq!(b.into_inner(), Some(Box::new("bis")));
    }

    #[test]
    fn atomic_option_box_swap_mut_works() {
        let b = AtomicOptionBox::new(Some(Box::new("hello world")));
        let mut bis = None;
        b.swap_mut(&mut bis);
        assert_eq!(bis, Some(Box::new("hello world")));
        bis = Some(Box::new("bis"));
        b.swap_mut(&mut bis);
        assert_eq!(bis, None);
        b.swap_mut(&mut bis);
        assert_eq!(bis, Some(Box::new("bis")));
    }

    #[test]
    fn atomic_option_box_pointer_identity() {
        let box1 = Box::new(1);
        let p1 = &*box1 as *const i32;
        let atom = AtomicOptionBox::new(Some(box1));

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
    fn atomic_box_drops() {
        struct K(Arc<AtomicUsize>, usize);

        impl Drop for K {
            fn drop(&mut self) {
                self.0.fetch_add(self.1, Ordering::Relaxed);
            }
        }

        let n = Arc::new(AtomicUsize::new(0));
        {
            let ab = AtomicOptionBox::new(Some(Box::new(K(n.clone(), 5))));
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

    #[test]
    fn debug_fmt() {
        let my_box = Box::new(32);
        let expected = format!("AtomicOptionBox({my_box:p})");
        assert_eq!(
            format!("{:?}", AtomicOptionBox::new(Some(my_box))),
            expected
        );
        assert_eq!(
            format!("{:?}", AtomicOptionBox::<String>::new(None)),
            "AtomicOptionBox(None)"
        );
    }
}
