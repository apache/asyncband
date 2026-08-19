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

// This is derived from https://github.com/jorendorff/atomicbox/blob/07756444/src/atomic_box.rs.

use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;

/// A type that holds a single `Box<T>` value and can be safely shared between threads.
pub struct AtomicBox<T> {
    ptr: AtomicPtr<T>,

    /// This effectively makes `AtomicBox<T>` non-`Send` and non-`Sync` if `T`
    /// is non-`Send`.
    phantom: PhantomData<Box<T>>,
}

/// Mark `AtomicBox<T>` as safe to share across threads.
///
/// This is safe because shared access to an `AtomicBox<T>` does not provide
/// shared access to any `T` value. However, it does provide the ability to get
/// a `Box<T>` from another thread, so `T: Send` is required.
unsafe impl<T> Sync for AtomicBox<T> where T: Send {}

impl<T> AtomicBox<T> {
    /// Creates a new `AtomicBox` with the given value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use asyncband::atomicbox::AtomicBox;
    ///
    /// let atomic_box = AtomicBox::new(Box::new(0));
    /// ```
    pub fn new(value: Box<T>) -> AtomicBox<T> {
        AtomicBox {
            ptr: AtomicPtr::new(Box::into_raw(value)),
            phantom: PhantomData,
        }
    }

    /// Atomically set this `AtomicBox` to `other` and return the previous value.
    ///
    /// This does not allocate or free memory, and it neither clones nor drops
    /// any values.  `other` is moved into `self`; the value previously in
    /// `self` is returned.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use asyncband::atomicbox::AtomicBox;
    ///
    /// let atom = AtomicBox::new(Box::new("one"));
    /// let prev_value = atom.swap(Box::new("two"));
    /// assert_eq!(*prev_value, "one");
    /// ```
    pub fn swap(&self, other: Box<T>) -> Box<T> {
        let mut result = other;
        self.swap_mut(&mut result);
        result
    }

    /// Atomically set this `AtomicBox` to `other` and drop its previous value.
    ///
    /// The `AtomicBox` takes ownership of `other`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use asyncband::atomicbox::AtomicBox;
    ///
    /// let atom = AtomicBox::new(Box::new("one"));
    /// atom.store(Box::new("two"));
    /// assert_eq!(atom.into_inner(), Box::new("two"));
    /// ```
    pub fn store(&self, other: Box<T>) {
        self.swap(other);
    }

    /// Atomically swaps the contents of this `AtomicBox` with the contents of `other`.
    ///
    /// This does not allocate or free memory, and it neither clones nor drops
    /// any values. The pointers in `*other` and `self` are simply exchanged.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use asyncband::atomicbox::AtomicBox;
    ///
    /// let atom = AtomicBox::new(Box::new("one"));
    /// let mut boxed = Box::new("two");
    /// atom.swap_mut(&mut boxed);
    /// assert_eq!(*boxed, "one");
    /// ```
    pub fn swap_mut(&self, other: &mut Box<T>) {
        let other_ptr = Box::into_raw(unsafe { ptr::read(other) });
        let ptr = self.ptr.swap(other_ptr, Ordering::AcqRel);
        unsafe { ptr::write(other, Box::from_raw(ptr)) };
    }

    /// Consume this `AtomicBox`, returning the last box value it contained.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use asyncband::atomicbox::AtomicBox;
    ///
    /// let atom = AtomicBox::new(Box::new("hello"));
    /// assert_eq!(atom.into_inner(), Box::new("hello"));
    /// ```
    pub fn into_inner(mut self) -> Box<T> {
        let result = unsafe { Box::from_raw(*self.ptr.get_mut()) };
        mem::forget(self);
        result
    }

    /// Returns a mutable reference to the contained value.
    ///
    /// This is safe because it borrows the `AtomicBox` mutably, which ensures
    /// that no other threads can concurrently access either the atomic pointer field
    /// or the boxed data it points to.
    pub fn get_mut(&mut self) -> &mut T {
        unsafe { &mut **self.ptr.get_mut() }
    }
}

impl<T> Drop for AtomicBox<T> {
    /// Dropping an `AtomicBox<T>` drops the final `Box<T>` value stored in it.
    fn drop(&mut self) {
        let ptr = *self.ptr.get_mut();
        unsafe { drop(Box::from_raw(ptr)) }
    }
}

impl<T> Default for AtomicBox<T>
where
    Box<T>: Default,
{
    /// The default `AtomicBox<T>` value boxes the default `T` value.
    fn default() -> AtomicBox<T> {
        AtomicBox::new(Default::default())
    }
}

impl<T> fmt::Debug for AtomicBox<T> {
    /// The `{:?}` format of an `AtomicBox<T>` looks like `"AtomicBox(0x12341234)"`.
    /// The address is the address of the `Box` allocation, not the address of
    /// the `AtomicBox`.
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let p = self.ptr.load(Ordering::Relaxed);
        f.write_str("AtomicBox(")?;
        fmt::Pointer::fmt(&p, f)?;
        f.write_str(")")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;

    use super::*;

    #[test]
    fn atomic_box_swap_works() {
        let b = AtomicBox::new(Box::new("hello world"));
        let bis = Box::new("bis");
        assert_eq!(b.swap(bis), Box::new("hello world"));
        assert_eq!(b.swap(Box::new("")), Box::new("bis"));
    }

    #[test]
    fn atomic_box_store_works() {
        let b = AtomicBox::new(Box::new("hello world"));
        let bis = Box::new("bis");
        b.store(bis);
        assert_eq!(b.into_inner(), Box::new("bis"));
    }

    #[test]
    fn atomic_box_swap_mut_works() {
        let b = AtomicBox::new(Box::new("hello world"));
        let mut bis = Box::new("bis");
        b.swap_mut(&mut bis);
        assert_eq!(bis, Box::new("hello world"));
        b.swap_mut(&mut bis);
        assert_eq!(bis, Box::new("bis"));
    }

    #[test]
    fn atomic_box_pointer_identity() {
        let box1 = Box::new(1);
        let p1 = format!("{box1:p}");
        let atom = AtomicBox::new(box1);

        let box2 = Box::new(2);
        let p2 = format!("{box2:p}");
        assert_ne!(p2, p1);

        let box3 = atom.swap(box2); // box1 out, box2 in
        let p3 = format!("{box3:p}");
        assert_eq!(p3, p1); // box3 is box1

        let box4 = atom.swap(Box::new(5)); // box2 out, throwaway value in
        let p4 = format!("{box4:p}");
        assert_eq!(p4, p2); // box4 is box2
    }

    #[test]
    fn atomic_box_drops() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        struct K(Arc<AtomicUsize>, usize);

        impl Drop for K {
            fn drop(&mut self) {
                self.0.fetch_add(self.1, Ordering::Relaxed);
            }
        }

        let n = Arc::new(AtomicUsize::new(0));
        {
            let ab = AtomicBox::new(Box::new(K(n.clone(), 5)));
            assert_eq!(n.load(Ordering::Relaxed), 0);
            let first = ab.swap(Box::new(K(n.clone(), 13)));
            assert_eq!(n.load(Ordering::Relaxed), 0);
            drop(first);
            assert_eq!(n.load(Ordering::Relaxed), 5);
        }
        assert_eq!(n.load(Ordering::Relaxed), 5 + 13);
    }

    #[test]
    fn atomic_threads() {
        const NTHREADS: usize = 9;

        let gate = Arc::new(Barrier::new(NTHREADS));
        let abox: Arc<AtomicBox<Vec<u8>>> = Arc::new(Default::default());
        let handles: Vec<_> = (0..NTHREADS as u8)
            .map(|t| {
                let my_gate = gate.clone();
                let my_box = abox.clone();
                std::thread::spawn(move || {
                    my_gate.wait();
                    let mut my_vec = Box::new(vec![]);
                    for _ in 0..100 {
                        my_vec = my_box.swap(my_vec);
                        my_vec.push(t);
                    }
                    my_vec
                })
            })
            .collect();

        let mut counts = [0usize; NTHREADS];
        for h in handles {
            for val in *h.join().unwrap() {
                counts[val as usize] += 1;
            }
        }

        // Don't forget the data still in `abox`!
        // There are NTHREADS+1 vectors in all.
        for val in *abox.swap(Box::new(vec![])) {
            counts[val as usize] += 1;
        }

        println!("{counts:?}");
        for count in counts {
            assert_eq!(count, 100);
        }
    }

    #[test]
    fn debug_fmt() {
        let my_box = Box::new(32);
        let expected = format!("AtomicBox({my_box:p})");
        assert_eq!(format!("{:?}", AtomicBox::new(my_box)), expected);
    }
}
