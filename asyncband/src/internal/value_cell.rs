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
use std::mem;
use std::mem::ManuallyDrop;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

/// Single-value storage with atomic publication and unsynchronized exclusive access.
///
/// Concurrently racing writers must be serialized by the caller before calling [`Self::publish`].
pub struct ValueCell<T> {
    published: AtomicBool,
    slot: UnsafeCell<MaybeUninit<T>>,
}

// SAFETY: Moving or dropping `ValueCell<T>` transfers ownership of any stored `T`.
unsafe impl<T: Send> Send for ValueCell<T> {}

// SAFETY: `publish` transfers `T` across threads via `&self`, and `get` exposes `&T` concurrently.
unsafe impl<T: Send + Sync> Sync for ValueCell<T> {}

impl<T> ValueCell<T> {
    pub const fn empty() -> Self {
        Self {
            published: AtomicBool::new(false),
            slot: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    pub const fn with_value(value: T) -> Self {
        Self {
            published: AtomicBool::new(true),
            slot: UnsafeCell::new(MaybeUninit::new(value)),
        }
    }

    pub fn is_published(&self) -> bool {
        self.published.load(Ordering::Acquire)
    }

    pub fn has_value_mut(&mut self) -> bool {
        *self.published.get_mut()
    }

    pub fn get(&self) -> Option<&T> {
        if !self.published.load(Ordering::Acquire) {
            return None;
        }
        // SAFETY: The Acquire load observed a completed `publish`, and shared references never
        // mutate or clear an already published slot.
        Some(unsafe { (*self.slot.get()).assume_init_ref() })
    }

    pub fn get_mut(&mut self) -> Option<&mut T> {
        if *self.published.get_mut() {
            // SAFETY: `published` is true and `&mut self` guarantees exclusive access.
            Some(unsafe { self.slot.get_mut().assume_init_mut() })
        } else {
            None
        }
    }

    /// Publishes `value` into an empty cell and returns a shared reference to it.
    ///
    /// # Safety
    ///
    /// The slot must currently be unpublished, and no concurrent operation may read, write, or
    /// mutably borrow the slot until `publish` returns; no shared operation may clear or overwrite
    /// the slot while the returned borrow is live.
    pub unsafe fn publish(&self, value: T) -> &T {
        debug_assert!(!self.published.load(Ordering::Relaxed));
        let slot = self.slot.get();
        // SAFETY: Caller guarantees exclusive write access while `published` is false.
        // `MaybeUninit<T>` is `#[repr(transparent)]` over `T`, so `slot.cast::<T>()` has valid
        // provenance and alignment for writing `T` without creating an intermediate `&mut`.
        unsafe { slot.cast::<T>().write(value) };
        self.published.store(true, Ordering::Release);
        // SAFETY: `slot` now holds an initialized `T` that remains immutable while `&self` is live.
        unsafe { (*slot).assume_init_ref() }
    }

    pub fn write_mut(&mut self, value: T) -> &mut T {
        if *self.published.get_mut() {
            // SAFETY: `published` indicates an initialized value in `slot`.
            unsafe { self.slot.get_mut().assume_init_drop() };
        }
        *self.published.get_mut() = true;
        self.slot.get_mut().write(value)
    }

    pub fn take(&mut self) -> Option<T> {
        if !mem::replace(self.published.get_mut(), false) {
            return None;
        }
        // SAFETY: `published` was true and was cleared before moving the value out.
        Some(unsafe { self.slot.get_mut().assume_init_read() })
    }

    pub fn into_inner(self) -> Option<T> {
        let mut this = ManuallyDrop::new(self);
        if *this.published.get_mut() {
            // SAFETY: `this` is wrapped in `ManuallyDrop` and `published` is true.
            Some(unsafe { this.slot.get_mut().assume_init_read() })
        } else {
            None
        }
    }
}

impl<T> Drop for ValueCell<T> {
    fn drop(&mut self) {
        if *self.published.get_mut() {
            // SAFETY: `published` is true and `drop` has exclusive access.
            unsafe { self.slot.get_mut().assume_init_drop() };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::thread;

    use super::*;

    struct DropCounter<'a>(&'a Cell<usize>);

    impl Drop for DropCounter<'_> {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    #[test]
    fn concurrent_readers_observe_published_reference() {
        let cell: ValueCell<Vec<i32>> = ValueCell::empty();

        let published = thread::scope(|scope| {
            let mut readers = Vec::with_capacity(2);
            for _ in 0..2 {
                readers.push(scope.spawn(|| {
                    loop {
                        if let Some(values) = cell.get() {
                            return values.as_slice();
                        }
                        thread::yield_now();
                    }
                }));
            }

            // SAFETY: No other writer exists and readers only observe via `get`.
            let published = unsafe { cell.publish(vec![10, 20, 30]) };
            for reader in readers {
                assert_eq!(reader.join().unwrap(), &[10, 20, 30]);
            }
            published.as_slice()
        });

        assert_eq!(published, &[10, 20, 30]);
    }

    #[test]
    fn exclusive_lifecycle_preserves_drop_counts() {
        let drops = Cell::new(0);

        let mut cell = ValueCell::empty();
        assert!(!cell.is_published());
        assert!(!cell.has_value_mut());
        assert!(cell.get().is_none());
        assert!(cell.get_mut().is_none());
        assert!(cell.take().is_none());

        cell.write_mut(DropCounter(&drops));
        assert!(cell.has_value_mut());
        assert_eq!(drops.get(), 0);

        // Overwriting an already populated slot must drop the prior value.
        cell.write_mut(DropCounter(&drops));
        assert_eq!(drops.get(), 1);

        let taken = cell.take();
        assert!(taken.is_some());
        assert!(!cell.has_value_mut());
        assert_eq!(drops.get(), 1);
        drop(taken);
        assert_eq!(drops.get(), 2);

        // Re-publishing after `take` works and transfers ownership to `into_inner`.
        // SAFETY: `cell` is empty and accessed from a single thread.
        unsafe { cell.publish(DropCounter(&drops)) };
        assert!(cell.is_published());
        let extracted = cell.into_inner();
        assert!(extracted.is_some());
        assert_eq!(drops.get(), 2);
        drop(extracted);
        assert_eq!(drops.get(), 3);

        // `with_value` and `Drop` drop the stored value once.
        let cell = ValueCell::with_value(DropCounter(&drops));
        drop(cell);
        assert_eq!(drops.get(), 4);
    }
}
