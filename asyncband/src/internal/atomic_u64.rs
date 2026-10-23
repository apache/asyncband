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

#[cfg(target_has_atomic = "64")]
pub use std::sync::atomic::AtomicU64;

#[cfg(not(target_has_atomic = "64"))]
pub use self::locked::AtomicU64;

#[cfg(any(test, not(target_has_atomic = "64")))]
mod locked {
    use std::sync::atomic::Ordering;

    use crate::internal::mutex::Mutex;

    /// A `u64` with the subset of the `AtomicU64` API that the bounded MPMC ring uses, for targets
    /// without native 64-bit atomics.
    ///
    /// Each operation runs as a critical section on the value's lock, so all operations on a value
    /// are totally ordered, and each happens after every operation ordered before it. That provides
    /// the acquire, release, and sequentially consistent orderings the ring requests, so the
    /// orderings passed in are ignored.
    #[derive(Debug)]
    pub struct AtomicU64(Mutex<u64>);

    impl AtomicU64 {
        pub const fn new(value: u64) -> Self {
            Self(Mutex::new(value))
        }

        pub fn load(&self, _: Ordering) -> u64 {
            *self.0.lock()
        }

        pub fn store(&self, value: u64, _: Ordering) {
            *self.0.lock() = value;
        }

        pub fn compare_exchange(
            &self,
            current: u64,
            new: u64,
            _: Ordering,
            _: Ordering,
        ) -> Result<u64, u64> {
            let mut value = self.0.lock();
            if *value == current {
                *value = new;
                Ok(current)
            } else {
                Err(*value)
            }
        }

        /// Behaves like `compare_exchange`; it never fails spuriously.
        pub fn compare_exchange_weak(
            &self,
            current: u64,
            new: u64,
            success: Ordering,
            failure: Ordering,
        ) -> Result<u64, u64> {
            self.compare_exchange(current, new, success, failure)
        }

        pub fn fetch_add(&self, delta: u64, _: Ordering) -> u64 {
            self.update(|value| value.wrapping_add(delta))
        }

        pub fn fetch_sub(&self, delta: u64, _: Ordering) -> u64 {
            self.update(|value| value.wrapping_sub(delta))
        }

        pub fn fetch_or(&self, bits: u64, _: Ordering) -> u64 {
            self.update(|value| value | bits)
        }

        pub fn fetch_and(&self, bits: u64, _: Ordering) -> u64 {
            self.update(|value| value & bits)
        }

        pub fn get_mut(&mut self) -> &mut u64 {
            self.0.get_mut()
        }

        /// Replaces the value with `update(value)` and returns the previous value.
        fn update(&self, update: impl FnOnce(u64) -> u64) -> u64 {
            let mut value = self.0.lock();
            let previous = *value;
            *value = update(previous);
            previous
        }
    }

    #[cfg(test)]
    mod tests {
        use std::thread;

        use super::*;

        #[test]
        fn operations_return_the_previous_value_and_wrap() {
            let mut atomic = AtomicU64::new(u64::MAX);
            assert_eq!(atomic.fetch_add(2, Ordering::SeqCst), u64::MAX);
            assert_eq!(atomic.fetch_sub(3, Ordering::SeqCst), 1);
            assert_eq!(atomic.load(Ordering::SeqCst), u64::MAX - 1);
            assert_eq!(atomic.fetch_and(0xff, Ordering::SeqCst), u64::MAX - 1);
            assert_eq!(atomic.fetch_or(1 << 40, Ordering::SeqCst), 0xfe);
            assert_eq!(
                atomic.compare_exchange(0, 7, Ordering::SeqCst, Ordering::SeqCst),
                Err((1 << 40) | 0xfe)
            );
            assert_eq!(
                atomic.compare_exchange_weak(
                    (1 << 40) | 0xfe,
                    7,
                    Ordering::SeqCst,
                    Ordering::SeqCst
                ),
                Ok((1 << 40) | 0xfe)
            );
            atomic.store(9, Ordering::SeqCst);
            *atomic.get_mut() += 1;
            assert_eq!(atomic.load(Ordering::SeqCst), 10);
        }

        #[test]
        fn concurrent_updates_are_not_lost() {
            const THREADS: u64 = 4;
            const UPDATES: u64 = if cfg!(miri) { 20 } else { 10_000 };

            let atomic = AtomicU64::new(0);
            thread::scope(|scope| {
                for _ in 0..THREADS {
                    scope.spawn(|| {
                        for _ in 0..UPDATES {
                            atomic.fetch_add(3, Ordering::SeqCst);
                            let mut current = atomic.load(Ordering::SeqCst);
                            while let Err(actual) = atomic.compare_exchange_weak(
                                current,
                                current - 1,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            ) {
                                current = actual;
                            }
                        }
                    });
                }
            });
            assert_eq!(atomic.load(Ordering::SeqCst), THREADS * UPDATES * 2);
        }
    }
}
