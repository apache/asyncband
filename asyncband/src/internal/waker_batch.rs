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

use std::mem;
use std::mem::MaybeUninit;
use std::ptr;
use std::task::Waker;

/// An owning FIFO of wakers that stores the first [`Self::STACK_SIZE`] entries without allocating.
///
/// Only pushed entries are initialized. [`Self::wake_all`] empties the batch in place so the
/// semaphore can reuse its storage between notifications.
pub struct WakerBatch {
    /// The initialized entries are exactly `0..inline_len`.
    inline: [MaybeUninit<Waker>; Self::STACK_SIZE],
    inline_len: usize,
    /// Entries pushed after the inline storage filled, woken after it.
    spilled: Vec<Waker>,
}

impl WakerBatch {
    /// Number of wakers stored inline before using overflow storage.
    ///
    /// The semaphore's permit-release loop uses this as its batch limit before unlocking.
    pub const STACK_SIZE: usize = 32;

    pub const fn new() -> Self {
        Self {
            inline: [const { MaybeUninit::uninit() }; Self::STACK_SIZE],
            inline_len: 0,
            spilled: vec![],
        }
    }

    /// Whether the next push would spill to the heap.
    ///
    /// The semaphore stops filling a batch here so it can release its lock and wake what it has
    /// before collecting more.
    #[inline]
    pub fn will_spill(&self) -> bool {
        self.inline_len == Self::STACK_SIZE
    }

    #[inline]
    pub fn push(&mut self, waker: Waker) {
        if self.will_spill() {
            self.spilled.push(waker);
        } else {
            self.inline[self.inline_len].write(waker);
            self.inline_len += 1;
        }
    }

    /// Wakes every entry in push order, retaining storage for the next batch.
    ///
    /// Call after releasing the owning primitive's state lock.
    #[inline]
    pub fn wake_all(&mut self) {
        let len = mem::take(&mut self.inline_len);
        for slot in &mut self.inline[..len] {
            // SAFETY: This prefix was initialized by `push` and is no longer owned by the batch.
            unsafe { slot.assume_init_read() }.wake();
        }
        self.spilled.drain(..).for_each(Waker::wake);
    }
}

impl FromIterator<Waker> for WakerBatch {
    #[inline]
    fn from_iter<T: IntoIterator<Item = Waker>>(iter: T) -> Self {
        let mut batch = Self::new();
        for waker in iter {
            batch.push(waker);
        }
        batch
    }
}

impl Drop for WakerBatch {
    #[inline]
    fn drop(&mut self) {
        let initialized = ptr::slice_from_raw_parts_mut(
            self.inline.as_mut_ptr().cast::<Waker>(),
            self.inline_len,
        );
        // SAFETY: The initialized entries are exactly `0..inline_len`.
        unsafe { ptr::drop_in_place(initialized) };
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::Wake;
    use std::task::Waker;

    use super::WakerBatch;

    const STACK_SIZE: usize = WakerBatch::STACK_SIZE;

    /// The ids of the wakers woken so far, in order.
    ///
    /// Every waker holds one clone of the `Arc<Log>`, so its strong count tells how many wakers
    /// are still alive.
    struct Log(Mutex<Vec<usize>>);

    struct Tagged {
        id: usize,
        log: Arc<Log>,
    }

    impl Wake for Tagged {
        fn wake(self: Arc<Self>) {
            self.log.0.lock().unwrap().push(self.id);
        }
    }

    fn log() -> Arc<Log> {
        Arc::new(Log(Mutex::new(vec![])))
    }

    fn waker(log: &Arc<Log>, id: usize) -> Waker {
        Waker::from(Arc::new(Tagged {
            id,
            log: Arc::clone(log),
        }))
    }

    fn push_wakers(batch: &mut WakerBatch, log: &Arc<Log>, count: usize) {
        for id in 0..count {
            batch.push(waker(log, id));
        }
    }

    fn alive(log: &Arc<Log>) -> usize {
        Arc::strong_count(log) - 1
    }

    fn woken(log: &Arc<Log>) -> Vec<usize> {
        log.0.lock().unwrap().clone()
    }

    #[test]
    fn wakes_in_push_order_across_the_spill() {
        for count in [
            0,
            1,
            STACK_SIZE - 1,
            STACK_SIZE,
            STACK_SIZE + 1,
            2 * STACK_SIZE + 1,
        ] {
            let log = log();
            let mut batch: WakerBatch = (0..count).map(|id| waker(&log, id)).collect();
            batch.wake_all();

            assert_eq!(woken(&log), (0..count).collect::<Vec<_>>());
            assert_eq!(alive(&log), 0);
        }
    }

    #[test]
    fn drops_unwoken_entries_exactly_once() {
        for previously_woken in [0, STACK_SIZE + 1] {
            for count in [0, 1, STACK_SIZE, STACK_SIZE + 1, 2 * STACK_SIZE + 1] {
                let log = log();
                let mut batch = WakerBatch::new();
                push_wakers(&mut batch, &log, previously_woken);
                batch.wake_all();
                push_wakers(&mut batch, &log, count);
                assert_eq!(alive(&log), count);

                drop(batch);
                assert_eq!(alive(&log), 0);
                assert_eq!(woken(&log), (0..previously_woken).collect::<Vec<_>>());
            }
        }
    }

    #[test]
    fn reuses_inline_storage_after_waking() {
        let log = log();
        let mut batch = WakerBatch::new();
        let mut expected = vec![];
        for count in [STACK_SIZE + 8, STACK_SIZE, 1, 0, STACK_SIZE + 1] {
            push_wakers(&mut batch, &log, count);
            batch.wake_all();
            expected.extend(0..count);

            assert_eq!(woken(&log), expected);
            assert!(!batch.will_spill());
            assert_eq!(alive(&log), 0);
        }
    }
}
