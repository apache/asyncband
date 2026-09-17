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

use std::collections::VecDeque;
use std::mem::MaybeUninit;
use std::ptr;
use std::task::Waker;

/// Wakers kept on the stack before the batch spills to the heap.
///
/// This is also the most wakers the semaphore collects per lock acquisition, so a drain that
/// wakes a typical waiter set never allocates; larger sets pay one allocation for the overflow.
pub const INLINE_CAPACITY: usize = 32;

/// An owning FIFO of wakers that stores the first [`INLINE_CAPACITY`] entries without allocating.
///
/// The batch is filled through [`WakerBatch::push`] or [`Extend`] and consumed as its own
/// iterator. Entries are written only as they are pushed, so constructing an empty or small batch
/// touches nothing beyond the two indices. Once every inline entry has been yielded the batch
/// reuses that storage, so a caller that alternates between filling and draining, as the
/// semaphore does, keeps running on the stack.
pub struct WakerBatch {
    /// The initialized entries are exactly `start..end`.
    inline: [MaybeUninit<Waker>; INLINE_CAPACITY],
    /// The next inline entry to yield.
    start: usize,
    /// The next inline slot to push into.
    end: usize,
    /// Entries pushed while the inline storage was full, yielded after it.
    ///
    /// While this is non-empty every push lands here, so the batch never yields a later push
    /// ahead of an earlier one.
    spilled: VecDeque<Waker>,
}

impl WakerBatch {
    pub const fn new() -> Self {
        Self {
            inline: [const { MaybeUninit::uninit() }; INLINE_CAPACITY],
            start: 0,
            end: 0,
            spilled: VecDeque::new(),
        }
    }

    /// Whether the next push would spill to the heap.
    ///
    /// The semaphore stops filling a batch here so it can release its lock and wake what it has
    /// before collecting more.
    pub fn is_full(&self) -> bool {
        self.end == INLINE_CAPACITY || !self.spilled.is_empty()
    }

    pub fn push(&mut self, waker: Waker) {
        if self.end < INLINE_CAPACITY && self.spilled.is_empty() {
            self.inline[self.end].write(waker);
            self.end += 1;
        } else {
            self.spilled.push_back(waker);
        }
    }
}

impl Extend<Waker> for WakerBatch {
    fn extend<I: IntoIterator<Item = Waker>>(&mut self, iter: I) {
        for waker in iter {
            self.push(waker);
        }
    }
}

impl Iterator for WakerBatch {
    type Item = Waker;

    fn next(&mut self) -> Option<Waker> {
        if self.start < self.end {
            let index = self.start;
            self.start += 1;
            // SAFETY: `index` was within the initialized range before advancing `start`.
            return Some(unsafe { self.inline[index].assume_init_read() });
        }

        // Every inline entry has been yielded, so later pushes can start over from the front.
        self.start = 0;
        self.end = 0;
        self.spilled.pop_front()
    }
}

impl Drop for WakerBatch {
    fn drop(&mut self) {
        let initialized = ptr::slice_from_raw_parts_mut(
            self.inline[self.start..self.end]
                .as_mut_ptr()
                .cast::<Waker>(),
            self.end - self.start,
        );
        // SAFETY: The initialized entries are exactly `start..end`.
        unsafe { ptr::drop_in_place(initialized) };
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::Wake;
    use std::task::Waker;

    use super::INLINE_CAPACITY;
    use super::WakerBatch;

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

    fn wakers(log: &Arc<Log>, count: usize) -> impl Iterator<Item = Waker> + '_ {
        (0..count).map(move |id| waker(log, id))
    }

    fn alive(log: &Arc<Log>) -> usize {
        Arc::strong_count(log) - 1
    }

    fn woken(log: &Arc<Log>) -> Vec<usize> {
        log.0.lock().unwrap().clone()
    }

    #[test]
    fn yields_in_push_order_across_the_spill() {
        let log = log();
        let count = INLINE_CAPACITY + 8;
        let mut batch = WakerBatch::new();
        batch.extend(wakers(&log, count));
        assert!(batch.is_full());

        for waker in &mut batch {
            waker.wake();
        }

        assert_eq!(woken(&log), (0..count).collect::<Vec<_>>());
        assert!(batch.next().is_none());
        assert_eq!(alive(&log), 0);
    }

    #[test]
    fn drops_unconsumed_entries_exactly_once() {
        let log = log();
        let count = INLINE_CAPACITY + 8;
        for consumed in [0, 5, INLINE_CAPACITY, INLINE_CAPACITY + 3, count] {
            let mut batch = WakerBatch::new();
            batch.extend(wakers(&log, count));
            for _ in 0..consumed {
                drop(batch.next().unwrap());
            }
            assert_eq!(alive(&log), count - consumed);

            drop(batch);
            assert_eq!(alive(&log), 0, "after consuming {consumed}");
        }
    }

    #[test]
    fn reuses_inline_storage_after_draining() {
        let log = log();
        let mut batch = WakerBatch::new();
        for round in 0..3 {
            batch.extend(wakers(&log, INLINE_CAPACITY));
            assert!(batch.is_full(), "round {round}");
            assert_eq!(batch.by_ref().count(), INLINE_CAPACITY);
            assert!(!batch.is_full(), "round {round}");
            assert_eq!(alive(&log), 0, "round {round}");
        }
    }

    #[test]
    fn keeps_push_order_while_spilled() {
        let log = log();
        let mut batch = WakerBatch::new();
        batch.extend(wakers(&log, INLINE_CAPACITY + 1));
        // Free inline room; the spilled entry must still come out before anything pushed now.
        for _ in 0..4 {
            batch.next().unwrap().wake();
        }
        batch.push(waker(&log, 999));
        assert!(batch.is_full());

        for waker in &mut batch {
            waker.wake();
        }

        let mut expected = (0..INLINE_CAPACITY + 1).collect::<Vec<_>>();
        expected.push(999);
        assert_eq!(woken(&log), expected);
        assert_eq!(alive(&log), 0);
    }
}
