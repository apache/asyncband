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
use std::task::Waker;

/// Refreshes an occupied registration unless both wakers are known to wake the same task.
#[inline]
#[must_use = "drop the replaced waker after releasing the state lock"]
pub fn update_waker(current: &mut Waker, waker: &Waker) -> Option<Waker> {
    if current.will_wake(waker) {
        None
    } else {
        Some(mem::replace(current, waker.clone()))
    }
}

/// Retains the current task waker, returning any replaced registration for unlocked destruction.
#[inline]
#[must_use = "drop the replaced waker after releasing the state lock"]
// Some feature subsets have no primitive that registers a single waker slot.
#[allow(dead_code)]
pub fn register_waker(slot: &mut Option<Waker>, waker: &Waker) -> Option<Waker> {
    match slot {
        Some(current) => update_waker(current, waker),
        None => slot.replace(waker.clone()),
    }
}

#[cfg(any(
    feature = "barrier",
    feature = "broadcast",
    feature = "event",
    feature = "completion",
    feature = "latch",
    feature = "lazy-cell",
    feature = "mpmc",
    feature = "spmc",
    feature = "mpsc",
    feature = "mutex",
    feature = "once",
    feature = "once-cell",
    feature = "phaser",
    feature = "pool",
    feature = "rwlock",
    feature = "semaphore",
    feature = "waitgroup",
    feature = "watch",
))]
// `WaitList` and `WakerSet` use different `Arena` operations. A single-primitive build therefore
// leaves part of this shared API unused, while the all-feature build uses it.
#[allow(dead_code)]
pub(crate) mod arena;

#[cfg(feature = "latch")]
pub(crate) mod countdown;

#[cfg(any(feature = "lazy-cell", feature = "once", feature = "once-cell"))]
// `Once` does not reset or complete via `&mut self`, and `LazyCell` does not register pure
// observers, so single-feature builds leave parts of `OnceState` unused.
#[allow(dead_code)]
pub(crate) mod once_state;

#[cfg(any(feature = "lazy-cell", feature = "once-cell"))]
// `LazyCell` and `OnceCell` use different subsets of `ValueCell`, so single-feature builds leave
// some operations in the shared implementation unused.
#[allow(dead_code)]
pub(crate) mod value_cell;

#[cfg(any(
    feature = "barrier",
    feature = "broadcast",
    feature = "event",
    feature = "completion",
    feature = "latch",
    feature = "lazy-cell",
    feature = "mpmc",
    feature = "spmc",
    feature = "mpsc",
    feature = "mutex",
    feature = "once",
    feature = "once-cell",
    feature = "phaser",
    feature = "pool",
    feature = "rwlock",
    feature = "semaphore",
    feature = "watch",
))]
// Some primitives use only shared access, leaving `Mutex::get_mut` unused.
#[allow(dead_code)]
pub(crate) mod mutex;

#[cfg(any(feature = "broadcast", feature = "mpmc", feature = "spmc"))]
// Channel families use different subsets of single- and batch-wakeup helpers.
#[allow(dead_code)]
pub(crate) mod retry_waiter;

#[cfg(feature = "waitgroup")]
pub(crate) mod rwlock;

#[cfg(any(
    feature = "mutex",
    feature = "pool",
    feature = "rwlock",
    feature = "semaphore",
))]
// Mutexes, pools, and rwlocks use acquire/release operations; the public semaphore also exposes
// permit accounting. Single-primitive builds leave part of this API unused.
#[allow(dead_code)]
pub(crate) mod semaphore;

#[cfg(any(
    feature = "broadcast",
    feature = "event",
    feature = "lazy-cell",
    feature = "mpmc",
    feature = "spmc",
    feature = "mpsc",
    feature = "mutex",
    feature = "once",
    feature = "once-cell",
    feature = "pool",
    feature = "rwlock",
    feature = "semaphore",
))]
// The event and semaphore-backed primitives use different queue operations. A single-feature
// build therefore leaves part of this shared API unused, while the all-feature build uses it.
#[allow(dead_code)]
pub(crate) mod waitlist;

#[cfg(any(
    feature = "barrier",
    feature = "broadcast",
    feature = "completion",
    feature = "event",
    feature = "latch",
    feature = "lazy-cell",
    feature = "mpmc",
    feature = "mutex",
    feature = "once",
    feature = "once-cell",
    feature = "phaser",
    feature = "pool",
    feature = "rwlock",
    feature = "semaphore",
    feature = "spmc",
    feature = "waitgroup",
    feature = "watch",
))]
// Terminal-only primitives wake WakerSet directly, leaving WakerBatch::wake_all unused.
#[allow(dead_code)]
pub(crate) mod waker_batch;

#[cfg(any(
    feature = "barrier",
    feature = "broadcast",
    feature = "completion",
    feature = "event",
    feature = "latch",
    feature = "lazy-cell",
    feature = "once",
    feature = "once-cell",
    feature = "phaser",
    feature = "waitgroup",
    feature = "watch",
))]
// Some feature subsets use only one constructor or one of the two notification paths.
#[allow(dead_code)]
pub(crate) mod wakerset;
