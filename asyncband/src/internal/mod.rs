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

#[cfg(any(feature = "mpsc", feature = "spsc"))]
pub(crate) mod atomic_waker;

#[cfg(any(
    feature = "barrier",
    feature = "broadcast",
    feature = "latch",
    feature = "mpmc",
    feature = "mpsc",
    feature = "mutex",
    feature = "rwlock",
    feature = "semaphore",
    feature = "spmc",
    feature = "spsc",
    feature = "watch",
    feature = "waitgroup",
))]
// `WaitList` and `WaitSet` use different `Arena` operations. A single-primitive build therefore
// leaves part of this shared API unused, while the all-feature build uses it.
#[allow(dead_code)]
pub mod arena;

#[cfg(any(feature = "latch", feature = "once", feature = "waitgroup"))]
// `waitgroup` increments and decrements the countdown, while `latch` and `once` only decrement it.
// Consequently, `increment` is unused when either of the latter features is built alone.
#[allow(dead_code)]
pub(crate) mod countdown;

#[cfg(any(feature = "once-map", feature = "singleflight"))]
// `OnceMap` and `singleflight` use different subsets of `OnceTable`, so single-feature builds
// leave some operations in the shared implementation unused.
#[allow(dead_code)]
pub(crate) mod once_table;

#[cfg(any(feature = "lazy-cell", feature = "once-cell"))]
// `LazyCell` and `OnceCell` use different subsets of `ValueCell`, so single-feature builds leave
// some operations in the shared implementation unused.
#[allow(dead_code)]
pub(crate) mod value_cell;

#[cfg(any(
    feature = "barrier",
    feature = "broadcast",
    feature = "latch",
    feature = "mpmc",
    feature = "mpsc",
    feature = "mutex",
    feature = "rwlock",
    feature = "semaphore",
    feature = "spmc",
    feature = "spsc",
    feature = "watch",
    feature = "waitgroup",
))]
pub(crate) mod mutex;

#[cfg(any(
    feature = "mpsc",
    feature = "mutex",
    feature = "rwlock",
    feature = "semaphore",
    feature = "spsc",
))]
// The MPSC backend, also reused by SPSC, uses `poll_acquire`, `release_if_nonempty`, and
// `notify_all`; mutexes and rwlocks use `acquire`, `try_acquire`, and `release`; the public
// semaphore also uses the accounting methods.
// Each single-primitive build intentionally leaves the other groups unused.
#[allow(dead_code)]
pub(crate) mod semaphore;

#[cfg(any(
    feature = "mpsc",
    feature = "mutex",
    feature = "rwlock",
    feature = "semaphore",
    feature = "spsc",
))]
pub(crate) mod waitlist;

#[cfg(any(
    feature = "barrier",
    feature = "broadcast",
    feature = "latch",
    feature = "mpmc",
    feature = "once",
    feature = "spmc",
    feature = "watch",
    feature = "waitgroup",
))]
// `barrier` constructs a wait set with `with_capacity`, while countdown-based primitives use
// `new`. One constructor is therefore unused in every single-primitive build.
#[allow(dead_code)]
pub(crate) mod waitset;
