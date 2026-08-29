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

#[cfg(feature = "mpsc")]
pub(crate) mod atomic_waker;

#[cfg(any(
    feature = "barrier",
    feature = "broadcast",
    feature = "latch",
    feature = "mpsc",
    feature = "mutex",
    feature = "rwlock",
    feature = "semaphore",
    feature = "waitgroup",
    feature = "watch",
))]
// `WaitList` and `WaitSet` use different `Arena` operations. A single-primitive build therefore
// leaves part of this shared API unused, while the all-feature build uses it.
#[allow(dead_code)]
pub(crate) mod arena;

#[cfg(any(feature = "latch", feature = "once", feature = "waitgroup"))]
// `waitgroup` increments and decrements the countdown, while `latch` and `once` only decrement it.
// Consequently, `increment` is unused when either of the latter features is built alone.
#[allow(dead_code)]
pub(crate) mod countdown;

#[cfg(any(feature = "once-map", feature = "singleflight"))]
pub(crate) mod cache_padded;

#[cfg(any(feature = "lazy-cell", feature = "once-cell"))]
// `LazyCell` and `OnceCell` use different subsets of `ValueCell`, so single-feature builds leave
// some operations in the shared implementation unused.
#[allow(dead_code)]
pub(crate) mod value_cell;

#[cfg(any(
    feature = "barrier",
    feature = "broadcast",
    feature = "latch",
    feature = "mpsc",
    feature = "mutex",
    feature = "rwlock",
    feature = "semaphore",
    feature = "waitgroup",
    feature = "watch",
))]
pub(crate) mod mutex;

#[cfg(feature = "once-map")]
pub(crate) mod rwlock;

#[cfg(any(
    feature = "mpsc",
    feature = "mutex",
    feature = "rwlock",
    feature = "semaphore",
))]
// `mpsc` uses `poll_acquire`, `release_if_nonempty`, and `notify_all`; mutexes and rwlocks use
// `acquire`, `try_acquire`, and `release`; the public semaphore also uses the accounting methods.
// Each single-primitive build intentionally leaves the other groups unused.
#[allow(dead_code)]
pub(crate) mod semaphore;

#[cfg(any(
    feature = "mpsc",
    feature = "mutex",
    feature = "rwlock",
    feature = "semaphore",
))]
pub(crate) mod waitlist;

#[cfg(any(
    feature = "barrier",
    feature = "broadcast",
    feature = "latch",
    feature = "once",
    feature = "waitgroup",
    feature = "watch",
))]
// `barrier` constructs a wait set with `with_capacity`, while countdown-based primitives use
// `new`. One constructor is therefore unused in every single-primitive build.
#[allow(dead_code)]
pub(crate) mod waitset;

#[cfg(any(feature = "once-map", feature = "singleflight"))]
pub fn default_shard_count() -> usize {
    // Contention benchmarks favor more shards than worker threads. Keep this policy internal so it
    // can be retuned as more architectures and workloads are measured.
    (std::thread::available_parallelism().map_or(1, |parallelism| parallelism.get()) * 8)
        .next_power_of_two()
}
