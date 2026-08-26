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
    feature = "latch",
    feature = "mpsc",
    feature = "mutex",
    feature = "rwlock",
    feature = "semaphore",
    feature = "waitgroup",
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
#[cfg_attr(not(feature = "once-map"), allow(dead_code))]
pub(crate) mod once_table;

#[cfg(any(
    feature = "barrier",
    feature = "latch",
    feature = "mpsc",
    feature = "mutex",
    feature = "rwlock",
    feature = "semaphore",
    feature = "waitgroup",
))]
pub(crate) mod mutex;

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
    feature = "latch",
    feature = "once",
    feature = "waitgroup",
))]
// `barrier` constructs a wait set with `with_capacity`, while countdown-based primitives use
// `new`. One constructor is therefore unused in every single-primitive build.
#[allow(dead_code)]
pub(crate) mod waitset;
