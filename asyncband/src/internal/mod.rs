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

#[cfg(any(
    feature = "admission",
    feature = "barrier",
    feature = "broadcast",
    feature = "condvar",
    feature = "latch",
    feature = "mpsc",
    feature = "mutex",
    feature = "once",
    feature = "rwlock",
    feature = "semaphore",
    feature = "singleflight",
    feature = "waitgroup",
))]
// Individual primitives intentionally use different subsets of this shared storage helper.
#[allow(dead_code)]
mod arena;
#[cfg(any(
    feature = "admission",
    feature = "barrier",
    feature = "broadcast",
    feature = "condvar",
    feature = "latch",
    feature = "mpsc",
    feature = "mutex",
    feature = "once",
    feature = "rwlock",
    feature = "semaphore",
    feature = "singleflight",
    feature = "waitgroup",
))]
pub(crate) use arena::*;

#[cfg(any(feature = "latch", feature = "once", feature = "waitgroup"))]
// Latches, once values, and wait groups use different countdown transitions.
#[allow(dead_code)]
mod countdown;
#[cfg(any(feature = "latch", feature = "once", feature = "waitgroup"))]
pub(crate) use countdown::*;

#[cfg(any(feature = "once", feature = "singleflight"))]
mod once_table;
#[cfg(any(feature = "once", feature = "singleflight"))]
pub(crate) use once_table::*;

#[cfg(any(
    feature = "admission",
    feature = "barrier",
    feature = "broadcast",
    feature = "condvar",
    feature = "latch",
    feature = "mpsc",
    feature = "mutex",
    feature = "once",
    feature = "rwlock",
    feature = "semaphore",
    feature = "singleflight",
    feature = "waitgroup",
))]
mod mutex;
#[cfg(any(
    feature = "admission",
    feature = "barrier",
    feature = "broadcast",
    feature = "condvar",
    feature = "latch",
    feature = "mpsc",
    feature = "mutex",
    feature = "once",
    feature = "rwlock",
    feature = "semaphore",
    feature = "singleflight",
    feature = "waitgroup",
))]
pub(crate) use mutex::*;

#[cfg(feature = "broadcast")]
mod rwlock;
#[cfg(feature = "broadcast")]
pub(crate) use rwlock::*;

#[cfg(any(
    feature = "mpsc",
    feature = "mutex",
    feature = "rwlock",
    feature = "semaphore",
))]
// Public primitives expose different subsets of the internal semaphore operations.
#[allow(dead_code)]
mod semaphore;
#[cfg(any(
    feature = "mpsc",
    feature = "mutex",
    feature = "rwlock",
    feature = "semaphore",
))]
pub(crate) use semaphore::*;

#[cfg(any(
    feature = "condvar",
    feature = "mpsc",
    feature = "mutex",
    feature = "rwlock",
    feature = "semaphore",
))]
mod waitlist;
#[cfg(any(
    feature = "condvar",
    feature = "mpsc",
    feature = "mutex",
    feature = "rwlock",
    feature = "semaphore",
))]
pub(crate) use waitlist::*;

#[cfg(any(
    feature = "barrier",
    feature = "broadcast",
    feature = "latch",
    feature = "once",
    feature = "waitgroup",
))]
// Individual primitives construct and inspect wait sets through different entry points.
#[allow(dead_code)]
mod waitset;
#[cfg(any(
    feature = "barrier",
    feature = "broadcast",
    feature = "latch",
    feature = "once",
    feature = "waitgroup",
))]
pub(crate) use waitset::*;
