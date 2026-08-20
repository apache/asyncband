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

// `doc_cfg` automatically infers feature badges from `cfg` attributes, so individual modules do
// not need matching `doc(cfg(...))` attributes.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

//! Apache Asyncband (Incubating) is a runtime-agnostic library providing essential synchronization
//! primitives for asynchronous Rust programming. The library offers a collection of well-tested,
//! efficient synchronization tools that work with any async runtime.
//!
//! > Apache Asyncband is an effort undergoing incubation at the Apache Software Foundation (ASF),
//! > sponsored by the Apache Incubator PMC. Please read the
//! > [DISCLAIMER](https://github.com/apache/asyncband/blob/main/DISCLAIMER).
//!
//! # Migrating from MEA
//!
//! Asyncband continues the project formerly published as `mea`. The old crate is deprecated and no
//! compatibility crate or re-export is provided. Replace the `mea` dependency with `asyncband` and
//! update `mea::` paths to `asyncband::`.
//!
//! # Cargo features
//!
//! The crate enables no primitives or utilities by default. Each public module has a same-named
//! opt-in feature, so applications only compile the APIs they use:
//!
//! ```toml
//! asyncband = { version = "0.7", features = ["mutex", "oneshot"] }
//! ```
//!
//! Features that build on other primitives enable those dependencies automatically. For example,
//! `condvar` enables `mutex`, while `shutdown` enables `latch` and `waitgroup`.
//!
//! # Primitive categories
//!
//! The public primitives are grouped by their primary user-facing purpose while their modules
//! remain at the crate root so module paths continue to match Cargo feature names:
//!
//! * Shared state: `Mutex`, `RwLock`, and `Condvar`.
//! * One-time initialization: `Once`, `OnceCell`, and `OnceMap`.
//! * Task coordination: `Barrier`, `Latch`, `WaitGroup`, and graceful shutdown.
//! * Channels: oneshot, bounded and unbounded MPSC, and overflowing broadcast channels.
//! * Workload control: `Semaphore`, fair-share admission control, and duplicate-call suppression.
//!
//! # Synchronous interoperability
//!
//! The optional [`blocking`] module lets synchronous code wait indefinitely or with a timeout on a
//! single runtime-agnostic future. It is an interoperability utility rather than an async primitive
//! or a general-purpose runtime.
//!
//! # Runtime Agnostic
//!
//! All synchronization primitives in this library are runtime-agnostic, meaning they can be used
//! with any async runtime like tokio, async-std, or others. This makes the library highly versatile
//! and portable.
//!
//! # Thread Safety
//!
//! Asyncband primitives and guards implement `Send` and `Sync` only when the protected or
//! transferred value satisfies the necessary bounds. In particular, owned read guards that may move
//! destruction to another thread require the protected value to be `Send` as well as `Sync`. See
//! each type's documentation for its exact bounds.
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
mod internal;

#[cfg(feature = "admission")]
pub mod admission;
#[cfg(feature = "barrier")]
pub mod barrier;
#[cfg(feature = "blocking")]
pub mod blocking;
#[cfg(feature = "broadcast")]
pub mod broadcast;
#[cfg(feature = "condvar")]
pub mod condvar;
#[cfg(feature = "latch")]
pub mod latch;
#[cfg(feature = "mpsc")]
pub mod mpsc;
#[cfg(feature = "mutex")]
pub mod mutex;
#[cfg(feature = "once")]
pub mod once;
#[cfg(feature = "oneshot")]
pub mod oneshot;
#[cfg(feature = "rwlock")]
pub mod rwlock;
#[cfg(feature = "semaphore")]
pub mod semaphore;
#[cfg(feature = "shutdown")]
pub mod shutdown;
#[cfg(feature = "singleflight")]
pub mod singleflight;
#[cfg(feature = "waitgroup")]
pub mod waitgroup;

#[cfg(all(doctest, feature = "mutex", feature = "rwlock"))]
pub mod guard_variance_tests;

#[cfg(all(test, feature = "once"))]
mod test_support;
