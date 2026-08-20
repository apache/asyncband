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

//! Runtime-agnostic synchronization primitives for asynchronous Rust.
//!
//! `asyncband` provides locks, initialization tools, task coordination, channels, and workload
//! controls without tying an application to a particular async runtime. The primitives use standard
//! futures and wakers, so they can run on Tokio, async-std, smol, or a custom executor.
//!
//! # Getting started
//!
//! Public APIs live in top-level modules. Each module is controlled by a same-named Cargo feature,
//! and no features are enabled by default. Enable the modules your application needs:
//!
//! ```toml
//! asyncband = { version = "0.7", features = ["mutex", "oneshot"] }
//! ```
//!
//! Then use the selected primitives directly:
//!
//! ```
//! # async fn example() {
//! use asyncband::mutex::Mutex;
//!
//! let counter = Mutex::new(0);
//! {
//!     let mut value = counter.lock().await;
//!     *value += 1;
//! }
//! assert_eq!(*counter.lock().await, 1);
//! # }
//! ```
//!
//! Features that build on other primitives enable their dependencies automatically: `condvar`
//! enables `mutex`, `once` enables `semaphore`, `shutdown` enables `latch` and `waitgroup`, and
//! `singleflight` enables `once`.
//!
//! # API guide
//!
//! | Use case                   | APIs                                                                                                                                    | Cargo features                              |
//! | -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
//! | Protect shared state       | [`mutex::Mutex`][mutex], [`rwlock::RwLock`][rwlock], [`condvar::Condvar`][condvar]                                                      | `mutex`, `rwlock`, `condvar`                |
//! | Initialize values once     | [`once::Once`][once], [`once::OnceCell`][once-cell], [`once::OnceMap`][once-map]                                                        | `once`                                      |
//! | Coordinate tasks           | [`barrier::Barrier`][barrier], [`latch::Latch`][latch], [`waitgroup::WaitGroup`][waitgroup], [`shutdown`][shutdown]                     | `barrier`, `latch`, `waitgroup`, `shutdown` |
//! | Send values                | [`oneshot::channel`][oneshot], [`mpsc::bounded`][mpsc-bounded], [`mpsc::unbounded`][mpsc-unbounded], [`broadcast::overflow`][broadcast] | `oneshot`, `mpsc`, `broadcast`              |
//! | Control workloads          | [`semaphore::Semaphore`][semaphore], [`admission::FairShare`][fair-share], [`singleflight::Group`][singleflight]                        | `semaphore`, `admission`, `singleflight`    |
//! | Wait from synchronous code | [`blocking::FutureExt`][blocking-future-ext]                                                                                            | `blocking`                                  |
//!
//! # Runtime and blocking model
//!
//! The async primitives do not start threads, spawn tasks, or require a runtime-specific reactor.
//! Await them inside any executor that polls standard Rust futures.
//!
//! Async APIs are the primary interface. The optional [`blocking`][blocking] module is a boundary
//! adapter for synchronous callers: its single-future executor parks the calling thread and resumes
//! it through the future's waker. It is not a general-purpose async runtime, and futures that
//! depend on a runtime-specific timer or I/O driver may not make progress. See the module
//! documentation for the full execution constraints.
//!
//! # Thread safety
//!
//! Primitives and guards implement `Send` and `Sync` only when their protected or transferred
//! values satisfy the required bounds. Consult each type's documentation for its exact contract.
//!
//! # Incubation status
//!
//! > Apache Asyncband (Incubating) is an effort undergoing incubation at the Apache Software
//! > Foundation (ASF), sponsored by the Apache Incubator PMC.
//! >
//! > Incubation is required of all newly accepted projects until a further review indicates that
//! > the infrastructure, communications, and decision making process have stabilized in a manner
//! > consistent with other successful ASF projects.
//! >
//! > While incubation status is not necessarily a reflection of the completeness or stability of
//! > the code, it does indicate that the project has yet to be fully endorsed by the ASF.
//!
//! [barrier]: https://docs.rs/asyncband/latest/asyncband/barrier/struct.Barrier.html
//! [blocking]: https://docs.rs/asyncband/latest/asyncband/blocking/
//! [blocking-future-ext]: https://docs.rs/asyncband/latest/asyncband/blocking/trait.FutureExt.html
//! [broadcast]: https://docs.rs/asyncband/latest/asyncband/broadcast/overflow/
//! [condvar]: https://docs.rs/asyncband/latest/asyncband/condvar/struct.Condvar.html
//! [fair-share]: https://docs.rs/asyncband/latest/asyncband/admission/struct.FairShare.html
//! [latch]: https://docs.rs/asyncband/latest/asyncband/latch/struct.Latch.html
//! [mpsc-bounded]: https://docs.rs/asyncband/latest/asyncband/mpsc/fn.bounded.html
//! [mpsc-unbounded]: https://docs.rs/asyncband/latest/asyncband/mpsc/fn.unbounded.html
//! [mutex]: https://docs.rs/asyncband/latest/asyncband/mutex/struct.Mutex.html
//! [once]: https://docs.rs/asyncband/latest/asyncband/once/struct.Once.html
//! [once-cell]: https://docs.rs/asyncband/latest/asyncband/once/struct.OnceCell.html
//! [once-map]: https://docs.rs/asyncband/latest/asyncband/once/struct.OnceMap.html
//! [oneshot]: https://docs.rs/asyncband/latest/asyncband/oneshot/fn.channel.html
//! [rwlock]: https://docs.rs/asyncband/latest/asyncband/rwlock/struct.RwLock.html
//! [semaphore]: https://docs.rs/asyncband/latest/asyncband/semaphore/struct.Semaphore.html
//! [shutdown]: https://docs.rs/asyncband/latest/asyncband/shutdown/
//! [singleflight]: https://docs.rs/asyncband/latest/asyncband/singleflight/struct.Group.html
//! [waitgroup]: https://docs.rs/asyncband/latest/asyncband/waitgroup/struct.WaitGroup.html
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
