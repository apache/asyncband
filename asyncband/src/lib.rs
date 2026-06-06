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

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

//! Composable, runtime-agnostic concurrency building blocks for async Rust.
//!
//! `asyncband` provides synchronization, initialization, task coordination, channels, resource
//! reuse, and workload control without choosing an executor for the application. Its async APIs use
//! standard futures and wakers, so they can run on Tokio, async-std, smol, or a custom executor.
//!
//! # Getting started
//!
//! Public APIs are enabled through opt-in Cargo features, and no features are enabled by default.
//! Enable the APIs your application needs:
//!
//! ```toml
//! asyncband = { version = "0.7", features = ["mutex", "oneshot"] }
//! ```
//!
//! Then use the selected APIs directly:
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
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
//! # API guide
//!
//! | Use case                   | APIs                                                                                  | Cargo features                              |
//! | -------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------- |
//! | Protect shared state       | [`mutex::Mutex`], [`rwlock::RwLock`], [`condvar::Condvar`]                            | `mutex`, `rwlock`, `condvar`                |
//! | Initialize values once     | [`once::Once`], [`once::OnceCell`], [`once::LazyCell`], [`once::OnceMap`]             | `once`, `once-cell`, `lazy-cell`, `once-map` |
//! | Coordinate tasks           | [`barrier::Barrier`], [`latch::Latch`], [`waitgroup::WaitGroup`], [`shutdown`]        | `barrier`, `latch`, `waitgroup`, `shutdown` |
//! | Send values                | [`oneshot::channel`], [`mpsc::bounded`], [`mpsc::unbounded`], [`broadcast::unbounded`] | `oneshot`, `mpsc`, `broadcast`              |
//! | Reuse objects              | [`pool::bounded`], [`pool::unbounded`]                                                | `pool`                                      |
//! | Coordinate workloads       | [`semaphore::Semaphore`], [`singleflight::Group`]                                     | `semaphore`, `singleflight`                 |
//! | Wait from synchronous code | [`blocking::FutureExt`]                                                               | `blocking`                                  |
//!
//! # Scope and runtime model
//!
//! The project is not limited to small or stateless primitives. Stateful tools such as
//! [`singleflight::Group`] and the [`pool`] module fit when they provide reusable coordination and
//! remain independent of executor policy.
//!
//! The async APIs do not start threads, spawn tasks, install timers, or require a runtime-specific
//! reactor. Task placement, deadlines, retries, periodic maintenance, and lifecycle orchestration
//! remain with the caller. Await Asyncband futures inside any executor that polls standard Rust
//! futures, and compose those runtime services around them.
//!
//! # Async first, blocking by adaptation
//!
//! Async and synchronous primitives have different optimization constraints. Asyncband designs its
//! primitives for async use and provides the optional [`blocking`] module as a boundary adapter
//! instead of duplicating synchronous methods across every type. Sync-first implementations can
//! exploit OS- or platform-specific facilities and remain the domain of dedicated libraries.
//!
//! The adapter's single-future executor parks the calling thread and resumes it through the
//! future's waker. It is not a general-purpose async runtime, and futures that depend on a
//! runtime-specific timer or I/O driver may not make progress. See the module documentation for the
//! full execution constraints.
//!
//! # Thread safety
//!
//! Asyncband types implement `Send` and `Sync` only when their protected, transferred, or managed
//! values satisfy the required bounds. Consult each API's documentation for its exact contract.
//!
//! # Disclaimer
//!
//! Apache Asyncband (Incubating) is an effort undergoing incubation at the Apache Software
//! Foundation (ASF), sponsored by the Apache Incubator PMC.
//!
//! Incubation is required of all newly accepted projects until a further review indicates that the
//! infrastructure, communications, and decision making process have stabilized in a manner
//! consistent with other successful ASF projects.
//!
//! While incubation status is not necessarily a reflection of the completeness or stability of the
//! code, it does indicate that the project has yet to be fully endorsed by the ASF.
mod channel;
mod internal;

#[cfg(feature = "barrier")]
pub mod barrier;
#[cfg(feature = "blocking")]
pub mod blocking;
#[cfg(feature = "broadcast")]
pub use self::channel::broadcast;
#[cfg(feature = "condvar")]
pub mod condvar;
#[cfg(feature = "latch")]
pub mod latch;
#[cfg(feature = "mpsc")]
pub use self::channel::mpsc;
#[cfg(feature = "mutex")]
pub mod mutex;
#[cfg(any(
    feature = "lazy-cell",
    feature = "once",
    feature = "once-cell",
    feature = "once-map"
))]
pub mod once;
#[cfg(feature = "oneshot")]
pub use self::channel::oneshot;
#[cfg(feature = "pool")]
pub mod pool;
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

#[cfg(all(test, any(feature = "once-map", feature = "singleflight")))]
mod test_support;
