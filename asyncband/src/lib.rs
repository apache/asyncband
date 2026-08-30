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
//! # #[cfg(feature = "mutex")]
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
//! # #[cfg(not(feature = "mutex"))]
//! # fn main() {}
//! ```
//!
//! # API map
//!
//! | Area                  | API                                           | Feature        | Use                                                                                                    |
//! |-----------------------|-----------------------------------------------|----------------|--------------------------------------------------------------------------------------------------------|
//! | Shared state          | [`Mutex`](mutex::Mutex)                       | `mutex`        | Protect shared data with asynchronous mutual exclusion.                                                |
//! |                       | [`RwLock`](rwlock::RwLock)                    | `rwlock`       | Allow multiple readers or one writer.                                                                  |
//! |                       | [`Condvar`](condvar::Condvar)                 | `condvar`      | Wait for notifications while releasing a mutex.                                                        |
//! | Initialization        | [`Once`](once::Once)                          | `once`         | Run asynchronous initialization exactly once.                                                          |
//! |                       | [`OnceCell`](once::OnceCell)                  | `once-cell`    | Initialize and store one asynchronous value.                                                           |
//! |                       | [`LazyCell`](once::LazyCell)                  | `lazy-cell`    | Lazily initialize a value with a stored asynchronous function.                                         |
//! |                       | [`OnceMap`](once::OnceMap)                    | `once-map`     | Initialize and store one value per key.                                                                |
//! | Task coordination     | [`Barrier`](barrier::Barrier)                 | `barrier`      | Wait until all participants reach a synchronization point.                                             |
//! |                       | [`Completion`](completion::Completion)       | `completion`   | Publish one shared result to any number of current and future observers.                                |
//! |                       | [`ManualResetEvent`](event::ManualResetEvent) | `event`        | Reuse a level-triggered signal that releases all current waiters.                                      |
//! |                       | [`Latch`](latch::Latch)                       | `latch`        | Wait until a one-way countdown completes.                                                              |
//! |                       | [`WaitGroup`](waitgroup::WaitGroup)           | `waitgroup`    | Wait for a dynamic group of tasks to finish.                                                           |
//! |                       | [`Shutdown`](shutdown::Shutdown)              | `shutdown`     | Coordinate shutdown signals and completion.                                                            |
//! | Channels              | [`oneshot`]                                   | `oneshot`      | Send one value from one sender to one receiver.                                                        |
//! |                       | [`mpsc`]                                      | `mpsc`         | Send each value from multiple producers to one receiver through a bounded or unbounded queue.          |
//! |                       | [`broadcast`]                                 | `broadcast`    | Broadcast values from one or more producers and retain them until every active receiver consumes them. |
//! |                       | [`watch`]                                     | `watch`        | Publish the latest state to independently tracked receivers and coalesce intermediate updates.         |
//! | Resource reuse        | [`pool`]                                      | `pool`         | Reuse objects through bounded or unbounded pool variants.                                              |
//! | Workload coordination | [`Semaphore`](semaphore::Semaphore)           | `semaphore`    | Control concurrent access with permits.                                                                |
//! |                       | [`Group`](singleflight::Group)                | `singleflight` | Coalesce concurrent calls for the same key.                                                            |
//! | Sync interop          | [`FutureExt`](blocking::FutureExt)            | `blocking`     | Drive one runtime-agnostic future from a blocking thread.                                              |
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
mod internal;

#[cfg(feature = "barrier")]
pub mod barrier;
#[cfg(feature = "blocking")]
pub mod blocking;
#[cfg(feature = "broadcast")]
pub mod broadcast;
#[cfg(feature = "completion")]
pub mod completion;
#[cfg(feature = "condvar")]
pub mod condvar;
#[cfg(feature = "event")]
pub mod event;
#[cfg(feature = "latch")]
pub mod latch;
#[cfg(feature = "mpsc")]
pub mod mpsc;
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
pub mod oneshot;
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
#[cfg(feature = "watch")]
pub mod watch;

#[cfg(all(test, any(feature = "once-map", feature = "singleflight")))]
mod test_support;
