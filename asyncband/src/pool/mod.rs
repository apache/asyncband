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

//! Runtime-agnostic object pools for async Rust.
//!
//! This module provides two implementations: [bounded pool](bounded::Pool) and
//! [unbounded pool](unbounded::Pool).
//!
//! # Bounded pool
//!
//! A bounded pool creates and recycles objects with full management. You _cannot_ put an object to
//! the pool manually.
//!
//! The pool is bounded by the `max_size` config option of [`PoolConfig`](bounded::PoolConfig). If
//! the pool reaches the maximum size, it will block all the [`Pool::get`](bounded::Pool::get) calls
//! until an object is returned to the pool or an object is detached from the pool.
//!
//! Bounded pools are useful for pooling database connections.
//!
//! ## Examples
//!
//! The following example shows the core managed-pool workflow.
//!
//! ```
//! use asyncband::pool::ManageObject;
//! use asyncband::pool::ObjectStatus;
//! use asyncband::pool::bounded::Pool;
//! use asyncband::pool::bounded::PoolConfig;
//!
//! struct Compute;
//! impl Compute {
//!     async fn do_work(&self) -> i32 {
//!         42
//!     }
//! }
//!
//! struct Manager;
//! impl ManageObject for Manager {
//!     type Object = Compute;
//!     type Error = ();
//!
//!     async fn create(&self) -> Result<Self::Object, Self::Error> {
//!         Ok(Compute)
//!     }
//!
//!     async fn is_recyclable(
//!         &self,
//!         o: &mut Self::Object,
//!         status: &ObjectStatus,
//!     ) -> Result<(), Self::Error> {
//!         Ok(())
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! let pool = Pool::new(PoolConfig::new(16), Manager);
//! let o = pool.get().await.unwrap();
//! assert_eq!(o.do_work().await, 42);
//! # }
//! ```
//!
//! # Unbounded pool
//!
//! An unbounded pool, on the other hand, allows you to put objects to the pool manually. You can
//! use it like Go's [`sync.Pool`](https://pkg.go.dev/sync#Pool).
//!
//! To configure a factory for creating objects when the pool is empty, like `sync.Pool`'s `New`,
//! you can create the unbounded pool via [`Pool::new`](unbounded::Pool::new) with an
//! implementation of [`ManageObject`].
//!
//! ## Examples
//!
//! The following example shows a manually populated unbounded pool.
//!
//! ```
//! use asyncband::pool::unbounded::Pool;
//! use asyncband::pool::unbounded::PoolConfig;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let pool = Pool::<Vec<u8>>::never_manage(PoolConfig::default());
//!
//! let result = pool.get().await;
//! assert_eq!(result.unwrap_err().to_string(), "unbounded pool is empty");
//!
//! pool.extend_one(Vec::with_capacity(1024));
//! let o = pool.get().await.unwrap();
//! assert_eq!(o.capacity(), 1024);
//! # }
//! ```
//!
//! # FAQ
//!
//! ## Why is timeout configuration outside the pool?
//!
//! Many async object pool implementations allow multiple timeout settings, such as wait, create,
//! and recycle timeouts.
//!
//! This introduces two major problems:
//!
//! First, implementing timeouts inside the pool requires a timer implementation such as
//! `tokio::time`. This would prevent the pool from being runtime-agnostic. The pool could depend on
//! a timer trait, but the Rust ecosystem does not yet have a standard one.
//!
//! Second, timeout options add configuration complexity without necessarily expressing the caller's
//! actual deadline. For example, end users often care about the total time used to obtain an
//! object. This is not solely a wait, create, or recycle timeout, but a conditional composition of
//! all internal operations.
//!
//! Thus, we propose a caller-side timeout solution:
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use asyncband::pool::bounded::Object;
//! use asyncband::pool::bounded::Pool;
//!
//! #[derive(Debug, Clone)]
//! pub struct ConnectionPool {
//!     pool: Arc<Pool<ManageConnection>>,
//! }
//!
//! impl ConnectionPool {
//!     pub async fn acquire(&self) -> Result<Object<ManageConnection>, Error> {
//!         const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(60);
//!
//!         // note that users can choose any timer implementation here
//!         let result = tokio::time::timeout(ACQUIRE_TIMEOUT, self.pool.get()).await;
//!
//!         // ... processing the result
//!     }
//! }
//! ```
//!
//! ## Why are before/after hooks outside the pool?
//!
//! Similar to the second point above, before/after hooks are hard to configure generally. Small
//! operations are easy to write in place, while passing larger blocks as closures can introduce
//! lifetime and ownership constraints. Error handling also depends on the surrounding application.
//!
//! The module provides an ordinary object-pool interface, so callers can add before/after logic in
//! the manager implementation or a wrapper.
//!
//! For example, all the "post-create", "pre-recycle", and "post-recycle" hooks can be implemented
//! as:
//!
//! ```
//! use asyncband::pool::ManageObject;
//! use asyncband::pool::ObjectStatus;
//!
//! struct Manager;
//! impl ManageObject for Manager {
//!     type Object = i32;
//!     type Error = std::convert::Infallible;
//!
//!     async fn create(&self) -> Result<Self::Object, Self::Error> {
//!         let o = 42;
//!         // any post-create hooks
//!         Ok(o)
//!     }
//!
//!     async fn is_recyclable(
//!         &self,
//!         _o: &mut Self::Object,
//!         _status: &ObjectStatus,
//!     ) -> Result<(), Self::Error> {
//!         // any pre-recycle hooks
//!         // determine whether the object is recyclable
//!         // any post-recycle hooks
//!         Ok(())
//!     }
//! }
//! ```

pub use common::ManageObject;
pub use common::ObjectStatus;
pub use common::QueueStrategy;
pub use common::RecycleCancelledStrategy;
pub use retain_spec::RetainResult;

mod common;
mod mutex;
mod retain_spec;

pub mod bounded;
pub mod unbounded;
