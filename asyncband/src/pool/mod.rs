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
//! This module provides a manager-created [bounded pool](bounded::Pool) and an
//! [unbounded pool](unbounded::Pool) that can also accept objects supplied by callers.
//!
//! Both implementations provide resource reuse without taking ownership of runtime policy. They do
//! not start maintenance tasks or install timers. Callers decide how to schedule maintenance and
//! can wrap operations such as [`bounded::Pool::get`] in the deadline mechanism of their runtime.
//!
//! # Bounded pool
//!
//! A bounded pool uses a [`ManageObject`] implementation to create, validate, and detach objects.
//! Objects cannot be inserted manually.
//!
//! The pool is bounded by the `max_size` config option of [`PoolConfig`](bounded::PoolConfig). If
//! the pool reaches the maximum size, additional [`Pool::get`](bounded::Pool::get) calls wait until
//! an object is returned to or detached from the pool.
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
//! An unbounded pool accepts manually supplied objects and can be used like Go's
//! [`sync.Pool`](https://pkg.go.dev/sync#Pool).
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
//! let pool = Pool::<Vec<u8>>::never_manage(PoolConfig::default());
//!
//! assert!(pool.try_get().is_none());
//!
//! pool.extend_one(Vec::with_capacity(1024));
//! let o = pool.try_get().unwrap();
//! assert_eq!(o.capacity(), 1024);
//! ```
//!
//! # FAQ
//!
//! ## Why does the caller control timeouts?
//!
//! A timer inside the pool would couple it to a runtime or require one adapter per timer ecosystem.
//! Separate wait, create, and recycle timeouts also do not necessarily express the caller's actual
//! deadline for the complete checkout operation. Asyncband therefore returns an ordinary future so
//! the caller can apply one end-to-end deadline with its chosen timer:
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
//!         // Callers can use the timer implementation of their runtime.
//!         let result = tokio::time::timeout(ACQUIRE_TIMEOUT, self.pool.get()).await;
//!
//!         // ... processing the result
//!     }
//! }
//! ```
//!
//! ## Why are general before/after hooks outside the pool?
//!
//! Before/after behavior is application policy. Small operations are clearer at the call site,
//! while larger behavior can live in the manager or an application wrapper without forcing a
//! general closure and error model into the pool.
//!
//! For example, create and recycle behavior can be expressed directly by [`ManageObject`]:
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
//!         object: &mut Self::Object,
//!         status: &ObjectStatus,
//!     ) -> Result<(), Self::Error> {
//!         // Validate or refresh `object`, using `status` when useful.
//!         Ok(())
//!     }
//! }
//! ```

mod common;
mod state;

pub mod bounded;
pub mod unbounded;

pub use self::common::ManageObject;
pub use self::common::ObjectStatus;
pub use self::common::QueueStrategy;
pub use self::common::RecycleCancelledStrategy;
pub use self::common::RetainResult;
