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

//! Asynchronous primitives for one-time coordination.
//!
//! # Choosing a primitive
//!
//! * `Once` runs a side-effecting initializer successfully once without storing a value. A
//!   cancelled or panicked attempt leaves it available for another caller to retry.
//! * `OnceCell` stores one value from an initializer supplied at access time. Failed, cancelled, or
//!   panicked attempts leave the cell empty and retryable.
//! * `LazyCell` owns one initializer and preserves the same in-flight future across caller
//!   cancellation. A panic poisons the cell instead of starting another attempt.
//! * `OnceMap` applies retryable `OnceCell` initialization per key and retains successful values
//!   until they are explicitly removed.
//!
//! These primitives retain completed state. Use `singleflight::Group` when only overlapping calls
//! for a key should share work and later calls should execute again.

#[cfg(feature = "lazy-cell")]
mod lazy_cell;
#[cfg(feature = "once")]
mod once;
#[cfg(feature = "once-cell")]
mod once_cell;
#[cfg(feature = "once-map")]
mod once_map;

#[cfg(feature = "lazy-cell")]
pub use self::lazy_cell::LazyCell;
#[cfg(feature = "once")]
pub use self::once::Once;
#[cfg(feature = "once-cell")]
pub use self::once_cell::OnceCell;
#[cfg(feature = "once-map")]
pub use self::once_map::OnceMap;
