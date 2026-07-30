// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Compile-fail tests for guard `Send` and `Sync` contracts.
//!
//! An owned guard can hold the last strong reference to its lock. Moving the guard can therefore
//! move destruction of the protected value to another thread, so the value must be `Send`.
//!
//! `std::sync::MutexGuard` is `Sync` but not `Send`, making it a useful regression case:
//!
//! ```compile_fail
//! use std::sync::MutexGuard;
//!
//! use mea::rwlock::OwnedRwLockReadGuard;
//!
//! fn assert_send<T: Send>() {}
//!
//! assert_send::<OwnedRwLockReadGuard<MutexGuard<'static, ()>>>();
//! ```
