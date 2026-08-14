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

//! Compile-fail tests for mutable guard variance.
//!
//! A guard that provides mutable access to its target must be invariant over the target type.
//!
//! `MutexGuard`:
//!
//! ```compile_fail
//! use asyncband::mutex::MutexGuard;
//!
//! fn shorten<'lock, 'short: 'lock>(
//!     guard: MutexGuard<'lock, &'static str>,
//!     value: &'short str,
//! ) -> MutexGuard<'lock, &'short str> {
//!     let mut guard: MutexGuard<'lock, &'short str> = guard;
//!     *guard = value;
//!     guard
//! }
//! ```
//!
//! `OwnedMutexGuard`:
//!
//! ```compile_fail
//! use asyncband::mutex::OwnedMutexGuard;
//!
//! fn shorten<'short>(
//!     guard: OwnedMutexGuard<&'static str>,
//!     value: &'short str,
//! ) -> OwnedMutexGuard<&'short str> {
//!     let mut guard: OwnedMutexGuard<&'short str> = guard;
//!     *guard = value;
//!     guard
//! }
//! ```
//!
//! `MappedMutexGuard`:
//!
//! ```compile_fail
//! use asyncband::mutex::MappedMutexGuard;
//!
//! fn shorten<'lock, 'short: 'lock>(
//!     guard: MappedMutexGuard<'lock, &'static str>,
//!     value: &'short str,
//! ) -> MappedMutexGuard<'lock, &'short str> {
//!     let mut guard: MappedMutexGuard<'lock, &'short str> = guard;
//!     *guard = value;
//!     guard
//! }
//! ```
//!
//! `OwnedMappedMutexGuard`:
//!
//! ```compile_fail
//! use asyncband::mutex::OwnedMappedMutexGuard;
//!
//! fn shorten<'short>(
//!     guard: OwnedMappedMutexGuard<(), &'static str>,
//!     value: &'short str,
//! ) -> OwnedMappedMutexGuard<(), &'short str> {
//!     let mut guard: OwnedMappedMutexGuard<(), &'short str> = guard;
//!     *guard = value;
//!     guard
//! }
//! ```
//!
//! `RwLockWriteGuard`:
//!
//! ```compile_fail
//! use asyncband::rwlock::RwLockWriteGuard;
//!
//! fn shorten<'lock, 'short: 'lock>(
//!     guard: RwLockWriteGuard<'lock, &'static str>,
//!     value: &'short str,
//! ) -> RwLockWriteGuard<'lock, &'short str> {
//!     let mut guard: RwLockWriteGuard<'lock, &'short str> = guard;
//!     *guard = value;
//!     guard
//! }
//! ```
//!
//! `OwnedRwLockWriteGuard`:
//!
//! ```compile_fail
//! use asyncband::rwlock::OwnedRwLockWriteGuard;
//!
//! fn shorten<'short>(
//!     guard: OwnedRwLockWriteGuard<&'static str>,
//!     value: &'short str,
//! ) -> OwnedRwLockWriteGuard<&'short str> {
//!     let mut guard: OwnedRwLockWriteGuard<&'short str> = guard;
//!     *guard = value;
//!     guard
//! }
//! ```
//!
//! `MappedRwLockWriteGuard`:
//!
//! ```compile_fail
//! use asyncband::rwlock::MappedRwLockWriteGuard;
//!
//! fn shorten<'lock, 'short: 'lock>(
//!     guard: MappedRwLockWriteGuard<'lock, &'static str>,
//!     value: &'short str,
//! ) -> MappedRwLockWriteGuard<'lock, &'short str> {
//!     let mut guard: MappedRwLockWriteGuard<'lock, &'short str> = guard;
//!     *guard = value;
//!     guard
//! }
//! ```
//!
//! `OwnedMappedRwLockWriteGuard`:
//!
//! ```compile_fail
//! use asyncband::rwlock::OwnedMappedRwLockWriteGuard;
//!
//! fn shorten<'short>(
//!     guard: OwnedMappedRwLockWriteGuard<(), &'static str>,
//!     value: &'short str,
//! ) -> OwnedMappedRwLockWriteGuard<(), &'short str> {
//!     let mut guard: OwnedMappedRwLockWriteGuard<(), &'short str> = guard;
//!     *guard = value;
//!     guard
//! }
//! ```
