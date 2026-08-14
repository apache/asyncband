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

//! Asynchronous primitives for one-time async coordination.
//!
//! The module currently provides:
//!
//! * [`Once`]: A primitive that ensures a one-time asynchronous operation runs at most once, even
//!   when called concurrently.
//! * [`OnceCell`]: A cell that can be written to at most once, storing a value produced
//!   asynchronously.
//! * [`OnceMap`]: A hash map that runs computation only once for each key and stores the result.

#[allow(clippy::module_inception)]
mod once;
mod once_cell;
mod once_map;

pub use self::once::Once;
pub use self::once_cell::OnceCell;
pub use self::once_map::OnceMap;
