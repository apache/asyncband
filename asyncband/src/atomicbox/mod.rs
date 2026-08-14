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

// This module is derived from https://github.com/jorendorff/atomicbox/.

//! [`AtomicBox`] and [`AtomicOptionBox`] are safe, owning versions of std's [`AtomicPtr`].
//!
//! This can be useful to avoid resource leaks when you forget to call `Box::from_raw` on
//! the pointer stored in `AtomicPtr`.
//!
//! Unfortunately, the only operations you can perform on an atomic box are swaps and stores: you
//! can not just use the box without taking ownership of it. Imagine a `Box` without `Deref` or
//! `DerefMut` implementations, and you can get the idea. Still, this is sufficient for some
//! lock-free data structures, so here it is.
//!
//! ## Why no `Deref`?
//!
//! It would not be safe. The point of an `AtomicBox` is that other threads can obtain the boxed
//! value, take ownership of it, even drop it, all without taking a lock. So there is no safe way to
//! borrow that value—except to swap it out of the `AtomicBox` yourself.
//!
//! This is pretty much the same reason you can not borrow a reference to the contents of any other
//! atomic type. It would invite data races. The only difference here is that those contents happen
//! to be on the heap.
//!
//! [`AtomicPtr`]: std::sync::atomic::AtomicPtr

mod atomic_box;
mod atomic_option_box;

pub use atomic_box::AtomicBox;
pub use atomic_option_box::AtomicOptionBox;
