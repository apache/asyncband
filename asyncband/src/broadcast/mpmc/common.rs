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

//! Shared receiving types and helpers for MPMC broadcast channels.

use std::sync::Arc;

/// A received message together with whether advancing the cursor vacated the head slot.
///
/// The two travel together because the caller acts on both with the channel unlocked: a bounded
/// channel releases the freed slot before touching the payload, and an owning receive uses
/// `reclaimed` to decide whether it can unwrap the `Arc` instead of cloning.
pub struct Received<T> {
    pub msg: Arc<T>,
    pub reclaimed: bool,
}

/// Yields the received message owned with the channel unlocked.
///
/// When `received.reclaimed` is true, advancing the cursor popped the message from the backlog, so
/// the caller may hold the only remaining reference and move the payload out instead of cloning
/// it.
///
/// Ownership is decided from that bookkeeping rather than by probing the reference count on every
/// receive. Calling [`Arc::unwrap_or_clone`] while the slot is still retained would fail under
/// fan-out, and its failed compare-exchange writes to a cache line that every receiver draining
/// the message shares.
///
/// This may run `T::clone` and `T::drop`, either of which can panic, so a bounded channel must
/// already have released the reclaimed capacity before calling it.
pub fn take_msg<T: Clone>(received: Received<T>) -> T {
    if !received.reclaimed {
        return (*received.msg).clone();
    }

    // Another receiver can still hold an in-flight reference to the same message, or an `Arc` it
    // kept from a shared receive, so the clone remains the fallback.
    Arc::unwrap_or_clone(received.msg)
}

#[cfg(test)]
pub mod tests {
    use std::cell::Cell;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::rc::Rc;

    pub struct InspectOnDrop(Box<dyn Fn()>);

    impl Drop for InspectOnDrop {
        fn drop(&mut self) {
            (self.0)();
        }
    }

    pub fn assert_overflow_drops_unlocked(
        inspect: impl Fn() -> Option<usize> + 'static,
        send: impl FnOnce(InspectOnDrop),
    ) {
        let observed = Rc::new(Cell::new(None));
        let result = observed.clone();
        let payload = InspectOnDrop(Box::new(move || result.set(inspect())));
        let overflow = catch_unwind(AssertUnwindSafe(|| send(payload)));
        assert!(overflow.is_err());
        assert_eq!(
            observed.get(),
            Some(0),
            "destructor could not inspect the backlog"
        );
    }
}
