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

//! Coordinate completion across a dynamically sized group of participants.
//!
//! A [`WaitGroup`] starts with one participant. Cloning its handle registers another participant.
//! Awaiting a handle marks that participant complete and waits until every other participant has
//! completed. Dropping a handle marks its participant complete without waiting.
//!
//! Participants are symmetric: any number of them may wait for the same completion. A waiting
//! participant no longer keeps the group pending, and all waiters are notified when the last
//! remaining handle is awaited or dropped. Existing handles may register more participants by
//! cloning until the group completes; completion is one-shot.
//!
//! A coordinator can hand a clone to each worker, let workers drop their handles when done,
//! and await its own handle. To share completion with independent observers, convert a handle
//! into a [`Wait`] once and clone that future. Cloning the group itself adds more work.
//!
//! Completion acquires the state published before every participant completed, so work performed
//! by those participants is visible after the wait returns.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use asyncband::waitgroup::WaitGroup;
//!
//! async fn do_work() {}
//!
//! let group = WaitGroup::new();
//! let mut tasks = vec![];
//!
//! for _ in 0..3 {
//!     let participant = group.clone();
//!     tasks.push(tokio::spawn(async move {
//!         do_work().await;
//!         drop(participant);
//!     }));
//! }
//!
//! group.await;
//! for task in tasks {
//!     task.await.unwrap();
//! }
//! # }
//! ```

use std::fmt;
use std::future::Future;
use std::future::IntoFuture;
use std::ops::Deref;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use crate::waitgroup::waiters::Registration;
use crate::waitgroup::waiters::Waiters;

mod waiters;

struct State {
    // Participants hold one collective observer count until completion has detached the wakers.
    // The remaining observer counts belong to individual Wait futures.
    handles: AtomicUsize,
    observers: AtomicUsize,
    waiters: Waiters,
}

impl State {
    fn new() -> Self {
        Self {
            handles: AtomicUsize::new(1),
            observers: AtomicUsize::new(1),
            waiters: Waiters::default(),
        }
    }

    #[inline]
    fn poll_wait(&self, token: &mut Registration, cx: &mut Context<'_>) -> Poll<()> {
        if self.handles.load(Ordering::Acquire) == 0 {
            *token = Registration::Set(None);
            return Poll::Ready(());
        }

        self.waiters.register(token, cx.waker(), &self.handles)
    }

    #[inline]
    fn unregister(&self, token: &mut Registration) {
        if !matches!(token, Registration::Set(None)) && self.handles.load(Ordering::Acquire) != 0 {
            self.waiters.unregister(token, &self.handles);
        }
    }
}

// One observer reference anchors the allocation until the last participant has notified waiters.
// Each Wait owns an independent observer reference. The participant count keeps the anchor alive
// across registration and notification. Duplicating this pointer requires acquiring a count.
struct Shared(NonNull<State>);

// SAFETY: all shared access goes through State, which is Send + Sync.
unsafe impl Send for Shared {}
unsafe impl Sync for Shared {}

impl Deref for Shared {
    type Target = State;
    #[inline]
    fn deref(&self) -> &State {
        // SAFETY: each caller holds a participant or an observer count.
        unsafe { self.0.as_ref() }
    }
}

impl Shared {
    #[inline]
    fn register_handle(&self) -> Self {
        // Borrowing a participant keeps the count above zero. Registration publishes no data.
        if self.handles.fetch_add(1, Ordering::Relaxed) >= isize::MAX as usize {
            std::process::abort();
        }
        Self(self.0)
    }

    #[inline]
    fn release_handle(&mut self) {
        // The last participant acquires the preceding releases before notifying observers.
        let previous = self.handles.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "a live handle must own one count");
        if previous == 1 {
            self.finish();
        }
    }

    // Keep notification and reclamation off the nonfinal participant drop path.
    #[inline(never)]
    fn finish(&mut self) {
        self.waiters.wake_all();
        self.release_observer();
    }

    #[inline]
    fn observe(&self) -> Self {
        if self.observers.fetch_add(1, Ordering::Relaxed) >= isize::MAX as usize {
            std::process::abort();
        }
        Self(self.0)
    }

    #[inline]
    fn release_observer(&mut self) {
        if self.observers.fetch_sub(1, Ordering::AcqRel) == 1 {
            // SAFETY: all observers and the collective participant anchor have been released.
            unsafe { drop(Box::from_raw(self.0.as_ptr())) };
        }
    }
}

/// A handle representing one participant in a dynamically sized wait group.
///
/// See the [module level documentation](self) for more.
pub struct WaitGroup {
    // All participants share one reference to the allocation.
    state: Shared,
}

impl fmt::Debug for WaitGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitGroup").finish_non_exhaustive()
    }
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitGroup {
    /// Creates a new `WaitGroup` containing one participant.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::waitgroup::WaitGroup;
    ///
    /// let wg = WaitGroup::new();
    /// ```
    #[inline]
    pub fn new() -> Self {
        Self {
            state: Shared(NonNull::from(Box::leak(Box::new(State::new())))),
        }
    }
}

impl Clone for WaitGroup {
    /// Registers another participant and returns its handle.
    ///
    /// The group completes after every participant has completed, either by awaiting or dropping
    /// its handle.
    #[inline]
    fn clone(&self) -> Self {
        Self {
            state: self.state.register_handle(),
        }
    }
}

impl Drop for WaitGroup {
    #[inline]
    fn drop(&mut self) {
        self.state.release_handle();
    }
}

impl IntoFuture for WaitGroup {
    type Output = ();
    type IntoFuture = Wait;

    /// Marks this participant complete and returns a future observing group completion.
    ///
    /// # Cancellation
    ///
    /// Conversion marks this participant complete immediately. Dropping the returned future does
    /// not undo that completion; see [`Wait`] for observer cancellation.
    #[inline]
    fn into_future(self) -> Self::IntoFuture {
        let state = self.state.observe();
        drop(self);
        Wait {
            token: Registration::Set(None),
            state,
        }
    }
}

/// A future that completes when every [`WaitGroup`] participant has completed.
///
/// Converting a [`WaitGroup`] into this future immediately marks that participant complete,
/// even before the future is polled. Cloning `Wait` adds an observer without a participant.
///
/// # Cancellation
///
/// Dropping a pending future unregisters only that observer. The participant remains complete
/// and other observers are unaffected.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Wait {
    token: Registration,
    state: Shared,
}

impl Clone for Wait {
    /// Creates a new future that observes the same group completion.
    ///
    /// This does not register another participant.
    ///
    /// # Cancellation
    ///
    /// Dropping the returned future unregisters only that observer; see [`Wait`].
    #[inline]
    fn clone(&self) -> Self {
        Wait {
            token: Registration::Set(None),
            state: self.state.observe(),
        }
    }
}

impl fmt::Debug for Wait {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wait").finish_non_exhaustive()
    }
}

impl Future for Wait {
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self { token, state } = self.get_mut();
        state.poll_wait(token, cx)
    }
}

impl Drop for Wait {
    #[inline]
    fn drop(&mut self) {
        self.state.unregister(&mut self.token);
        self.state.release_observer();
    }
}
