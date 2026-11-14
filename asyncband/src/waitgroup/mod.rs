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
//! A [`WaitGroup`] starts with one participant. Cloning a handle registers another participant,
//! dropping a handle marks its participant complete without waiting, and awaiting a handle marks
//! its participant complete before waiting for every other participant to complete.
//!
//! Participants are symmetric: workers can drop their handles when done while a coordinator awaits
//! its own handle, or multiple participants can each await completion. Converting a handle into
//! [`Wait`] marks that participant complete immediately, and cloning [`Wait`] creates additional
//! observers without registering new participants. Existing handles may register more participants
//! until the last live handle completes; completion is one-shot.
//!
//! Completion acquires the state published before every participant completed, so writes performed
//! before each handle completed are visible after [`Wait`] completes.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use std::sync::Arc;
//! use std::sync::atomic::AtomicBool;
//! use std::sync::atomic::Ordering;
//!
//! use asyncband::waitgroup::WaitGroup;
//!
//! let group = WaitGroup::new();
//! let completed = Arc::new([const { AtomicBool::new(false) }; 3]);
//!
//! for slot in 0..3 {
//!     let participant = group.clone();
//!     let completed = completed.clone();
//!     tokio::spawn(async move {
//!         completed[slot].store(true, Ordering::Relaxed);
//!         drop(participant);
//!     });
//! }
//!
//! group.await;
//! assert!(completed.iter().all(|done| done.load(Ordering::Relaxed)));
//! # }
//! ```

use std::fmt;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::register_waker;
use crate::internal::rwlock::RwLock;
use crate::internal::wakerset::WakerSet;
use crate::internal::wakerset::WakerToken;

#[derive(Default)]
enum Registration {
    #[default]
    Unregistered,
    Inline,
    Overflow(WakerToken),
}

#[derive(Default)]
struct Waiters {
    inline: Option<Waker>,
    overflow: Option<Box<WakerSet>>,
}

impl Waiters {
    #[inline]
    #[must_use = "drop the replaced waker after releasing the waiter lock"]
    fn register(&mut self, registration: &mut Registration, waker: &Waker) -> Option<Waker> {
        match registration {
            Registration::Unregistered if self.inline.is_none() => {
                self.inline = Some(waker.clone());
                *registration = Registration::Inline;
                None
            }
            Registration::Unregistered => {
                let token = self.overflow.get_or_insert_default().insert(waker);
                *registration = Registration::Overflow(token);
                None
            }
            Registration::Inline => {
                debug_assert!(
                    self.inline.is_some(),
                    "inline registration must occupy the inline waker slot"
                );
                register_waker(&mut self.inline, waker)
            }
            Registration::Overflow(token) => self
                .overflow
                .as_mut()
                .expect("overflow registration must have an allocated waker set")
                .update(token, waker),
        }
    }

    #[inline]
    #[must_use = "drop the removed waker after releasing the waiter lock"]
    fn unregister(&mut self, registration: &mut Registration) -> Option<Waker> {
        match mem::take(registration) {
            Registration::Unregistered => None,
            Registration::Inline => self.inline.take(),
            Registration::Overflow(token) => Some(
                self.overflow
                    .as_mut()
                    .expect("overflow registration must have an allocated waker set")
                    .remove(token),
            ),
        }
    }

    #[inline]
    fn wake_all(self) {
        if let Some(waker) = self.inline {
            waker.wake();
        }
        if let Some(overflow) = self.overflow {
            overflow.wake_all();
        }
    }
}

struct State {
    // Live `WaitGroup` handles share one collective reference in `refs` until the last participant
    // finishes notifying waiters; each `Wait` future owns an additional reference in `refs`.
    participants: AtomicUsize,
    refs: AtomicUsize,
    // Waiter mutations always acquire exclusive access, but `std::sync::RwLock` keeps uncontended
    // lock state inline on Darwin where `std::sync::Mutex` boxes a `pthread_mutex_t` on first
    // lock.
    waiters: RwLock<Waiters>,
}

impl State {
    #[inline]
    fn poll_wait(&self, registration: &mut Registration, cx: &mut Context<'_>) -> Poll<()> {
        if self.participants.load(Ordering::Acquire) == 0 {
            *registration = Registration::Unregistered;
            return Poll::Ready(());
        }

        self.register_wait(registration, cx.waker())
    }

    fn register_wait(&self, registration: &mut Registration, waker: &Waker) -> Poll<()> {
        let mut waiters = self.waiters.write();
        // `finish` detaches waiters under this same lock after storing zero, so either it wakes our
        // registration or we observe zero here and complete immediately.
        if self.participants.load(Ordering::Acquire) == 0 {
            *registration = Registration::Unregistered;
            return Poll::Ready(());
        }

        let retired_waker = waiters.register(registration, waker);
        drop(waiters);
        drop(retired_waker);
        Poll::Pending
    }

    #[inline]
    fn unregister(&self, registration: &mut Registration) {
        if matches!(registration, Registration::Unregistered) {
            return;
        }
        if self.participants.load(Ordering::Acquire) == 0 {
            *registration = Registration::Unregistered;
            return;
        }

        self.unregister_slow(registration);
    }

    fn unregister_slow(&self, registration: &mut Registration) {
        let mut waiters = self.waiters.write();
        if self.participants.load(Ordering::Acquire) == 0 {
            *registration = Registration::Unregistered;
            return;
        }

        let removed_waker = waiters.unregister(registration);
        drop(waiters);
        drop(removed_waker);
    }
}

struct Shared(NonNull<State>, PhantomData<State>);

// SAFETY: Shared state is synchronized through atomics and `RwLock<Waiters>`, and `State: Send +
// Sync`.
unsafe impl Send for Shared {}
unsafe impl Sync for Shared {}

impl Deref for Shared {
    type Target = State;

    #[inline]
    fn deref(&self) -> &State {
        // SAFETY: Every `Shared` owns either a participant count (anchoring the collective ref)
        // or an observer ref in `State::refs`.
        unsafe { self.0.as_ref() }
    }
}

impl Shared {
    fn new() -> Self {
        let state = Box::new(State {
            participants: AtomicUsize::new(1),
            refs: AtomicUsize::new(1),
            waiters: RwLock::new(Waiters::default()),
        });
        Self(NonNull::from(Box::leak(state)), PhantomData)
    }

    #[inline]
    fn clone_participant(&self) -> Self {
        // Borrowing a live participant keeps `participants` above zero. Registration publishes no
        // caller data; abort before wrapping `isize::MAX`.
        if self.participants.fetch_add(1, Ordering::Relaxed) >= isize::MAX as usize {
            std::process::abort();
        }
        Self(self.0, PhantomData)
    }

    #[inline]
    fn release_participant(&mut self) {
        // The last participant acquires all preceding releases before waking waiters.
        let previous = self.participants.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "a live WaitGroup must own one participant");
        if previous == 1 {
            self.finish();
        }
    }

    // Keep notification and reclamation off the inline non-final participant drop path.
    #[inline(never)]
    fn finish(&mut self) {
        let waiters = mem::take(&mut *self.waiters.write());
        waiters.wake_all();
        self.release_ref();
    }

    #[inline]
    fn clone_ref(&self) -> Self {
        if self.refs.fetch_add(1, Ordering::Relaxed) >= isize::MAX as usize {
            std::process::abort();
        }
        Self(self.0, PhantomData)
    }

    #[inline]
    fn release_ref(&mut self) {
        if self.refs.fetch_sub(1, Ordering::AcqRel) == 1 {
            // SAFETY: All observer references and the collective participant reference have ended.
            unsafe { drop(Box::from_raw(self.0.as_ptr())) };
        }
    }
}

/// A handle representing one participant in a dynamically sized wait group.
///
/// See the [module level documentation](self) for more.
pub struct WaitGroup {
    // All live participants share one collective reference in `State::refs`.
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
            state: Shared::new(),
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
            state: self.state.clone_participant(),
        }
    }
}

impl Drop for WaitGroup {
    #[inline]
    fn drop(&mut self) {
        self.state.release_participant();
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
        let state = self.state.clone_ref();
        drop(self);
        Wait {
            registration: Registration::Unregistered,
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
    registration: Registration,
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
            registration: Registration::Unregistered,
            state: self.state.clone_ref(),
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
        let Self {
            registration,
            state,
        } = self.get_mut();
        state.poll_wait(registration, cx)
    }
}

impl Drop for Wait {
    #[inline]
    fn drop(&mut self) {
        self.state.unregister(&mut self.registration);
        self.state.release_ref();
    }
}
