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
//!         participant.await;
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
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use crate::internal::mutex::Mutex;
use crate::internal::wake_all;
use crate::internal::wakerset::WakerSet;
use crate::internal::wakerset::WakerToken;

#[derive(Debug)]
struct State {
    // Wait futures also own the state allocation, so Arc's strong count cannot represent handles.
    // Zero is terminal: after it is published, no new handle or waiter can be registered.
    handles: AtomicUsize,
    waiters: Mutex<WakerSet>,
}

impl State {
    fn new() -> Self {
        Self {
            handles: AtomicUsize::new(1),
            waiters: Mutex::new(WakerSet::new()),
        }
    }

    fn register_handle(&self) {
        // The borrowed source handle keeps the count above zero. Registration publishes no data,
        // so it does not need to synchronize with completion.
        self.handles.fetch_add(1, Ordering::Relaxed);
    }

    fn release_handle(&self) {
        // Every decrement is an RMW in the release sequence. A waiter that acquires zero therefore
        // observes work published before every preceding handle release.
        let previous = self.handles.fetch_sub(1, Ordering::Release);
        debug_assert!(previous > 0, "a live handle must own one count");
        if previous != 1 {
            return;
        }

        let wakers = {
            let mut waiters = self.waiters.lock();
            waiters.take_all()
        };
        wake_all(wakers);
    }

    fn poll_wait(&self, token: &mut Option<WakerToken>, cx: &mut Context<'_>) -> Poll<()> {
        if self.handles.load(Ordering::Acquire) == 0 {
            *token = None;
            return Poll::Ready(());
        }

        let mut waiters = self.waiters.lock();
        if self.handles.load(Ordering::Acquire) == 0 {
            *token = None;
            return Poll::Ready(());
        }

        let retired_waker = waiters.register(token, cx.waker());
        drop(waiters);
        drop(retired_waker);
        Poll::Pending
    }

    fn unregister(&self, token: &mut Option<WakerToken>) {
        if token.is_none() {
            return;
        }

        let mut waiters = self.waiters.lock();
        // Reaching zero is terminal, so the zero transition either owns this waker or has already
        // taken it. Unlike reusable waker sets, no epoch is needed to disambiguate a later
        // registration.
        if self.handles.load(Ordering::Acquire) == 0 {
            *token = None;
            return;
        }

        let removed_waker = waiters.unregister(token);
        drop(waiters);
        drop(removed_waker);
    }
}

/// A handle representing one participant in a dynamically sized wait group.
///
/// See the [module level documentation](self) for more.
pub struct WaitGroup {
    // Keeping this optional lets `into_future` transfer the allocation to `Wait` without an
    // otherwise redundant Arc increment/decrement pair. The option retains Arc's pointer niche.
    state: Option<Arc<State>>,
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
    pub fn new() -> Self {
        Self {
            state: Some(Arc::new(State::new())),
        }
    }
}

impl Clone for WaitGroup {
    /// Registers another participant and returns its handle.
    ///
    /// The group completes after every participant has completed, either by awaiting or dropping
    /// its handle.
    fn clone(&self) -> Self {
        let state = self
            .state
            .as_ref()
            .expect("a live WaitGroup owns its state")
            .clone();
        // Every handle owns one strong reference, while Wait observers may own additional ones.
        // Arc's own overflow guard therefore fires before this equally wide counter can wrap.
        state.register_handle();
        Self { state: Some(state) }
    }
}

impl Drop for WaitGroup {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            state.release_handle();
        }
    }
}

impl IntoFuture for WaitGroup {
    type Output = ();
    type IntoFuture = Wait;

    /// Marks this participant complete and waits for every other participant to complete.
    fn into_future(mut self) -> Self::IntoFuture {
        let state = self.state.take().expect("a live WaitGroup owns its state");
        state.release_handle();
        Wait { token: None, state }
    }
}

/// A future that completes when every [`WaitGroup`] participant has completed.
///
/// Converting a [`WaitGroup`] into this future marks that handle's participant complete. Cloning a
/// `Wait` creates another observer without registering a participant. Dropping a pending `Wait`
/// only unregisters that observer; the participant remains complete and other observers are not
/// affected.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Wait {
    token: Option<WakerToken>,
    state: Arc<State>,
}

impl Clone for Wait {
    /// Creates a new future that observes the same group completion.
    ///
    /// This does not register another participant.
    fn clone(&self) -> Self {
        Wait {
            token: None,
            state: self.state.clone(),
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

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self { token, state } = self.get_mut();
        state.poll_wait(token, cx)
    }
}

impl Drop for Wait {
    fn drop(&mut self) {
        self.state.unregister(&mut self.token);
    }
}
