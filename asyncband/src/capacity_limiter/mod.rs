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

//! Limits concurrent work to one permit per caller-supplied borrower identity.
//!
//! [`CapacityLimiter`] keeps the identity registry, available capacity, resize deficit, and FIFO
//! waiter queue under one mutex. [`Permit`] returns capacity and removes its identity when dropped.
//! Async acquisitions register on first poll; a duplicate pending or held identity is rejected.
//!
//! This is a prototype for issue #224. A concrete workload is still needed to justify adding it
//! to the crate. The existing semaphore remains sufficient when borrower checks are unnecessary.
//!
//! # Identity and callbacks
//!
//! Identities are explicit `Eq + Hash + Clone` values, not runtime task identities. Key `Hash` and
//! `Eq` implementations must be stable, non-panicking, and must not re-enter the limiter: they run
//! under its state lock. Key cloning and destruction, and waker callbacks, run outside that lock.
//!
//! # Diagnostics
//!
//! `borrowed()` counts permits delivered to callers. `waiting()` counts registered acquisitions
//! not yet delivered, including committed grants awaiting another poll. Each accessor is an
//! individual snapshot; calling several accessors does not produce a combined atomic snapshot.
//!
//! Debug output for the limiter contains counts only. A permit or acquisition prints its own
//! borrower, without traversing the limiter or revealing other borrower identities.
//!
//! # Example
//!
//! ```
//! use asyncband::capacity_limiter::CapacityLimiter;
//! use asyncband::capacity_limiter::TryAcquireError;
//!
//! let limiter = CapacityLimiter::new(2);
//! let permit = limiter.try_acquire("request-1").unwrap();
//! assert_eq!(
//!     limiter.try_acquire("request-1").unwrap_err(),
//!     TryAcquireError::AlreadyBorrowed
//! );
//! limiter.set_total(0); // The existing permit remains valid.
//! drop(permit); // Repays the deficit.
//! assert_eq!(limiter.available(), 0);
//! limiter.set_total(1);
//! assert!(limiter.try_acquire("request-1").is_ok());
//! ```

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::wake_all;

#[derive(Debug)]
struct WaitNode {
    /// Set by the releasing side once this waiter has been handed a token.
    granted: bool,
    waker: Option<Waker>,
}

#[derive(Debug)]
struct State<B> {
    permits: usize,
    total: usize,
    /// Tokens owed to a shrink that could not be satisfied from available capacity.
    deficit: usize,
    borrowed: usize,
    borrowers: HashSet<B>,
    waiters: WaitList<WaitNode>,
}

impl<B> State<B> {
    /// Returns one token to the limiter and reports the waiter to wake, if any.
    ///
    /// The caller must wake outside the lock.
    fn release_one(&mut self) -> Option<Waker> {
        if self.deficit > 0 {
            self.deficit -= 1;
            return None;
        }

        match self.waiters.unlink_first_waiter(|node| {
            node.granted = true;
            true
        }) {
            // The node stays addressable until the waiter polls or is dropped.
            Some((_, node)) => node.waker.take(),
            None => {
                self.permits += 1;
                None
            }
        }
    }
}

/// A capacity limiter whose registry, capacity, and waiter queue share one lock.
pub struct CapacityLimiter<B> {
    state: Mutex<State<B>>,
}

impl<B> fmt::Debug for CapacityLimiter<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (total, available, borrowed, waiting) = {
            let state = self.state.lock();
            (
                state.total,
                state.permits,
                state.borrowed,
                state.borrowers.len() - state.borrowed,
            )
        };
        f.debug_struct("CapacityLimiter")
            .field("total", &total)
            .field("available", &available)
            .field("borrowed", &borrowed)
            .field("waiting", &waiting)
            .finish_non_exhaustive()
    }
}

impl<B> CapacityLimiter<B> {
    /// Creates a limiter that admits `total` concurrent borrowers.
    pub fn new(total: usize) -> Self {
        Self {
            state: Mutex::new(State {
                permits: total,
                total,
                deficit: 0,
                borrowed: 0,
                borrowers: HashSet::new(),
                waiters: WaitList::new(),
            }),
        }
    }

    /// Returns the configured total capacity.
    ///
    /// This is a snapshot taken under the same lock as permit accounting.
    pub fn total(&self) -> usize {
        self.state.lock().total
    }

    /// Returns the number of permits delivered to callers and not yet dropped.
    ///
    /// Committed grants awaiting another poll are not counted here.
    pub fn borrowed(&self) -> usize {
        self.state.lock().borrowed
    }

    /// Returns the number of tokens available without waiting.
    pub fn available(&self) -> usize {
        self.state.lock().permits
    }

    /// Returns the number of registered acquisitions not yet delivered to callers.
    ///
    /// This includes committed grants awaiting another poll. The count is exact at the instant
    /// this method holds the lock; it is not the length of the ungranted FIFO queue.
    pub fn waiting(&self) -> usize {
        let state = self.state.lock();
        state.borrowers.len() - state.borrowed
    }

    /// Sets the total capacity without revoking existing grants or delivered permits.
    ///
    /// A shrink first consumes available capacity, then records a deficit repaid by releases.
    /// Growth first repays that deficit, then grants queued borrowers in FIFO order.
    /// Zero and `usize::MAX` are valid totals.
    ///
    /// Concurrent calls commit under the state lock. Wake callbacks run after unlocking and may
    /// resize reentrantly. The adjustment is committed before waking; if one callback panics,
    /// remaining notifications are still attempted before the panic continues.
    pub fn set_total(&self, total: usize) {
        let mut wakers = Vec::new();
        {
            let mut state = self.state.lock();
            let previous = state.total;
            state.total = total;

            if total > previous {
                let mut added = total - previous;
                let repaid = added.min(state.deficit);
                state.deficit -= repaid;
                added -= repaid;
                while added > 0 && !state.waiters.is_empty() {
                    wakers.extend(state.release_one());
                    added -= 1;
                }
                state.permits += added;
            } else {
                let shortfall = previous - total;
                let taken = shortfall.min(state.permits);
                state.permits -= taken;
                state.deficit += shortfall - taken;
            }
        }

        wake_all(wakers.into_iter());
    }
}

impl<B: Eq + Hash + Clone> CapacityLimiter<B> {
    /// Acquires a token for `borrower`, waiting until capacity is available.
    ///
    /// Registration and duplicate checking happen on first poll. A borrower already queued or
    /// holding a permit is rejected with [`AlreadyBorrowed`] without waiting.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending acquisition removes its identity and queue position. If it was already
    /// granted a token, the token is returned or passed to the next queued borrower.
    pub fn acquire(&self, borrower: B) -> Acquire<'_, B> {
        Acquire {
            limiter: self,
            borrower: Some(borrower),
            id: None,
            registered: false,
        }
    }

    /// Attempts to acquire a token for `borrower` without waiting.
    pub fn try_acquire(&self, borrower: B) -> Result<Permit<'_, B>, TryAcquireError> {
        let key = borrower.clone();
        let mut state = self.state.lock();

        // Check before insertion so a duplicate cannot replace and drop a stored key under the
        // lock. This intentionally pays for separate lookup and insertion on the success path.
        if state.borrowers.contains(&borrower) {
            return Err(TryAcquireError::AlreadyBorrowed);
        }
        if state.permits == 0 {
            return Err(TryAcquireError::NoCapacity);
        }
        state.borrowers.insert(key);

        state.permits -= 1;
        state.borrowed += 1;
        drop(state);

        Ok(Permit {
            limiter: self,
            borrower: Some(borrower),
        })
    }
}

/// The future returned by [`CapacityLimiter::acquire`].
#[must_use = "futures do nothing unless polled"]
pub struct Acquire<'a, B: Eq + Hash + Clone> {
    limiter: &'a CapacityLimiter<B>,
    /// Taken once the token is handed to a permit, so `Drop` knows whether it still owns cleanup.
    borrower: Option<B>,
    id: Option<WaiterId>,
    registered: bool,
}

impl<B: Eq + Hash + Clone + fmt::Debug> fmt::Debug for Acquire<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Acquire")
            .field("borrower", &self.borrower)
            .field("registered", &self.registered)
            .finish_non_exhaustive()
    }
}

// The future holds no self-references; its waiter node lives in the limiter's arena.
impl<B: Eq + Hash + Clone> Unpin for Acquire<'_, B> {}

impl<'a, B: Eq + Hash + Clone> Future for Acquire<'a, B> {
    type Output = Result<Permit<'a, B>, AlreadyBorrowed>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let limiter = this.limiter;
        let mut key = if this.registered {
            None
        } else {
            Some(this.borrower.as_ref().expect("pending borrower").clone())
        };
        let mut prepared_waker = None;
        loop {
            let mut state = limiter.state.lock();
            if !this.registered {
                let borrower = this.borrower.as_ref().expect("pending borrower");
                // As in try_acquire, avoid replacing and dropping a duplicate key under the lock.
                if state.borrowers.contains(borrower) {
                    return Poll::Ready(Err(AlreadyBorrowed));
                }
                state
                    .borrowers
                    .insert(key.take().expect("unregistered borrower key"));
                this.registered = true;
                if state.permits > 0 {
                    state.permits -= 1;
                    state.borrowed += 1;
                    drop(state);
                    return Poll::Ready(Ok(this.take_permit()));
                }
                // Register before cloning: a reentrant release can commit this waiter while the
                // lock is released for the callback. The next iteration rechecks the grant.
                this.id = Some(state.waiters.push_back(WaitNode {
                    granted: false,
                    waker: None,
                }));
            }

            let id = this.id.expect("pending waiter id");
            if state.waiters.waiter_mut(id).granted {
                let node = state.waiters.remove_unlinked_waiter(id);
                state.borrowed += 1;
                this.id = None;
                drop(state);
                let permit = this.take_permit();
                drop(node);
                return Poll::Ready(Ok(permit));
            }

            let waker = &mut state.waiters.waiter_mut(id).waker;
            if let Some(prepared) = prepared_waker.take() {
                let old = waker.replace(prepared);
                drop(state);
                drop(old);
                return Poll::Pending;
            }
            if waker
                .as_ref()
                .is_some_and(|held| held.will_wake(context.waker()))
            {
                return Poll::Pending;
            }
            drop(state);
            prepared_waker = Some(context.waker().clone());
        }
    }
}

impl<'a, B: Eq + Hash + Clone> Acquire<'a, B> {
    fn take_permit(&mut self) -> Permit<'a, B> {
        Permit {
            limiter: self.limiter,
            borrower: Some(
                self.borrower
                    .take()
                    .expect("a granted acquire still owns its borrower"),
            ),
        }
    }
}

impl<B: Eq + Hash + Clone> Drop for Acquire<'_, B> {
    fn drop(&mut self) {
        // `None` means the token was handed to a permit, which owns the cleanup from here.
        let Some(borrower) = self.borrower.take() else {
            return;
        };
        // Never registered: either never polled, or rejected as a duplicate of someone else's
        // entry, which must not be removed here.
        if !self.registered {
            return;
        }

        let (waker, node, key) = {
            let mut state = self.limiter.state.lock();
            let key = state.borrowers.take(&borrower);
            let node = self.id.take().map(|id| {
                state.waiters.unlink_waiter(id, |_| true);
                state.waiters.remove_unlinked_waiter(id)
            });
            let waker = if node.as_ref().is_some_and(|node| node.granted) {
                state.release_one()
            } else {
                None
            };
            (waker, node, key)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        drop(node);
        drop(key);
    }
}

/// A token borrowed from a [`CapacityLimiter`].
#[must_use = "tokens are returned immediately when dropped"]
pub struct Permit<'a, B: Eq + Hash + Clone> {
    limiter: &'a CapacityLimiter<B>,
    borrower: Option<B>,
}

impl<B: Eq + Hash + Clone + fmt::Debug> fmt::Debug for Permit<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Permit")
            .field("borrower", &self.borrower)
            .finish_non_exhaustive()
    }
}

impl<B: Eq + Hash + Clone> Permit<'_, B> {
    /// Returns the borrower this token was acquired for.
    pub fn borrower(&self) -> &B {
        self.borrower
            .as_ref()
            .expect("a permit holds its borrower until it is dropped")
    }
}

impl<B: Eq + Hash + Clone> Drop for Permit<'_, B> {
    fn drop(&mut self) {
        let Some(borrower) = self.borrower.take() else {
            return;
        };

        let (waker, key) = {
            let mut state = self.limiter.state.lock();
            let key = state.borrowers.take(&borrower);
            state.borrowed -= 1;
            (state.release_one(), key)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        drop(key);
    }
}

/// The error returned when a borrower attempts to hold two tokens at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlreadyBorrowed;

impl fmt::Display for AlreadyBorrowed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "borrower already holds a token from this limiter")
    }
}

impl std::error::Error for AlreadyBorrowed {}

/// The error returned by [`CapacityLimiter::try_acquire`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryAcquireError {
    /// No capacity is available without waiting.
    NoCapacity,
    /// The borrower already holds a token from this limiter.
    AlreadyBorrowed,
}

impl From<AlreadyBorrowed> for TryAcquireError {
    fn from(_: AlreadyBorrowed) -> Self {
        TryAcquireError::AlreadyBorrowed
    }
}

impl fmt::Display for TryAcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryAcquireError::NoCapacity => write!(f, "no capacity available"),
            TryAcquireError::AlreadyBorrowed => AlreadyBorrowed.fmt(f),
        }
    }
}

impl std::error::Error for TryAcquireError {}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod regression;
