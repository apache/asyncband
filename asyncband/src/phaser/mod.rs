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

//! Coordinate repeated rounds of work with a dynamic participant set.
//!
//! A [`Phaser`] is a shared coordination handle. Cloning it creates another observer, without
//! registering a participant. Each [`PhaserParticipant`] owns one arrival obligation per phase.
//! Register participants before starting their tasks, or keep a coordinator participant registered
//! while setting up a group so that the first workers cannot finish the phase prematurely.
//!
//! # Arriving and waiting
//!
//! [`PhaserParticipant::wait`] arrives and waits for the other participants. To overlap independent
//! work with that wait, call [`arrive`](PhaserParticipant::arrive) first. The subsequent `wait`
//! observes that arrival's phase even if it has already completed. Explicitly arriving again
//! replaces the pending observation with the current phase; repeated arrivals within one phase
//! do not count twice.
//!
//! ```
//! use asyncband::phaser::Phaser;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), asyncband::phaser::Closed> {
//! let phaser = Phaser::new();
//! let mut participants = phaser.register_many(2)?;
//! let mut worker = participants.pop().unwrap();
//! let mut coordinator = participants.pop().unwrap();
//!
//! let task = tokio::spawn(async move {
//!     for _ in 0..3 {
//!         // Finish this round's work before arriving.
//!         let completed = worker.arrive()?;
//!         // Independent work can run here without delaying the other participants.
//!         assert_ne!(worker.wait().await?, completed);
//!     }
//!     Ok::<_, asyncband::phaser::Closed>(())
//! });
//!
//! for _ in 0..3 {
//!     coordinator.wait().await?;
//! }
//! task.await.unwrap()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Membership and cancellation
//!
//! Registration joins the phase current at the registration's synchronization point. In
//! particular, [`register_many`](Phaser::register_many) registers its entire batch in one phase.
//! Dropping or [`deregistering`](PhaserParticipant::deregister) a participant removes its future
//! obligations and discharges any outstanding arrival in the current phase. An empty phaser is
//! dormant and can be reused; it does not advance repeatedly or close automatically.
//!
//! A participant's `wait` records arrival on its first poll. Dropping an unpolled future has no
//! effect. Cancelling a polled wait preserves its arrival and pending phase: retrying `wait` on
//! that participant observes the same phase instead of arriving in a later one. Dropping the
//! participant itself withdraws it from the group. Withdrawal does not certify successful work;
//! applications that require all workers to succeed should close the phaser on failure.
//!
//! [`Phaser::wait_for_advance`] is an independent, cancel-safe observation. It never registers a
//! participant or records an arrival. Observers may miss intermediate phases; this is not an
//! event stream with one notification per phase.
//!
//! # Closure and synchronization
//!
//! [`Phaser::close`] permanently freezes the current phase, rejects registration and arrival,
//! and releases waits for the unfinished phase with [`Closed`]. A previously completed phase
//! remains successful even if the phaser closes before its waiter is polled again.
//!
//! Work performed before an arrival or deregistration happens before work performed after a
//! successful wait for that phase's completion. No such all-participants-completed guarantee is
//! provided by a wait that returns `Closed`.
//!
//! Phase numbers start at zero and wrap from `u64::MAX` to zero. Pass a value previously obtained
//! from the same phaser to `wait_for_advance`; it tests for a different phase, not a target number
//! or numeric threshold. An observation must not be retained across a full counter cycle.
//!
//! # Java Phaser use cases
//!
//! The following examples are in the repository's `examples` package. Run one with
//! `cargo run -p examples --example <name>`.
//!
//! | Use case | Rust expression | Runnable example |
//! |----------|-----------------|------------------|
//! | Dynamic registration, repeated rounds, and a one-shot start gate | Shared handles, `register_many`, participant `wait`, and `deregister` | `phaser_rounds` |
//! | Split arrival/wait, progress observation, numeric targets, and cancellation retry | `arrive`, participant `wait`, and `wait_for_advance` | `phaser_rounds` |
//! | `onAdvance` aggregation, asynchronous finalization, and stopping at convergence | A coordinator and separate ready/resume phasers | `phaser_completion` |
//! | Aborting the group after a worker fails | `close`, including an application-owned abort guard | `phaser_completion` |
//! | Grouped fan-in before global release | Local ready/resume phasers and one root participant per group | `phaser_groups` |
//!
//! These compositions do not supply Java's native parent/child phasers, automatic parent
//! registration, a globally shared phase counter across nodes, or an in-primitive `onAdvance`
//! hook. The grouped example has an explicit coordinator task per group; local counters are not
//! global phase numbers. Membership changes in the ready/resume protocols must be applied at
//! coordinated round boundaries. A slow observer cannot run an exactly-once completion hook.
//! Timeouts and task scheduling remain with the caller's runtime.
//!
//! For the Java contracts being mapped, see the
//! [Java Phaser documentation](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Phaser.html).

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::mutex::Mutex;
use crate::internal::wake_all;
use crate::internal::wakerset::WakerSet;
use crate::internal::wakerset::WakerToken;

#[cfg(test)]
mod tests;

/// The phaser was closed before this operation could complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Closed;

impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("phaser is closed")
    }
}

impl std::error::Error for Closed {}

/// A shared handle to a reusable phase barrier with dynamic participants.
///
/// Cloning this handle does not register a participant. Use [`register`](Self::register) to
/// create a participant that can be moved into an independently spawned task.
#[derive(Clone)]
pub struct Phaser {
    state: Arc<Mutex<State>>,
}

struct State {
    phase: u64,
    closed: bool,
    registered: u32,
    unarrived: u32,
    waiters: WakerSet,
}

impl State {
    fn advance_if_ready(&mut self) -> Option<impl Iterator<Item = Waker> + 'static> {
        if self.closed || self.unarrived != 0 {
            return None;
        }
        self.phase = self.phase.wrapping_add(1);
        self.unarrived = self.registered;
        Some(self.waiters.drain())
    }

    fn completion(&self, observed: u64) -> Poll<Result<u64, Closed>> {
        // Completion wins over a later close; the unfinished phase itself never advances on close.
        if self.phase != observed {
            Poll::Ready(Ok(self.phase))
        } else if self.closed {
            Poll::Ready(Err(Closed))
        } else {
            Poll::Pending
        }
    }
}

impl fmt::Debug for Phaser {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock();
        f.debug_struct("Phaser")
            .field("phase", &state.phase)
            .field("closed", &state.closed)
            .field("registered", &state.registered)
            .field("unarrived", &state.unarrived)
            .finish_non_exhaustive()
    }
}

impl Default for Phaser {
    fn default() -> Self {
        Self::new()
    }
}

impl Phaser {
    /// Creates an open, dormant phaser at phase zero with no registered participants.
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                phase: 0,
                closed: false,
                registered: 0,
                unarrived: 0,
                waiters: WakerSet::new(),
            })),
        }
    }

    /// Returns the current phase number, which remains fixed after closure.
    pub fn phase(&self) -> u64 {
        self.state.lock().phase
    }

    /// Returns whether this phaser has been permanently closed.
    pub fn is_closed(&self) -> bool {
        self.state.lock().closed
    }

    /// Closes this phaser and wakes all pending observers without completing the current phase.
    ///
    /// Closure is idempotent and affects every handle and participant. Existing participants may
    /// still deregister or be dropped; their removal no longer advances the phase.
    ///
    /// # Panics
    ///
    /// If a waker panics, closure remains committed and notification is attempted for the other
    /// waiters before the panic resumes.
    pub fn close(&self) {
        let wakers = {
            let mut state = self.state.lock();
            if state.closed {
                return;
            }
            state.closed = true;
            state.waiters.take_all()
        };
        wake_all(wakers);
    }

    /// Returns an instantaneous count of registered participants, including those already arrived.
    ///
    /// Counts can change between separate queries. This is not a synchronization operation.
    pub fn registered_parties(&self) -> u32 {
        self.state.lock().registered
    }

    /// Returns an instantaneous count of participants that have arrived in the current phase.
    pub fn arrived_parties(&self) -> u32 {
        let state = self.state.lock();
        state.registered - state.unarrived
    }

    /// Returns an instantaneous count of outstanding arrivals in the current phase.
    pub fn unarrived_parties(&self) -> u32 {
        self.state.lock().unarrived
    }

    /// Registers one participant in the current phase, or returns [`Closed`].
    ///
    /// Registration racing with advancement joins the phase before or after that advancement.
    /// The returned participant owns a shared handle and does not borrow this one.
    ///
    /// # Panics
    ///
    /// Panics if the registered count would exceed `u32::MAX`.
    pub fn register(&self) -> Result<PhaserParticipant, Closed> {
        let phaser = self.clone();
        self.register_inner(1)?;
        Ok(PhaserParticipant::new(phaser))
    }

    /// Registers an entire batch in one phase, or returns [`Closed`] without registering anyone.
    ///
    /// On an open phaser, a zero-sized batch does nothing. The batch is reserved before any
    /// participant count is changed. Every returned handle must be used or dropped.
    ///
    /// # Panics
    ///
    /// Panics if the batch cannot fit in a vector or the registered count would exceed `u32::MAX`.
    pub fn register_many(&self, parties: u32) -> Result<Vec<PhaserParticipant>, Closed> {
        let capacity = usize::try_from(parties)
            .expect("Phaser participant count must fit in the platform's usize");
        let mut participants = Vec::with_capacity(capacity);
        self.register_inner(parties)?;
        participants.extend((0..parties).map(|_| PhaserParticipant::new(self.clone())));
        Ok(participants)
    }

    fn register_inner(&self, parties: u32) -> Result<(), Closed> {
        let mut state = self.state.lock();
        if state.closed {
            return Err(Closed);
        }
        let registered = state
            .registered
            .checked_add(parties)
            .expect("Phaser registered-party count overflow");
        let unarrived = state
            .unarrived
            .checked_add(parties)
            .expect("Phaser unarrived-party count overflow");
        state.registered = registered;
        state.unarrived = unarrived;
        Ok(())
    }

    /// Waits until the current phase differs from a phase previously observed on this phaser.
    ///
    /// Returns the current phase, possibly skipping intermediate phases. This does not wait for
    /// a future target number: a number different from the current phase returns immediately.
    /// It neither registers a participant nor records an arrival.
    ///
    /// Returns [`Closed`] if the observed phase is still current when the phaser closes. A phase
    /// completed before closure remains successful. Phase numbers wrap; do not retain an
    /// observation across a full `u64` cycle or use a number obtained from another phaser.
    ///
    /// # Cancel safety
    ///
    /// Cancelling only unregisters this wait's waker. The same observation can be retried.
    pub async fn wait_for_advance(&self, observed: u64) -> Result<u64, Closed> {
        PhaserWait {
            phaser: self,
            observed,
            token: None,
        }
        .await
    }
}

/// One participant's arrival obligation in every phase until it deregisters or is dropped.
///
/// This handle is not cloneable. Dropping it withdraws the participant, including any outstanding
/// current arrival. It does not report successful work or close the other participants.
#[must_use = "dropping a participant withdraws it from the phaser"]
#[derive(Debug)]
pub struct PhaserParticipant {
    phaser: Phaser,
    arrived: Option<u64>,
    pending: Option<u64>,
    registered: bool,
}

impl PhaserParticipant {
    fn new(phaser: Phaser) -> Self {
        Self {
            phaser,
            arrived: None,
            pending: None,
            registered: true,
        }
    }

    /// Returns the shared coordination handle without registering another participant.
    pub fn phaser(&self) -> &Phaser {
        &self.phaser
    }

    /// Records this participant's arrival and remembers that phase for [`wait`](Self::wait).
    ///
    /// Returns the phase in which arrival was recorded, or [`Closed`] without recording one.
    /// Repeated calls within one phase count only once. After advancement, an explicit new call
    /// arrives in the new phase and replaces any previous pending observation.
    ///
    /// Arrival and the pending observation remain committed if notifying a waker panics.
    pub fn arrive(&mut self) -> Result<u64, Closed> {
        let (phase, wakers) = {
            let mut state = self.phaser.state.lock();
            if state.closed {
                return Err(Closed);
            }
            let phase = state.phase;
            if self.arrived != Some(phase) {
                state.unarrived -= 1;
                self.arrived = Some(phase);
            }
            self.pending = Some(phase);
            (phase, state.advance_if_ready())
        };
        wake_all(wakers.into_iter().flatten());
        Ok(phase)
    }

    /// Arrives if necessary and waits for this participant's pending phase to complete.
    ///
    /// After an explicit [`arrive`](Self::arrive), this observes that arrival even if the phase
    /// already completed. With no pending observation, the first poll arrives in the current
    /// phase. Success consumes the pending observation and returns the new current phase.
    ///
    /// # Cancel safety
    ///
    /// An unpolled call has no effect. Once polled, arrival is committed. Cancelling preserves
    /// the pending observation, so retrying this method waits for the same phase without
    /// counting another arrival. An explicit new `arrive` replaces that pending observation.
    pub async fn wait(&mut self) -> Result<u64, Closed> {
        let observed = match self.pending {
            Some(phase) => phase,
            None => self.arrive()?,
        };
        let next = self.phaser.wait_for_advance(observed).await?;
        self.pending = None;
        Ok(next)
    }

    /// Withdraws this participant and returns the phase from which it withdrew, or [`Closed`].
    ///
    /// Any outstanding arrival is discharged without counting an already-arrived participant
    /// twice. This always removes the registration, including after closure. The pending
    /// observation is abandoned. Dropping a participant has the same membership effect.
    pub fn deregister(mut self) -> Result<u64, Closed> {
        self.deregister_inner()
    }

    fn deregister_inner(&mut self) -> Result<u64, Closed> {
        let (result, wakers) = {
            let mut state = self.phaser.state.lock();
            self.registered = false;
            state.registered -= 1;
            if self.arrived != Some(state.phase) {
                state.unarrived -= 1;
            }
            let result = if state.closed {
                Err(Closed)
            } else {
                Ok(state.phase)
            };
            (result, state.advance_if_ready())
        };
        wake_all(wakers.into_iter().flatten());
        result
    }
}

impl Drop for PhaserParticipant {
    fn drop(&mut self) {
        if self.registered {
            let _ = self.deregister_inner();
        }
    }
}

#[must_use = "futures do nothing unless you .await or poll them"]
struct PhaserWait<'a> {
    phaser: &'a Phaser,
    observed: u64,
    token: Option<WakerToken>,
}

impl Future for PhaserWait<'_> {
    type Output = Result<u64, Closed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        {
            let state = this.phaser.state.lock();
            if let ready @ Poll::Ready(_) = state.completion(this.observed) {
                this.token = None;
                return ready;
            }
        }

        // Waker cloning may reenter or close this phaser. Recheck completion before registering.
        let waker = cx.waker().clone();
        let mut state = this.phaser.state.lock();
        if let ready @ Poll::Ready(_) = state.completion(this.observed) {
            this.token = None;
            drop(state);
            drop(waker);
            return ready;
        }
        let retired = state.waiters.register_owned(&mut this.token, waker);
        drop(state);
        drop(retired);
        Poll::Pending
    }
}

impl Drop for PhaserWait<'_> {
    fn drop(&mut self) {
        if self.token.is_none() {
            return;
        }
        let mut state = self.phaser.state.lock();
        if state.completion(self.observed).is_ready() {
            return;
        }
        let retired = state.waiters.unregister(&mut self.token);
        drop(state);
        drop(retired);
    }
}
