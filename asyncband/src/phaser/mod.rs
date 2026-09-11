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
//! # Example: build a shared dictionary before encoding documents
//!
//! An indexing job needs the same numeric ID for a word in every document. Workers first collect
//! vocabulary in parallel. Once all documents have contributed their words, the coordinator assigns
//! IDs in sorted order. A second rendezvous keeps workers from encoding documents before that
//! shared dictionary is ready. The same participants coordinate both steps.
//!
//! ```
//! use std::collections::BTreeMap;
//! use std::sync::Arc;
//! use std::sync::Mutex;
//!
//! use asyncband::phaser::Closed;
//! use asyncband::phaser::Phaser;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Closed> {
//! let documents = ["rust async rust", "async tasks"];
//! let dictionary = Arc::new(Mutex::new(BTreeMap::new()));
//! let phaser = Phaser::new();
//! let mut coordinator = phaser.register_one()?;
//! let participants = phaser.register(documents.len())?;
//! let mut tasks = Vec::new();
//!
//! for (document, mut participant) in documents.into_iter().zip(participants) {
//!     let dictionary = dictionary.clone();
//!     tasks.push(tokio::spawn(async move {
//!         let words: Vec<_> = document.split_whitespace().collect();
//!         {
//!             let mut dictionary = dictionary.lock().unwrap();
//!             for &word in &words {
//!                 dictionary.entry(word).or_insert(0);
//!             }
//!         }
//!         participant.wait().await?; // All vocabulary has been collected.
//!         participant.wait().await?; // The coordinator has assigned the IDs.
//!
//!         let dictionary = dictionary.lock().unwrap();
//!         let encoded: Vec<_> = words.iter().map(|word| dictionary[word]).collect();
//!         Ok::<_, Closed>(encoded)
//!     }));
//! }
//!
//! coordinator.wait().await?;
//! for (id, value) in dictionary.lock().unwrap().values_mut().enumerate() {
//!     *value = id;
//! }
//! coordinator.wait().await?;
//!
//! let mut encoded_documents = Vec::new();
//! for task in tasks {
//!     encoded_documents.push(task.await.unwrap()?);
//! }
//! // Every document uses the same dictionary: async = 0, rust = 1, tasks = 2.
//! assert_eq!(encoded_documents, [vec![1, 0, 1], vec![0, 2]]);
//! # Ok(())
//! # }
//! ```
//!
//! # Arriving and waiting
//!
//! [`PhaserParticipant::wait`] arrives and waits for the other participants. To overlap independent
//! work with that wait, call [`arrive`](PhaserParticipant::arrive) first. The subsequent `wait`
//! observes that arrival's phase even if it has already completed. Explicitly arriving again
//! replaces the pending observation with the current phase; repeated arrivals within one phase
//! do not count twice.
//!
//! # Membership and cancellation
//!
//! Registration joins the phase current at the registration's synchronization point. In
//! particular, [`register`](Phaser::register) registers its entire batch in one phase.
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
//! [`Phaser::wait`] is an independent, cancel-safe observation. It never registers a
//! participant or records an arrival. Observers may miss intermediate phases; this is not an
//! event stream with one notification per phase.
//!
//! # Closing and synchronization
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
//! from the same phaser to `wait`; it tests for a different phase, not a target number
//! or numeric threshold. An observation must not be retained across a full counter cycle.

use std::fmt;
use std::future::Future;
use std::iter::FusedIterator;
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
///
/// This error is returned by phaser operations and cannot be constructed directly by callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed(());

impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Phaser is closed")
    }
}

impl std::error::Error for Closed {}

/// A shared handle to a reusable phase barrier with dynamic participants.
///
/// Cloning this handle does not register a participant. Use [`register_one`](Self::register_one) to
/// create a participant that can be moved into an independently spawned task.
#[derive(Clone)]
pub struct Phaser {
    state: Arc<Mutex<State>>,
}

struct State {
    phase: u64,
    closed: bool,
    registered: usize,
    unarrived: usize,
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
            Poll::Ready(Err(Closed(())))
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

    /// Returns the current phase number, which remains fixed once the phaser is closed.
    pub fn phase(&self) -> u64 {
        self.state.lock().phase
    }

    /// Returns whether this phaser has been permanently closed.
    pub fn is_closed(&self) -> bool {
        self.state.lock().closed
    }

    /// Closes this phaser and wakes all pending observers without completing the current phase.
    ///
    /// This operation is idempotent and affects every handle and participant. Existing participants
    /// may still deregister or be dropped; their removal no longer advances the phase.
    ///
    /// # Panics
    ///
    /// If a waker panics, the phaser remains closed and notification is attempted for the other
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
    pub fn registered_parties(&self) -> usize {
        self.state.lock().registered
    }

    /// Returns an instantaneous count of participants that have arrived in the current phase.
    pub fn arrived_parties(&self) -> usize {
        let state = self.state.lock();
        state.registered - state.unarrived
    }

    /// Returns an instantaneous count of outstanding arrivals in the current phase.
    pub fn unarrived_parties(&self) -> usize {
        self.state.lock().unarrived
    }

    /// Registers one participant in the current phase, or returns [`Closed`].
    ///
    /// Registration racing with advancement joins the phase before or after that advancement.
    /// The returned participant owns a shared handle and does not borrow this one.
    ///
    /// # Panics
    ///
    /// Panics if the registered count would exceed `usize::MAX`.
    pub fn register_one(&self) -> Result<PhaserParticipant, Closed> {
        let phaser = self.clone();
        self.do_register(1)?;
        Ok(PhaserParticipant::new(phaser))
    }

    /// Registers an entire batch in one phase and returns an iterator over its participants.
    ///
    /// Registration is immediate, including participants not yet yielded by the iterator. No
    /// storage is allocated for the batch. Dropping the iterator withdraws its remaining
    /// participants; yielded handles keep their registrations. The iterator owns a shared handle
    /// and does not borrow this phaser.
    ///
    /// Returns [`Closed`] without registering anyone if the phaser is closed. On an open phaser,
    /// a zero-sized batch does nothing. Collect this iterator into a collection of your choice.
    ///
    /// # Panics
    ///
    /// Panics if the registered count would exceed `usize::MAX`.
    pub fn register(&self, parties: usize) -> Result<PhaserParticipants, Closed> {
        let phaser = self.clone();
        self.do_register(parties)?;
        Ok(PhaserParticipants {
            phaser,
            remaining: parties,
        })
    }

    fn do_register(&self, parties: usize) -> Result<(), Closed> {
        let mut state = self.state.lock();
        if state.closed {
            return Err(Closed(()));
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
    /// completed before the phaser was closed remains successful. Phase numbers wrap; do not retain
    /// an observation across a full `u64` cycle or use a number obtained from another phaser.
    ///
    /// # Cancel safety
    ///
    /// Cancelling only unregisters this wait's waker. The same observation can be retried.
    pub async fn wait(&self, observed: u64) -> Result<u64, Closed> {
        PhaserWait {
            phaser: self,
            observed,
            token: None,
        }
        .await
    }
}

/// An owning iterator over a batch registered by [`Phaser::register`].
///
/// All participants are already registered, so even those not yet yielded hold back phase
/// advancement. Dropping this iterator withdraws the remaining participants in one state
/// transition. Yielded participants are independent and retain their registrations.
///
/// Closing the phaser does not prevent iteration over this existing batch, but arrival and waiting
/// on the yielded participants return [`Closed`]. This iterator is not cloneable.
#[must_use = "dropping the iterator withdraws participants that have not been yielded"]
#[derive(Debug)]
pub struct PhaserParticipants {
    phaser: Phaser,
    remaining: usize,
}

impl Iterator for PhaserParticipants {
    type Item = PhaserParticipant;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let participant = PhaserParticipant::new(self.phaser.clone());
        self.remaining -= 1;
        Some(participant)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for PhaserParticipants {}

impl FusedIterator for PhaserParticipants {}

impl Drop for PhaserParticipants {
    fn drop(&mut self) {
        if self.remaining == 0 {
            return;
        }
        let wakers = {
            let mut state = self.phaser.state.lock();
            // Unyielded participants have never arrived and prevent their phase from advancing.
            state.registered -= self.remaining;
            state.unarrived -= self.remaining;
            self.remaining = 0;
            state.advance_if_ready()
        };
        wake_all(wakers.into_iter().flatten());
    }
}

/// One participant's arrival obligation in every phase until it deregisters or is dropped.
///
/// This handle is not cloneable. Dropping it withdraws the participant, including any outstanding
/// current arrival. It does not report successful work or close the other participants.
#[must_use = "dropping a participant withdraws it from the Phaser"]
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
                return Err(Closed(()));
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
        let next = self.phaser.wait(observed).await?;
        self.pending = None;
        Ok(next)
    }

    /// Withdraws this participant and returns the phase from which it withdrew, or [`Closed`].
    ///
    /// Any outstanding arrival is discharged without counting an already-arrived participant
    /// twice. This always removes the registration, even if the phaser is closed. The pending
    /// observation is abandoned. Dropping a participant has the same membership effect.
    pub fn deregister(mut self) -> Result<u64, Closed> {
        self.do_deregister()
    }

    fn do_deregister(&mut self) -> Result<u64, Closed> {
        let (result, wakers) = {
            let mut state = self.phaser.state.lock();
            self.registered = false;
            state.registered -= 1;
            if self.arrived != Some(state.phase) {
                state.unarrived -= 1;
            }
            let result = if state.closed {
                Err(Closed(()))
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
            let _ = self.do_deregister();
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
