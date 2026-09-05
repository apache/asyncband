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

//! A reusable phase barrier with a dynamic participant set.
//!
//! A phaser coordinates repeated rounds of work with dynamically registered parties.
//!
//! Each [`PhaserParticipant`] represents one registered party.
//!
//! A phase advances after every party registered for that phase has arrived or deregistered.
//!
//! Registration increases both the registered and current unarrived counts.
//!
//! Arrival reduces the unarrived count.
//!
//! Deregistration also removes a party from later phases.
//!
//! [`Phaser::arrived_parties`] is the difference between the registered and unarrived counts.
//!
//! All state transitions and waiter registration share one synchronization point.
//!
//! A registration racing with advancement joins either the phase before or after the advancement.
//!
//! Which phase it joins is determined by which operation linearizes first.
//!
//! A completed phase is never reopened.
//!
//! Dropping a registered participant is equivalent to arriving and deregistering.
//!
//! This prevents an abandoned task from permanently blocking phase advancement.
//!
//! Consequently, dropping the last outstanding participant can advance the phase.
//!
//! # Cancellation
//!
//! Waiting with [`Phaser::wait_for_advance`] never registers a party or records an arrival.
//!
//! Cancelling that wait only removes its waker.
//!
//! [`PhaserParticipant::arrive_and_wait`] commits its arrival when first polled.
//!
//! Constructing and dropping that future without polling has no effect.
//!
//! Cancelling after arrival does not retract it.
//!
//! A retry waits for the stored phase, even after advancement, without arriving in the next phase.
//!
//! # Zero parties
//!
//! A phaser with no registered parties is dormant rather than terminated.
//!
//! Completing the last party's phase advances once.
//!
//! A later registration joins the current dormant phase.
//!
//! # Phase identity
//!
//! Phases advance using wrapping arithmetic.
//!
//! [`Phase`] supports equality but no ordering or arithmetic contract across wraparound.
//!
//! Waiters compare phase identity instead of inferring transitions from party counts.
//!
//! # Examples
//!
//! ```
//! use std::sync::Arc;
//!
//! use asyncband::phaser::Phaser;
//!
//! let phaser = Arc::new(Phaser::new());
//! let initial = phaser.phase();
//! let mut first = phaser.register();
//! let second = phaser.register();
//!
//! assert_eq!(first.arrive(), initial);
//! assert_eq!(second.arrive_and_deregister(), initial);
//! assert_ne!(phaser.phase(), initial);
//! ```

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

/// The identity of one phaser generation.
///
/// Phases advance with wrapping arithmetic.
///
/// Equality is meaningful, but ordering across wraparound is not guaranteed.
///
/// The numeric value is intended for diagnostics rather than synchronization arithmetic.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Phase(u64);

impl Phase {
    /// Returns the underlying wrapping phase counter for diagnostics.
    pub const fn get(self) -> u64 {
        self.0
    }

    const fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

/// A reusable phase barrier with a dynamic participant set.
///
/// Store a phaser in an [`Arc`] before registering participants.
///
/// Each participant owns an `Arc` clone and can move into an independently spawned task.
#[derive(Debug)]
pub struct Phaser {
    state: Mutex<PhaserState>,
}

struct PhaserState {
    phase: Phase,
    registered: u32,
    unarrived: u32,
    waiters: WakerSet,
}

impl fmt::Debug for PhaserState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhaserState")
            .field("phase", &self.phase)
            .field("registered", &self.registered)
            .field("arrived", &(self.registered - self.unarrived))
            .field("unarrived", &self.unarrived)
            .finish_non_exhaustive()
    }
}

impl Default for Phaser {
    fn default() -> Self {
        Self::new()
    }
}

impl Phaser {
    /// Creates a dormant phaser with no registered parties.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::phaser::Phaser;
    ///
    /// let phaser = Phaser::new();
    /// assert_eq!(phaser.phase().get(), 0);
    /// assert_eq!(phaser.registered_parties(), 0);
    /// ```
    pub const fn new() -> Self {
        Self {
            state: Mutex::new(PhaserState {
                phase: Phase(0),
                registered: 0,
                unarrived: 0,
                waiters: WakerSet::new(),
            }),
        }
    }

    /// Returns the current phase identity.
    pub fn phase(&self) -> Phase {
        self.state.lock().phase
    }

    /// Returns the number of currently registered parties.
    ///
    /// This is an instantaneous observation and may change immediately after the method returns.
    pub fn registered_parties(&self) -> u32 {
        self.state.lock().registered
    }

    /// Returns the number of registered parties that have arrived in the current phase.
    ///
    /// This is an instantaneous observation and may change immediately after the method returns.
    pub fn arrived_parties(&self) -> u32 {
        let state = self.state.lock();
        state.registered - state.unarrived
    }

    /// Returns the number of registered parties that have not arrived in the current phase.
    ///
    /// This is an instantaneous observation and may change immediately after the method returns.
    pub fn unarrived_parties(&self) -> u32 {
        self.state.lock().unarrived
    }

    /// Registers one unarrived party in the current phase.
    ///
    /// The returned participant owns an [`Arc`] clone of this phaser.
    ///
    /// Registration is linearized with phase advancement.
    ///
    /// A concurrent advancement places the participant in either adjacent phase, never both.
    ///
    /// # Panics
    ///
    /// Panics if the registered-party count would overflow `u32`.
    pub fn register(self: &Arc<Self>) -> PhaserParticipant {
        let phaser = Arc::clone(self);
        let phase = self.register_inner(1);
        PhaserParticipant {
            phaser,
            phase,
            arrived: false,
            registered: true,
            pending_wait: None,
        }
    }

    /// Registers `parties` unarrived parties in one current-phase state transition.
    ///
    /// One participant handle is returned for each party.
    ///
    /// Passing zero returns an empty vector and leaves the phaser unchanged.
    ///
    /// Storage for all handles is reserved before registration is committed.
    ///
    /// # Panics
    ///
    /// Panics if either count cannot be represented by its public integer type.
    pub fn register_many(self: &Arc<Self>, parties: u32) -> Vec<PhaserParticipant> {
        let capacity = usize::try_from(parties)
            .expect("Phaser participant count must fit in the platform's usize");
        let mut participants = Vec::with_capacity(capacity);
        if parties == 0 {
            return participants;
        }

        let phase = self.register_inner(parties);
        participants.extend((0..parties).map(|_| PhaserParticipant {
            phaser: Arc::clone(self),
            phase,
            arrived: false,
            registered: true,
            pending_wait: None,
        }));
        participants
    }

    /// Waits until the current phase differs from `observed`.
    ///
    /// This operation does not register a party and does not record an arrival.
    ///
    /// It resolves immediately when `observed` is no longer current.
    ///
    /// # Cancellation
    ///
    /// Cancelling only unregisters the current waker.
    ///
    /// It does not change any party count or committed arrival.
    pub async fn wait_for_advance(&self, observed: Phase) -> Phase {
        PhaserWait {
            token: None,
            observed,
            phaser: self,
        }
        .await
    }

    fn register_inner(&self, parties: u32) -> Phase {
        let mut state = self.state.lock();
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
        state.phase
    }

    fn record_arrival(
        &self,
        participant_phase: &mut Phase,
        participant_arrived: &mut bool,
        deregister: bool,
    ) -> (Phase, Option<Vec<Waker>>) {
        {
            let mut state = self.state.lock();
            if *participant_phase != state.phase {
                *participant_phase = state.phase;
                *participant_arrived = false;
            }

            let arrival_phase = state.phase;
            let discharged = if deregister {
                state.registered = state
                    .registered
                    .checked_sub(1)
                    .expect("registered Phaser participant must have a registered party");
                if *participant_arrived {
                    false
                } else {
                    state.unarrived = state
                        .unarrived
                        .checked_sub(1)
                        .expect("unarrived Phaser participant must have an arrival obligation");
                    true
                }
            } else if *participant_arrived {
                false
            } else {
                state.unarrived = state
                    .unarrived
                    .checked_sub(1)
                    .expect("unarrived Phaser participant must have an arrival obligation");
                *participant_arrived = true;
                true
            };

            debug_assert!(state.unarrived <= state.registered);
            let wakers = if discharged && state.unarrived == 0 {
                state.phase = state.phase.next();
                state.unarrived = state.registered;
                *participant_phase = state.phase;
                *participant_arrived = false;
                Some(state.waiters.drain().collect())
            } else {
                None
            };
            (arrival_phase, wakers)
        }
    }

    fn arrive(
        &self,
        participant_phase: &mut Phase,
        participant_arrived: &mut bool,
        deregister: bool,
    ) -> Phase {
        let (arrival_phase, wakers) =
            self.record_arrival(participant_phase, participant_arrived, deregister);
        if let Some(wakers) = wakers {
            wake_all(wakers.into_iter());
        }
        arrival_phase
    }

    fn poll_wait(
        &self,
        token: &mut Option<WakerToken>,
        observed: Phase,
        cx: &mut Context<'_>,
    ) -> Poll<Phase> {
        let mut state = self.state.lock();
        if state.phase != observed {
            let phase = state.phase;
            *token = None;
            return Poll::Ready(phase);
        }

        let _retired_waker = state.waiters.register(token, cx.waker());
        drop(state);
        Poll::Pending
    }

    fn unregister_waker(&self, token: &mut Option<WakerToken>, observed: Phase) {
        if token.is_none() {
            return;
        }

        let mut state = self.state.lock();
        if state.phase != observed {
            *token = None;
            return;
        }

        let _removed_waker = state.waiters.unregister(token);
        drop(state);
    }
}

/// A capability representing one registered party in a [`Phaser`].
///
/// A participant contributes at most one arrival to each phase.
///
/// It owns an [`Arc`] that keeps its phaser alive.
///
/// Dropping a registered participant is equivalent to arriving and deregistering.
#[must_use = "dropping a participant arrives and deregisters it from the phaser"]
#[derive(Debug)]
pub struct PhaserParticipant {
    phaser: Arc<Phaser>,
    phase: Phase,
    arrived: bool,
    registered: bool,
    pending_wait: Option<Phase>,
}

impl PhaserParticipant {
    /// Arrives in the current phase without waiting for it to advance.
    ///
    /// Repeated calls in one phase return its identity without changing counts again.
    ///
    /// Calling this after a cancelled `arrive_and_wait` abandons that pending wait.
    ///
    /// If advancement occurred, this records an arrival in the new current phase.
    pub fn arrive(&mut self) -> Phase {
        self.pending_wait = None;
        self.phaser
            .arrive(&mut self.phase, &mut self.arrived, false)
    }

    /// Arrives in the current phase and waits for that phase to advance.
    ///
    /// The returned value is the new current phase.
    ///
    /// # Cancellation
    ///
    /// Arrival is committed when the returned future is first polled.
    ///
    /// Constructing and dropping an unpolled future has no effect.
    ///
    /// Cancelling after arrival removes only the waiter's waker.
    ///
    /// Retrying waits for the stored phase without arriving in a later phase.
    pub async fn arrive_and_wait(&mut self) -> Phase {
        let observed = match self.pending_wait {
            Some(phase) => phase,
            None => {
                let (phase, wakers) =
                    self.phaser
                        .record_arrival(&mut self.phase, &mut self.arrived, false);
                self.pending_wait = Some(phase);
                if let Some(wakers) = wakers {
                    wake_all(wakers.into_iter());
                }
                phase
            }
        };

        let next = self.phaser.wait_for_advance(observed).await;
        self.pending_wait = None;
        self.phase = next;
        self.arrived = false;
        next
    }

    /// Arrives in the current phase and deregisters from later phases.
    ///
    /// This consumes the participant and returns its final arrival phase.
    ///
    /// Any pending wait from a cancelled `arrive_and_wait` is abandoned.
    pub fn arrive_and_deregister(mut self) -> Phase {
        self.registered = false;
        self.pending_wait = None;
        self.phaser.arrive(&mut self.phase, &mut self.arrived, true)
    }
}

impl Drop for PhaserParticipant {
    fn drop(&mut self) {
        if self.registered {
            self.registered = false;
            self.pending_wait = None;
            self.phaser.arrive(&mut self.phase, &mut self.arrived, true);
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct PhaserWait<'a> {
    token: Option<WakerToken>,
    observed: Phase,
    phaser: &'a Phaser,
}

impl fmt::Debug for PhaserWait<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhaserWait")
            .field("observed", &self.observed)
            .finish_non_exhaustive()
    }
}

impl Future for PhaserWait<'_> {
    type Output = Phase;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self {
            token,
            observed,
            phaser,
        } = self.get_mut();
        phaser.poll_wait(token, *observed, cx)
    }
}

impl Drop for PhaserWait<'_> {
    fn drop(&mut self) {
        self.phaser.unregister_waker(&mut self.token, self.observed);
    }
}
