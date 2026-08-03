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

//! Runtime-independent timer primitives driven by an explicit reactor context.
//!
//! [`TimerDriver`] owns time advancement. It starts no thread and opens no I/O resource; an event
//! loop calls [`TimerDriver::turn`] and uses [`TimerDriver::next_poll_at`] to choose its next poll
//! deadline. Tasks receive a cloneable [`TimerContext`] and create lazy [`Delay`] futures from it.
//!
//! # Reactor contract
//!
//! After each timer turn, the integrating event loop must:
//!
//! 1. use a zero timeout when [`TurnResult::has_more_work`] is true;
//! 2. otherwise call [`TimerDriver::register_wake`] before parking and use a zero timeout if it
//!    returns `false`;
//! 3. only after a successful registration, read [`TimerDriver::next_poll_at`] and calculate the
//!    timeout against a fresh clock observation.
//!
//! Even when timer work remains, the reactor should perform one non-blocking I/O poll and dispatch
//! ready I/O before calling `turn` again. This preserves progress for both sources.
//!
//! # Resolution
//!
//! The driver uses a 1 ms scheduling resolution. Future deadlines are rounded up to the next tick,
//! so a timer never fires early because of wheel rounding, but may become ready up to one tick
//! after its requested deadline in addition to any reactor wake-up delay.
//!
//! # Examples
//!
//! ```
//! use std::future::Future;
//! use std::pin::pin;
//! use std::task::Context;
//! use std::task::Poll;
//! use std::task::Waker;
//! use std::time::Duration;
//! use std::time::Instant;
//!
//! use mea::time::TimerDriver;
//! use mea::time::TurnBudget;
//!
//! let start = Instant::now();
//! let deadline = start + Duration::from_millis(10);
//! let (mut driver, timer) = TimerDriver::new_at(start);
//! let mut delay = pin!(timer.delay_until(deadline));
//! let mut cx = Context::from_waker(Waker::noop());
//!
//! assert!(delay.as_mut().poll(&mut cx).is_pending());
//! // The first poll queued a registration, so a reactor may not park yet.
//! assert!(!driver.register_wake(Waker::noop()));
//! assert!(!driver.turn(start, TurnBudget::default()).has_more_work());
//!
//! // With the queue drained, the reactor can arm its wake and use the timer deadline.
//! assert!(driver.register_wake(Waker::noop()));
//! assert_eq!(driver.next_poll_at(), Some(deadline));
//! let _ = driver.turn(start + Duration::from_millis(10), TurnBudget::default());
//! assert!(matches!(delay.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
//! ```

#[cfg(test)]
mod tests;
mod wheel;

use std::fmt;
use std::future::Future;
use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::ops::AsyncFnMut;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use crate::atomicbox::AtomicOptionBox;
use crate::mpsc;
use crate::mpsc::TryRecvError;
use crate::time::wheel::Deadline;
use crate::time::wheel::Step;
use crate::time::wheel::Wheel;

const STATE_SUBMITTED: u8 = 0;
const STATE_REGISTERED: u8 = 1;
const STATE_FIRED: u8 = 2;
const STATE_CANCELLED: u8 = 3;
const STATE_CLOSED: u8 = 4;
const NO_SLOT: usize = usize::MAX;

/// Work admitted during one [`TimerDriver::turn`].
///
/// The default admits up to 1,024 operation-queue messages and 4,096 timer entries per turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TurnBudget {
    max_operations: NonZeroUsize,
    max_timer_entries: NonZeroUsize,
}

impl TurnBudget {
    /// Creates a turn budget from non-zero operation and timer-entry limits.
    pub const fn new(max_operations: NonZeroUsize, max_timer_entries: NonZeroUsize) -> Self {
        Self {
            max_operations,
            max_timer_entries,
        }
    }

    /// Returns the maximum number of operation-queue messages processed by one turn.
    pub const fn max_operations(self) -> NonZeroUsize {
        self.max_operations
    }

    /// Returns the maximum number of wheel entries examined by one turn.
    pub const fn max_timer_entries(self) -> NonZeroUsize {
        self.max_timer_entries
    }
}

impl Default for TurnBudget {
    fn default() -> Self {
        Self::new(
            NonZeroUsize::new(1024).unwrap(),
            NonZeroUsize::new(4096).unwrap(),
        )
    }
}

/// The result of one [`TimerDriver::turn`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "the reactor must inspect whether timer work remains"]
pub struct TurnResult {
    has_more_work: bool,
}

impl TurnResult {
    /// Returns whether immediately runnable timer work remains.
    ///
    /// An integrating reactor must not block when this is `true`, but should still give I/O one
    /// non-blocking poll between timer turns.
    pub const fn has_more_work(self) -> bool {
        self.has_more_work
    }
}

/// Error returned when the driver backing a timer context has been dropped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimerClosed;

impl fmt::Display for TimerClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("timer driver is closed")
    }
}

impl std::error::Error for TimerClosed {}

#[derive(Clone, Copy)]
enum ClockMode {
    System,
    Driven,
}

struct DriverWake {
    pending: AtomicBool,
    slot: AtomicOptionBox<Waker>,
}

impl DriverWake {
    fn new() -> Self {
        Self {
            pending: AtomicBool::new(false),
            slot: AtomicOptionBox::none(),
        }
    }

    fn notify_after_send(&self) {
        // ORDERING: Paired with `mark_empty`; the completed channel send precedes this fence.
        fence(Ordering::SeqCst);
        let first = !self.pending.swap(true, Ordering::AcqRel);
        // ORDERING: Paired with `arm`; the pending publication precedes this fence. It remains
        // unconditional because a producer that finds `pending` set can race a concurrent clear.
        fence(Ordering::SeqCst);
        if !first {
            return;
        }
        if let Some(waker) = self.slot.take() {
            waker.wake();
        }
    }

    fn arm(&self, waker: &Waker) -> bool {
        self.slot.store(Some(Box::new(waker.clone())));
        // ORDERING: Paired with `notify_after_send`; acquire/release on separate atomics is
        // insufficient.
        fence(Ordering::SeqCst);
        if self.pending.load(Ordering::Acquire) {
            drop(self.slot.take());
            false
        } else {
            true
        }
    }

    fn mark_empty(&self) {
        self.pending.store(false, Ordering::Release);
        // ORDERING: Paired with the leading fence in `notify_after_send`. The caller rechecks the
        // queue immediately after this clear; without a store-load barrier the driver could both
        // overwrite a producer's `pending` and miss its queued operation.
        fence(Ordering::SeqCst);
    }

    fn mark_pending(&self) {
        self.pending.store(true, Ordering::Release);
    }

    fn is_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    fn disarm(&self) {
        drop(self.slot.take());
    }
}

struct Shared {
    clock_mode: ClockMode,
    closed: AtomicBool,
    observed: Mutex<Instant>,
    wake: DriverWake,
}

impl Shared {
    fn observation(&self) -> Instant {
        let published = *self
            .observed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match self.clock_mode {
            ClockMode::System => Instant::now().max(published),
            ClockMode::Driven => published,
        }
    }
}

struct DelayWakeSlot(AtomicOptionBox<Waker>);

impl DelayWakeSlot {
    fn new() -> Self {
        Self(AtomicOptionBox::none())
    }

    fn register_and_load(&self, waker: &Waker, lifecycle: &AtomicU8) -> u8 {
        self.0.store(Some(Box::new(waker.clone())));
        // ORDERING: Paired with `take_after_publish`; both sides may not miss the other's store.
        fence(Ordering::SeqCst);
        lifecycle.load(Ordering::Acquire)
    }

    fn take_after_publish(&self) -> Option<Waker> {
        // ORDERING: Paired with `register_and_load`; the lifecycle transition precedes this fence.
        fence(Ordering::SeqCst);
        self.0.take().map(|waker| *waker)
    }

    fn clear(&self) {
        drop(self.0.take());
    }
}

struct TimerState {
    fired_at: OnceLock<Instant>,
    lifecycle: AtomicU8,
    slot: AtomicUsize,
    waker: DelayWakeSlot,
}

impl TimerState {
    fn new() -> Self {
        Self {
            fired_at: OnceLock::new(),
            lifecycle: AtomicU8::new(STATE_SUBMITTED),
            slot: AtomicUsize::new(NO_SLOT),
            waker: DelayWakeSlot::new(),
        }
    }

    fn publish_terminal(&self, from: u8, to: u8) -> Option<Waker> {
        if self
            .lifecycle
            .compare_exchange(from, to, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.waker.take_after_publish()
        } else {
            None
        }
    }
}

struct RegisterOp {
    deadline: Deadline,
    state: Option<Arc<TimerState>>,
}

impl RegisterOp {
    fn into_state(mut self) -> Arc<TimerState> {
        self.state.take().expect("register op must own its state")
    }
}

impl Drop for RegisterOp {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        if let Some(waker) = state.publish_terminal(STATE_SUBMITTED, STATE_CLOSED) {
            waker.wake();
        }
    }
}

enum Operation {
    Register(RegisterOp),
    Cancel(Arc<TimerState>),
}

/// A cheap-to-clone timer capability passed explicitly to tasks.
///
/// See the [module level documentation](self) for the driver and reactor integration model.
#[derive(Clone)]
pub struct TimerContext {
    sender: mpsc::UnboundedSender<Operation>,
    shared: Arc<Shared>,
}

impl fmt::Debug for TimerContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimerContext").finish_non_exhaustive()
    }
}

impl TimerContext {
    /// Creates a lazy delay that completes at or after `deadline`.
    pub fn delay_until(&self, deadline: Instant) -> Delay {
        self.delay_to(Deadline::At(deadline))
    }

    /// Creates a lazy delay for `duration` from the context's current observation.
    ///
    /// An unrepresentable addition creates a delay that can complete only when its driver closes.
    pub fn delay(&self, duration: Duration) -> Delay {
        self.delay_to(Deadline::checked_add(self.now(), duration))
    }

    fn delay_to(&self, deadline: Deadline) -> Delay {
        Delay {
            context: self.clone(),
            deadline,
            closed: false,
            fired_at: None,
            registered_waker: None,
            state: None,
        }
    }

    fn now(&self) -> Instant {
        self.shared.observation()
    }

    fn send(&self, operation: Operation) -> Result<(), Operation> {
        match self.sender.send(operation) {
            Ok(()) => {
                self.shared.wake.notify_after_send();
                Ok(())
            }
            Err(err) => Err(err.into_inner()),
        }
    }
}

/// Reactor-owned timer source.
///
/// The driver starts no thread and performs no I/O. A reactor advances it with [`turn`](Self::turn)
/// and arms its own wake primitive through [`register_wake`](Self::register_wake).
///
/// See the [module level documentation](self) for the complete reactor contract.
pub struct TimerDriver {
    last_now: Instant,
    prefer_non_immediate: bool,
    receiver: Option<mpsc::UnboundedReceiver<Operation>>,
    shared: Arc<Shared>,
    wheel: Wheel<Arc<TimerState>>,
}

impl fmt::Debug for TimerDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimerDriver")
            .field("last_now", &self.last_now)
            .finish_non_exhaustive()
    }
}

impl TimerDriver {
    /// Constructs a system-clock driver and its context.
    pub fn new() -> (Self, TimerContext) {
        let genesis = Instant::now();
        Self::with_clock(genesis, ClockMode::System)
    }

    /// Constructs a deterministic driver whose context clock advances only through
    /// [`turn`](Self::turn).
    ///
    /// This constructor is useful for reactor tests and simulations. It does not read the system
    /// clock after construction.
    pub fn new_at(genesis: Instant) -> (Self, TimerContext) {
        Self::with_clock(genesis, ClockMode::Driven)
    }

    fn with_clock(genesis: Instant, clock_mode: ClockMode) -> (Self, TimerContext) {
        let (sender, receiver) = mpsc::unbounded();
        let shared = Arc::new(Shared {
            clock_mode,
            closed: AtomicBool::new(false),
            observed: Mutex::new(genesis),
            wake: DriverWake::new(),
        });
        let context = TimerContext {
            sender,
            shared: shared.clone(),
        };
        let driver = Self {
            last_now: genesis,
            prefer_non_immediate: true,
            receiver: Some(receiver),
            shared,
            wheel: Wheel::new(genesis),
        };
        (driver, context)
    }

    /// Returns when the driver should next be turned.
    ///
    /// Pending operations or due timer work produce the driver's current observation, requesting an
    /// immediate turn. `None` means there is no finite timer deadline; the driver may be empty or
    /// contain only delays created by overflowing relative-deadline arithmetic.
    pub fn next_poll_at(&self) -> Option<Instant> {
        if self.shared.wake.is_pending() {
            Some(self.last_now)
        } else {
            self.wheel.next_poll_at(self.last_now)
        }
    }

    /// Registers a replaceable one-shot wake notification before the reactor parks.
    ///
    /// Returns `false` when operations are already pending. In that case the reactor must perform
    /// a non-blocking I/O poll and call [`turn`](Self::turn) again instead of parking.
    pub fn register_wake(&self, waker: &Waker) -> bool {
        self.shared.wake.arm(waker)
    }

    /// Applies bounded operations and advances timers through `now`.
    ///
    /// A release build clamps a backwards `now` to the previous observation.
    ///
    /// # Panics
    ///
    /// Panics in debug builds when `now` is earlier than the previous observation.
    pub fn turn(&mut self, now: Instant, budget: TurnBudget) -> TurnResult {
        debug_assert!(now >= self.last_now, "timer driver cannot move backwards");
        let now = now.max(self.last_now);
        self.last_now = now;
        *self
            .shared
            .observed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = now;

        self.drain_operations(now, budget.max_operations.get());

        let mut entries = 0;
        // Alternate at entry granularity so neither continuously arriving immediate timers nor
        // previously registered wheel work can consume every bounded turn.
        // Once empty, non-immediate work stays empty for this turn: `now` is fixed and subsequent
        // steps can only remove immediate entries.
        let mut non_immediate_empty = false;
        while entries < budget.max_timer_entries.get() {
            let step = if self.prefer_non_immediate {
                if non_immediate_empty {
                    self.wheel.step_immediate()
                } else {
                    match self.wheel.step_non_immediate(now) {
                        Some(step) => Some(step),
                        None => {
                            non_immediate_empty = true;
                            self.wheel.step_immediate()
                        }
                    }
                }
            } else {
                self.wheel.step_immediate().or_else(|| {
                    if non_immediate_empty {
                        None
                    } else {
                        let step = self.wheel.step_non_immediate(now);
                        non_immediate_empty = step.is_none();
                        step
                    }
                })
            };
            let Some(step) = step else {
                break;
            };
            entries += 1;
            self.prefer_non_immediate = !self.prefer_non_immediate;
            self.apply_step(step, now);
        }

        let timer_more = self.wheel.has_due(now);
        if !timer_more {
            self.wheel.settle(now);
        }
        TurnResult {
            has_more_work: timer_more || self.shared.wake.is_pending(),
        }
    }

    fn apply_step(&mut self, step: Step, now: Instant) {
        if let Step::Fire(id) = step {
            self.fire(id, now);
        }
    }

    fn drain_operations(&mut self, now: Instant, limit: usize) {
        let mut processed = 0;
        while processed < limit {
            let operation = match self.receiver_mut().try_recv() {
                Ok(operation) => operation,
                Err(TryRecvError::Disconnected) => {
                    self.shared.wake.mark_empty();
                    break;
                }
                Err(TryRecvError::Empty) => {
                    self.shared.wake.mark_empty();
                    match self.receiver_mut().try_recv() {
                        Ok(operation) => {
                            self.shared.wake.mark_pending();
                            operation
                        }
                        Err(TryRecvError::Disconnected | TryRecvError::Empty) => break,
                    }
                }
            };
            processed += 1;
            self.apply_operation(operation, now);
        }
    }

    fn apply_operation(&mut self, operation: Operation, now: Instant) {
        match operation {
            Operation::Register(operation) => self.register(operation, now),
            Operation::Cancel(state) => self.cancel(&state),
        }
    }

    fn register(&mut self, operation: RegisterOp, now: Instant) {
        let state = operation
            .state
            .as_ref()
            .expect("register op must own its state")
            .clone();
        if state.lifecycle.load(Ordering::Acquire) != STATE_SUBMITTED {
            return;
        }

        let id = self.wheel.insert(operation.deadline, now, state.clone());
        state.slot.store(id, Ordering::Relaxed);
        if state
            .lifecycle
            .compare_exchange(
                STATE_SUBMITTED,
                STATE_REGISTERED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            state.slot.store(NO_SLOT, Ordering::Relaxed);
            self.wheel.remove(id);
        }
    }

    fn cancel(&mut self, state: &Arc<TimerState>) {
        let id = state.slot.load(Ordering::Relaxed);
        if id == NO_SLOT {
            return;
        }
        let Some(found) = self.wheel.get(id) else {
            return;
        };
        if !Arc::ptr_eq(found, state) {
            return;
        }
        state.slot.store(NO_SLOT, Ordering::Relaxed);
        self.wheel.remove(id);
    }

    fn fire(&mut self, id: usize, now: Instant) {
        let Some(state) = self.wheel.get(id).cloned() else {
            return;
        };
        state
            .fired_at
            .set(now)
            .expect("timer completion observation must be published once");
        let waker = state.publish_terminal(STATE_REGISTERED, STATE_FIRED);
        state.slot.store(NO_SLOT, Ordering::Relaxed);
        self.wheel.remove(id);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn receiver_mut(&mut self) -> &mut mpsc::UnboundedReceiver<Operation> {
        self.receiver.as_mut().expect("timer receiver must be live")
    }
}

impl Drop for TimerDriver {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.wake.disarm();

        let mut wakers = Vec::new();
        for state in self.wheel.drain() {
            state.slot.store(NO_SLOT, Ordering::Relaxed);
            if let Some(waker) = state.publish_terminal(STATE_REGISTERED, STATE_CLOSED) {
                wakers.push(waker);
            }
        }
        for waker in wakers {
            waker.wake();
        }

        // Dropping the receiver drops queued RegisterOps, which close timers still in
        // STATE_SUBMITTED.
        drop(self.receiver.take());
    }
}

/// A lazy future that completes at or after its deadline.
///
/// Registration happens on the first poll. Dropping an incomplete delay cancels its registration.
/// The future returns [`TimerClosed`] if its backing driver is dropped before observing the
/// deadline, and subsequent polls repeat the same terminal result.
///
/// See the [module level documentation](self) for timer resolution and driving requirements.
#[must_use = "delays do nothing unless polled or awaited"]
pub struct Delay {
    context: TimerContext,
    deadline: Deadline,
    closed: bool,
    fired_at: Option<Instant>,
    registered_waker: Option<Waker>,
    state: Option<Arc<TimerState>>,
}

impl fmt::Debug for Delay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delay")
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

impl Delay {
    fn completion_observation(&self) -> Instant {
        self.fired_at
            .expect("completed delay must have a completion observation")
    }

    fn is_elapsed_at(&self, now: Instant) -> bool {
        self.deadline
            .as_instant()
            .is_some_and(|deadline| deadline <= now)
    }

    fn poll_lifecycle(&mut self, lifecycle: u8) -> Poll<Result<(), TimerClosed>> {
        match lifecycle {
            STATE_FIRED => {
                let state = self.state.take().expect("polled delay must have state");
                state.waker.clear();
                self.fired_at = Some(
                    *state
                        .fired_at
                        .get()
                        .expect("fired delay must have a completion observation"),
                );
                self.registered_waker = None;
                Poll::Ready(Ok(()))
            }
            STATE_CLOSED => {
                let state = self.state.take().expect("polled delay must have state");
                state.waker.clear();
                self.closed = true;
                self.registered_waker = None;
                Poll::Ready(Err(TimerClosed))
            }
            STATE_SUBMITTED | STATE_REGISTERED => Poll::Pending,
            STATE_CANCELLED => unreachable!(
                "only Delay::drop writes STATE_CANCELLED, so a live Delay cannot observe it"
            ),
            state => panic!("invalid timer state {state}"),
        }
    }
}

impl Future for Delay {
    type Output = Result<(), TimerClosed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.fired_at.is_some() {
            return Poll::Ready(Ok(()));
        }
        if this.closed {
            return Poll::Ready(Err(TimerClosed));
        }
        if this.state.is_none() {
            let now = this.context.now();
            if this.is_elapsed_at(now) {
                this.fired_at = Some(now);
                return Poll::Ready(Ok(()));
            }
            if this.context.shared.closed.load(Ordering::Acquire) {
                this.closed = true;
                return Poll::Ready(Err(TimerClosed));
            }

            let state = Arc::new(TimerState::new());
            state.waker.register_and_load(cx.waker(), &state.lifecycle);
            this.registered_waker = Some(cx.waker().clone());
            this.state = Some(state.clone());
            let operation = Operation::Register(RegisterOp {
                deadline: this.deadline,
                state: Some(state),
            });
            if let Err(operation) = this.context.send(operation) {
                let Operation::Register(operation) = operation else {
                    unreachable!("a failed send must return the submitted operation")
                };
                let state = operation.into_state();
                state.lifecycle.store(STATE_CLOSED, Ordering::Release);
                return this.poll_lifecycle(STATE_CLOSED);
            }

            let lifecycle = this
                .state
                .as_ref()
                .expect("submitted delay must have state")
                .lifecycle
                .load(Ordering::Acquire);
            return this.poll_lifecycle(lifecycle);
        }

        let state = this.state.as_ref().expect("polled delay must have state");
        let lifecycle = if this
            .registered_waker
            .as_ref()
            .is_some_and(|registered| registered.will_wake(cx.waker()))
        {
            // ORDERING: The shared slot still holds an equivalent waker from an earlier poll, so
            // there is nothing to publish and no fence to pair. A non-terminal lifecycle implies
            // the slot is still armed: the driver empties it only in `take_after_publish`, which
            // happens after the terminal transition, and `poll_lifecycle` clears it only on a
            // terminal state. Skipping the store avoids boxing a waker clone on every repoll.
            state.lifecycle.load(Ordering::Acquire)
        } else {
            let lifecycle = state.waker.register_and_load(cx.waker(), &state.lifecycle);
            this.registered_waker = Some(cx.waker().clone());
            lifecycle
        };
        this.poll_lifecycle(lifecycle)
    }
}

impl Drop for Delay {
    fn drop(&mut self) {
        let Some(state) = self.state.as_ref() else {
            return;
        };
        loop {
            match state.lifecycle.load(Ordering::Acquire) {
                STATE_SUBMITTED => {
                    if state
                        .lifecycle
                        .compare_exchange(
                            STATE_SUBMITTED,
                            STATE_CANCELLED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        state.waker.clear();
                        return;
                    }
                }
                STATE_REGISTERED => {
                    if state
                        .lifecycle
                        .compare_exchange(
                            STATE_REGISTERED,
                            STATE_CANCELLED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        state.waker.clear();
                        let _ = self.context.send(Operation::Cancel(state.clone()));
                        return;
                    }
                }
                STATE_FIRED | STATE_CANCELLED | STATE_CLOSED => return,
                state => panic!("invalid timer state {state}"),
            }
        }
    }
}

/// Why a timed operation did not produce a value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeoutError {
    /// The deadline was reached first.
    Elapsed,
    /// The timer driver was dropped before the deadline was observed.
    Closed,
}

impl fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Elapsed => f.write_str("operation timed out"),
            Self::Closed => f.write_str("timer driver is closed"),
        }
    }
}

impl std::error::Error for TimeoutError {}

/// Runs `future` until it completes or `duration` elapses.
///
/// The guarded future wins when both branches become ready in the same poll.
///
/// # Examples
///
/// ```no_run
/// use std::time::Duration;
///
/// use mea::time::TimeoutError;
/// use mea::time::TimerContext;
/// use mea::time::timeout;
///
/// # async fn example(timer: &TimerContext) -> Result<(), TimeoutError> {
/// let value = timeout(timer, Duration::from_secs(1), async { 42 }).await?;
/// assert_eq!(value, 42);
/// # Ok(())
/// # }
/// ```
pub async fn timeout<F>(
    timer: &TimerContext,
    duration: Duration,
    future: F,
) -> Result<F::Output, TimeoutError>
where
    F: Future,
{
    timeout_with_delay(timer.delay(duration), future).await
}

/// Runs `future` until it completes or `deadline` is reached.
///
/// The guarded future wins when both branches become ready in the same poll.
pub async fn timeout_at<F>(
    timer: &TimerContext,
    deadline: Instant,
    future: F,
) -> Result<F::Output, TimeoutError>
where
    F: Future,
{
    timeout_with_delay(timer.delay_until(deadline), future).await
}

async fn timeout_with_delay<F>(delay: Delay, future: F) -> Result<F::Output, TimeoutError>
where
    F: Future,
{
    let mut future = std::pin::pin!(future);
    let mut delay = std::pin::pin!(delay);
    poll_fn(|cx| {
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }
        match delay.as_mut().poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Err(TimeoutError::Elapsed)),
            Poll::Ready(Err(TimerClosed)) => Poll::Ready(Err(TimeoutError::Closed)),
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}

/// How an [`Interval`] responds when one or more ticks were missed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MissedTickBehavior {
    /// Preserve every scheduled deadline and catch up with immediately ready ticks.
    #[default]
    Burst,
    /// Schedule the next tick one period after the completed tick was observed.
    Delay,
    /// Stay on the original grid while discarding deadlines at or before the observation.
    Skip,
}

/// A periodic timer built from sequential [`Delay`] futures.
///
/// See the [module level documentation](self) for timer resolution and driving requirements.
#[derive(Debug)]
#[must_use = "an interval advances only when tick is awaited"]
pub struct Interval {
    behavior: MissedTickBehavior,
    deadline: Deadline,
    delay: Delay,
    period: Duration,
    timer: TimerContext,
}

impl Interval {
    /// Waits for and consumes the next tick, returning its scheduled deadline.
    ///
    /// Cancelling this future before completion does not consume the tick.
    pub async fn tick(&mut self) -> Result<Instant, TimerClosed> {
        (&mut self.delay).await?;
        let observation = self.delay.completion_observation();
        let scheduled = self
            .deadline
            .as_instant()
            .expect("a never deadline cannot complete successfully");
        self.deadline = self.next_deadline(scheduled, observation);
        self.delay = self.timer.delay_to(self.deadline);
        Ok(scheduled)
    }

    /// Returns the current missed-tick behavior.
    pub const fn missed_tick_behavior(&self) -> MissedTickBehavior {
        self.behavior
    }

    /// Changes the missed-tick behavior used after the next completed tick.
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.behavior = behavior;
    }

    fn next_deadline(&self, scheduled: Instant, observation: Instant) -> Deadline {
        match self.behavior {
            MissedTickBehavior::Burst => Deadline::checked_add(scheduled, self.period),
            MissedTickBehavior::Delay => Deadline::checked_add(observation, self.period),
            MissedTickBehavior::Skip => skip_deadline(scheduled, self.period, observation),
        }
    }
}

/// Creates an interval with an immediate first tick.
///
/// # Examples
///
/// ```no_run
/// use std::time::Duration;
///
/// use mea::time::TimerClosed;
/// use mea::time::TimerContext;
/// use mea::time::interval;
///
/// # async fn example(timer: &TimerContext) -> Result<(), TimerClosed> {
/// let mut ticks = interval(timer, Duration::from_secs(1));
/// let scheduled_at = ticks.tick().await?;
/// # let _ = scheduled_at;
/// # Ok(())
/// # }
/// ```
///
/// # Panics
///
/// Panics when `period` is zero.
pub fn interval(timer: &TimerContext, period: Duration) -> Interval {
    interval_from_deadline(timer, Deadline::At(timer.now()), period)
}

/// Creates an interval whose first tick is scheduled at `start`.
///
/// # Panics
///
/// Panics when `period` is zero.
pub fn interval_at(timer: &TimerContext, start: Instant, period: Duration) -> Interval {
    interval_from_deadline(timer, Deadline::At(start), period)
}

fn interval_from_deadline(timer: &TimerContext, deadline: Deadline, period: Duration) -> Interval {
    assert!(!period.is_zero(), "interval period must be non-zero");
    Interval {
        behavior: MissedTickBehavior::Burst,
        deadline,
        delay: timer.delay_to(deadline),
        period,
        timer: timer.clone(),
    }
}

fn skip_deadline(scheduled: Instant, period: Duration, observation: Instant) -> Deadline {
    let Some(elapsed) = observation.checked_duration_since(scheduled) else {
        return Deadline::checked_add(scheduled, period);
    };
    let elapsed_nanos = elapsed.as_nanos();
    let period_nanos = period.as_nanos();
    let periods = elapsed_nanos / period_nanos + 1;
    let Some(total_nanos) = period_nanos.checked_mul(periods) else {
        return Deadline::Never;
    };
    let seconds = total_nanos / 1_000_000_000;
    let Ok(seconds) = u64::try_from(seconds) else {
        return Deadline::Never;
    };
    let nanos = (total_nanos % 1_000_000_000) as u32;
    Deadline::checked_add(scheduled, Duration::new(seconds, nanos))
}

/// Repeatedly runs `task`, waiting `delay` after each completion.
///
/// `None` starts the first task immediately; `Some(duration)` delays the first invocation.
/// This future otherwise runs until it is dropped, returning [`TimerClosed`] only if the driver
/// closes.
///
/// # Panics
///
/// Panics when `delay` is zero.
pub async fn schedule_with_fixed_delay<F>(
    timer: &TimerContext,
    initial_delay: Option<Duration>,
    delay: Duration,
    mut task: F,
) -> Result<(), TimerClosed>
where
    F: AsyncFnMut(),
{
    assert!(!delay.is_zero(), "fixed delay must be non-zero");
    if let Some(initial_delay) = initial_delay {
        timer.delay(initial_delay).await?;
    }
    loop {
        task().await;
        timer.delay(delay).await?;
    }
}

/// Repeatedly runs `task` on a fixed grid, skipping missed invocations without overlap.
///
/// `None` starts the first task immediately; `Some(duration)` delays the first invocation.
/// This future otherwise runs until it is dropped, returning [`TimerClosed`] only if the driver
/// closes.
///
/// A task that outruns its period does not cause the following invocations to run back to back.
/// The next deadline is the first grid point strictly after the task completes, so the schedule
/// stays aligned to the original grid and merely drops the invocations it missed.
///
/// # Panics
///
/// Panics when `period` is zero.
pub async fn schedule_at_fixed_rate<F>(
    timer: &TimerContext,
    initial_delay: Option<Duration>,
    period: Duration,
    mut task: F,
) -> Result<(), TimerClosed>
where
    F: AsyncFnMut(),
{
    assert!(!period.is_zero(), "fixed rate must be non-zero");
    let now = timer.now();
    let mut scheduled = match initial_delay {
        Some(delay) => Deadline::checked_add(now, delay),
        None => Deadline::At(now),
    };
    loop {
        timer.delay_to(scheduled).await?;
        let completed = scheduled
            .as_instant()
            .expect("a never deadline cannot complete successfully");
        task().await;
        // The observation is read *after* the task returns. Deriving the next grid point from
        // the tick's own completion instead would let an overrunning task collapse the schedule
        // into back-to-back invocations, because every subsequent deadline would already be in
        // the past by the time it was awaited.
        scheduled = skip_deadline(completed, period, timer.now());
    }
}

/// Repeatedly runs `task`, using each returned instant as the next deadline.
///
/// `None` starts the first task immediately; `Some(duration)` delays the first invocation.
/// Returning an elapsed instant repeatedly creates an intentionally busy loop.
/// This future otherwise runs until it is dropped, returning [`TimerClosed`] only if the driver
/// closes.
pub async fn schedule_with_arbitrary_delay<F>(
    timer: &TimerContext,
    initial_delay: Option<Duration>,
    mut task: F,
) -> Result<(), TimerClosed>
where
    F: AsyncFnMut() -> Instant,
{
    if let Some(initial_delay) = initial_delay {
        timer.delay(initial_delay).await?;
    }
    loop {
        let next = task().await;
        timer.delay_until(next).await?;
    }
}
