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

//! Track independently spawned futures and consume their outputs as they complete.
//!
//! A [`TaskGroup`] owns one queue that stores outputs in the order the futures finish. Its
//! cloneable [`Registrar`] wraps futures before the caller spawns them on an executor. Asyncband
//! never chooses an executor or spawns a task itself.
//!
//! [`close`](TaskGroup::close) prevents further registration. Once the group is closed, every
//! tracked future has either completed or been dropped, and every queued output has been consumed,
//! [`join_next`](TaskGroup::join_next) returns `None`. Requiring `close` lets `join_next` tell an
//! open group with no current work from a group that will never receive more work.
//!
//! # Example
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use asyncband::task_group::TaskGroup;
//!
//! let (mut group, registrar) = TaskGroup::new();
//! let first = tokio::spawn(registrar.track(async { 21 }).unwrap());
//! let second = tokio::spawn(registrar.track(async { 2 }).unwrap());
//! group.close();
//!
//! let mut outputs = group.join().await;
//! outputs.sort_unstable();
//! assert_eq!(outputs, [2, 21]);
//! first.await.unwrap();
//! second.await.unwrap();
//! # }
//! ```
//!
//! # Task lifetime and failure
//!
//! Successful registration counts the returned [`Tracked`] future as active immediately. The
//! count is released when that wrapper completes or is dropped, including when an executor aborts
//! its task or drops it after a panic. Only normal completion produces an output; task aborts and
//! panics are therefore reported only by the caller's executor.
//!
//! Dropping the [`TaskGroup`] discards queued and future outputs and makes all registrars reject
//! new work. It does not cancel or abort tracked futures. Callers can wrap each future with their
//! own cancellation mechanism.
//!
//! The output queue can grow without limit until the owner consumes it or is dropped. Call
//! [`join_next`](TaskGroup::join_next) continuously if futures may finish faster than the owner can
//! consume their outputs.

use std::any::type_name;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::internal::mutex::Mutex;

#[cfg(test)]
mod tests;

/// The single owner of outputs from a dynamically registered group of futures.
///
/// Create a group and its first [`Registrar`] with [`TaskGroup::new`]. The owner is deliberately
/// not cloneable: only one task may consume outputs in the order the futures finish. Methods that
/// consume outputs require mutable access, so only one join operation can wait at a time.
pub struct TaskGroup<T = ()> {
    shared: Arc<Shared<T>>,
}

impl<T> fmt::Debug for TaskGroup<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.shared.state.lock();
        f.debug_struct("TaskGroup")
            .field("active", &self.shared.lifecycle.active_tasks())
            .field("closed", &self.shared.lifecycle.is_closed())
            .field("queued_outputs", &state.outputs.len())
            .finish_non_exhaustive()
    }
}

impl<T> Drop for TaskGroup<T> {
    fn drop(&mut self) {
        let (outputs, waiter) = self.shared.drop_owner();
        // Dropping an output or waker may use a registrar, so do it after releasing the state lock.
        drop((outputs, waiter));
    }
}

impl<T> TaskGroup<T> {
    /// Creates an open task group and its first registration handle.
    pub fn new() -> (Self, Registrar<T>) {
        let shared = Arc::new(Shared {
            lifecycle: Lifecycle::new(),
            state: Mutex::new(State {
                owner_alive: true,
                discard_outputs: false,
                outputs: VecDeque::new(),
                waiter: None,
            }),
        });
        let registrar = Registrar {
            shared: Arc::downgrade(&shared),
        };
        (Self { shared }, registrar)
    }

    /// Returns another handle for registering futures in this group.
    pub fn registrar(&self) -> Registrar<T> {
        Registrar {
            shared: Arc::downgrade(&self.shared),
        }
    }

    /// Returns whether this group rejects new registrations.
    pub fn is_closed(&self) -> bool {
        self.shared.is_closed()
    }

    /// Permanently prevents new futures from being registered.
    ///
    /// Existing tracked futures continue running. Calling this method more than once has no
    /// additional effect. Joining an idle open group remains pending, so callers must close the
    /// group when no more work can be registered.
    ///
    /// # Panics
    ///
    /// Panics if waking a pending join operation panics. The group remains closed.
    pub fn close(&self) {
        if let Some(waker) = self.shared.close() {
            waker.wake();
        }
    }

    /// Returns the next normally completed output, in completion order.
    ///
    /// Returns `None` only after the group is closed, all tracked futures have completed or been
    /// dropped, and all earlier outputs have been consumed. A tracked future that is dropped,
    /// aborted, or dropped after a panic produces no output.
    ///
    /// Canceling this operation while it waits does not consume an output.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::task_group::TaskGroup;
    /// use tokio::sync::oneshot;
    ///
    /// let (mut group, registrar) = TaskGroup::new();
    /// let (send, receive) = oneshot::channel();
    /// let later = tokio::spawn(
    ///     registrar
    ///         .track(async move {
    ///             receive.await.unwrap();
    ///             "later"
    ///         })
    ///         .unwrap(),
    /// );
    /// let first = tokio::spawn(registrar.track(async { "first" }).unwrap());
    /// group.close();
    ///
    /// assert_eq!(group.join_next().await, Some("first"));
    /// send.send(()).unwrap();
    /// assert_eq!(group.join_next().await, Some("later"));
    /// assert_eq!(group.join_next().await, None);
    /// first.await.unwrap();
    /// later.await.unwrap();
    /// # }
    /// ```
    pub fn join_next(&mut self) -> JoinNext<'_, T> {
        JoinNext {
            group: self,
            registered: false,
        }
    }

    /// Waits for the group to finish and discards every output.
    ///
    /// This operation does not close the group. It remains pending while the group is open, even
    /// when no futures are active. If it is canceled, outputs discarded by this call cannot be
    /// recovered by a later join.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::task_group::TaskGroup;
    ///
    /// let (mut group, registrar) = TaskGroup::new();
    /// let task = tokio::spawn(registrar.track(async { 42 }).unwrap());
    /// group.close();
    ///
    /// group.wait().await;
    /// assert_eq!(group.join_next().await, None);
    /// task.await.unwrap();
    /// # }
    /// ```
    pub async fn wait(&mut self) {
        let (_discarding, outputs) = DiscardOutputs::new(self.shared.clone());
        drop(outputs);
        while self.join_next().await.is_some() {}
    }

    /// Waits for the group to finish and collects its outputs in completion order.
    ///
    /// This operation does not close the group. If it is canceled, outputs already collected by
    /// this call are dropped and cannot be recovered by a later join.
    pub async fn join(&mut self) -> Vec<T> {
        let (queued, capacity) = self.shared.take_outputs_and_capacity_hint();
        let additional_capacity = capacity.saturating_sub(queued.len());
        let mut outputs = Vec::from(queued);
        outputs.reserve(additional_capacity);
        while let Some(output) = self.join_next().await {
            outputs.push(output);
            outputs.extend(self.shared.take_outputs());
        }
        outputs
    }
}

/// A cloneable handle that registers futures in one [`TaskGroup`].
///
/// The handle does not keep the group alive. Registration fails after the owner is dropped or
/// [`TaskGroup::close`] is called.
pub struct Registrar<T = ()> {
    shared: Weak<Shared<T>>,
}

impl<T> Clone for Registrar<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> fmt::Debug for Registrar<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registrar").finish_non_exhaustive()
    }
}

impl<T> Registrar<T> {
    /// Returns whether this handle can no longer register new futures.
    pub fn is_closed(&self) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            return true;
        };
        shared.is_closed()
    }

    /// Registers `future` immediately and returns a wrapper for the caller to poll or spawn.
    ///
    /// Dropping the returned wrapper before it completes releases its registration without
    /// producing an output. If registration fails, the error returns ownership of `future`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::task_group::TaskGroup;
    ///
    /// let (group, registrar) = TaskGroup::new();
    /// group.close();
    ///
    /// let future = async { 42 };
    /// let Err(error) = registrar.track(future) else {
    ///     panic!("a closed group accepted a future");
    /// };
    /// assert_eq!(error.into_inner().await, 42);
    /// # }
    /// ```
    pub fn track<F>(&self, future: F) -> Result<Tracked<F>, TrackError<F>>
    where
        F: Future<Output = T>,
    {
        let Some(shared) = self.shared.upgrade() else {
            return Err(TrackError(future));
        };
        if !shared.register() {
            return Err(TrackError(future));
        }
        Ok(Tracked {
            future,
            registration: Registration {
                shared: Some(shared),
            },
        })
    }
}

/// A future whose lifetime and successful output are tracked by a [`TaskGroup`].
///
/// This wrapper returns `()` after moving the inner future's output to the group. Dropping it
/// before normal completion releases its registration without producing an output.
#[must_use = "a tracked future must be polled, spawned, or dropped to release its registration"]
pub struct Tracked<F>
where
    F: Future,
{
    future: F,
    // Fields are dropped from top to bottom. If `Tracked` is dropped before completion, the inner
    // future is therefore dropped before the active task count is decreased.
    registration: Registration<F::Output>,
}

impl<F> fmt::Debug for Tracked<F>
where
    F: Future + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tracked")
            .field("future", &self.future)
            .finish_non_exhaustive()
    }
}

impl<F> Future for Tracked<F>
where
    F: Future,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(
            self.as_ref().get_ref().registration.shared.is_some(),
            "a tracked future cannot be polled after completion"
        );
        // SAFETY: This manually projects the outer pin onto `future`. Once `Tracked` is pinned,
        // `future` stays at the same address until it is dropped.
        let future = unsafe { self.as_mut().map_unchecked_mut(|this| &mut this.future) };
        let Poll::Ready(output) = future.poll(cx) else {
            return Poll::Pending;
        };

        // SAFETY: The pin applies only to `future`; accessing `registration` does not move it.
        unsafe { self.get_unchecked_mut() }
            .registration
            .complete(output);
        Poll::Ready(())
    }
}

/// A future returned by [`TaskGroup::join_next`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct JoinNext<'a, T> {
    group: &'a mut TaskGroup<T>,
    registered: bool,
}

impl<T> fmt::Debug for JoinNext<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinNext").finish_non_exhaustive()
    }
}

impl<T> Future for JoinNext<'_, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = this.group.shared.poll_join_next(cx);
        this.registered = result.is_pending();
        result
    }
}

impl<T> Drop for JoinNext<'_, T> {
    fn drop(&mut self) {
        if self.registered {
            self.group.shared.unregister_waiter();
        }
    }
}

/// A task could not be tracked because its group was closed or dropped.
///
/// The error retains the future so the caller can recover it with [`into_inner`](Self::into_inner).
#[derive(Clone, PartialEq, Eq)]
pub struct TrackError<F>(F);

impl<F> TrackError<F> {
    /// Returns a reference to the future that was not tracked.
    pub fn as_inner(&self) -> &F {
        &self.0
    }

    /// Consumes the error and returns the future that was not tracked.
    pub fn into_inner(self) -> F {
        self.0
    }
}

impl<F> fmt::Display for TrackError<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tracking a future in a closed task group")
    }
}

impl<F> fmt::Debug for TrackError<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TrackError<{}>(..)", type_name::<F>())
    }
}

impl<F> std::error::Error for TrackError<F> {}

struct Shared<T> {
    lifecycle: Lifecycle,
    state: Mutex<State<T>>,
}

struct State<T> {
    owner_alive: bool,
    discard_outputs: bool,
    outputs: VecDeque<T>,
    waiter: Option<Waker>,
}

impl<T> Shared<T> {
    fn register(&self) -> bool {
        self.lifecycle.try_register()
    }

    fn complete(&self, output: T) -> (Option<T>, Option<Waker>) {
        let mut state = self.state.lock();
        let discarded = if state.owner_alive && !state.discard_outputs {
            state.outputs.push_back(output);
            None
        } else {
            Some(output)
        };
        let finished = self.lifecycle.retire_task();
        let waker = if state.owner_alive && (!state.discard_outputs || finished) {
            state.waiter.take()
        } else {
            None
        };
        (discarded, waker)
    }

    fn abandon(&self) -> Option<Waker> {
        if !self.lifecycle.retire_task() {
            return None;
        }

        let mut state = self.state.lock();
        if state.owner_alive && state.outputs.is_empty() {
            state.waiter.take()
        } else {
            None
        }
    }

    fn poll_join_next(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let (poll, waiter) = {
            let mut state = self.state.lock();
            if let Some(output) = state.outputs.pop_front() {
                (Poll::Ready(Some(output)), state.waiter.take())
            } else if self.lifecycle.is_finished() {
                (Poll::Ready(None), state.waiter.take())
            } else if state
                .waiter
                .as_ref()
                .is_some_and(|waker| waker.will_wake(cx.waker()))
            {
                (Poll::Pending, None)
            } else {
                let waiter = state.waiter.replace(cx.waker().clone());
                (Poll::Pending, waiter)
            }
        };
        drop(waiter);
        poll
    }

    fn unregister_waiter(&self) {
        let waiter = self.state.lock().waiter.take();
        drop(waiter);
    }

    fn close(&self) -> Option<Waker> {
        if !self.lifecycle.close() {
            return None;
        }

        let mut state = self.state.lock();
        if state.outputs.is_empty() {
            state.waiter.take()
        } else {
            None
        }
    }

    fn drop_owner(&self) -> (VecDeque<T>, Option<Waker>) {
        let _ = self.lifecycle.close();
        let mut state = self.state.lock();
        state.owner_alive = false;
        (mem::take(&mut state.outputs), state.waiter.take())
    }

    fn is_closed(&self) -> bool {
        self.lifecycle.is_closed()
    }

    fn take_outputs_and_capacity_hint(&self) -> (VecDeque<T>, usize) {
        let (outputs, capacity, waiter) = {
            let mut state = self.state.lock();
            let active_tasks = self.lifecycle.active_tasks();
            let capacity = state.outputs.len().saturating_add(active_tasks);
            let outputs = mem::take(&mut state.outputs);
            let waiter = if outputs.is_empty() {
                None
            } else {
                state.waiter.take()
            };
            (outputs, capacity, waiter)
        };
        drop(waiter);
        (outputs, capacity)
    }

    fn take_outputs(&self) -> VecDeque<T> {
        let (outputs, waiter) = {
            let mut state = self.state.lock();
            let outputs = mem::take(&mut state.outputs);
            let waiter = if outputs.is_empty() {
                None
            } else {
                state.waiter.take()
            };
            (outputs, waiter)
        };
        drop(waiter);
        outputs
    }

    fn begin_discarding_outputs(&self) -> VecDeque<T> {
        let (outputs, waiter) = {
            let mut state = self.state.lock();
            state.discard_outputs = true;
            (mem::take(&mut state.outputs), state.waiter.take())
        };
        drop(waiter);
        outputs
    }

    fn end_discarding_outputs(&self) {
        self.state.lock().discard_outputs = false;
    }
}

struct Lifecycle(AtomicUsize);

impl Lifecycle {
    // The high bit closes registration; the remaining bits count active tracked futures. Keeping
    // both in one atomic gives registration and close a single, unambiguous order without locking
    // the output queue.
    const CLOSED_BIT: usize = 1 << (usize::BITS - 1);
    const ACTIVE_TASKS_MASK: usize = Self::CLOSED_BIT - 1;

    const fn new() -> Self {
        Self(AtomicUsize::new(0))
    }

    fn active_tasks(&self) -> usize {
        // This value is used only for debug output and as an approximate vector size.
        self.0.load(Ordering::Relaxed) & Self::ACTIVE_TASKS_MASK
    }

    fn is_closed(&self) -> bool {
        // This only reads the closed bit and does not need to make any other memory visible.
        self.0.load(Ordering::Relaxed) & Self::CLOSED_BIT != 0
    }

    fn is_finished(&self) -> bool {
        let current = self.0.load(Ordering::Acquire);
        current & Self::CLOSED_BIT != 0 && current & Self::ACTIVE_TASKS_MASK == 0
    }

    fn try_register(&self) -> bool {
        // The compare-exchange decides whether registration or close happened first. It changes
        // only this count and passes no other data between threads, so Relaxed ordering is enough.
        let mut current = self.0.load(Ordering::Relaxed);
        loop {
            if current & Self::CLOSED_BIT != 0 {
                return false;
            }
            assert_ne!(
                current,
                Self::ACTIVE_TASKS_MASK,
                "a task group cannot track more than isize::MAX futures"
            );

            match self.0.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn retire_task(&self) -> bool {
        // A Release decrement makes this task's earlier work visible when the group finishes. The
        // final task uses an Acquire fence so it also sees work released by the earlier tasks
        // before waking the owner.
        let previous = self.0.fetch_sub(1, Ordering::Release);
        let active_tasks = previous & Self::ACTIVE_TASKS_MASK;
        debug_assert!(active_tasks > 0, "a tracked future owns one active count");
        let finished = previous & Self::CLOSED_BIT != 0 && active_tasks == 1;
        if finished {
            fence(Ordering::Acquire);
        }
        finished
    }

    fn close(&self) -> bool {
        // If the group is already empty, Acquire makes the finished tasks' earlier work visible
        // here. Otherwise, Release lets the final task see that the group was closed before it
        // wakes the owner.
        let previous = self.0.fetch_or(Self::CLOSED_BIT, Ordering::AcqRel);
        previous & Self::CLOSED_BIT == 0 && previous & Self::ACTIVE_TASKS_MASK == 0
    }
}

struct Registration<T> {
    shared: Option<Arc<Shared<T>>>,
}

impl<T> Registration<T> {
    fn complete(&mut self, output: T) {
        // Keep `shared` here until the output has been stored. If growing the queue panics,
        // `Registration::drop` can still decrease the active task count. After the output is
        // stored, take `shared` before dropping an output or waking a task so a panic cannot
        // decrease the count twice.
        let (discarded, waker) = self
            .shared
            .as_ref()
            .expect("a tracked future cannot be polled after completion")
            .complete(output);
        let _shared = self
            .shared
            .take()
            .expect("the registration was present when completion began");
        if let Some(waker) = waker {
            waker.wake();
        }
        drop(discarded);
    }
}

impl<T> Drop for Registration<T> {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take()
            && let Some(waker) = shared.abandon()
        {
            waker.wake();
        }
    }
}

struct DiscardOutputs<T> {
    shared: Arc<Shared<T>>,
}

impl<T> DiscardOutputs<T> {
    fn new(shared: Arc<Shared<T>>) -> (Self, VecDeque<T>) {
        let outputs = shared.begin_discarding_outputs();
        (Self { shared }, outputs)
    }
}

impl<T> Drop for DiscardOutputs<T> {
    fn drop(&mut self) {
        self.shared.end_discarding_outputs();
    }
}
