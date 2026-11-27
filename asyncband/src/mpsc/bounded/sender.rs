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

use std::fmt;
use std::future::Future;
use std::mem;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::internal::waitlist::WaiterId;
use crate::mpsc::SendError;
use crate::mpsc::TrySendError;
use crate::mpsc::bounded::queue::Shared;

/// The sending endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`](crate::mpsc::bounded) function.
pub struct BoundedSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for BoundedSender<T> {
    #[inline]
    fn clone(&self) -> Self {
        let shared = self.shared.clone();
        shared.clone_sender();
        BoundedSender { shared }
    }
}

impl<T> fmt::Debug for BoundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedSender").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedSender<T> {
    #[inline]
    fn drop(&mut self) {
        self.shared.drop_sender();
    }
}

impl<T> BoundedSender<T> {
    pub(crate) fn new(shared: Arc<Shared<T>>) -> Self {
        Self { shared }
    }

    /// Sends a value, waiting for capacity when the queue is full.
    ///
    /// Once capacity is acquired, claiming the next queue slot and copying `value` proceeds as
    /// described on [`try_send`](Self::try_send).
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] containing the unsent value if the receiver has been dropped.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending `send` future loses its place waiting for capacity and drops `value`;
    /// a future that has returned `Pending` has not sent the message. Use
    /// [`try_send`](Self::try_send) when the caller must retain ownership if capacity is
    /// unavailable, or [`reserve`](Self::reserve) to wait for capacity before constructing the
    /// message.
    ///
    /// # Panics
    ///
    /// Polling the returned future after it has returned [`Poll::Ready`] panics.
    #[inline]
    pub fn send(&self, value: T) -> impl Future<Output = Result<(), SendError<T>>> {
        SendValue {
            shared: Some(&self.shared),
            waiter: None,
            value: MaybeUninit::new(value),
        }
    }

    /// Reserves capacity for one message before the caller supplies its value.
    ///
    /// The returned [`Permit`] holds one capacity slot. Dropping it releases that slot;
    /// [`Permit::send`] publishes a value without waiting for space. Holding a permit does not
    /// establish message order or keep a receiver alive.
    ///
    /// # Errors
    ///
    /// Returns `SendError(())` if the receiver has been dropped. A successful reservation
    /// can still fail to send if disconnection happens before [`Permit::send`].
    ///
    /// # Cancellation
    ///
    /// Dropping a pending `reserve` future loses its place in the wait queue. If capacity has
    /// already been granted, it is released to the next waiter or made available to a new sender.
    ///
    /// # Panics
    ///
    /// Polling the returned future after it has returned [`Poll::Ready`] panics.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mpsc;
    ///
    /// let (tx, mut rx) = mpsc::bounded(1);
    /// let permit = tx.reserve().await.unwrap();
    /// let message = String::from("constructed after capacity became available");
    /// permit.send(message).unwrap();
    /// assert_eq!(
    ///     rx.recv().await.unwrap(),
    ///     "constructed after capacity became available"
    /// );
    /// # }
    /// ```
    #[inline]
    pub fn reserve(&self) -> impl Future<Output = Result<Permit<'_, T>, SendError<()>>> {
        Reserve {
            shared: Some(&self.shared),
            waiter: None,
        }
    }

    /// Reserves capacity for one message without waiting.
    ///
    /// Dropping the returned [`Permit`] releases its capacity. See [`reserve`](Self::reserve)
    /// for reservation and message ordering.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] if capacity is unavailable, or
    /// [`TrySendError::Disconnected`] if the receiver has been dropped. Errors carry `()`
    /// because no message has been supplied.
    #[inline]
    pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        self.shared.try_acquire_capacity()?;
        Ok(Permit {
            shared: &self.shared,
        })
    }

    /// Sends a value without waiting for capacity.
    ///
    /// Acquiring capacity and claiming a queue position are separate steps: a concurrent sender can
    /// observe [`TrySendError::Full`] as soon as the last capacity unit is taken, including while
    /// another sender holds a [`Permit`] or has just acquired capacity and has not yet claimed its
    /// queue slot.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] if capacity is unavailable, or
    /// [`TrySendError::Disconnected`] if the receiver has been dropped. Both errors contain the
    /// unsent value; nothing is enqueued on error.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpsc::TrySendError;
    /// use asyncband::mpsc::bounded;
    ///
    /// let (tx, mut rx) = bounded(1);
    /// tx.try_send(10).unwrap();
    /// assert_eq!(tx.try_send(20), Err(TrySendError::Full(20)));
    ///
    /// assert_eq!(rx.try_recv(), Ok(10));
    /// tx.try_send(20).unwrap();
    /// drop(rx);
    /// assert_eq!(tx.try_send(30), Err(TrySendError::Disconnected(30)));
    /// ```
    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        match self.shared.try_acquire_capacity() {
            Ok(()) => {
                let value = MaybeUninit::new(value);
                // SAFETY: `value` is initialized; `publish_from_ptr` takes logical ownership on
                // `true` and leaves `value` untouched on `false`.
                if unsafe { self.shared.publish_from_ptr(value.as_ptr()) } {
                    Ok(())
                } else {
                    // SAFETY: `publish_from_ptr` returned `false` without reading or dropping
                    // `value`.
                    Err(TrySendError::Disconnected(unsafe { value.assume_init() }))
                }
            }
            Err(TrySendError::Full(())) => Err(TrySendError::Full(value)),
            Err(TrySendError::Disconnected(())) => Err(TrySendError::Disconnected(value)),
        }
    }

    /// Returns `true` if the receiver has been dropped.
    ///
    /// Once this returns `true`, every later [`send`](Self::send), [`try_send`](Self::try_send),
    /// [`reserve`](Self::reserve) or [`try_reserve`](Self::try_reserve) fails with a disconnection
    /// error, as does [`Permit::send`] on a permit reserved earlier. A `false` result can be
    /// outdated as soon as it is returned, because the receiver may be dropped concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpsc::bounded;
    ///
    /// let (tx, rx) = bounded::<u8>(1);
    /// assert!(!tx.is_disconnected());
    /// drop(rx);
    /// assert!(tx.is_disconnected());
    /// ```
    #[inline]
    pub fn is_disconnected(&self) -> bool {
        !self.shared.has_receiver()
    }
}

/// Capacity reserved for one message on a bounded channel.
///
/// Created by [`BoundedSender::reserve`] or [`BoundedSender::try_reserve`]. Holding a permit
/// reduces available capacity but does not prevent other messages from being received. Dropping
/// it without sending releases capacity and notifies a waiting sender.
#[must_use = "dropping the permit releases its reserved capacity"]
pub struct Permit<'a, T> {
    shared: &'a Shared<T>,
}

impl<T> fmt::Debug for Permit<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Permit").finish_non_exhaustive()
    }
}

impl<T> Permit<'_, T> {
    /// Publishes a value using this reserved capacity without waiting for space.
    ///
    /// This consumes the permit, including when sending fails.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] containing the unsent value if the receiver has been dropped.
    #[inline]
    pub fn send(self, value: T) -> Result<(), SendError<T>> {
        let shared = self.shared;
        mem::forget(self);
        let value = MaybeUninit::new(value);
        // SAFETY: `value` is initialized; `publish_from_ptr` takes logical ownership on `true` and
        // leaves `value` untouched on `false`.
        if unsafe { shared.publish_from_ptr(value.as_ptr()) } {
            Ok(())
        } else {
            // SAFETY: `publish_from_ptr` returned `false` without reading or dropping `value`.
            Err(SendError::new(unsafe { value.assume_init() }))
        }
    }
}

impl<T> Drop for Permit<'_, T> {
    #[inline]
    fn drop(&mut self) {
        self.shared.release_capacity();
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct SendValue<'a, T> {
    shared: Option<&'a Shared<T>>,
    waiter: Option<WaiterId>,
    value: MaybeUninit<T>,
}

// `SendValue` never pins `value`; it transfers `T` by value into a ring slot or back via error.
impl<T> Unpin for SendValue<'_, T> {}

impl<T> SendValue<'_, T> {
    #[inline(always)]
    fn publish_acquired(&mut self, shared: &Shared<T>) -> Poll<Result<(), SendError<T>>> {
        self.shared = None;
        // SAFETY: `self.value` is initialized and `self.shared = None` prevents `Drop` from
        // dropping it again if waking panics. `MaybeUninit<T>` carries no validity invariant after
        // ownership moves into the ring, so moving the completed future remains sound for `Box<T>`
        // (including on pre-1.96 toolchains) and `&mut T`.
        if unsafe { shared.publish_from_ptr(self.value.as_ptr()) } {
            self.shared = None;
            Poll::Ready(Ok(()))
        } else {
            unsafe { self.finish_disconnected() }
        }
    }

    #[cold]
    #[inline(never)]
    unsafe fn finish_disconnected(&mut self) -> Poll<Result<(), SendError<T>>> {
        self.shared = None;
        // SAFETY: Caller guarantees `self.value` is initialized and has not been moved or dropped.
        Poll::Ready(Err(SendError::new(unsafe {
            self.value.assume_init_read()
        })))
    }
}

impl<T> Future for SendValue<'_, T> {
    type Output = Result<(), SendError<T>>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let shared = this
            .shared
            .expect("bounded mpsc send polled after completion");

        if this.waiter.is_none() {
            match shared.try_acquire_capacity() {
                Ok(()) => return this.publish_acquired(shared),
                Err(TrySendError::Disconnected(())) => {
                    // SAFETY: `this.value` has not been moved or dropped.
                    return unsafe { this.finish_disconnected() };
                }
                Err(TrySendError::Full(())) => {}
            }
        }

        match shared.poll_acquire_capacity_slow(&mut this.waiter, cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => this.publish_acquired(shared),
            // SAFETY: `this.value` has not been moved or dropped.
            Poll::Ready(Err(_)) => unsafe { this.finish_disconnected() },
        }
    }
}

impl<T> Drop for SendValue<'_, T> {
    #[inline]
    fn drop(&mut self) {
        let Some(shared) = self.shared else {
            return;
        };
        if let Some(id) = self.waiter.take() {
            shared.cancel_waiter_id(id);
        }
        if mem::needs_drop::<T>() {
            // SAFETY: `self.shared.is_some()` means `self.value` was never published or returned.
            unsafe {
                self.value.assume_init_drop();
            }
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct Reserve<'a, T> {
    shared: Option<&'a Shared<T>>,
    waiter: Option<WaiterId>,
}

impl<'a, T> Future for Reserve<'a, T> {
    type Output = Result<Permit<'a, T>, SendError<()>>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let shared = this
            .shared
            .expect("bounded mpsc reserve polled after completion");

        if this.waiter.is_none() {
            match shared.try_acquire_capacity() {
                Ok(()) => {
                    this.shared = None;
                    return Poll::Ready(Ok(Permit { shared }));
                }
                Err(TrySendError::Disconnected(())) => {
                    this.shared = None;
                    return Poll::Ready(Err(SendError::new(())));
                }
                Err(TrySendError::Full(())) => {}
            }
        }
        match shared.poll_acquire_capacity_slow(&mut this.waiter, cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                this.shared = None;
                Poll::Ready(Ok(Permit { shared }))
            }
            Poll::Ready(Err(error)) => {
                this.shared = None;
                Poll::Ready(Err(error))
            }
        }
    }
}

impl<T> Drop for Reserve<'_, T> {
    #[inline]
    fn drop(&mut self) {
        if let Some((shared, id)) = self.shared.zip(self.waiter.take()) {
            shared.cancel_waiter_id(id);
        }
    }
}
