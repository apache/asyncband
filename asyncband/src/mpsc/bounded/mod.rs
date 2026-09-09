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

//! A bounded multi-producer, single-consumer queue for sending values between asynchronous
//! tasks with backpressure control.

use std::fmt;
use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::RecvError;
use super::SendError;
use super::TryRecvError;
use super::TrySendError;
use crate::internal::atomic_waker::AtomicWaker;
use crate::internal::cache_padded::CachePadded;
use crate::internal::mutex::Mutex;
use crate::internal::waitlist::WaitList;
use crate::internal::waitlist::WaiterId;
use crate::internal::wake_all;
use crate::internal::waker_batch::WakerBatch;

mod storage;
mod zero_sized;

#[cfg(test)]
mod tests;

/// Creates a bounded mpsc channel with room for `buffer` queued messages.
///
/// [`BoundedSender::send`] waits for capacity when the buffer is full. Receiving a message releases
/// one slot for a waiting sender. Capacity is granted in the order that pending sends and
/// reservations enter the wait queue; new senders cannot take an already granted slot.
///
/// Storage for nonzero-sized messages is preallocated. The channel's capacity is exactly
/// `buffer`. Zero-sized messages need no per-slot storage.
///
/// # Panics
///
/// Panics if `buffer` is zero or the preallocated message buffer exceeds the allocation size
/// limit. There is no additional channel-specific capacity limit.
#[track_caller]
pub fn bounded<T>(buffer: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(buffer > 0, "mpsc bounded channel requires buffer > 0");
    let (sender, receiver) = storage::channel(buffer);
    let shared = Arc::new(Shared {
        senders: AtomicUsize::new(1),
        tx_permits: CachePadded::new(Semaphore::new(buffer)),
        rx_waker: CachePadded::new(AtomicWaker::new()),
        sender,
    });
    let sender = BoundedSender {
        shared: shared.clone(),
    };
    let receiver = BoundedReceiver { shared, receiver };
    (sender, receiver)
}

struct Shared<T> {
    senders: AtomicUsize,
    tx_permits: CachePadded<Semaphore>,
    rx_waker: CachePadded<AtomicWaker>,
    sender: storage::Sender<T>,
}

// This channel-local semaphore grants one permit at a time and can close its wait queue.
// The general-purpose semaphore has neither a close operation nor acquisition errors.
struct Semaphore {
    available: AtomicUsize,
    closed: AtomicBool,
    waiters: Mutex<WaitList<Waiter>>,
}

impl Semaphore {
    fn new(available: usize) -> Self {
        Self {
            available: AtomicUsize::new(available),
            closed: AtomicBool::new(false),
            waiters: Mutex::new(WaitList::new()),
        }
    }

    fn try_acquire(&self) -> Result<(), TrySendError<()>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TrySendError::Disconnected(()));
        }
        let mut available = self.available.load(Ordering::Relaxed);
        loop {
            if available == 0 {
                return Err(TrySendError::Full(()));
            }
            match self.available.compare_exchange_weak(
                available,
                available - 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => available = actual,
            }
        }
    }

    fn release(&self) {
        let wake = self.release_locked(&mut self.waiters.lock());
        if let Some(waker) = wake {
            waker.wake();
        }
    }

    fn release_locked(&self, waiters: &mut WaitList<Waiter>) -> Option<Waker> {
        if self.closed.load(Ordering::Relaxed) {
            return None;
        }
        if let Some((_, waiter)) = waiters.unlink_first_waiter(|_| true) {
            // Grant ownership before waking; new arrivals cannot steal this capacity.
            waiter.granted = true;
            return waiter.waker.take();
        }
        // Only releases add permits, and all releases hold the wait queue lock. A linked
        // waiter therefore always sees zero available permits until it receives its own grant.
        self.available.fetch_add(1, Ordering::Release);
        None
    }

    fn close(&self) -> WakerBatch {
        let mut waiters = self.waiters.lock();
        self.closed.store(true, Ordering::Release);
        let mut wakers = WakerBatch::new();
        while let Some((_, waiter)) = waiters.unlink_first_waiter(|_| true) {
            if let Some(waker) = waiter.waker.take() {
                wakers.push(waker);
            }
        }
        wakers
    }
}

struct Waiter {
    granted: bool,
    waker: Option<Waker>,
}

struct Reservation<'a, T> {
    sender: &'a BoundedSender<T>,
    index: Option<WaiterId>,
}

impl<'a, T> Reservation<'a, T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Permit<'a, T>, SendError<()>>> {
        let semaphore = &self.sender.shared.tx_permits;
        let mut cloned_waker = None;
        loop {
            if self.index.is_none() {
                match semaphore.try_acquire() {
                    Ok(()) => {
                        let permit = Permit {
                            sender: Some(self.sender),
                        };
                        // The permit owns capacity before an unused cloned waker can panic.
                        drop(cloned_waker);
                        return Poll::Ready(Ok(permit));
                    }
                    Err(TrySendError::Disconnected(())) => {
                        return Poll::Ready(Err(SendError::new(())));
                    }
                    Err(TrySendError::Full(())) => {}
                }
            }
            let mut waiters = semaphore.waiters.lock();
            if semaphore.closed.load(Ordering::Relaxed) {
                // Drop removes any remaining registration, including an unused grant.
                return Poll::Ready(Err(SendError::new(())));
            }
            if let Some(index) = self.index {
                let waiter = waiters.waiter_mut(index);
                if waiter.granted {
                    let waiter = waiters.remove_unlinked_waiter(index);
                    self.index = None;
                    let permit = Permit {
                        sender: Some(self.sender),
                    };
                    drop(waiters);
                    drop(waiter);
                    drop(cloned_waker);
                    return Poll::Ready(Ok(permit));
                }
                if waiter
                    .waker
                    .as_ref()
                    .is_some_and(|w| w.will_wake(cx.waker()))
                {
                    return Poll::Pending;
                }
                if let Some(waker) = cloned_waker.take() {
                    let old = waiter.waker.replace(waker);
                    drop(waiters);
                    drop(old);
                    return Poll::Pending;
                }
            } else if semaphore.try_acquire().is_ok() {
                // A release may have raced with the fast path; recheck under the queue lock
                // before committing to wait so no permit can be stranded without a wake.
                let permit = Permit {
                    sender: Some(self.sender),
                };
                drop(waiters);
                drop(cloned_waker);
                return Poll::Ready(Ok(permit));
            } else if let Some(waker) = cloned_waker.take() {
                self.index = Some(waiters.push_back(Waiter {
                    granted: false,
                    waker: Some(waker),
                }));
                return Poll::Pending;
            }
            drop(waiters);
            // Clone outside the lock, then recheck capacity and closure before registering.
            cloned_waker = Some(cx.waker().clone());
        }
    }
}

impl<T> Drop for Reservation<'_, T> {
    fn drop(&mut self) {
        let Some(index) = self.index else { return };
        let semaphore = &self.sender.shared.tx_permits;
        let (waiter, wake) = {
            let mut waiters = semaphore.waiters.lock();
            waiters.unlink_waiter(index, |_| true);
            let waiter = waiters.remove_unlinked_waiter(index);
            let wake = if waiter.granted {
                semaphore.release_locked(&mut waiters)
            } else {
                None
            };
            (waiter, wake)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        drop(waiter);
    }
}

/// The sending endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`] function.
pub struct BoundedSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        BoundedSender {
            shared: self.shared.clone(),
        }
    }
}

impl<T> fmt::Debug for BoundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedSender").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedSender<T> {
    fn drop(&mut self) {
        if self.shared.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.rx_waker.wake();
        }
    }
}

impl<T> BoundedSender<T> {
    /// Sends a message, waiting until the channel has capacity when necessary.
    ///
    /// If the receiver has been dropped, the returned error contains `value`.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending `send` loses its place waiting for capacity and drops `value`; a call
    /// that has returned `Pending` has not sent the message. Use [`Self::try_send`] when the
    /// caller must retain ownership if capacity is unavailable, or [`Self::reserve`] to wait for
    /// capacity before constructing the message.
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        match self.reserve().await {
            Ok(permit) => permit.send(value),
            Err(_) => Err(SendError::new(value)),
        }
    }

    /// Reserves capacity for one message before constructing it.
    ///
    /// A successful reservation returns a [`Permit`]. Dropping the permit releases capacity;
    /// [`Permit::send`] publishes a value without waiting for space. Reservations do not establish
    /// message order: other producers may send while a permit is held.
    ///
    /// Returns `SendError(())` if the receiver has been dropped. A permit obtained earlier does
    /// not keep the receiver alive; sending with it can still return the unsent value on
    /// disconnect.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending reservation loses its place in the wait queue. If capacity has already
    /// been granted, it is released to the next waiter or made available to a new sender.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (tx, mut rx) = asyncband::mpsc::bounded(1);
    /// let permit = tx.reserve().await.unwrap();
    /// let message = String::from("constructed after capacity became available");
    /// permit.send(message).unwrap();
    /// assert_eq!(
    ///     rx.recv().await.unwrap(),
    ///     "constructed after capacity became available"
    /// );
    /// # }
    /// ```
    pub async fn reserve(&self) -> Result<Permit<'_, T>, SendError<()>> {
        let mut reservation = Reservation {
            sender: self,
            index: None,
        };
        poll_fn(|cx| reservation.poll(cx)).await
    }

    /// Reserves capacity for one message without waiting.
    ///
    /// Returns [`TrySendError::Full`] if queued messages and outstanding permits occupy the
    /// buffer, or [`TrySendError::Disconnected`] if the receiver has been dropped.
    pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        self.shared.tx_permits.try_acquire()?;
        Ok(Permit { sender: Some(self) })
    }

    /// Attempts to send a message without waiting for capacity.
    ///
    /// A full buffer returns [`TrySendError::Full`], while a dropped receiver returns
    /// [`TrySendError::Disconnected`]. Both errors return ownership of the unsent value.
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
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        match self.try_reserve() {
            Ok(permit) => permit
                .send(value)
                .map_err(|error| TrySendError::Disconnected(error.into_inner())),
            Err(TrySendError::Full(())) => Err(TrySendError::Full(value)),
            Err(TrySendError::Disconnected(())) => Err(TrySendError::Disconnected(value)),
        }
    }
}

/// Capacity reserved for one message on a bounded channel.
///
/// Created by [`BoundedSender::reserve`] or [`BoundedSender::try_reserve`]. Holding a permit
/// reduces available capacity but does not prevent other messages from being received. Dropping
/// it without sending releases capacity and notifies a waiting sender.
#[must_use = "dropping the permit releases its reserved capacity"]
pub struct Permit<'a, T> {
    sender: Option<&'a BoundedSender<T>>,
}

impl<T> fmt::Debug for Permit<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Permit").finish_non_exhaustive()
    }
}

impl<T> Permit<'_, T> {
    /// Publishes a message using this reservation, without waiting for capacity.
    ///
    /// If the receiver has been dropped, the returned error contains the unsent value.
    pub fn send(mut self, value: T) -> Result<(), SendError<T>> {
        let shared = &self.sender.unwrap().shared;
        if shared.tx_permits.closed.load(Ordering::Acquire) {
            return Err(SendError::new(value));
        }
        shared.sender.send(value).map_err(SendError::new)?;
        // Publication owns the capacity before a wake callback can panic.
        self.sender = None;
        shared.rx_waker.wake();
        Ok(())
    }
}

impl<T> Drop for Permit<'_, T> {
    fn drop(&mut self) {
        if let Some(sender) = self.sender {
            sender.shared.tx_permits.release();
        }
    }
}

/// The receiving endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`] function.
/// Dropping the receiver discards queued values and wakes senders waiting for capacity.
pub struct BoundedReceiver<T> {
    shared: Arc<Shared<T>>,
    receiver: storage::Receiver<T>,
}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedReceiver<T> {
    fn drop(&mut self) {
        let wakers = self.shared.tx_permits.close();
        self.shared.sender.close();
        let receiver_waker = self.shared.rx_waker.take();
        wake_all(wakers.into_iter());
        drop(receiver_waker);
    }
}

impl<T> BoundedReceiver<T> {
    /// Attempts to receive the next queued value without waiting for a new message.
    ///
    /// Receiving a value frees one buffer slot. An empty channel returns [`TryRecvError::Empty`]
    /// while at least one sender remains, or [`TryRecvError::Disconnected`] after every sender has
    /// been dropped and all queued values have been consumed.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpsc::TryRecvError;
    /// use asyncband::mpsc::bounded;
    ///
    /// let (tx, mut rx) = bounded(2);
    /// tx.try_send("first").unwrap();
    /// tx.try_send("second").unwrap();
    ///
    /// assert_eq!(rx.try_recv(), Ok("first"));
    /// assert_eq!(rx.try_recv(), Ok("second"));
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    /// drop(tx);
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    /// ```
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut disconnected = false;
        loop {
            if let Some(value) = self.receiver.recv() {
                self.shared.tx_permits.release();
                return Ok(value);
            }
            if disconnected {
                return Err(TryRecvError::Disconnected);
            }
            if self.shared.senders.load(Ordering::Acquire) != 0 {
                return Err(TryRecvError::Empty);
            }
            // Acquire the last sender's completed publications before checking again.
            disconnected = true;
        }
    }

    /// Waits for and receives the next value, freeing one buffer slot.
    ///
    /// If no value is queued, this method waits until a sender adds one or the last sender is
    /// dropped. It returns [`RecvError::Disconnected`] only after all senders are gone and the
    /// buffer has been drained.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending `recv` does not remove a message from the channel. A later receive
    /// operation can still observe the next queued value, so `recv` may safely be raced with other
    /// futures in a selection construct.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mpsc;
    /// let (tx, mut rx) = mpsc::bounded(2);
    ///
    /// tx.send("first").await.unwrap();
    /// tx.send("second").await.unwrap();
    /// drop(tx);
    ///
    /// assert_eq!(rx.recv().await, Ok("first"));
    /// assert_eq!(rx.recv().await, Ok("second"));
    /// assert_eq!(rx.recv().await, Err(mpsc::RecvError::Disconnected));
    /// # }
    /// ```
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        for registered in [false, true] {
            match self.try_recv() {
                Ok(value) => return Poll::Ready(Ok(value)),
                Err(TryRecvError::Disconnected) => {
                    drop(self.shared.rx_waker.take());
                    return Poll::Ready(Err(RecvError::Disconnected));
                }
                Err(TryRecvError::Empty) => {}
            }
            if !registered {
                self.shared.rx_waker.register(cx.waker());
            }
        }
        Poll::Pending
    }
}
