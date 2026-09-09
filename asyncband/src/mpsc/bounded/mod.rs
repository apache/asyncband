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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use self::buffer::Buffer;
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

mod buffer;

/// Creates a bounded mpsc channel with room for `buffer` queued messages.
///
/// [`BoundedSender::send`] waits for capacity when the buffer is full. Receiving a message releases
/// one slot for a waiting sender. Capacity is granted in the order that pending sends and
/// reservations enter the wait queue; new senders cannot take an already granted slot.
///
/// Storage for nonzero-sized messages is preallocated and rounded up to a power of two; the
/// channel's capacity remains exactly `buffer`. Zero-sized messages need no per-slot storage.
///
/// # Panics
///
/// Panics if `buffer` is zero or exceeds the maximum capacity of `usize::MAX >> 1`.
#[track_caller]
pub fn bounded<T>(buffer: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(buffer > 0, "mpsc bounded channel requires buffer > 0");
    assert!(
        buffer <= MAX_CAPACITY,
        "mpsc bounded channel capacity {buffer} exceeds the maximum of {MAX_CAPACITY}"
    );
    let shared = Arc::new(Shared {
        senders: AtomicUsize::new(1),
        tx_permits: CachePadded::new(Semaphore::new(buffer)),
        rx_waker: CachePadded::new(AtomicWaker::new()),
        buffer: Buffer::new(buffer),
    });
    let sender = BoundedSender {
        shared: shared.clone(),
    };
    let receiver = BoundedReceiver { shared, head: 0 };
    (sender, receiver)
}

struct Shared<T> {
    senders: AtomicUsize,
    tx_permits: CachePadded<Semaphore>,
    rx_waker: CachePadded<AtomicWaker>,
    buffer: Buffer<T>,
}

/// The largest capacity accepted by [`bounded`].
///
/// The shared permit counter reserves two sentinel values above the usable range, and the
/// zero-sized queue length packs a closed flag into its top bit. This bound also keeps the
/// rounded-up slot storage from overflowing a power of two.
const MAX_CAPACITY: usize = usize::MAX >> 1;

// This channel-local semaphore keeps its permit counter and channel state in one atomic.
// The general-purpose semaphore has neither a close operation nor acquisition errors.
//
// `state` is the available permit count, plus two sentinel values at the top of the range:
//
// * `CLOSED`: the receiver is gone. No permits are issued or returned, and waiters drain with an
//   error.
// * `WAITING`: the wait queue may be non-empty. Releases then take the locked path and grant the
//   permit directly to the oldest waiter instead of returning it to the counter, so capacity is
//   handed out in registration order and new arrivals cannot steal an already granted slot. The
//   counter is zero while this sentinel stands: waiters only register after observing exhaustion,
//   and grants bypass the counter.
//
// With neither sentinel installed, acquire and release are single lock-free operations on `state`.
// Wait-queue mutations always hold the queue lock; a registration installs `WAITING` before its
// final capacity recheck, which switches any racing release to the locked path and strands no
// permit without a wake.
struct Semaphore {
    state: AtomicUsize,
    waiters: Mutex<WaitList<Waiter>>,
}

const CLOSED: usize = usize::MAX;
const WAITING: usize = usize::MAX - 1;

impl Semaphore {
    fn new(available: usize) -> Self {
        Self {
            state: AtomicUsize::new(available),
            waiters: Mutex::new(WaitList::new()),
        }
    }

    fn try_acquire(&self) -> Result<(), TrySendError<()>> {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state == CLOSED {
                return Err(TrySendError::Disconnected(()));
            }
            if state == WAITING || state == 0 {
                return Err(TrySendError::Full(()));
            }
            match self.state.compare_exchange_weak(
                state,
                state - 1,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => state = actual,
            }
        }
    }

    fn is_closed(&self) -> bool {
        self.state.load(Ordering::Acquire) == CLOSED
    }

    // Installs WAITING over an exhausted counter. A permit that arrived first wins the compare
    // exchange, and the caller's recheck under the queue lock picks it up instead.
    fn set_waiting(&self) {
        let _ = self
            .state
            .compare_exchange(0, WAITING, Ordering::AcqRel, Ordering::Acquire);
    }

    // Removes WAITING, keeping whatever count a racing grant restoration left behind.
    fn clear_waiting(&self) {
        let _ = self
            .state
            .compare_exchange(WAITING, 0, Ordering::Release, Ordering::Relaxed);
    }

    fn release(&self) {
        // Fast path: with no waiting sender and no close in sight, the permit goes straight
        // back to the counter.
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            if state == WAITING || state == CLOSED {
                break;
            }
            match self.state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => state = actual,
            }
        }
        let wake = self.release_locked(&mut self.waiters.lock());
        if let Some(waker) = wake {
            waker.wake();
        }
    }

    fn release_locked(&self, waiters: &mut WaitList<Waiter>) -> Option<Waker> {
        if self.is_closed() {
            return None;
        }
        if let Some((_, waiter)) = waiters.unlink_first_waiter(|_| true) {
            // Grant ownership before waking; new arrivals cannot steal this capacity.
            waiter.granted = true;
            let waker = waiter.waker.take();
            if waiters.is_empty() {
                self.clear_waiting();
            }
            return waker;
        }
        // The queue is empty: return the permit to the counter. An outstanding grant already
        // owns its capacity. Adding to a plain count is safe because only lock-holding
        // operations install a sentinel, and this operation holds the lock; WAITING itself
        // must be displaced rather than incremented, because WAITING + 1 is CLOSED.
        if self.state.load(Ordering::Relaxed) == WAITING {
            let _displaced =
                self.state
                    .compare_exchange(WAITING, 1, Ordering::Release, Ordering::Relaxed);
            debug_assert_eq!(_displaced, Ok(WAITING));
        } else {
            self.state.fetch_add(1, Ordering::Release);
        }
        None
    }

    fn close(&self) -> WakerBatch {
        let mut waiters = self.waiters.lock();
        self.state.store(CLOSED, Ordering::Release);
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
        let result = loop {
            if self.index.is_none() {
                match semaphore.try_acquire() {
                    Ok(()) => {
                        break Ok(Permit {
                            sender: Some(self.sender),
                        });
                    }
                    Err(TrySendError::Disconnected(())) => break Err(SendError::new(())),
                    Err(TrySendError::Full(())) => {}
                }
            }
            let mut waiters = semaphore.waiters.lock();
            if semaphore.is_closed() {
                // Drop removes any remaining registration, including an unused grant.
                break Err(SendError::new(()));
            }
            if let Some(index) = self.index {
                let waiter = waiters.waiter_mut(index);
                if waiter.granted {
                    let waiter = waiters.remove_unlinked_waiter(index);
                    self.index = None;
                    drop(waiters);
                    drop(waiter);
                    break Ok(Permit {
                        sender: Some(self.sender),
                    });
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
            } else {
                // Install WAITING before the final capacity recheck: if a permit arrived
                // first, the installation loses the compare exchange and the recheck picks
                // the permit up; otherwise a racing release switches to the locked path, so
                // no permit can be stranded without a wake. Waiting senders already in the
                // queue take priority over this recheck.
                semaphore.set_waiting();
                if waiters.is_empty() && semaphore.try_acquire().is_ok() {
                    semaphore.clear_waiting();
                    break Ok(Permit {
                        sender: Some(self.sender),
                    });
                }
                if let Some(waker) = cloned_waker.take() {
                    self.index = Some(waiters.push_back(Waiter {
                        granted: false,
                        waker: Some(waker),
                    }));
                    return Poll::Pending;
                }
            }
            drop(waiters);
            // Clone outside the lock, then recheck capacity and closure before registering.
            cloned_waker = Some(cx.waker().clone());
        };
        // The permit owns capacity before an unused cloned waker can panic.
        drop(cloned_waker);
        Poll::Ready(result)
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
                if waiters.is_empty() {
                    semaphore.clear_waiting();
                }
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
        // SAFETY: This permit owns one capacity unit. Claiming a slot and writing it is a
        // synchronous operation with no user callbacks or await points between the two.
        unsafe { shared.buffer.push(value) }.map_err(SendError::new)?;
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
/// Dropping the receiver discards queued values. The backing allocation remains alive until
/// all endpoints are dropped, so a concurrent sender can safely finish returning an unsent value.
pub struct BoundedReceiver<T> {
    shared: Arc<Shared<T>>,
    head: usize,
}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedReceiver<T> {
    fn drop(&mut self) {
        // SAFETY: Receiver ownership provides exclusive access to the consumption cursor.
        // The drain first prevents new claims. Its destructor completes cleanup on unwinding.
        let drain = unsafe { self.shared.buffer.close(self.head) };
        let wakers = self.shared.tx_permits.close();
        let receiver_waker = self.shared.rx_waker.take();
        wake_all(wakers.into_iter());
        drop(receiver_waker);
        drop(drain);
    }
}

impl<T> BoundedReceiver<T> {
    /// Attempts to receive the next queued value without waiting for a new message.
    ///
    /// Receiving a value frees one buffer slot. An empty channel returns [`TryRecvError::Empty`]
    /// while at least one sender remains, or [`TryRecvError::Disconnected`] after every sender has
    /// been dropped and all queued values have been consumed.
    ///
    /// If a producer is still completing a synchronous publication at the queue head, this
    /// method waits for that publication. Use [`Self::recv`] to wait asynchronously instead.
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
        let mut spins = 0;
        loop {
            match self.try_pop() {
                Poll::Ready(result) => return result,
                Poll::Pending => {
                    // A synchronous publisher already owns the head. Reporting Empty here
                    // could hide a later send that has completed. Async recv parks instead.
                    if spins < 32 {
                        std::hint::spin_loop();
                        spins += 1;
                    } else {
                        std::thread::yield_now();
                    }
                }
            }
        }
    }

    fn try_pop(&mut self) -> Poll<Result<T, TryRecvError>> {
        let mut disconnected = false;
        loop {
            // SAFETY: Only this receiver owns head. Capacity is released after the buffer
            // finishes reading and advances the cursor, so no producer can overwrite the value.
            match unsafe { self.shared.buffer.pop(&mut self.head) } {
                Poll::Ready(Some(value)) => {
                    self.shared.tx_permits.release();
                    return Poll::Ready(Ok(value));
                }
                Poll::Ready(None) if disconnected => {
                    return Poll::Ready(Err(TryRecvError::Disconnected));
                }
                Poll::Ready(None) if self.shared.senders.load(Ordering::Acquire) == 0 => {
                    // Acquire the last sender's completed publications before checking again.
                    disconnected = true;
                }
                Poll::Ready(None) => return Poll::Ready(Err(TryRecvError::Empty)),
                Poll::Pending => return Poll::Pending,
            }
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
            match self.try_pop() {
                Poll::Ready(Ok(value)) => return Poll::Ready(Ok(value)),
                Poll::Ready(Err(TryRecvError::Disconnected)) => {
                    drop(self.shared.rx_waker.take());
                    return Poll::Ready(Err(RecvError::Disconnected));
                }
                Poll::Pending | Poll::Ready(Err(TryRecvError::Empty)) => {}
            }
            if !registered {
                self.shared.rx_waker.register(cx.waker());
            }
        }
        Poll::Pending
    }
}
