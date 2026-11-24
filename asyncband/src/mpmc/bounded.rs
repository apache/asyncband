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
use std::future::poll_fn;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::internal::waitlist::WaiterId;
use crate::mpmc::RecvError;
use crate::mpmc::SendError;
use crate::mpmc::TryRecvError;
use crate::mpmc::TrySendError;
use crate::mpmc::queue::Permit;
use crate::mpmc::queue::Shared;

/// Creates a bounded multi-producer, multi-consumer queue.
///
/// Queued values, held permits, and capacity granted to waiting senders occupy at most `capacity`
/// slots. Pending `send`, `reserve`, and `reserve_owned` operations receive capacity in wait-queue
/// order. Sending waits for a receiver to free capacity when none is available.
///
/// The `try_*` methods do not wait for capacity or messages, but may briefly block on an internal
/// mutex.
///
/// # Panics
///
/// Panics if `capacity` is zero.
#[track_caller]
pub fn bounded<T>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(capacity > 0, "mpmc bounded queue requires capacity > 0");
    let shared = Arc::new(Shared::bounded(capacity));
    (
        BoundedSender {
            shared: shared.clone(),
        },
        BoundedReceiver { shared },
    )
}

/// Sends values to the associated [`BoundedReceiver`] handles.
///
/// Instances are created by [`bounded`] and can be cloned to add producers.
pub struct BoundedSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        self.shared.clone_sender();
        Self {
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
        self.shared.drop_sender();
    }
}

impl<T> BoundedSender<T> {
    /// Sends a value, waiting for capacity when the queue is full.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] containing the unsent value if all receivers have been dropped.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending `send` future releases its waiting resources before dropping `value`,
    /// without sending it or retaining capacity. Use [`reserve`](Self::reserve) to wait for
    /// capacity before constructing a value.
    ///
    /// # Panics
    ///
    /// Panics if growing the internal queue buffer overflows the allocation size limit. On panic,
    /// `value` is dropped outside the queue lock, nothing is enqueued, and any capacity granted to
    /// this operation is released to the next waiting sender or to available capacity.
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.shared.send(value).await
    }

    /// Reserves capacity for one message before the caller supplies its value.
    ///
    /// The returned [`Permit`] holds one capacity slot and borrows this sender for its lifetime.
    /// Dropping it releases that slot; [`Permit::send`] publishes a value without waiting for
    /// space. Holding a permit does not establish message order or keep a receiver alive. Use
    /// [`reserve_owned`](Self::reserve_owned) when the reservation must not borrow the sender.
    ///
    /// # Errors
    ///
    /// Returns `SendError(())` if all receivers have been dropped. A successful reservation
    /// can still fail to send if disconnection happens before [`Permit::send`].
    ///
    /// # Cancellation
    ///
    /// Dropping a pending `reserve` future releases its place in the wait queue. If it was already
    /// granted capacity, that capacity passes to the next waiter or becomes available again.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::bounded(1);
    /// let permit = sender.reserve().await.unwrap();
    /// let message = String::from("constructed after capacity became available");
    /// permit.send(message).unwrap();
    /// assert_eq!(
    ///     receiver.recv().await.unwrap(),
    ///     "constructed after capacity became available"
    /// );
    /// # }
    /// ```
    pub async fn reserve(&self) -> Result<Permit<'_, T>, SendError<()>> {
        self.shared.reserve().await
    }

    /// Reserves capacity for one message without waiting.
    ///
    /// Dropping the returned [`Permit`] releases its capacity. See [`reserve`](Self::reserve)
    /// for reservation and message ordering.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] if capacity is unavailable, or
    /// [`TrySendError::Disconnected`] if all receivers have been dropped. Errors carry `()`
    /// because no message has been supplied.
    pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        self.shared.try_reserve()
    }

    /// Reserves capacity for one message before the caller supplies its value, consuming this
    /// sender.
    ///
    /// The returned [`OwnedPermit`] holds one capacity slot and owns this sender until the permit
    /// is sent, released, or dropped, so receivers do not report disconnection while a reservation
    /// is pending or held. Pending sends and reservations receive capacity in wait-queue order.
    /// Holding a permit does not establish message order or keep a receiver alive.
    ///
    /// # Errors
    ///
    /// Returns `SendError(())` and drops the consumed sender if all receivers have been dropped. A
    /// successful reservation can still fail to send if disconnection happens before
    /// [`OwnedPermit::send`].
    ///
    /// # Cancellation
    ///
    /// Calling `reserve_owned` moves `self` into the returned future, while slot acquisition
    /// begins on the first poll. Dropping the future before its first poll does not acquire a slot
    /// or enter the wait queue, but still drops the moved sender and notifies waiting receivers if
    /// it was the last one. Dropping a pending future after polling withdraws its place in the
    /// wait queue and drops the moved sender; if capacity was already granted to it, that capacity
    /// passes to the next waiter or becomes available again. Retrying with another sender queues
    /// at the back of the wait queue.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::bounded(1);
    /// let permit = sender.reserve_owned().await.unwrap();
    /// let message = String::from("constructed after capacity was reserved");
    /// let sender = permit.send(message).unwrap();
    /// assert_eq!(
    ///     receiver.recv().await.unwrap(),
    ///     "constructed after capacity was reserved"
    /// );
    /// drop(sender);
    /// assert!(receiver.recv().await.is_err());
    /// # }
    /// ```
    pub async fn reserve_owned(self) -> Result<OwnedPermit<T>, SendError<()>> {
        let mut reserve = ReserveOwned {
            sender: Some(self),
            waiter: None,
        };
        poll_fn(|cx| reserve.poll(cx)).await
    }

    /// Reserves capacity for one message without waiting, consuming this sender.
    ///
    /// The returned [`OwnedPermit`] holds one capacity slot and owns this sender until the permit
    /// is sent, released, or dropped. See [`reserve_owned`](Self::reserve_owned) for how an owned
    /// reservation keeps its sender alive, and [`reserve`](Self::reserve) for message ordering.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] with this sender if capacity is unavailable, or
    /// [`TrySendError::Disconnected`] with this sender if all receivers have been dropped. In both
    /// cases ownership of the sender is returned to the caller: a full error leaves the sender
    /// ready to retry, while a disconnected error returns it for inspection or cleanup without
    /// dropping it first.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    /// use asyncband::mpmc::TrySendError;
    ///
    /// let (sender, receiver) = mpmc::bounded(1);
    /// sender.try_send(1).unwrap();
    /// let sender = match sender.try_reserve_owned() {
    ///     Ok(_) => panic!("capacity should be unavailable"),
    ///     Err(TrySendError::Full(sender)) => sender,
    ///     Err(TrySendError::Disconnected(_)) => panic!("a receiver remains"),
    /// };
    /// assert_eq!(receiver.try_recv(), Ok(1));
    /// sender.try_send(2).unwrap();
    /// assert_eq!(receiver.try_recv(), Ok(2));
    /// ```
    pub fn try_reserve_owned(self) -> Result<OwnedPermit<T>, TrySendError<Self>> {
        match self.shared.acquire() {
            Ok(()) => Ok(OwnedPermit { sender: Some(self) }),
            Err(TrySendError::Full(())) => Err(TrySendError::Full(self)),
            Err(TrySendError::Disconnected(())) => Err(TrySendError::Disconnected(self)),
        }
    }

    /// Sends a value without waiting for capacity.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] if capacity is unavailable, or
    /// [`TrySendError::Disconnected`] if all receivers have been dropped. Both errors contain the
    /// unsent value; nothing is enqueued on error.
    ///
    /// # Panics
    ///
    /// Panics if growing the internal queue buffer overflows the allocation size limit. On panic,
    /// `value` is dropped outside the queue lock, nothing is enqueued, and available capacity is
    /// unchanged.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.shared.try_send(value)
    }

    /// Returns `true` if every receiver has been dropped.
    ///
    /// Once this returns `true`, every later [`send`](Self::send), [`try_send`](Self::try_send),
    /// [`reserve`](Self::reserve), [`try_reserve`](Self::try_reserve),
    /// [`reserve_owned`](Self::reserve_owned), or [`try_reserve_owned`](Self::try_reserve_owned)
    /// fails with a disconnection error, as does [`Permit::send`] or [`OwnedPermit::send`] on a
    /// permit reserved earlier. A `false` result can be outdated as soon as it is returned,
    /// because the last receiver may be dropped concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::bounded::<u8>(1);
    /// let competing = receiver.clone();
    /// drop(receiver);
    /// assert!(!sender.is_disconnected());
    /// drop(competing);
    /// assert!(sender.is_disconnected());
    /// ```
    pub fn is_disconnected(&self) -> bool {
        !self.shared.has_receivers()
    }

    /// Returns `true` if no queued value is waiting to be received.
    ///
    /// Held permits and sends still waiting for capacity are not queued values. The result is an
    /// instantaneous observation and may become stale as other tasks send or receive.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::bounded(1);
    /// assert!(sender.is_empty());
    /// sender.try_send(1).unwrap();
    /// assert!(!sender.is_empty());
    /// assert_eq!(receiver.try_recv(), Ok(1));
    /// assert!(sender.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.shared.is_empty()
    }

    /// Returns the number of values currently queued for receivers.
    ///
    /// Held [`Permit`]s and [`OwnedPermit`]s, capacity granted to waiting senders, and senders
    /// still waiting for capacity do not count, so the result never exceeds
    /// [`capacity`](Self::capacity). It is an instantaneous observation and may become stale as
    /// other tasks send or receive.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::bounded(4);
    /// let permit = sender.try_reserve().unwrap();
    /// sender.try_send(1).unwrap();
    /// sender.try_send(2).unwrap();
    /// assert_eq!(sender.len(), 2);
    /// permit.send(3).unwrap();
    /// assert_eq!(sender.len(), 3);
    /// assert_eq!(receiver.try_recv(), Ok(1));
    /// assert_eq!(receiver.len(), 2);
    /// ```
    pub fn len(&self) -> usize {
        self.shared.len()
    }

    /// Returns the fixed number of slots the queue allows across queued values, held permits, and
    /// capacity granted to waiting senders.
    ///
    /// This is the value passed to [`bounded`] and never changes.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::bounded::<u8>(3);
    /// assert_eq!(sender.capacity(), 3);
    /// assert_eq!(receiver.capacity(), 3);
    /// ```
    pub fn capacity(&self) -> usize {
        self.shared
            .capacity()
            .expect("bounded queue has a fixed capacity")
    }

    /// Returns the number of live senders.
    ///
    /// Each pending [`reserve_owned`](Self::reserve_owned) future and each held [`OwnedPermit`]
    /// keeps one sender alive. The result is an instantaneous observation and may become stale as
    /// other tasks clone or drop senders.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, _receiver) = mpmc::bounded::<u8>(1);
    /// assert_eq!(sender.sender_count(), 1);
    /// let permit = sender.clone().try_reserve_owned().unwrap();
    /// assert_eq!(sender.sender_count(), 2);
    /// drop(permit);
    /// assert_eq!(sender.sender_count(), 1);
    /// ```
    pub fn sender_count(&self) -> usize {
        self.shared.sender_count()
    }

    /// Returns the number of live receivers.
    ///
    /// The result is an instantaneous observation and may become stale as other tasks clone or
    /// drop receivers.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::bounded::<u8>(1);
    /// assert_eq!(sender.receiver_count(), 1);
    /// let competing = receiver.clone();
    /// assert_eq!(sender.receiver_count(), 2);
    /// drop(receiver);
    /// assert_eq!(sender.receiver_count(), 1);
    /// drop(competing);
    /// assert_eq!(sender.receiver_count(), 0);
    /// ```
    pub fn receiver_count(&self) -> usize {
        self.shared.receiver_count()
    }

    /// Returns `true` if `self` and `other` were created from the same call to [`bounded`].
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, _receiver) = mpmc::bounded::<u8>(1);
    /// let clone = sender.clone();
    /// let (other_sender, _other_receiver) = mpmc::bounded(1);
    /// assert!(sender.same_channel(&clone));
    /// assert!(!sender.same_channel(&other_sender));
    /// ```
    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}

/// Capacity reserved for one value on a bounded MPMC queue, together with the sender that
/// reserved it.
///
/// Like [`Permit`], the permit holds one capacity slot without claiming message order, and
/// [`send`](OwnedPermit::send) publishes a value without waiting for space. It owns its
/// [`BoundedSender`] instead of borrowing one, so it can be moved into another task or stored
/// while keeping its sender alive. [`send`](OwnedPermit::send) and
/// [`release`](OwnedPermit::release) hand the sender back to the caller; dropping the permit
/// releases its slot and then drops the sender.
#[must_use = "dropping the permit releases its reserved capacity"]
pub struct OwnedPermit<T> {
    // A consuming method takes the sender together with its slot, leaving nothing for a later
    // drop.
    sender: Option<BoundedSender<T>>,
}

impl<T> fmt::Debug for OwnedPermit<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedPermit").finish_non_exhaustive()
    }
}

impl<T> OwnedPermit<T> {
    /// Publishes a value using this reserved capacity without waiting for space, returning the
    /// sender that reserved it.
    ///
    /// This consumes the permit, including when sending fails.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] containing the unsent value if all receivers have been dropped. The
    /// reserved capacity is then released and the owned sender is dropped.
    ///
    /// # Panics
    ///
    /// Panics if growing the internal queue buffer overflows the allocation size limit. On panic,
    /// `value` is dropped outside the queue lock, the reserved capacity is released to the next
    /// waiting sender or to available capacity, and the owned sender is dropped.
    pub fn send(mut self, value: T) -> Result<BoundedSender<T>, SendError<T>> {
        // Keep `self.sender` armed until enqueue succeeds so unwinding releases the slot and drops
        // the owned sender via `OwnedPermit::drop`.
        let sent = {
            let sender = self
                .sender
                .as_ref()
                .expect("a permit keeps its sender until it is consumed");
            sender.shared.send_reserved(value)
        };
        match sent {
            // The queued value now owns the reserved slot.
            Ok(waker) => {
                let sender = self.take_sender();
                if let Some(waker) = waker {
                    waker.wake();
                }
                Ok(sender)
            }
            // Returning leaves `self` armed so `OwnedPermit::drop` releases the slot and drops the
            // owned sender once.
            Err(value) => Err(SendError::new(value)),
        }
    }

    /// Releases the reserved capacity without sending a value, returning the sender that reserved
    /// it.
    ///
    /// The released capacity passes to the next waiting sender or becomes available again.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    /// use asyncband::mpmc::TrySendError;
    ///
    /// let (sender, receiver) = mpmc::bounded(1);
    /// let permit = sender.clone().try_reserve_owned().unwrap();
    /// assert_eq!(sender.try_send(1), Err(TrySendError::Full(1)));
    /// let sender = permit.release();
    /// sender.try_send(2).unwrap();
    /// assert_eq!(receiver.try_recv(), Ok(2));
    /// ```
    pub fn release(mut self) -> BoundedSender<T> {
        let sender = self.take_sender();
        Self::release_capacity(&sender);
        sender
    }

    /// Takes the sender this permit keeps alive. The caller settles the reserved slot itself.
    fn take_sender(&mut self) -> BoundedSender<T> {
        self.sender
            .take()
            .expect("a permit keeps its sender until it is consumed")
    }

    /// Returns the reserved slot to the next waiting sender or to available capacity.
    fn release_capacity(sender: &BoundedSender<T>) {
        if let Some(waker) = sender.shared.release_reserved() {
            waker.wake();
        }
    }
}

impl<T> Drop for OwnedPermit<T> {
    fn drop(&mut self) {
        // The sender drops at the end of this block, after its slot has been released.
        if let Some(sender) = self.sender.take() {
            Self::release_capacity(&sender);
        }
    }
}

struct ReserveOwned<T> {
    // The sender moves into the reserved permit, leaving `None` behind.
    sender: Option<BoundedSender<T>>,
    waiter: Option<WaiterId>,
}

impl<T> ReserveOwned<T> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<OwnedPermit<T>, SendError<()>>> {
        // Polling after `Ready` violates the `Future` contract, and only then is the sender gone.
        let shared = &self
            .sender
            .as_ref()
            .expect("reserve_owned polled after completion")
            .shared;
        shared.poll_reserve(&mut self.waiter, cx).map(|reserved| {
            reserved.map(|()| OwnedPermit {
                sender: self.sender.take(),
            })
        })
    }
}

impl<T> Drop for ReserveOwned<T> {
    fn drop(&mut self) {
        if let (Some(sender), Some(id)) = (&self.sender, self.waiter.take()) {
            sender.shared.cancel_reserve(id);
        }
    }
}

/// Receives values from the associated [`BoundedSender`] handles.
///
/// Cloned receivers compete for values, and every accepted value is returned by exactly one
/// receiver while a receiver remains. Dropping the final receiver releases buffered values.
pub struct BoundedReceiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for BoundedReceiver<T> {
    fn clone(&self) -> Self {
        self.shared.clone_receiver();
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedReceiver<T> {
    fn drop(&mut self) {
        self.shared.drop_receiver();
    }
}

impl<T> BoundedReceiver<T> {
    /// Waits for and receives the next queued value, freeing one capacity slot.
    ///
    /// Queued values remain receivable after disconnection.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Disconnected`] once all senders have been dropped and the queue is
    /// empty.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending future does not remove a value from the queue or prevent another receiver
    /// from receiving it.
    pub async fn recv(&self) -> Result<T, RecvError> {
        self.shared.recv().await
    }

    /// Receives the next queued value without waiting for a message, freeing one capacity slot.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] when the queue is empty but a sender remains.
    /// Returns [`TryRecvError::Disconnected`] once all senders have been dropped and the queue is
    /// empty.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.shared.try_recv()
    }

    /// Returns `true` if every sender has been dropped.
    ///
    /// A pending [`reserve_owned`](BoundedSender::reserve_owned) future or a held [`OwnedPermit`]
    /// keeps its sender alive and delays disconnection like any other sender.
    ///
    /// Buffered values stay receivable after disconnection, so [`recv`](Self::recv) and
    /// [`try_recv`](Self::try_recv) report disconnection only once the queue has been drained. A
    /// `false` result can be outdated as soon as it is returned, because the last sender may be
    /// dropped concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::bounded(1);
    /// sender.try_send(1).unwrap();
    /// assert!(!receiver.is_disconnected());
    /// drop(sender);
    /// assert!(receiver.is_disconnected());
    /// assert_eq!(receiver.try_recv(), Ok(1));
    /// ```
    pub fn is_disconnected(&self) -> bool {
        !self.shared.has_senders()
    }

    /// Returns `true` if no queued value is waiting to be received.
    ///
    /// Held permits and sends still waiting for capacity are not queued values. The result is an
    /// instantaneous observation and may become stale as other tasks send or receive.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::bounded(1);
    /// assert!(receiver.is_empty());
    /// sender.try_send(1).unwrap();
    /// assert!(!receiver.is_empty());
    /// assert_eq!(receiver.try_recv(), Ok(1));
    /// assert!(receiver.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.shared.is_empty()
    }

    /// Returns the number of values currently queued for receivers.
    ///
    /// Held [`Permit`]s and [`OwnedPermit`]s, capacity granted to waiting senders, and senders
    /// still waiting for capacity do not count, so the result never exceeds
    /// [`capacity`](Self::capacity). It is an instantaneous observation and may become stale as
    /// other tasks send or receive.
    pub fn len(&self) -> usize {
        self.shared.len()
    }

    /// Returns the fixed number of slots the queue allows across queued values, held permits, and
    /// capacity granted to waiting senders.
    ///
    /// This is the value passed to [`bounded`] and never changes.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::bounded::<u8>(3);
    /// sender.try_send(1).unwrap();
    /// assert_eq!(receiver.capacity(), 3);
    /// ```
    pub fn capacity(&self) -> usize {
        self.shared
            .capacity()
            .expect("bounded queue has a fixed capacity")
    }

    /// Returns the number of live senders.
    ///
    /// Each pending [`reserve_owned`](BoundedSender::reserve_owned) future and each held
    /// [`OwnedPermit`] keeps one sender alive. The result is an instantaneous observation and may
    /// become stale as other tasks clone or drop senders.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::bounded::<u8>(1);
    /// assert_eq!(receiver.sender_count(), 1);
    /// let permit = sender.try_reserve_owned().unwrap();
    /// assert_eq!(receiver.sender_count(), 1);
    /// assert!(!receiver.is_disconnected());
    /// drop(permit);
    /// assert_eq!(receiver.sender_count(), 0);
    /// assert!(receiver.is_disconnected());
    /// ```
    pub fn sender_count(&self) -> usize {
        self.shared.sender_count()
    }

    /// Returns the number of live receivers, including this one.
    ///
    /// The result is an instantaneous observation and may become stale as other tasks clone or
    /// drop receivers.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (_sender, receiver) = mpmc::bounded::<u8>(1);
    /// assert_eq!(receiver.receiver_count(), 1);
    /// let competing = receiver.clone();
    /// assert_eq!(receiver.receiver_count(), 2);
    /// drop(competing);
    /// assert_eq!(receiver.receiver_count(), 1);
    /// ```
    pub fn receiver_count(&self) -> usize {
        self.shared.receiver_count()
    }

    /// Returns `true` if `self` and `other` were created from the same call to [`bounded`].
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (_sender, receiver) = mpmc::bounded::<u8>(1);
    /// let clone = receiver.clone();
    /// let (_other_sender, other_receiver) = mpmc::bounded(1);
    /// assert!(receiver.same_channel(&clone));
    /// assert!(!receiver.same_channel(&other_receiver));
    /// ```
    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::future::Future;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::pin::pin;
    use std::rc::Rc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Wake;
    use std::task::Waker;

    use super::*;

    #[derive(Default)]
    struct WakeFlag(AtomicBool);

    impl Wake for WakeFlag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    struct ReenterSenderOnDrop {
        probe: BoundedSender<Option<ReenterSenderOnDrop>>,
        observed_len: Rc<Cell<Option<usize>>>,
    }

    impl Drop for ReenterSenderOnDrop {
        fn drop(&mut self) {
            // Reentering `len()` acquires the queue lock, so this would deadlock if `T::drop`
            // ran while the send path still held it.
            self.observed_len.set(Some(self.probe.len()));
        }
    }

    struct ReenterReceiverOnDrop {
        probe: BoundedReceiver<Option<ReenterReceiverOnDrop>>,
        observed_len: Rc<Cell<Option<usize>>>,
    }

    impl Drop for ReenterReceiverOnDrop {
        fn drop(&mut self) {
            self.observed_len.set(Some(self.probe.len()));
        }
    }

    #[test]
    fn owned_permit_send_overflow_drops_value_unlocked_and_restores_slot() {
        let (sender, receiver) = bounded::<Option<ReenterSenderOnDrop>>(1);
        let permit = sender.clone().try_reserve_owned().unwrap();
        let observed = Rc::new(Cell::new(None));
        let payload = Some(ReenterSenderOnDrop {
            probe: sender.clone(),
            observed_len: observed.clone(),
        });
        assert_eq!(sender.sender_count(), 3);

        sender.shared.fail_next_reserve();
        let overflow = catch_unwind(AssertUnwindSafe(|| permit.send(payload)));
        assert!(overflow.is_err());
        assert_eq!(observed.get(), Some(0));
        assert_eq!(sender.sender_count(), 1);

        // The reserved slot was released on unwind and can be claimed again.
        let recovered = sender.try_reserve().unwrap();
        recovered.send(None).unwrap();
        assert!(receiver.try_recv().unwrap().is_none());
    }

    #[test]
    fn owned_permit_send_overflow_wakes_next_waiter_and_drops_sender() {
        let (sender, receiver) = bounded::<Option<ReenterSenderOnDrop>>(1);
        let permit = sender.clone().try_reserve_owned().unwrap();
        let woke = Arc::new(WakeFlag::default());
        let waker = Waker::from(woke.clone());
        let mut cx = Context::from_waker(&waker);
        let mut waiting = pin!(sender.send(None));
        assert!(waiting.as_mut().poll(&mut cx).is_pending());

        let observed = Rc::new(Cell::new(None));
        let payload = Some(ReenterSenderOnDrop {
            probe: sender.clone(),
            observed_len: observed.clone(),
        });
        sender.shared.fail_next_reserve();
        let overflow = catch_unwind(AssertUnwindSafe(|| permit.send(payload)));
        assert!(overflow.is_err());
        assert_eq!(observed.get(), Some(0));
        assert_eq!(sender.sender_count(), 1);
        assert!(woke.0.load(Ordering::Relaxed));
        assert!(matches!(sender.try_reserve(), Err(TrySendError::Full(()))));
        assert!(waiting.as_mut().poll(&mut cx).is_ready());
        assert!(receiver.try_recv().unwrap().is_none());
    }

    #[test]
    fn owned_permit_send_overflow_as_last_sender_notifies_waiting_receiver() {
        let (sender, receiver) = bounded::<Option<ReenterReceiverOnDrop>>(1);
        let permit = sender.try_reserve_owned().unwrap();
        let woke = Arc::new(WakeFlag::default());
        let waker = Waker::from(woke.clone());
        let mut cx = Context::from_waker(&waker);
        let mut recv = pin!(receiver.recv());
        assert!(recv.as_mut().poll(&mut cx).is_pending());

        let observed = Rc::new(Cell::new(None));
        let payload = Some(ReenterReceiverOnDrop {
            probe: receiver.clone(),
            observed_len: observed.clone(),
        });
        receiver.shared.fail_next_reserve();
        let overflow = catch_unwind(AssertUnwindSafe(|| permit.send(payload)));
        assert!(overflow.is_err());
        assert_eq!(observed.get(), Some(0));
        assert_eq!(receiver.sender_count(), 0);
        assert!(receiver.is_disconnected());
        assert!(woke.0.load(Ordering::Relaxed));
        assert!(matches!(
            recv.as_mut().poll(&mut cx),
            Poll::Ready(Err(RecvError::Disconnected))
        ));
    }

    #[test]
    fn permit_try_send_and_pending_send_overflow_drop_unlocked_without_leaking_slots() {
        // `try_send`: unwind releases lock before `T::drop` and leaves capacity intact.
        let (sender, receiver) = bounded::<Option<ReenterSenderOnDrop>>(1);
        let observed = Rc::new(Cell::new(None));
        let payload = Some(ReenterSenderOnDrop {
            probe: sender.clone(),
            observed_len: observed.clone(),
        });
        sender.shared.fail_next_reserve();
        let overflow = catch_unwind(AssertUnwindSafe(|| sender.try_send(payload)));
        assert!(overflow.is_err());
        assert_eq!(observed.get(), Some(0));
        let permit = sender.try_reserve().unwrap();

        // `Permit::send` and granted `BoundedSender::send`: each unwinds unlocked and hands its
        // slot to the next waiting sender.
        let mut noop_cx = Context::from_waker(Waker::noop());
        let pending_observed = Rc::new(Cell::new(None));
        let mut pending_send = pin!(sender.send(Some(ReenterSenderOnDrop {
            probe: sender.clone(),
            observed_len: pending_observed.clone(),
        })));
        assert!(pending_send.as_mut().poll(&mut noop_cx).is_pending());

        let woke = Arc::new(WakeFlag::default());
        let waker = Waker::from(woke.clone());
        let mut cx = Context::from_waker(&waker);
        let mut trailing = pin!(sender.reserve());
        assert!(trailing.as_mut().poll(&mut cx).is_pending());

        let permit_observed = Rc::new(Cell::new(None));
        let permit_payload = Some(ReenterSenderOnDrop {
            probe: sender.clone(),
            observed_len: permit_observed.clone(),
        });
        sender.shared.fail_next_reserve();
        let overflow = catch_unwind(AssertUnwindSafe(|| permit.send(permit_payload)));
        assert!(overflow.is_err());
        assert_eq!(permit_observed.get(), Some(0));
        assert!(!woke.0.load(Ordering::Relaxed));

        sender.shared.fail_next_reserve();
        let overflow = catch_unwind(AssertUnwindSafe(|| {
            pending_send.as_mut().poll(&mut noop_cx)
        }));
        assert!(overflow.is_err());
        assert_eq!(pending_observed.get(), Some(0));
        assert!(woke.0.load(Ordering::Relaxed));

        let Poll::Ready(Ok(granted)) = trailing.as_mut().poll(&mut cx) else {
            panic!("trailing reservation should have received the freed slot");
        };
        granted.send(None).unwrap();
        assert!(receiver.try_recv().unwrap().is_none());
    }
}
