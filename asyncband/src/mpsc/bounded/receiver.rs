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
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::mpsc::RecvError;
use crate::mpsc::TryRecvError;
use crate::mpsc::bounded::queue::Shared;

/// The receiving endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`](crate::mpsc::bounded) function. Dropping the receiver
/// discards queued values and causes pending `send` and `reserve` operations to fail.
pub struct BoundedReceiver<T> {
    shared: Arc<Shared<T>>,
    head: u64,
}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedReceiver<T> {
    fn drop(&mut self) {
        self.shared.drop_receiver(self.head);
    }
}

impl<T> BoundedReceiver<T> {
    pub(in crate::mpsc::bounded) fn new(shared: Arc<Shared<T>>) -> Self {
        Self { shared, head: 0 }
    }

    /// Receives the next queued value without waiting for a message, freeing one capacity slot.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] when the queue is empty but a sender remains.
    /// Returns [`TryRecvError::Disconnected`] once all senders have been dropped and the queue is
    /// empty.
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
    #[inline]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let head = self.head;
        let base = self.shared.claim_read_slot(head)?;
        self.head = head + 1;
        // SAFETY: `claim_read_slot` confirmed readiness of `base` at `head`.
        Ok(unsafe { self.shared.read_claimed_slot(base) })
    }

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
    /// Dropping a pending future does not remove a value from the queue. A later call on this
    /// receiver can still receive the next value.
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
    #[inline]
    pub fn recv(&mut self) -> impl Future<Output = Result<T, RecvError>> {
        Recv {
            receiver: Some(self),
            _pinned: PhantomPinned,
        }
    }

    /// Returns `true` if every sender has been dropped.
    ///
    /// Queued messages stay receivable after disconnection, so [`recv`](Self::recv) and
    /// [`try_recv`](Self::try_recv) report disconnection only once the buffer has been drained. A
    /// `false` result can be outdated as soon as it is returned, because the last sender may be
    /// dropped concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpsc::bounded;
    ///
    /// let (tx, mut rx) = bounded(1);
    /// tx.try_send(1).unwrap();
    /// assert!(!rx.is_disconnected());
    /// drop(tx);
    /// assert!(rx.is_disconnected());
    /// assert_eq!(rx.try_recv(), Ok(1));
    /// ```
    #[inline]
    pub fn is_disconnected(&self) -> bool {
        !self.shared.has_senders()
    }

    /// Returns `true` if no queued value is waiting to be received.
    ///
    /// Held permits and sends still waiting for capacity are not queued values. A `true` result
    /// may become stale as senders add values. A `false` result guarantees that the next receive
    /// on this receiver finds a value, because receiving requires `&mut self`.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpsc::bounded;
    ///
    /// let (tx, mut rx) = bounded(1);
    /// assert!(rx.is_empty());
    /// tx.try_send(1).unwrap();
    /// assert!(!rx.is_empty());
    /// assert_eq!(rx.try_recv(), Ok(1));
    /// assert!(rx.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.shared.is_empty_at(self.head)
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct Recv<'a, T> {
    receiver: Option<&'a mut BoundedReceiver<T>>,
    _pinned: PhantomPinned,
}

impl<T> Future for Recv<'_, T> {
    type Output = Result<T, RecvError>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `Recv` never stores self-referential pointers; `PhantomPinned` only keeps
        // `!Unpin` parity on the opaque `recv` future.
        let this = unsafe { self.get_unchecked_mut() };
        let Some(receiver) = this.receiver.as_deref_mut() else {
            panic!("bounded mpsc recv polled after completion");
        };
        loop {
            let head = receiver.head;
            match receiver.shared.claim_read_slot(head) {
                Ok(base) => {
                    receiver.head = head + 1;
                    // SAFETY: `claim_read_slot` confirmed readiness of `base` at `head`.
                    let out = unsafe { receiver.shared.read_claimed_slot_poll(base) };
                    this.receiver = None;
                    return out;
                }
                Err(TryRecvError::Disconnected) => {
                    receiver.shared.clear_recv_waker_if_waiting();
                    this.receiver = None;
                    return Poll::Ready(Err(RecvError::Disconnected));
                }
                Err(TryRecvError::Empty) => {
                    if !receiver.shared.register_recv_wait(head, cx) {
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}
