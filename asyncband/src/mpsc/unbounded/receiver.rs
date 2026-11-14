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

use std::collections::VecDeque;
use std::fmt;
use std::future::poll_fn;
use std::mem;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::internal::mutex::Mutex;
use crate::internal::register_waker;
use crate::mpsc::RecvError;
use crate::mpsc::TryRecvError;
use crate::mpsc::unbounded::State;
use crate::mpsc::unbounded::buffer::Buffer;
use crate::mpsc::unbounded::buffer::pop_batch;

/// The receiving endpoint of an unbounded mpsc channel.
///
/// Instances are created by the [`unbounded`](crate::mpsc::unbounded) function. Dropping the
/// receiver discards queued values and makes subsequent attempts to send fail.
pub struct UnboundedReceiver<T> {
    shared: Arc<Mutex<State<T>>>,
    // Receiving borrows it through `get_mut`; `is_empty` locks it from `&self`. The mutex also
    // keeps the receiver Sync for Send-only payloads.
    batch: Mutex<VecDeque<T>>,
}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for UnboundedReceiver<T> {
    fn drop(&mut self) {
        let batch = mem::take(self.batch.get_mut());
        let (buffer, waker) = {
            let mut state = self.shared.lock();
            state.receiver = false;
            (
                mem::replace(&mut state.buffer, Buffer::new()),
                state.recv_waker.take(),
            )
        };
        // Destructors may send again. A waker may also own a sender and form an ownership cycle.
        drop((batch, buffer, waker));
    }
}

impl<T> UnboundedReceiver<T> {
    pub(super) fn new(shared: Arc<Mutex<State<T>>>) -> Self {
        Self {
            shared,
            batch: Mutex::new(VecDeque::new()),
        }
    }

    /// Receives the next queued value without waiting for a message.
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
    /// use asyncband::mpsc::unbounded;
    ///
    /// let (tx, mut rx) = unbounded();
    /// tx.send("first").unwrap();
    /// tx.send("second").unwrap();
    ///
    /// assert_eq!(rx.try_recv(), Ok("first"));
    /// assert_eq!(rx.try_recv(), Ok("second"));
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    /// drop(tx);
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    /// ```
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let batch = self.batch.get_mut();
        if batch.is_empty() {
            let mut state = self.shared.lock();
            let retired = state.buffer.refill(batch);
            let disconnected = state.senders == 0;
            drop(state);
            drop(retired);
            if batch.is_empty() {
                return Err(if disconnected {
                    TryRecvError::Disconnected
                } else {
                    TryRecvError::Empty
                });
            }
        }
        Ok(pop_batch(batch))
    }

    /// Waits for and receives the next queued value.
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
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use asyncband::mpsc;
    /// let (tx, mut rx) = mpsc::unbounded();
    ///
    /// tx.send("first").unwrap();
    /// tx.send("second").unwrap();
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

    /// Returns `true` if every sender has been dropped.
    ///
    /// Queued messages stay receivable after disconnection, so [`recv`](Self::recv) and
    /// [`try_recv`](Self::try_recv) report disconnection only once the queue has been drained. A
    /// `false` result can be outdated as soon as it is returned, because the last sender may be
    /// dropped concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpsc::unbounded;
    ///
    /// let (tx, mut rx) = unbounded();
    /// tx.send(1).unwrap();
    /// assert!(!rx.is_disconnected());
    /// drop(tx);
    /// assert!(rx.is_disconnected());
    /// assert_eq!(rx.try_recv(), Ok(1));
    /// ```
    pub fn is_disconnected(&self) -> bool {
        self.shared.lock().senders == 0
    }

    /// Returns `true` if no queued value is waiting to be received.
    ///
    /// A `true` result may become stale as senders add values. A `false` result guarantees that
    /// the next receive on this receiver finds a value, because receiving requires `&mut self`.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpsc::unbounded;
    ///
    /// let (tx, mut rx) = unbounded();
    /// assert!(rx.is_empty());
    /// tx.send(1).unwrap();
    /// assert!(!rx.is_empty());
    /// assert_eq!(rx.try_recv(), Ok(1));
    /// assert!(rx.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        // Values already moved into the local batch are still unreceived, and checking them
        // first keeps a receiver with buffered values from contending with senders.
        self.batch.lock().is_empty() && self.shared.lock().buffer.is_empty()
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        let batch = self.batch.get_mut();
        if !batch.is_empty() {
            return Poll::Ready(Ok(pop_batch(batch)));
        }
        let mut state = self.shared.lock();
        let retired = state.buffer.refill(batch);
        if !batch.is_empty() {
            drop(state);
            drop(retired);
            return Poll::Ready(Ok(pop_batch(batch)));
        }
        if state.senders == 0 {
            let old = state.recv_waker.take();
            drop(state);
            drop(retired);
            drop(old);
            return Poll::Ready(Err(RecvError::Disconnected));
        }
        let old = register_waker(&mut state.recv_waker, cx.waker());
        drop(state);
        drop(retired);
        drop(old);
        Poll::Pending
    }
}

// No operation relies on a pinned location for the receiver batch or its values.
impl<T> Unpin for UnboundedReceiver<T> {}
