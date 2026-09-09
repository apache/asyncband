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

use super::State;
use super::buffer::Buffer;
use super::buffer::pop_batch;
use crate::internal::mutex::Mutex;
use crate::mpsc::RecvError;
use crate::mpsc::TryRecvError;

/// The receiving endpoint of an unbounded mpsc channel.
///
/// Instances are created by the [`unbounded`](crate::mpsc::unbounded) function. Dropping the
/// receiver discards queued values and makes subsequent sends fail.
pub struct UnboundedReceiver<T> {
    shared: Arc<Mutex<State<T>>>,
    // Only accessed through `get_mut`; the mutex preserves Sync for Send-only payloads.
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

    /// Attempts to receive the next queued value without waiting for a new message.
    ///
    /// An empty channel returns [`TryRecvError::Empty`] while at least one sender remains, or
    /// [`TryRecvError::Disconnected`] after every sender has been dropped and all queued values
    /// have been consumed.
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
            state.buffer.refill(batch);
            if batch.is_empty() {
                return Err(if state.senders == 0 {
                    TryRecvError::Disconnected
                } else {
                    TryRecvError::Empty
                });
            }
        }
        Ok(pop_batch(batch))
    }

    /// Waits for and receives the next value.
    ///
    /// If no value is queued, this method waits until a sender adds one or the last sender is
    /// dropped. It returns [`RecvError::Disconnected`] only after all senders are gone and the
    /// queue has been drained.
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

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        let batch = self.batch.get_mut();
        if !batch.is_empty() {
            return Poll::Ready(Ok(pop_batch(batch)));
        }
        // Waker clone/drop callbacks can send into this channel, so run them outside the lock.
        let waker = cx.waker().clone();
        let mut state = self.shared.lock();
        state.buffer.refill(batch);
        if !batch.is_empty() {
            drop(state);
            return Poll::Ready(Ok(pop_batch(batch)));
        }
        if state.senders == 0 {
            let old = state.recv_waker.take();
            drop(state);
            drop(old);
            return Poll::Ready(Err(RecvError::Disconnected));
        }
        let old = state.recv_waker.replace(waker);
        drop(state);
        drop(old);
        Poll::Pending
    }
}

// No operation relies on a pinned location for the receiver batch or its values.
impl<T> Unpin for UnboundedReceiver<T> {}
