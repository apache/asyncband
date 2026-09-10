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
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use super::State;
use super::queue::Consumer;
use super::queue::Pop;
use crate::mpsc::RecvError;
use crate::mpsc::TryRecvError;

/// The receiving endpoint of an unbounded mpsc channel.
///
/// Instances are created by the [`unbounded`](crate::mpsc::unbounded) function. Dropping the
/// receiver discards queued values and makes subsequent sends fail.
pub struct UnboundedReceiver<T> {
    shared: Arc<State<T>>,
    consumer: Consumer<T>,
}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for UnboundedReceiver<T> {
    fn drop(&mut self) {
        let waker = self.shared.recv.take();
        // Closing precedes arbitrary callbacks. Keep the waker owned locally so even a payload
        // destructor panic releases a waker that owns a sender and would otherwise form a cycle.
        self.shared.queue.close(&mut self.consumer);
        drop(waker);
    }
}

impl<T> UnboundedReceiver<T> {
    pub(super) fn new(shared: Arc<State<T>>, consumer: Consumer<T>) -> Self {
        Self { shared, consumer }
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
        match self.consumer.pop(&self.shared.queue) {
            Pop::Value(value) => return Ok(value),
            Pop::Empty => {}
        }
        if self.shared.senders.0.load(Ordering::SeqCst) == 0 {
            // Acquire the last sender's publication before deciding the queue is drained.
            match self.consumer.pop(&self.shared.queue) {
                Pop::Value(value) => Ok(value),
                Pop::Empty => Err(TryRecvError::Disconnected),
            }
        } else {
            Err(TryRecvError::Empty)
        }
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
        match self.try_recv() {
            Ok(value) => return Poll::Ready(Ok(value)),
            Err(TryRecvError::Disconnected) => return Poll::Ready(Err(RecvError::Disconnected)),
            Err(TryRecvError::Empty) => {}
        }
        self.shared.recv.register(cx.waker());
        match self.try_recv() {
            Ok(value) => {
                drop(self.shared.recv.take());
                Poll::Ready(Ok(value))
            }
            Err(TryRecvError::Disconnected) => {
                drop(self.shared.recv.take());
                Poll::Ready(Err(RecvError::Disconnected))
            }
            Err(TryRecvError::Empty) => Poll::Pending,
        }
    }
}

// Moving the receiver cannot move a value in its separately allocated queue storage.
impl<T> Unpin for UnboundedReceiver<T> {}
