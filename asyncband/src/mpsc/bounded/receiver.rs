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
use std::mem;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::internal::mutex::Mutex;
use crate::internal::register_waker;
use crate::internal::waitlist::WaitList;
use crate::mpsc::RecvError;
use crate::mpsc::TryRecvError;
use crate::mpsc::bounded::State;

/// The receiving endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`](crate::mpsc::bounded) function. Dropping the receiver
/// discards queued values and causes pending `send` and `reserve` operations to fail.
pub struct BoundedReceiver<T> {
    shared: Arc<Mutex<State<T>>>,
}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedReceiver<T> {
    fn drop(&mut self) {
        let (queue, recv_waker, mut waiters) = {
            let mut state = self.shared.lock();
            state.receiver = false;
            let queue = mem::take(&mut state.queue);
            let recv_waker = state.recv_waker.take();
            // The disconnected state invalidates both queued and granted waiter IDs.
            let waiters = mem::replace(&mut state.send_waiters, WaitList::new());
            (queue, recv_waker, waiters)
        };
        // Notify blocked senders before destroying buffered values, whose destructors may panic.
        while let Some((_, waiter)) = waiters.unlink_first_waiter(|_| true) {
            if let Some(waker) = waiter.waker.take() {
                waker.wake();
            }
        }
        drop(recv_waker);
        drop(queue);
    }
}

impl<T> BoundedReceiver<T> {
    pub(super) fn new(shared: Arc<Mutex<State<T>>>) -> Self {
        Self { shared }
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
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut state = self.shared.lock();
        let value = match state.queue.pop_front() {
            Some(value) => value,
            None if state.senders == 0 => return Err(TryRecvError::Disconnected),
            None => return Err(TryRecvError::Empty),
        };
        let wake = state.release();
        drop(state);
        if let Some(waker) = wake {
            waker.wake();
        }
        Ok(value)
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
    /// # Cancel safety
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
    pub fn is_disconnected(&self) -> bool {
        self.shared.lock().senders == 0
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
    pub fn is_empty(&self) -> bool {
        self.shared.lock().queue.is_empty()
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        let mut state = self.shared.lock();
        match state.queue.pop_front() {
            Some(value) => {
                let wake = state.release();
                drop(state);
                if let Some(waker) = wake {
                    waker.wake();
                }
                Poll::Ready(Ok(value))
            }
            None if state.senders == 0 => {
                let old = state.recv_waker.take();
                drop(state);
                drop(old);
                Poll::Ready(Err(RecvError::Disconnected))
            }
            None => {
                let old = register_waker(&mut state.recv_waker, cx.waker());
                drop(state);
                drop(old);
                Poll::Pending
            }
        }
    }
}
