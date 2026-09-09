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

use super::State;
use crate::internal::mutex::Mutex;
use crate::internal::wake_all;
use crate::internal::waker_batch::WakerBatch;
use crate::mpsc::RecvError;
use crate::mpsc::TryRecvError;

/// The receiving endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`](crate::mpsc::bounded) function. Dropping the receiver
/// discards queued values and disconnects pending sends and reservations.
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
        let (queue, recv_waker, wakers) = {
            let mut state = self.shared.lock();
            state.receiver = false;
            let queue = mem::take(&mut state.queue);
            let recv_waker = state.recv_waker.take();
            let mut wakers = WakerBatch::new();
            while let Some((_, waiter)) = state.send_waiters.unlink_first_waiter(|_| true) {
                if let Some(waker) = waiter.waker.take() {
                    wakers.push(waker);
                }
            }
            (queue, recv_waker, wakers)
        };
        // Local ownership also drains the queue if a wake or waker destructor unwinds.
        wake_all(wakers.into_iter());
        drop(recv_waker);
        drop(queue);
    }
}

impl<T> BoundedReceiver<T> {
    pub(super) fn new(shared: Arc<Mutex<State<T>>>) -> Self {
        Self { shared }
    }

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
        let (value, wake) = self.shared.lock().pop()?;
        if let Some(waker) = wake {
            waker.wake();
        }
        Ok(value)
    }

    /// Waits for and receives the next value, freeing one buffer slot.
    ///
    /// If no value is queued, this method waits until a sender adds one or the last sender is
    /// dropped. It returns [`RecvError::Disconnected`] only after all senders are gone and the
    /// buffer has been drained.
    ///
    /// # Cancel safety
    ///
    /// Dropping a pending `recv` does not remove a message from the channel. A later `recv` call
    /// can still observe the next queued value, so `recv` may safely be raced with other futures
    /// in a selection construct.
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
        let mut cloned_waker = None;
        loop {
            let mut state = self.shared.lock();
            match state.pop() {
                Ok((value, wake)) => {
                    drop(state);
                    if let Some(waker) = wake {
                        waker.wake();
                    }
                    return Poll::Ready(Ok(value));
                }
                Err(TryRecvError::Disconnected) => {
                    let old = state.recv_waker.take();
                    drop(state);
                    drop(old);
                    return Poll::Ready(Err(RecvError::Disconnected));
                }
                Err(TryRecvError::Empty) => {}
            }
            if state
                .recv_waker
                .as_ref()
                .is_some_and(|w| w.will_wake(cx.waker()))
            {
                return Poll::Pending;
            }
            if let Some(waker) = cloned_waker.take() {
                let old = state.recv_waker.replace(waker);
                drop(state);
                drop(old);
                return Poll::Pending;
            }
            drop(state);
            // Clone can reenter the channel, so check the queue again after acquiring the lock.
            cloned_waker = Some(cx.waker().clone());
        }
    }
}
