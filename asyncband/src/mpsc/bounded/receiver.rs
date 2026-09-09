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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::Shared;
use crate::internal::cache_padded::CachePadded;
use crate::internal::mutex::Mutex;
use crate::internal::wake_all;
use crate::mpsc::RecvError;
use crate::mpsc::TryRecvError;

/// The receiving endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`](crate::mpsc::bounded) function. Dropping the receiver
/// discards queued values.
/// The backing allocation remains alive until all endpoints are dropped, so a concurrent sender
/// can safely finish returning an unsent value.
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
    pub(super) fn new(shared: Arc<Shared<T>>) -> Self {
        Self { shared, head: 0 }
    }

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
            match self.pull() {
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

    /// One attempt to take the head value: a message, an empty-or-disconnected classification,
    /// or `Pending` while a claimed head waits for its publication.
    fn pull(&mut self) -> Poll<Result<T, TryRecvError>> {
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

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        for registered in [false, true] {
            match self.pull() {
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

// The receiver checks the queue after registering; publishers check this flag after publication.
// Paired SeqCst fences prevent both sides from missing the other's transition. The stable false
// flag avoids modifying the waker's cache line for every message while the receiver is running.
pub struct ReceiverWaker {
    waiting: CachePadded<AtomicBool>,
    waker: Mutex<Option<Waker>>,
}

impl ReceiverWaker {
    pub fn new() -> Self {
        Self {
            waiting: CachePadded::new(AtomicBool::new(false)),
            waker: Mutex::new(None),
        }
    }

    pub fn register(&self, waker: &Waker) {
        let mut current = self.waker.lock();
        let old = if current.as_ref().is_some_and(|old| old.will_wake(waker)) {
            None
        } else {
            // Only the receiver registers. User clone callbacks run outside the lock.
            drop(current);
            let waker = waker.clone();
            current = self.waker.lock();
            current.replace(waker)
        };
        self.waiting.store(true, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        drop(current);
        drop(old);
    }

    pub fn wake(&self) {
        fence(Ordering::SeqCst);
        if !self.waiting.load(Ordering::Relaxed) || !self.waiting.swap(false, Ordering::Relaxed) {
            return;
        }
        if let Some(waker) = self.take() {
            waker.wake();
        }
    }

    pub fn take(&self) -> Option<Waker> {
        let mut current = self.waker.lock();
        // Clearing under the lock also takes responsibility for a newer registration.
        self.waiting.store(false, Ordering::Relaxed);
        current.take()
    }
}
