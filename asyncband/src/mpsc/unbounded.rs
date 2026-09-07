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

//! An unbounded multi-producer, single-consumer queue for sending values between asynchronous
//! tasks.

use std::collections::VecDeque;
use std::fmt;
use std::future::poll_fn;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::RecvError;
use super::SendError;
use super::TryRecvError;
use crate::internal::mutex::Mutex;

/// Creates an unbounded mpsc channel whose send operation never waits for capacity.
///
/// While the receiver is alive, each send appends its value immediately. Pending messages can
/// therefore grow with producer demand and are limited only by successful memory allocation. Use a
/// bounded channel or external admission control when producers may outpace the receiver.
///
/// After all messages have been received, large backing allocations are released; small buffers
/// may be retained for reuse. Partially consumed batches can retain their original allocation.
pub fn unbounded<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let state = Arc::new(UnboundedState {
        senders: AtomicUsize::new(1),
        inbox: Mutex::new(Inbox {
            messages: VecDeque::new(),
            receiver_alive: true,
            rx_waker: None,
        }),
    });
    let sender = UnboundedSender {
        state: state.clone(),
    };
    let receiver = UnboundedReceiver {
        state,
        batch: Mutex::new(VecDeque::new()),
    };
    (sender, receiver)
}

struct UnboundedState<T> {
    // Endpoint cloning and ordinary drops do not contend with message traffic.
    senders: AtomicUsize,
    inbox: Mutex<Inbox<T>>,
}

// Queue contents, receiver liveness, and its wake registration share one lock. Registering a
// wait and checking its condition cannot race with sending or receiver disconnection.
struct Inbox<T> {
    messages: VecDeque<T>,
    receiver_alive: bool,
    rx_waker: Option<Waker>,
}

/// The sending endpoint of an unbounded mpsc channel.
///
/// Instances are created by the [`unbounded`] function.
pub struct UnboundedSender<T> {
    state: Arc<UnboundedState<T>>,
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        self.state.senders.fetch_add(1, Ordering::Release);
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T> fmt::Debug for UnboundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedSender").finish_non_exhaustive()
    }
}

impl<T> Drop for UnboundedSender<T> {
    fn drop(&mut self) {
        if self.state.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            let waker = self.state.inbox.lock().rx_waker.take();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

impl<T> UnboundedSender<T> {
    /// Enqueues a message without waiting for capacity.
    ///
    /// This operation is synchronous because the channel has no capacity limit. If the receiver has
    /// been dropped, the returned error contains `value`.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let waker = {
            let mut state = self.state.inbox.lock();
            if !state.receiver_alive {
                return Err(SendError::new(value));
            }
            state.messages.push_back(value);
            state.rx_waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }
}

/// The receiving endpoint of an unbounded mpsc channel.
///
/// Instances are created by the [`unbounded`] function.
pub struct UnboundedReceiver<T> {
    state: Arc<UnboundedState<T>>,
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
        let (shared, waker) = {
            let mut state = self.state.inbox.lock();
            state.receiver_alive = false;
            (mem::take(&mut state.messages), state.rx_waker.take())
        };
        // Destructors may send again. A waker may also own a sender and form an ownership cycle.
        drop((batch, shared, waker));
    }
}

impl<T> UnboundedReceiver<T> {
    /// Attempts to receive the next queued value without waiting.
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
            let mut state = self.state.inbox.lock();
            if state.messages.is_empty() {
                // Holding the inbox lock excludes a final send between the empty observation and
                // the sender-count check; disconnection needs no second queue read.
                return Err(if self.state.senders.load(Ordering::Acquire) == 0 {
                    TryRecvError::Disconnected
                } else {
                    TryRecvError::Empty
                });
            }
            mem::swap(batch, &mut state.messages);
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
        // Waker clone/drop callbacks can reenter this channel. Clone outside the lock, then
        // recheck the condition before registering; keep replaced wakers outside the lock too.
        let mut new_waker = None;
        loop {
            let mut state = self.state.inbox.lock();
            if !state.messages.is_empty() {
                mem::swap(batch, &mut state.messages);
                drop(state);
                return Poll::Ready(Ok(pop_batch(batch)));
            }
            if self.state.senders.load(Ordering::Acquire) == 0 {
                return Poll::Ready(Err(RecvError::Disconnected));
            }
            if state
                .rx_waker
                .as_ref()
                .is_some_and(|waker| waker.will_wake(cx.waker()))
            {
                return Poll::Pending;
            }
            if let Some(waker) = new_waker.take() {
                let old_waker = state.rx_waker.replace(waker);
                drop(state);
                drop(old_waker);
                return Poll::Pending;
            }
            drop(state);
            new_waker = Some(cx.waker().clone());
        }
    }
}

// No operation relies on a pinned location for the receiver batch or its values.
impl<T> Unpin for UnboundedReceiver<T> {}

// Retain small buffers for reuse, measuring inline storage rather than separately boxed payloads.
const BATCH_CACHE_BYTES: usize = 64 * 1024;

fn pop_batch<T>(batch: &mut VecDeque<T>) -> T {
    if batch.len() == 1 && batch.capacity().saturating_mul(mem::size_of::<T>()) > BATCH_CACHE_BYTES
    {
        // Retire the allocation on the last value, outside the inbox lock. Keep this as a tail
        // expression to avoid intermediate storage for large inline values.
        mem::take(batch).pop_front()
    } else {
        batch.pop_front()
    }
    .expect("receiver batch must not be empty")
}

#[cfg(test)]
mod tests {
    use super::unbounded;
    use crate::mpsc::TryRecvError;

    #[test]
    fn batches_preserve_order_across_refills() {
        let (tx, mut rx) = unbounded();
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert_eq!(rx.try_recv(), Ok(1));
        tx.send(3).unwrap();
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Ok(3));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn releases_large_batches_after_the_last_value() {
        let (tx, mut rx) = unbounded();
        for value in 0..128u8 {
            tx.send([value; 1024]).unwrap();
        }
        for value in 0..64u8 {
            assert_eq!(rx.try_recv(), Ok([value; 1024]));
        }
        // A partially consumed batch survives while producers start filling the next batch.
        tx.send([128; 1024]).unwrap();
        for value in 64..128u8 {
            assert_eq!(rx.try_recv(), Ok([value; 1024]));
        }
        // Reclaim on the final successful receive, without requiring an extra empty poll.
        assert_eq!(rx.batch.get_mut().capacity(), 0);
        assert_eq!(rx.try_recv(), Ok([128; 1024]));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn reuses_small_batches_on_refill() {
        let (tx, mut rx) = unbounded();
        for value in 0..32usize {
            tx.send(value).unwrap();
        }
        assert_eq!(rx.try_recv(), Ok(0));
        let capacity = rx.batch.get_mut().capacity();
        for value in 1..32 {
            assert_eq!(rx.try_recv(), Ok(value));
        }
        assert_eq!(rx.batch.get_mut().capacity(), capacity);

        tx.send(32).unwrap();
        assert_eq!(rx.try_recv(), Ok(32));
        assert_eq!(rx.state.inbox.lock().messages.capacity(), capacity);
    }

    #[test]
    fn drains_zero_sized_values() {
        let (tx, mut rx) = unbounded();
        for _ in 0..32 {
            tx.send(()).unwrap();
        }
        for _ in 0..32 {
            assert_eq!(rx.try_recv(), Ok(()));
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }
}
