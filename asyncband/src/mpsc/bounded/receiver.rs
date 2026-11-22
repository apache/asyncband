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
#[cfg(target_has_atomic = "64")]
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use crate::internal::register_waker;
use crate::internal::waitlist::WaitList;
use crate::mpsc::RecvError;
use crate::mpsc::TryRecvError;
#[cfg(target_has_atomic = "64")]
use crate::mpsc::bounded::CLOSED_BIT;
use crate::mpsc::bounded::NO_SLOT;
#[cfg(target_has_atomic = "64")]
use crate::mpsc::bounded::RECV_WAITING_BIT;
use crate::mpsc::bounded::Shared;
use crate::mpsc::bounded::Waiter;
#[cfg(target_has_atomic = "64")]
use crate::mpsc::bounded::inbox_head;

/// The receiving endpoint of a bounded mpsc channel.
///
/// Instances are created by the [`bounded`](crate::mpsc::bounded) function. Dropping the receiver
/// discards queued values and causes pending `send` and `reserve` operations to fail.
pub struct BoundedReceiver<T> {
    shared: Arc<Shared<T>>,
    // Head of the receiver's private FIFO chain of detached slot indices (`NO_SLOT` when empty).
    // Stored in `AtomicU32` so `is_empty(&self)` can inspect it lock-free while `&mut self`
    // accesses it through `get_mut()`.
    #[cfg(target_has_atomic = "64")]
    local_head: AtomicU32,
    // Uninitialized physical slot owned exclusively by this receiver and swapped back into
    // `free_ring` (or the oldest waiting sender) on each receive.
    #[cfg(target_has_atomic = "64")]
    spare: u32,
}

impl<T> fmt::Debug for BoundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for BoundedReceiver<T> {
    fn drop(&mut self) {
        let (detached_lifo, locked_queue, recv_waker, mut waiters) = {
            let mut state = self.shared.state.lock();
            #[cfg(target_has_atomic = "64")]
            let detached_lifo = if self.shared.use_slot_pool() {
                inbox_head(
                    self.shared
                        .inbox
                        .swap(CLOSED_BIT | u64::from(NO_SLOT), Ordering::AcqRel),
                )
            } else {
                NO_SLOT
            };
            #[cfg(not(target_has_atomic = "64"))]
            let detached_lifo = NO_SLOT;
            state.receiver = false;
            self.shared.closed.store(true, Ordering::Release);
            let locked_queue = mem::take(&mut state.locked_queue);
            let recv_waker = state.recv_waker.take();
            // The disconnected state invalidates both queued and granted waiter IDs.
            let waiters = mem::replace(&mut state.send_waiters, WaitList::new());
            (detached_lifo, locked_queue, recv_waker, waiters)
        };
        #[cfg(not(target_has_atomic = "64"))]
        let _ = detached_lifo;

        // Notify blocked senders before destroying buffered values, whose destructors may panic.
        while let Some((id, _)) = waiters.unlink_first_waiter() {
            let Waiter::Waiting(waker) = waiters.remove_unlinked_waiter(id) else {
                unreachable!("only waiting senders remain linked");
            };
            waker.wake();
        }
        drop(recv_waker);

        #[cfg(target_has_atomic = "64")]
        if self.shared.use_slot_pool() {
            struct LeavePool<'a, T>(&'a Shared<T>);

            impl<T> Drop for LeavePool<'_, T> {
                fn drop(&mut self) {
                    self.0.leave_pool();
                }
            }

            // Declare `_leave_pool` before `drop_guard` so reverse drop order keeps the receiver's
            // pool lease alive until all queued values have been dropped, even during unwinding.
            let _leave_pool = LeavePool(&self.shared);

            let detached_fifo = if detached_lifo == NO_SLOT {
                NO_SLOT
            } else {
                // SAFETY: `_leave_pool` holds the receiver's `pool_refs` lease.
                unsafe { self.shared.reverse_msg_chain(detached_lifo) }
            };

            struct DropQueued<'a, T> {
                shared: &'a Shared<T>,
                local_head: u32,
                detached_head: u32,
            }

            impl<T> DropQueued<'_, T> {
                fn pop_next_slot(&mut self) -> Option<u32> {
                    // SAFETY: `_leave_pool` outlives `DropQueued` and holds a `pool_refs` lease.
                    let next_link = &unsafe { self.shared.pool() }.next_link;
                    if self.local_head != NO_SLOT {
                        let slot = self.local_head;
                        self.local_head = next_link[slot as usize].load(Ordering::Relaxed);
                        return Some(slot);
                    }
                    if self.detached_head != NO_SLOT {
                        let slot = self.detached_head;
                        self.detached_head = next_link[slot as usize].load(Ordering::Relaxed);
                        return Some(slot);
                    }
                    None
                }
            }

            impl<T> Drop for DropQueued<'_, T> {
                fn drop(&mut self) {
                    while let Some(slot) = self.pop_next_slot() {
                        // SAFETY: Every slot in `local_head` and `detached_head` was initialized
                        // before publication and is exclusively owned by this closing receiver.
                        unsafe { self.shared.drop_slot(slot) };
                    }
                }
            }

            let mut drop_guard = DropQueued {
                shared: &self.shared,
                local_head: *self.local_head.get_mut(),
                detached_head: detached_fifo,
            };
            *self.local_head.get_mut() = NO_SLOT;

            while let Some(slot) = drop_guard.pop_next_slot() {
                // SAFETY: Advancing the head before `drop_slot` ensures `drop_guard`'s Drop
                // implementation drops the remaining queued values if a destructor panics.
                unsafe { drop_guard.shared.drop_slot(slot) };
            }
        }

        drop(locked_queue);
    }
}

impl<T> BoundedReceiver<T> {
    pub(super) fn new(shared: Arc<Shared<T>>, spare: u32) -> Self {
        #[cfg(not(target_has_atomic = "64"))]
        let _ = spare;
        Self {
            shared,
            #[cfg(target_has_atomic = "64")]
            local_head: AtomicU32::new(NO_SLOT),
            #[cfg(target_has_atomic = "64")]
            spare,
        }
    }

    #[cfg(target_has_atomic = "64")]
    #[inline]
    fn pop_published_slot(&mut self) -> Option<u32> {
        // SAFETY: `BoundedReceiver` holds its `pool_refs` lease throughout `&mut self`.
        let next_link = &unsafe { self.shared.pool() }.next_link;
        let local = *self.local_head.get_mut();
        if local != NO_SLOT {
            let next = next_link[local as usize].load(Ordering::Relaxed);
            self.local_head.store(next, Ordering::Relaxed);
            return Some(local);
        }

        let lifo_head = self.shared.detach_inbox();
        if lifo_head == NO_SLOT {
            return None;
        }
        let second = next_link[lifo_head as usize].load(Ordering::Relaxed);
        if second == NO_SLOT {
            return Some(lifo_head);
        }

        // SAFETY: `BoundedReceiver` holds its `pool_refs` lease.
        let oldest = unsafe { self.shared.reverse_msg_chain(lifo_head) };
        let next = next_link[oldest as usize].load(Ordering::Relaxed);
        self.local_head.store(next, Ordering::Relaxed);
        Some(oldest)
    }

    #[cfg(target_has_atomic = "64")]
    #[inline]
    fn finish_slot_recv(&mut self, slot: u32) -> T {
        let old_spare = mem::replace(&mut self.spare, slot);
        // SAFETY: `slot` was detached from `inbox` after a producer initialized it, and
        // `self.spare` now owns `slot` exclusively until a later receive swaps it out.
        let value = unsafe { self.shared.read_slot(slot) };
        if let Some(waker) = self.shared.release_slot_from_receiver(old_spare) {
            waker.wake();
        }
        value
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
        if !self.shared.use_slot_pool() {
            let mut state = self.shared.state.lock();
            if let Some(value) = state.locked_queue.pop_front() {
                let wake = self.shared.release_slot_locked(&mut state, NO_SLOT);
                drop(state);
                if let Some(waker) = wake {
                    waker.wake();
                }
                return Ok(value);
            }
            return if self.shared.senders.load(Ordering::Acquire) == 0 {
                Err(TryRecvError::Disconnected)
            } else {
                Err(TryRecvError::Empty)
            };
        }

        #[cfg(target_has_atomic = "64")]
        {
            if let Some(slot) = self.pop_published_slot() {
                return Ok(self.finish_slot_recv(slot));
            }
            if self.shared.senders.load(Ordering::Acquire) == 0 {
                // Re-check `inbox` in case the final sender published right before dropping.
                if let Some(slot) = self.pop_published_slot() {
                    return Ok(self.finish_slot_recv(slot));
                }
                Err(TryRecvError::Disconnected)
            } else {
                Err(TryRecvError::Empty)
            }
        }
        #[cfg(not(target_has_atomic = "64"))]
        {
            unreachable!("slot pool requires 64-bit atomics")
        }
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
        self.shared.senders.load(Ordering::Acquire) == 0
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
        if !self.shared.use_slot_pool() {
            return self.shared.state.lock().locked_queue.is_empty();
        }
        #[cfg(target_has_atomic = "64")]
        {
            self.local_head.load(Ordering::Acquire) == NO_SLOT
                && inbox_head(self.shared.inbox.load(Ordering::Acquire)) == NO_SLOT
        }
        #[cfg(not(target_has_atomic = "64"))]
        {
            unreachable!("slot pool requires 64-bit atomics")
        }
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        if !self.shared.use_slot_pool() {
            let mut state = self.shared.state.lock();
            if let Some(value) = state.locked_queue.pop_front() {
                let wake = self.shared.release_slot_locked(&mut state, NO_SLOT);
                drop(state);
                if let Some(waker) = wake {
                    waker.wake();
                }
                return Poll::Ready(Ok(value));
            }
            if self.shared.senders.load(Ordering::Acquire) == 0 {
                let old = state.recv_waker.take();
                drop(state);
                drop(old);
                return Poll::Ready(Err(RecvError::Disconnected));
            }
            let old = register_waker(&mut state.recv_waker, cx.waker());
            drop(state);
            drop(old);
            return Poll::Pending;
        }

        #[cfg(target_has_atomic = "64")]
        {
            loop {
                if let Some(slot) = self.pop_published_slot() {
                    return Poll::Ready(Ok(self.finish_slot_recv(slot)));
                }

                let mut state = self.shared.state.lock();
                if inbox_head(self.shared.inbox.load(Ordering::Acquire)) != NO_SLOT {
                    drop(state);
                    continue;
                }
                if self.shared.senders.load(Ordering::Acquire) == 0 {
                    // Re-check `inbox` after observing `senders == 0` in case the final sender
                    // published a message right before dropping its handle.
                    if inbox_head(self.shared.inbox.load(Ordering::Acquire)) != NO_SLOT {
                        drop(state);
                        continue;
                    }
                    let stored = state.recv_waker.take();
                    drop(state);
                    drop(stored);
                    return Poll::Ready(Err(RecvError::Disconnected));
                }

                let old_waker = register_waker(&mut state.recv_waker, cx.waker());
                let mut old_inbox = self.shared.inbox.load(Ordering::Acquire);
                loop {
                    if inbox_head(old_inbox) != NO_SLOT {
                        // A producer published while we registered; retry the lock-free pop.
                        let stored = state.recv_waker.take();
                        drop(state);
                        drop(old_waker);
                        drop(stored);
                        break;
                    }
                    if (old_inbox & RECV_WAITING_BIT) != 0 {
                        drop(state);
                        drop(old_waker);
                        return Poll::Pending;
                    }
                    match self.shared.inbox.compare_exchange_weak(
                        old_inbox,
                        old_inbox | RECV_WAITING_BIT,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            drop(state);
                            drop(old_waker);
                            return Poll::Pending;
                        }
                        Err(actual) => old_inbox = actual,
                    }
                }
            }
        }
        #[cfg(not(target_has_atomic = "64"))]
        {
            unreachable!("slot pool requires 64-bit atomics")
        }
    }
}
