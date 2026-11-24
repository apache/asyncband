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
use std::sync::Arc;

use crate::mpmc::RecvError;
use crate::mpmc::SendError;
use crate::mpmc::TryRecvError;
use crate::mpmc::TrySendError;
use crate::mpmc::queue::Shared;

/// Creates an unbounded multi-producer, multi-consumer queue.
///
/// Sends are synchronous and values may be buffered until available memory is exhausted.
///
/// Sending and trying to receive never wait for capacity or new messages, but may briefly block
/// on an internal mutex.
pub fn unbounded<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let shared = Arc::new(Shared::unbounded());
    (
        UnboundedSender {
            shared: shared.clone(),
        },
        UnboundedReceiver { shared },
    )
}

/// Sends values to the associated [`UnboundedReceiver`] handles.
///
/// Instances are created by [`unbounded`] and can be cloned to add producers.
pub struct UnboundedSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        self.shared.clone_sender();
        Self {
            shared: self.shared.clone(),
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
        self.shared.drop_sender();
    }
}

impl<T> UnboundedSender<T> {
    /// Enqueues a value without waiting for capacity.
    ///
    /// Success does not guarantee that a receiver will consume the value before being dropped.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] containing the unsent value if all receivers have been dropped.
    ///
    /// # Panics
    ///
    /// Panics if growing the internal queue buffer overflows the allocation size limit. On panic,
    /// `value` is dropped outside the queue lock and nothing is enqueued.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        match self.shared.try_send(value) {
            Ok(()) => Ok(()),
            Err(TrySendError::Disconnected(value)) => Err(SendError::new(value)),
            Err(TrySendError::Full(_)) => unreachable!("unbounded queue cannot be full"),
        }
    }

    /// Returns `true` if every receiver has been dropped.
    ///
    /// Once this returns `true`, every later [`send`](Self::send) fails with a disconnection
    /// error. A `false` result can be outdated as soon as it is returned, because the last
    /// receiver may be dropped concurrently.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::unbounded::<u8>();
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
    /// The result is an instantaneous observation and may become stale as other tasks send or
    /// receive.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::unbounded();
    /// assert!(sender.is_empty());
    /// sender.send(1).unwrap();
    /// assert!(!sender.is_empty());
    /// assert_eq!(receiver.try_recv(), Ok(1));
    /// assert!(sender.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.shared.is_empty()
    }

    /// Returns the number of values currently queued for receivers.
    ///
    /// The result is an instantaneous observation and may become stale as other tasks send or
    /// receive.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::unbounded();
    /// sender.send(1).unwrap();
    /// sender.send(2).unwrap();
    /// assert_eq!(sender.len(), 2);
    /// assert_eq!(receiver.try_recv(), Ok(1));
    /// assert_eq!(receiver.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.shared.len()
    }

    /// Returns the number of live senders.
    ///
    /// The result is an instantaneous observation and may become stale as other tasks clone or
    /// drop senders.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, _receiver) = mpmc::unbounded::<u8>();
    /// assert_eq!(sender.sender_count(), 1);
    /// let clone = sender.clone();
    /// assert_eq!(sender.sender_count(), 2);
    /// drop(clone);
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
    /// let (sender, receiver) = mpmc::unbounded::<u8>();
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

    /// Returns `true` if `self` and `other` were created from the same call to [`unbounded`].
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, _receiver) = mpmc::unbounded::<u8>();
    /// let clone = sender.clone();
    /// let (other_sender, _other_receiver) = mpmc::unbounded();
    /// assert!(sender.same_channel(&clone));
    /// assert!(!sender.same_channel(&other_sender));
    /// ```
    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}

/// Receives values from the associated [`UnboundedSender`] handles.
///
/// Cloned receivers compete for values, and every accepted value is returned by exactly one
/// receiver while a receiver remains. Dropping the final receiver releases buffered values.
pub struct UnboundedReceiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for UnboundedReceiver<T> {
    fn clone(&self) -> Self {
        self.shared.clone_receiver();
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedReceiver").finish_non_exhaustive()
    }
}

impl<T> Drop for UnboundedReceiver<T> {
    fn drop(&mut self) {
        self.shared.drop_receiver();
    }
}

impl<T> UnboundedReceiver<T> {
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
    /// Dropping a pending future does not remove a value from the queue or prevent another receiver
    /// from receiving it.
    pub async fn recv(&self) -> Result<T, RecvError> {
        self.shared.recv().await
    }

    /// Receives the next queued value without waiting for a message.
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
    /// let (sender, receiver) = mpmc::unbounded();
    /// sender.send(1).unwrap();
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
    /// The result is an instantaneous observation and may become stale as other tasks send or
    /// receive.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::unbounded();
    /// assert!(receiver.is_empty());
    /// sender.send(1).unwrap();
    /// assert!(!receiver.is_empty());
    /// assert_eq!(receiver.try_recv(), Ok(1));
    /// assert!(receiver.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.shared.is_empty()
    }

    /// Returns the number of values currently queued for receivers.
    ///
    /// The result is an instantaneous observation and may become stale as other tasks send or
    /// receive.
    pub fn len(&self) -> usize {
        self.shared.len()
    }

    /// Returns the number of live senders.
    ///
    /// The result is an instantaneous observation and may become stale as other tasks clone or
    /// drop senders.
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (sender, receiver) = mpmc::unbounded::<u8>();
    /// assert_eq!(receiver.sender_count(), 1);
    /// let clone = sender.clone();
    /// assert_eq!(receiver.sender_count(), 2);
    /// drop(clone);
    /// drop(sender);
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
    /// let (_sender, receiver) = mpmc::unbounded::<u8>();
    /// assert_eq!(receiver.receiver_count(), 1);
    /// let competing = receiver.clone();
    /// assert_eq!(receiver.receiver_count(), 2);
    /// drop(competing);
    /// assert_eq!(receiver.receiver_count(), 1);
    /// ```
    pub fn receiver_count(&self) -> usize {
        self.shared.receiver_count()
    }

    /// Returns `true` if `self` and `other` were created from the same call to [`unbounded`].
    ///
    /// # Examples
    ///
    /// ```
    /// use asyncband::mpmc;
    ///
    /// let (_sender, receiver) = mpmc::unbounded::<u8>();
    /// let clone = receiver.clone();
    /// let (_other_sender, other_receiver) = mpmc::unbounded();
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
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::rc::Rc;

    use super::*;

    struct ReenterOnDrop {
        probe: UnboundedSender<Option<ReenterOnDrop>>,
        observed_len: Rc<Cell<Option<usize>>>,
    }

    impl Drop for ReenterOnDrop {
        fn drop(&mut self) {
            self.observed_len.set(Some(self.probe.len()));
        }
    }

    #[test]
    fn unbounded_send_overflow_drops_value_outside_the_queue_lock() {
        let (sender, receiver) = unbounded::<Option<ReenterOnDrop>>();
        let observed = Rc::new(Cell::new(None));
        let payload = Some(ReenterOnDrop {
            probe: sender.clone(),
            observed_len: observed.clone(),
        });

        sender.shared.fail_next_reserve();
        let overflow = catch_unwind(AssertUnwindSafe(|| sender.send(payload)));
        assert!(overflow.is_err());
        assert_eq!(observed.get(), Some(0));
        assert_eq!(sender.sender_count(), 1);

        sender.send(None).unwrap();
        assert!(receiver.try_recv().unwrap().is_none());
    }
}
