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

// This file contains code derived from the oneshot crate.
// Asyncband uses the upstream crate's Apache-2.0 license option for that code.
// The incorporated code has been modified for use in Apache Asyncband.
// Upstream sources (oneshot v0.1.11, before its channel code was split into modules):
// https://github.com/faern/oneshot/blob/25274e995ee0a702b3e9e1ac81e577f8c3ce0892/src/lib.rs
// https://github.com/faern/oneshot/blob/25274e995ee0a702b3e9e1ac81e577f8c3ce0892/src/errors.rs

use std::any::type_name;
use std::error::Error;
use std::fmt;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;

use crate::oneshot::Channel;
use crate::oneshot::DISCONNECTED;
use crate::oneshot::EMPTY;
use crate::oneshot::MESSAGE;
use crate::oneshot::RECEIVING;
#[cfg(doc)]
use crate::oneshot::Receiver;
use crate::oneshot::deallocate_empty_channel;
use crate::oneshot::drop_message_and_deallocate_channel;

/// The sending half of a [`oneshot`](crate::oneshot) channel.
pub struct Sender<T> {
    channel_ptr: NonNull<Channel<T>>,
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T: Sync> Sync for Sender<T> {}

impl<T> Sender<T> {
    /// Sends `message` to the paired [`Receiver`], consuming this sender.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] containing the unsent message if the receiver has been dropped.
    pub fn send(self, message: T) -> Result<(), SendError<T>> {
        // `send` takes over endpoint cleanup, so `Sender::drop` must not run afterward.
        let sender = ManuallyDrop::new(self);
        let channel_ptr = sender.channel_ptr;

        // SAFETY: The live sender keeps the allocation valid until it publishes MESSAGE. That
        // publication can let the receiver reclaim it, so no channel access may follow unless
        // receiver cancellation transfers allocation cleanup to this sender.
        let channel = unsafe { channel_ptr.as_ref() };

        // SAFETY: `message` is only read after `MESSAGE` is published via the atomic transition
        // below, giving the sender exclusive write access here.
        unsafe { channel.write_message(message) };

        // Publish the message directly, or take ownership of waking a parked receiver:
        //
        // * `EMPTY + 1 = MESSAGE`
        // * `RECEIVING + 1 = AWAKING`
        //
        // ORDERING: `Release` publishes the initialized message on `EMPTY -> MESSAGE` and orders it
        // before the `AWAKING` handoff's final publication. Branches that inspect receiver-side
        // state acquire below.
        match channel.state.fetch_add(1, Ordering::Release) {
            // Receiver has not registered a waker yet; it will read and drop the payload.
            EMPTY => Ok(()),
            // Receiver is parked; extract its waker and finalize the state transition.
            RECEIVING => {
                // SAFETY: Observing `RECEIVING` and setting `AWAKING` transferred exclusive waker
                // ownership to this sender; `message` was initialized before the RMW.
                let (waker, receiver_owns_allocation) =
                    unsafe { Channel::finish_sender_awakening(channel_ptr, MESSAGE) };
                if receiver_owns_allocation {
                    waker.wake();
                } else {
                    // Send succeeded before receiver cancellation transitioned out of `AWAKING`.
                    //
                    // SAFETY: Cancellation transferred allocation cleanup to this sender. The
                    // waker was moved out and the message slot is initialized.
                    unsafe { drop_message_and_deallocate_channel(channel_ptr) };
                }
                Ok(())
            }
            // Receiver already disconnected; return the unsent message inside `SendError`.
            //
            // SAFETY: The `Acquire` fence pairs with receiver disconnection so this endpoint now
            // owns the channel allocation and its initialized message exclusively.
            DISCONNECTED => {
                fence(Ordering::Acquire);
                Err(SendError { channel_ptr })
            }
            state => unreachable!("unexpected channel state: {}", state),
        }
    }

    /// Reports whether the paired [`Receiver`] has already been dropped.
    ///
    /// When this returns `true`, a later call to [`send`](Sender::send) is guaranteed to fail with
    /// [`SendError`]. A `false` result does not prevent the receiver from dropping concurrently.
    pub fn is_disconnected(&self) -> bool {
        // SAFETY: While `self` is live, dropping the receiver never frees the channel allocation.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: `DISCONNECTED` is terminal, so `Relaxed` suffices to observe permanent
        // disconnection; `false` makes no synchronization guarantee against concurrent drops.
        matches!(channel.state.load(Ordering::Relaxed), DISCONNECTED)
    }

    pub(super) fn new(channel_ptr: NonNull<Channel<T>>) -> Self {
        Self { channel_ptr }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // SAFETY: While `Sender::drop` runs, neither `MESSAGE` (blocked by `ManuallyDrop` in
        // `send`) nor receiver-reclaimed `DISCONNECTED` can have freed the allocation. No channel
        // access may follow publishing `DISCONNECTED` unless receiver cancellation hands cleanup
        // back to this sender.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // Publish disconnection directly, or take ownership of waking a parked receiver:
        //
        // * `EMPTY ^ 0b001 = DISCONNECTED`
        // * `RECEIVING ^ 0b001 = AWAKING`
        //
        // ORDERING: `Release` publishes the sender's drop before any waiting handoff; branches that
        // reclaim resources acquire below.
        match channel.state.fetch_xor(0b001, Ordering::Release) {
            // Receiver is alive without a waker and will reclaim the empty channel.
            EMPTY => {}
            // Receiver is parked; extract its waker and finalize disconnection.
            RECEIVING => {
                // SAFETY: Observing `RECEIVING` and setting `AWAKING` transferred exclusive waker
                // ownership; no message was written.
                let (waker, receiver_owns_allocation) =
                    unsafe { Channel::finish_sender_awakening(self.channel_ptr, DISCONNECTED) };
                if receiver_owns_allocation {
                    waker.wake();
                } else {
                    // SAFETY: Cancellation transferred cleanup; both payload and waker slots are
                    // empty.
                    unsafe { deallocate_empty_channel(self.channel_ptr) };
                }
            }
            // Receiver already disconnected and observed this live sender; reclaim the allocation.
            DISCONNECTED => {
                // ORDERING: Pairs with the receiver's `Release` drop before freeing the channel.
                fence(Ordering::Acquire);
                // SAFETY: Receiver is gone and left an empty channel allocation for this sender.
                unsafe { deallocate_empty_channel(self.channel_ptr) };
            }
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

/// Error returned by [`Sender::send`] when the paired [`Receiver`] was dropped before receiving.
///
/// Owns the unsent value; call [`as_inner`](Self::as_inner) to inspect it or
/// [`into_inner`](Self::into_inner) to recover it.
pub struct SendError<T> {
    channel_ptr: NonNull<Channel<T>>,
}

// SAFETY: SendError exclusively owns the channel allocation and its initialized message. If the
// message is Send, the error may transfer that ownership to another thread.
unsafe impl<T: Send> Send for SendError<T> {}

// SAFETY: SendError retains exclusive ownership while shared references only expose `&T`, which
// may cross threads when T is Sync.
unsafe impl<T: Sync> Sync for SendError<T> {}

impl<T> SendError<T> {
    /// Returns a reference to the message that failed to be sent.
    pub fn as_inner(&self) -> &T {
        // SAFETY: SendError exclusively owns the allocation and its initialized message.
        unsafe { self.channel_ptr.as_ref().message() }
    }

    /// Consumes the error and returns the message that failed to be sent.
    pub fn into_inner(self) -> T {
        // The returned message and this method take over cleanup, so `SendError::drop` must not
        // run.
        let error = ManuallyDrop::new(self);
        let channel_ptr = error.channel_ptr;

        // SAFETY: SendError exclusively owns the allocation.
        let channel: &Channel<T> = unsafe { channel_ptr.as_ref() };

        // SAFETY: The send path initialized the message before constructing SendError.
        let message = unsafe { channel.take_message() };

        // SAFETY: SendError exclusively owns the allocation, so its original pointer provenance may
        // be reclaimed as a Box. The message has been moved out and no waker remains initialized.
        unsafe { deallocate_empty_channel(channel_ptr) };

        message
    }
}

impl<T> Drop for SendError<T> {
    fn drop(&mut self) {
        // SAFETY: SendError exclusively owns the initialized message and allocation, so its
        // original pointer provenance may be reclaimed as a Box. No waker remains in the
        // channel.
        unsafe { drop_message_and_deallocate_channel(self.channel_ptr) };
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sending on a disconnected channel")
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SendError<{}>(..)", type_name::<T>())
    }
}

impl<T> Error for SendError<T> {}

#[cfg(test)]
impl<T> Sender<T> {
    pub(super) fn channel_ptr(&self) -> NonNull<Channel<T>> {
        self.channel_ptr
    }
}
