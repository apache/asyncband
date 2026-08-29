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

use std::any::type_name;
use std::fmt;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;

use super::Channel;
use super::DISCONNECTED;
use super::EMPTY;
use super::MESSAGE;
use super::RECEIVING;
#[cfg(doc)]
use super::Receiver;
use super::deallocate_empty_channel;
use super::drop_message_and_deallocate_channel;

/// Sends a value to the associated [`Receiver`].
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
    /// Attempts to send a value on this channel, returning an error containing the message if it
    /// could not be sent.
    pub fn send(self, message: T) -> Result<(), SendError<T>> {
        // `send` takes over endpoint cleanup, so `Sender::drop` must not run afterward.
        let sender = ManuallyDrop::new(self);
        let channel_ptr = sender.channel_ptr;

        // SAFETY: The channel exists on the heap for the entire duration of this method, and we
        // only ever acquire shared references to it. Note that if the receiver disconnects it
        // does not free the channel.
        let channel = unsafe { channel_ptr.as_ref() };

        // Write the message into the channel on the heap.
        //
        // SAFETY: The receiver only ever accesses this memory location if we are in the MESSAGE
        // state, and since we are responsible for setting that state, we can guarantee that we have
        // exclusive access to this memory location to perform this write.
        unsafe { channel.write_message(message) };

        // Publish the message directly, or begin awakening a receiving task:
        //
        // * EMPTY + 1 = MESSAGE
        // * RECEIVING + 1 = AWAKING
        // * DISCONNECTED + 1 = EMPTY (invalid), however this state is never observed
        //
        // ORDERING: Release publishes the message directly for EMPTY-to-MESSAGE and orders it
        // before the waiting path's final publication. The RMW's load half is Relaxed, so
        // branches that consume receiver-published resources use an Acquire fence.
        match channel.state.fetch_add(1, Ordering::Release) {
            // The receiver is alive and has not started waiting. Send done.
            EMPTY => Ok(()),
            // The receiver is waiting. Wake it up so it can return the message.
            RECEIVING => {
                // SAFETY: fetch_add observed RECEIVING and changed it to AWAKING, transferring
                // exclusive ownership of the published waker to this sender. The message was
                // initialized before the RMW and is ready to publish.
                let (waker, receiver_owns_allocation) =
                    unsafe { channel.finish_sender_awakening(MESSAGE) };
                if receiver_owns_allocation {
                    waker.wake();
                } else {
                    // The send remains successful because this sender owned the waker before the
                    // receiver cancelled.
                    //
                    // SAFETY: Receiver cancellation transferred message and allocation cleanup to
                    // this sender. The original pointer provenance may therefore be reclaimed as a
                    // Box; the message is initialized and the waker has been moved out.
                    unsafe { drop_message_and_deallocate_channel(channel_ptr) };
                }
                Ok(())
            }
            // The receiver was already dropped. The error is responsible for freeing the channel.
            //
            // SAFETY: The acquire fence in this arm synchronizes with the receiver's write of the
            // DISCONNECTED state. Since the receiver will no longer access `channel_ptr`, the error
            // takes exclusive ownership of the channel's resources.
            // Moreover, since we just placed the message in the channel, the channel contains a
            // valid message.
            DISCONNECTED => {
                // ORDERING: The RMW read DISCONNECTED from the receiver's Release endpoint drop.
                // This Acquire completes the ownership handoff before SendError accesses the
                // allocation.
                fence(Ordering::Acquire);
                Err(SendError { channel_ptr })
            }
            state => unreachable!("unexpected channel state: {}", state),
        }
    }

    /// Returns `true` if the channel is disconnected.
    ///
    /// This occurs when the associated receiving endpoint is dropped.
    ///
    /// If `true` is returned, a future call to [`send`](Sender::send) is guaranteed to return an
    /// error.
    pub fn is_disconnected(&self) -> bool {
        // SAFETY: The channel exists on the heap for the entire duration of this method, and we
        // only ever acquire shared references to it. Note that if the receiver disconnects it
        // does not free the channel.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: Relaxed is sufficient for the method's contract: if this returns true, a
        // future call to send is guaranteed to return an error.
        //
        // Once true has been observed, it will remain true. However, if false is observed,
        // the receiver might have just disconnected but this thread has not observed it yet.
        matches!(channel.state.load(Ordering::Relaxed), DISCONNECTED)
    }

    pub(super) fn new(channel_ptr: NonNull<Channel<T>>) -> Self {
        Self { channel_ptr }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // SAFETY: The receiver only ever frees the channel if we are in the MESSAGE or
        // DISCONNECTED states.
        //
        // * If we are in the MESSAGE state, then `send` suppressed `Sender::drop`, so we should not
        //   be in this function call.
        // * If we are in the DISCONNECTED state, then the receiver either received the message,
        //   making this statement unreachable, or was dropped and observed that our side was still
        //   alive, and thus didn't free the channel.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // Disconnect directly, or begin awakening a receiving task:
        //
        // * EMPTY ^ 001 = DISCONNECTED
        // * RECEIVING ^ 001 = AWAKING
        // * DISCONNECTED ^ 001 = EMPTY (invalid), but this state is never observed
        //
        // ORDERING: Release publishes a direct disconnect and orders it before the waiting path's
        // final publication. The RMW's load half is Relaxed, so branches that consume
        // receiver-published resources use an Acquire fence.
        match channel.state.fetch_xor(0b001, Ordering::Release) {
            // The receiver is not waiting, nor is it dropped. The receiver is responsible for
            // deallocating the channel.
            EMPTY => {}
            // The receiver is waiting. Wake it up so it can detect that the channel disconnected.
            RECEIVING => {
                // SAFETY: fetch_xor observed RECEIVING and changed it to AWAKING, transferring
                // exclusive ownership of the published waker to this sender. Sender::drop has no
                // message to publish.
                let (waker, receiver_owns_allocation) =
                    unsafe { channel.finish_sender_awakening(DISCONNECTED) };
                if receiver_owns_allocation {
                    waker.wake();
                } else {
                    // SAFETY: Receiver cancellation transferred allocation cleanup to this sender.
                    // Both slots are uninitialized, and the original pointer provenance may be
                    // reclaimed as a Box.
                    unsafe { deallocate_empty_channel(self.channel_ptr) };
                }
            }
            // The receiver was already dropped. We are responsible for freeing the channel.
            DISCONNECTED => {
                // ORDERING: The RMW read DISCONNECTED from the receiver's Release endpoint drop.
                // Acquire makes all preceding receiver accesses happen before deallocation.
                fence(Ordering::Acquire);
                // SAFETY: when the receiver switches the state to DISCONNECTED they have received
                // the message or will no longer be trying to receive the message, and have
                // observed that the sender is still alive, meaning that we are responsible for
                // freeing the channel allocation. The acquire ordering above synchronizes with
                // the receiver's final write of the state.
                unsafe { deallocate_empty_channel(self.channel_ptr) };
            }
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

/// An error returned when trying to send on a disconnected channel. Returned from
/// [`Sender::send`] if the corresponding [`Receiver`] has already been dropped.
///
/// The message that could not be sent can be retrieved again with [`SendError::into_inner`].
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
    /// Get a reference to the message that failed to be sent.
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

impl<T> std::error::Error for SendError<T> {}

#[cfg(test)]
impl<T> Sender<T> {
    pub(super) fn channel_ptr(&self) -> NonNull<Channel<T>> {
        self.channel_ptr
    }
}
