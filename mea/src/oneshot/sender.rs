// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::type_name;
use std::fmt;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;

use crate::oneshot::Channel;
use crate::oneshot::DISCONNECTED;
use crate::oneshot::EMPTY;
use crate::oneshot::READY;
use crate::oneshot::REGISTERED;
#[cfg(doc)]
use crate::oneshot::Receiver;
use crate::oneshot::dealloc_empty_channel;
use crate::oneshot::drop_message_and_dealloc_channel;
use crate::oneshot::take_waker_and_publish;

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
        // SAFETY: The receiver only ever accesses this memory location if we are in the READY
        // state, and since we are responsible for setting that state, we can guarantee that we have
        // exclusive access to this memory location to perform this write.
        unsafe { channel.write_message(message) };

        // Claim a registered waker, or publish the message if no waker is registered:
        //
        // * EMPTY + 1 = READY
        // * REGISTERED + 1 = CLAIMED
        // * DISCONNECTED + 1 = EMPTY (invalid), however this state is never observed
        //
        // ORDERING: release publishes the message. The common EMPTY branch does not consume any
        // receiver data; the other branches use an acquire fence before accessing resources whose
        // ownership the receiver published through the state.
        match channel.state.fetch_add(1, Ordering::Release) {
            // The receiver is alive and has not started waiting. Send done.
            EMPTY => Ok(()),
            // The receiver is waiting. Wake it up so it can return the message.
            REGISTERED => {
                fence(Ordering::Acquire);
                let (waker, receiver_owns_allocation) = take_waker_and_publish(channel, READY);
                if receiver_owns_allocation {
                    waker.wake();
                } else {
                    // The sender claimed the registered waker before cancellation, so it remains
                    // successful while the sender performs the receiver's message cleanup.
                    unsafe { drop_message_and_dealloc_channel(channel_ptr) };
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
                fence(Ordering::Acquire);
                Err(SendError { channel_ptr })
            }
            state => unreachable!("unexpected channel state: {}", state),
        }
    }

    /// Returns true if the associated [`Receiver`] has been dropped.
    ///
    /// If true is returned, a future call to send is guaranteed to return an error.
    pub fn is_closed(&self) -> bool {
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

    #[cfg(test)]
    pub(super) fn channel_ptr(&self) -> NonNull<Channel<T>> {
        self.channel_ptr
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // SAFETY: The receiver only ever frees the channel if we are in the READY or
        // DISCONNECTED states.
        //
        // * If we are in the READY state, then `send` suppressed `Sender::drop`, so we should not
        //   be in this function call.
        // * If we are in the DISCONNECTED state, then the receiver either received the message,
        //   making this statement unreachable, or was dropped and observed that our side was still
        //   alive, and thus didn't free the channel.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // Claim a registered waker, or disconnect the channel if no waker is registered:
        //
        // * EMPTY ^ 001 = DISCONNECTED
        // * REGISTERED ^ 001 = CLAIMED
        // * DISCONNECTED ^ 001 = EMPTY (invalid), but this state is never observed
        //
        // ORDERING: release publishes the sender's final state. The common EMPTY branch does not
        // consume receiver data; the other branches use an acquire fence before accessing
        // resources whose ownership the receiver published through the state.
        match channel.state.fetch_xor(0b001, Ordering::Release) {
            // The receiver is not waiting, nor is it dropped. The receiver is responsible for
            // deallocating the channel.
            EMPTY => {}
            // The receiver is waiting. Wake it up so it can detect that the channel disconnected.
            REGISTERED => {
                fence(Ordering::Acquire);
                let (waker, receiver_owns_allocation) =
                    take_waker_and_publish(channel, DISCONNECTED);
                if receiver_owns_allocation {
                    waker.wake();
                } else {
                    unsafe { dealloc_empty_channel(self.channel_ptr) };
                }
            }
            // The receiver was already dropped. We are responsible for freeing the channel.
            DISCONNECTED => {
                fence(Ordering::Acquire);
                // SAFETY: when the receiver switches the state to DISCONNECTED they have received
                // the message or will no longer be trying to receive the message, and have
                // observed that the sender is still alive, meaning that we are responsible for
                // freeing the channel allocation. The acquire ordering above synchronizes with
                // the receiver's final write of the state.
                unsafe { dealloc_empty_channel(self.channel_ptr) };
            }
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

/// An error returned when trying to send on a closed channel. Returned from
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

        // SAFETY: SendError exclusively owns the allocation.
        unsafe { dealloc_empty_channel(channel_ptr) };

        message
    }
}

impl<T> Drop for SendError<T> {
    fn drop(&mut self) {
        // SAFETY: SendError exclusively owns the channel.
        unsafe { drop_message_and_dealloc_channel(self.channel_ptr) };
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sending on a closed channel")
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SendError<{}>(..)", type_name::<T>())
    }
}

impl<T> std::error::Error for SendError<T> {}
