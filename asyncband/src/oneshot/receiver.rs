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

use std::error::Error;
use std::fmt;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::task::Context;
use std::task::Poll;

use crate::oneshot::AWAKING;
use crate::oneshot::Channel;
use crate::oneshot::DISCONNECTED;
use crate::oneshot::EMPTY;
use crate::oneshot::MESSAGE;
use crate::oneshot::RECEIVING;
#[cfg(doc)]
use crate::oneshot::Sender;
use crate::oneshot::deallocate_empty_channel;
use crate::oneshot::drop_message_and_deallocate_channel;

/// The receiving half of a [`oneshot`](crate::oneshot) channel.
///
/// Awaiting converts this receiver into a [`Recv`] future linked to the paired [`Sender`].
///
/// # Cancellation
///
/// Dropping either the receiver or its future disconnects the channel and discards any unread
/// message.
///
/// To keep a pending operation alive when another branch wins, call [`Receiver::into_future`]
/// before selecting and borrow the resulting future as `&mut Recv`. The receiver itself does not
/// implement [`Future`].
pub struct Receiver<T> {
    channel_ptr: NonNull<Channel<T>>,
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

unsafe impl<T: Send> Send for Receiver<T> {}

// Receiver must not be `Sync`: `try_recv` takes `&self` but assumes exclusive access to the
// message.

impl<T> Unpin for Receiver<T> {}

impl<T> IntoFuture for Receiver<T> {
    type Output = Result<T, RecvError>;

    type IntoFuture = Recv<T>;

    /// Converts this receiver into a future that owns the receiving endpoint.
    ///
    /// # Errors
    ///
    /// The future reports [`RecvError::Disconnected`] if the sender drops without sending or
    /// the message was already taken. See [`Recv`].
    ///
    /// # Cancellation
    ///
    /// Dropping the returned future disconnects the channel and discards any unread message,
    /// even before its first poll. Retain and borrow the future to keep waiting after selection.
    fn into_future(self) -> Self::IntoFuture {
        // `Recv` takes over receiver-side cleanup, so `Receiver::drop` must not run afterward.
        let receiver = ManuallyDrop::new(self);
        let channel_ptr = receiver.channel_ptr;
        Recv { channel_ptr }
    }
}

impl<T> Receiver<T> {
    /// Reports whether the channel can never yield a message because the paired [`Sender`] was
    /// dropped without sending, or because the message was already extracted.
    ///
    /// Once this returns `true`, subsequent attempts to receive fail.
    pub fn is_disconnected(&self) -> bool {
        // SAFETY: The existence of `self` guarantees that the receiver is still alive. If the
        // sender was dropped, it observed the live receiver and left allocation cleanup to it, so
        // `channel_ptr` remains valid.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: `DISCONNECTED` is terminal, so a `Relaxed` load suffices to report a permanent
        // state; observing `false` makes no synchronization claim against a concurrent drop.
        matches!(channel.state.load(Ordering::Relaxed), DISCONNECTED)
    }

    /// Reports whether a sent message is buffered and ready to be taken.
    ///
    /// Once this returns `true`, the next receive operation on this handle completes without
    /// waiting.
    pub fn has_message(&self) -> bool {
        // SAFETY: The existence of `self` guarantees that the receiver is still alive. If the
        // sender was dropped, it observed the live receiver and left allocation cleanup to it, so
        // `channel_ptr` remains valid.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: This method only observes the atomic state. MESSAGE is terminal for the sender,
        // and receiver operations cannot run concurrently, so atomic coherence preserves this
        // observation until the message is received. Accessing the message synchronizes separately.
        matches!(channel.state.load(Ordering::Relaxed), MESSAGE)
    }

    /// Takes the message if it is available, without waiting.
    ///
    /// A successful receive consumes the only message. Later receives report disconnection.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] if the sender is alive but has not sent a message.
    /// Returns [`TryRecvError::Disconnected`] if the sender dropped without sending or the
    /// message was already taken.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        // SAFETY: The channel remains allocated until this receiver drops or converts into `Recv`.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: Only the `MESSAGE` branch accesses sender-written storage; it performs the
        // conditional `Acquire` synchronization below.
        match channel.state.load(Ordering::Relaxed) {
            MESSAGE => {
                // `MESSAGE` is terminal for the sender, so marking disconnection needs no RMW and
                // publishes no data to another thread.
                channel.state.store(DISCONNECTED, Ordering::Relaxed);

                // ORDERING: Pairs with the sender's `Release` publication of `MESSAGE` before
                // reading the initialized payload.
                fence(Ordering::Acquire);

                // SAFETY: `MESSAGE` is uniquely owned by this receiver after synchronization.
                Ok(unsafe { channel.take_message() })
            }
            EMPTY => Err(TryRecvError::Empty),
            DISCONNECTED => Err(TryRecvError::Disconnected),
            state => unreachable!("unexpected channel state: {}", state),
        }
    }

    pub(super) fn new(channel_ptr: NonNull<Channel<T>>) -> Self {
        Self { channel_ptr }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // SAFETY: The live receiver guarantees that a dropped sender left allocation cleanup to
        // this side.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: Bidirectional ownership handoff: `Release` publishes the receiver's final
        // access when the sender must reclaim the channel, while `Acquire` synchronizes
        // with a sender-published message or disconnection before receiver-side cleanup.
        match channel.state.swap(DISCONNECTED, Ordering::AcqRel) {
            // Sender is still alive; sender-side `send` or `drop` reclaims the empty channel.
            EMPTY => {}
            // Sender already published a message; drop both the payload and the channel.
            MESSAGE => {
                // SAFETY: `MESSAGE` plus the swap's `Acquire` guarantees the initialized payload
                // and sender completion are synchronized before reclamation.
                unsafe { drop_message_and_deallocate_channel(self.channel_ptr) };
            }
            // Sender already dropped or this receiver already extracted the message; reclaim here.
            DISCONNECTED => {
                // SAFETY: Any prior sender accesses happened before the `Acquire` half of the swap
                // if the sender wrote `DISCONNECTED`.
                unsafe { deallocate_empty_channel(self.channel_ptr) };
            }
            // A plain `Receiver` never registers a waker, so waking states are unreachable here.
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

/// A future that receives the channel's only message.
///
/// Created by [`Receiver::into_future`], this future owns the receiving endpoint. A completed
/// future must not be polled again.
///
/// # Errors
///
/// Resolves to [`RecvError::Disconnected`] if the sender drops without sending or the
/// message was already taken before conversion.
///
/// # Cancellation
///
/// Dropping this future disconnects the channel and discards any unread message, even if the
/// future was never polled. To keep the operation alive when another branch wins a selection,
/// retain the same future and select on `&mut Recv`.
pub struct Recv<T> {
    channel_ptr: NonNull<Channel<T>>,
}

unsafe impl<T: Send> Send for Recv<T> {}

impl<T> fmt::Debug for Recv<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recv").finish_non_exhaustive()
    }
}

impl<T> Future for Recv<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: The existence of `self` guarantees that the receiver is still alive. If the
        // sender was dropped, it observed the live receiver and left allocation cleanup to it, so
        // `channel_ptr` remains valid.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: This load only selects a state-machine branch. Branches that access a published
        // message or waker perform their own Acquire operation.
        match channel.state.load(Ordering::Relaxed) {
            // The sender is alive but has not sent anything yet.
            EMPTY => {
                let waker = cx.waker().clone();
                // SAFETY: EMPTY means no waker is initialized or owned by the sender.
                unsafe { channel.register_waker(waker) }
            }
            // The sender sent the message.
            MESSAGE => {
                // ORDERING: The sender has completed, so this receiver-only terminal update does
                // not publish data to another thread.
                channel.state.store(DISCONNECTED, Ordering::Relaxed);

                // ORDERING: The preceding Relaxed load read MESSAGE from the sender's Release
                // publication. This conditional Acquire makes the message visible before it is
                // taken.
                fence(Ordering::Acquire);

                // SAFETY: we are in the MESSAGE state and have synchronized with the sender.
                Poll::Ready(Ok(unsafe { channel.take_message() }))
            }
            // We were polled again while waiting for the sender. Replace the waker unless it
            // already wakes the polling task.
            RECEIVING => {
                // ORDERING: On success, Acquire synchronizes with the Release that published the
                // stored waker before this poll reclaims it. Failure does not access that waker.
                match channel.state.compare_exchange(
                    RECEIVING,
                    EMPTY,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    // The state is EMPTY again.
                    Ok(_) => {
                        // SAFETY: The successful exchange makes the state EMPTY, so the sender
                        // cannot take the stored waker. The acquire ordering synchronizes with the
                        // waker write.
                        unsafe { channel.reregister_waker(cx.waker()) }
                    }
                    // The sender sent the message while we prepared to replace the waker.
                    // We take the message and mark the channel disconnected.
                    // The sender has already taken the waker.
                    Err(MESSAGE) => {
                        // ORDERING: The sender has completed, so this receiver-only terminal update
                        // does not publish data to another thread.
                        channel.state.store(DISCONNECTED, Ordering::Relaxed);

                        // ORDERING: The failed CAS read MESSAGE from the sender's Release
                        // publication. This conditional Acquire makes the message visible before it
                        // is taken.
                        fence(Ordering::Acquire);

                        // SAFETY: The state tells us the sender has initialized the message, and
                        // the fence above synchronizes with that write.
                        Poll::Ready(Ok(unsafe { channel.take_message() }))
                    }
                    // The sender started awakening us while we prepared to replace the waker.
                    Err(AWAKING) => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    // The sender was dropped before sending anything while we prepared to park.
                    // The sender has taken the waker already.
                    Err(DISCONNECTED) => Poll::Ready(Err(RecvError::Disconnected)),
                    Err(state) => unreachable!("unexpected channel state: {}", state),
                }
            }
            // The sender is publishing the final state and owns the stored waker. Schedule this
            // poll's potentially different waker and return without waiting for the
            // sender to make progress.
            AWAKING => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            // The sender was dropped before sending anything.
            DISCONNECTED => Poll::Ready(Err(RecvError::Disconnected)),
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

impl<T> Drop for Recv<T> {
    fn drop(&mut self) {
        // SAFETY: The live receiver guarantees that a dropped sender left allocation cleanup to
        // this side.
        let channel = unsafe { self.channel_ptr.as_ref() };

        loop {
            // ORDERING: Acquire synchronizes terminal sender publications before cleanup and the
            // receiver's earlier waker publication before reclaiming it. EMPTY and AWAKING need
            // only the atomic state observation, but using one load keeps the cleanup
            // paths fence-free.
            match channel.state.load(Ordering::Acquire) {
                // The sender has not sent anything, nor is it dropped. Mark the receiver as
                // dropped; the sender is responsible for deallocating the channel.
                EMPTY => {
                    if channel
                        .state
                        .compare_exchange(EMPTY, DISCONNECTED, Ordering::Release, Ordering::Relaxed)
                        .is_ok()
                    {
                        break;
                    }
                }
                // The sender already sent something. We must drop it, and free the channel.
                MESSAGE => {
                    // SAFETY: The MESSAGE state plus acquire ordering guarantees the sender has
                    // written a message and that it has a happens-before relationship with this
                    // drop. The same load orders sender accesses before allocation reclamation.
                    unsafe { drop_message_and_deallocate_channel(self.channel_ptr) };
                    break;
                }
                // This receiver was previously polled, but was not polled to completion. Move away
                // from RECEIVING before dropping the waker so the sender cannot take the same
                // waker.
                //
                // A successful exchange creates a short EMPTY window before the next iteration can
                // mark DISCONNECTED. This branch owns and drops the stored waker first. A sender
                // that observes EMPTY does not touch the waker. It either stores MESSAGE and
                // leaves the message and allocation to this loop, or stores DISCONNECTED and
                // leaves the allocation to this loop. If this loop marks DISCONNECTED first, the
                // sender observes DISCONNECTED and owns any send error cleanup.
                RECEIVING => {
                    if channel
                        .state
                        .compare_exchange(RECEIVING, EMPTY, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok()
                    {
                        // SAFETY: The successful exchange makes the state EMPTY, so the sender
                        // cannot take the stored waker. The preceding Acquire load synchronized
                        // with its publication. No transition can recreate RECEIVING, so the
                        // successful Relaxed CAS still claims that same waker.
                        unsafe { channel.drop_waker() };
                    }
                }
                // The sender owns the waker. Transfer allocation cleanup to it instead of waiting
                // for it to publish the terminal state.
                AWAKING => {
                    if channel
                        .state
                        .compare_exchange(
                            AWAKING,
                            DISCONNECTED,
                            Ordering::Release,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                // The sender was already dropped, or this future was previously polled to
                // completion. We are responsible for freeing the channel.
                DISCONNECTED => {
                    // SAFETY: If the sender published DISCONNECTED, the Acquire load makes its
                    // preceding accesses happen before reclamation. If this future wrote
                    // DISCONNECTED after taking the message, no cross-thread synchronization is
                    // needed.
                    unsafe { deallocate_empty_channel(self.channel_ptr) };
                    break;
                }
                state => unreachable!("unexpected channel state: {}", state),
            }
        }
    }
}

/// Error returned by [`Receiver::try_recv`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TryRecvError {
    /// No message is currently available, but the sender remains and may still send one.
    Empty,
    /// No message can become available because the sender was dropped without sending or the
    /// message has already been received.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TryRecvError::Empty => "receiving on an empty channel",
            TryRecvError::Disconnected => "receiving on a disconnected channel",
        })
    }
}

impl Error for TryRecvError {}

/// Error returned when awaiting a [`Receiver`] or [`Recv`] future that can no longer yield a
/// message.
///
/// Reported when the paired [`Sender`] was dropped without sending, or when the single message
/// was already extracted via [`Receiver::try_recv`] before awaiting.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RecvError {
    /// No message can become available because the sender was dropped without sending or the
    /// message has already been received.
    Disconnected,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("receiving on a disconnected channel")
    }
}

impl Error for RecvError {}
