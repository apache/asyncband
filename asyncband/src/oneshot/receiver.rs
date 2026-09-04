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

// Derived from the oneshot crate at commit 83fd0864:
// https://github.com/faern/oneshot/blob/83fd0864be7289067ce96cc79cd96c0928742979/src/lib.rs

use std::fmt;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::task::Context;
use std::task::Poll;

use super::AWAKING;
use super::Channel;
use super::DISCONNECTED;
use super::EMPTY;
use super::MESSAGE;
use super::RECEIVING;
#[cfg(doc)]
use super::Sender;
use super::deallocate_empty_channel;
use super::drop_message_and_deallocate_channel;

/// Receives a value from the associated [`Sender`].
pub struct Receiver<T> {
    channel_ptr: NonNull<Channel<T>>,
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

unsafe impl<T: Send> Send for Receiver<T> {}

// Receiver must not be `Sync`: receive operations taking `&self` assume that no other receive
// operation runs concurrently.

impl<T> Unpin for Receiver<T> {}

impl<T> IntoFuture for Receiver<T> {
    type Output = Result<T, RecvError>;

    type IntoFuture = Recv<T>;

    fn into_future(self) -> Self::IntoFuture {
        // `Recv` takes over receiver-side cleanup, so `Receiver::drop` must not run afterward.
        let receiver = ManuallyDrop::new(self);
        let channel_ptr = receiver.channel_ptr;
        Recv { channel_ptr }
    }
}

impl<T> Receiver<T> {
    /// Returns `true` if the channel is disconnected.
    ///
    /// This occurs when the associated [`Sender`] is dropped without sending a message, or after
    /// the message is received.
    ///
    /// If `true` is returned, all future receive operations are guaranteed to return an error.
    pub fn is_disconnected(&self) -> bool {
        // SAFETY: The existence of `self` guarantees that the receiver is still alive. If the
        // sender was dropped, it observed the live receiver and left allocation cleanup to it, so
        // `channel_ptr` remains valid.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: Relaxed is sufficient to enforce the method's contract.
        //
        // Once true has been observed, it will remain true. However, if false is observed, the
        // sender might just have been dropped without this thread observing it yet.
        matches!(channel.state.load(Ordering::Relaxed), DISCONNECTED)
    }

    /// Returns true if there is a message in the channel, ready to be received.
    ///
    /// If `true` is returned, the next call to receive the message is guaranteed to return
    /// the message immediately.
    pub fn has_message(&self) -> bool {
        // SAFETY: The existence of `self` guarantees that the receiver is still alive. If the
        // sender was dropped, it observed the live receiver and left allocation cleanup to it, so
        // `channel_ptr` remains valid.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: This method only observes the atomic state. MESSAGE is terminal for the sender,
        // and receiver operations cannot run concurrently, so atomic coherence preserves this
        // observation for the next receive. Accessing the message synchronizes separately.
        matches!(channel.state.load(Ordering::Relaxed), MESSAGE)
    }

    /// Checks if there is a message in the channel without blocking. Returns:
    ///
    /// * `Ok(message)` if there was a message in the channel.
    /// * `Err(TryRecvError::Empty)` if the [`Sender`] is alive, but has not yet sent a message.
    /// * `Err(TryRecvError::Disconnected)` if the [`Sender`] was dropped before sending anything or
    ///   if the message has already been extracted by a previous `try_recv` call.
    ///
    /// If a message is returned, the channel is disconnected and any subsequent receive operation
    /// using this receiver will return an error: [`TryRecvError::Disconnected`] for `try_recv`,
    /// or [`RecvError::Disconnected`] for [`recv`](Receiver::into_future).
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        // SAFETY: The channel will not be freed while this method is still running.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: Relaxed is fine since the only branch that needs synchronization is MESSAGE,
        // and that branch has its own synchronization.
        match channel.state.load(Ordering::Relaxed) {
            MESSAGE => {
                // It is okay to break up the load and store since once we are in the MESSAGE state,
                // the sender no longer modifies the state
                //
                // ORDERING: The sender has completed, so this receiver-only terminal update does
                // not publish data to another thread.
                channel.state.store(DISCONNECTED, Ordering::Relaxed);

                // ORDERING: The preceding Relaxed load read MESSAGE from the sender's Release
                // publication. This conditional Acquire makes the message visible before it is
                // taken.
                fence(Ordering::Acquire);

                // SAFETY: we are in the MESSAGE state so the message is present and synchronized.
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

        // Set the channel state to disconnected and read what state the channel was in.
        //
        // ORDERING: This is a bidirectional ownership handoff. Release publishes the receiver's
        // last access when the sender must reclaim the allocation; Acquire receives a
        // sender-published message or disconnected state before receiver-side cleanup.
        match channel.state.swap(DISCONNECTED, Ordering::AcqRel) {
            // The sender has not sent anything, nor is it dropped. The sender is responsible for
            // deallocating the channel.
            EMPTY => {}
            // The sender already sent something. We must drop it, and free the channel.
            MESSAGE => {
                // SAFETY: The MESSAGE state plus acquire ordering guarantees the sender has
                // written a message and that it has a happens-before relationship with this drop.
                // In addition, the acquire ordering above synchronizes with the sender's final
                // write of the state, so we can safely deallocate the channel.
                unsafe { drop_message_and_deallocate_channel(self.channel_ptr) };
            }
            // The sender was already dropped. We are responsible for freeing the channel.
            DISCONNECTED => {
                // SAFETY: If the sender published DISCONNECTED, the swap's Acquire half makes its
                // preceding accesses happen before reclamation. If this receiver previously wrote
                // DISCONNECTED after taking the message, no cross-thread synchronization is needed.
                unsafe { deallocate_empty_channel(self.channel_ptr) };
            }
            // NOTE: the receiver, unless transformed into a future, will never see the RECEIVING or
            // AWAKING states, so we can ignore them here.
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

/// A future that completes when the message is sent from the associated [`Sender`], or the
/// [`Sender`] is dropped before sending a message.
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
            // We were polled again while waiting for the sender. Replace the waker with the new
            // one.
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
                        let waker = cx.waker().clone();

                        // SAFETY: The successful exchange makes the state EMPTY, so the sender
                        // cannot take the stored waker. The acquire ordering synchronizes with the
                        // waker write.
                        unsafe { channel.drop_waker() };

                        // SAFETY: The old waker was dropped while the state was EMPTY, so no waker
                        // remains initialized or owned by the sender.
                        unsafe { channel.register_waker(waker) }
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

impl std::error::Error for TryRecvError {}

/// An error returned when awaiting the message via [`Receiver`].
///
/// This error indicates that the corresponding [`Sender`] was dropped before sending any message.
/// Note that if a message was already received (e.g., via [`Receiver::try_recv`]), subsequent
/// `try_recv` calls will return [`TryRecvError::Disconnected`] instead.
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

impl std::error::Error for RecvError {}
