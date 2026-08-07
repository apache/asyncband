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

// This implementation is derived from the `oneshot` crate [1], with significant simplifications
// because this crate supports only asynchronous receive operations.
//
// [1] https://github.com/faern/oneshot/blob/83fd0864/src/lib.rs

//! A one-shot channel is used for sending a single message between asynchronous tasks. The
//! [`channel`] function is used to create a [`Sender`] and [`Receiver`] pair that form the channel.
//!
//! The sender is used by the producer to send the value. The receiver is used by the consumer
//! to receive the value.
//!
//! The sender and receiver can be used by separate tasks.
//!
//! Since [`Sender::send`] is not async, it can be used anywhere. This includes sending between
//! two runtimes, and using it from non-async code.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use mea::oneshot;
//!
//! let (tx, rx) = oneshot::channel();
//!
//! tokio::spawn(async move {
//!     if let Err(_) = tx.send(3) {
//!         println!("the receiver dropped");
//!     }
//! });
//!
//! match rx.await {
//!     Ok(v) => println!("got = {:?}", v),
//!     Err(_) => println!("the sender dropped"),
//! }
//! # }
//! ```
//!
//! If the sender is dropped without sending, the receiver will fail with [`RecvError`]:
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use mea::oneshot;
//!
//! let (tx, rx) = oneshot::channel::<u32>();
//!
//! tokio::spawn(async move { drop(tx) });
//!
//! match rx.await {
//!     Ok(_) => panic!("This doesn't happen"),
//!     Err(_) => println!("the sender dropped"),
//! }
//! # }
//! ```
//!
//! If the receiver is dropped before receiving, the sender will fail with [`SendError`]:
//!
//! ```
//! use mea::oneshot;
//!
//! let (tx, rx) = oneshot::channel::<u32>();
//!
//! drop(rx);
//!
//! match tx.send(42) {
//!     Ok(_) => panic!("This doesn't happen"),
//!     Err(_) => println!("the receiver dropped"),
//! }
//! ```

use std::any::type_name;
use std::cell::UnsafeCell;
use std::fmt;
use std::future::Future;
use std::future::IntoFuture;
use std::mem::ManuallyDrop;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

#[cfg(test)]
mod tests;

/// Creates a new oneshot channel and returns the [`Sender`] and [`Receiver`].
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let channel_ptr = NonNull::from(Box::leak(Box::new(Channel::new())));
    (Sender { channel_ptr }, Receiver { channel_ptr })
}

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

#[inline(always)]
fn take_waker_and_publish<T>(channel: &Channel<T>, terminal_state: u8) -> (Waker, bool) {
    debug_assert!(matches!(terminal_state, READY | DISCONNECTED));

    // SAFETY: The sender changed REGISTERED to CLAIMED and synchronized with the receiver's
    // publication, so it exclusively owns the initialized waker.
    let waker = unsafe { channel.take_waker() };

    // Release publishes the terminal state. If receiver cancellation replaced CLAIMED with
    // DISCONNECTED, the acquire fence synchronizes with that transfer before the sender frees
    // the allocation.
    let previous_state = channel.state.swap(terminal_state, Ordering::Release);
    let receiver_owns_allocation = previous_state == CLAIMED;
    if !receiver_owns_allocation {
        debug_assert_eq!(previous_state, DISCONNECTED);
        fence(Ordering::Acquire);
    }

    (waker, receiver_owns_allocation)
}

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
    /// Returns true if the associated [`Sender`] was dropped before sending a message, or if the
    /// message has already been received.
    ///
    /// If `true` is returned, all future calls to receive the message are guaranteed to return
    /// [`RecvError`]. Future calls to this method are also guaranteed to return `true`.
    pub fn is_closed(&self) -> bool {
        // SAFETY: The existence of `self` guarantees that the receiver is still alive. If the
        // sender was dropped, it observed the live receiver and left allocation cleanup to it, so
        // `channel_ptr` remains valid.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: Relaxed is sufficient to enforce the method's contract.
        //
        // Once true has been observed, it will remain true. However, if false is observed,
        // the sender might have just disconnected but this thread has not observed it yet.
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

        // ORDERING: Acquire guarantees that no subsequent load is reordered
        // before this one. This upholds the contract that if true is returned, the next call to
        // receive the message is guaranteed to also observe the `READY` state and return the
        // message immediately.
        matches!(channel.state.load(Ordering::Acquire), READY)
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

        // ORDERING: Relaxed is fine since the only branch that needs synchronization is READY,
        // and that branch has its own synchronization.
        match channel.state.load(Ordering::Relaxed) {
            READY => {
                // It is okay to break up the load and store since once we are in the READY state,
                // the sender no longer modifies the state
                //
                // ORDERING: at this point the sender has done its job and is no longer active, so
                // we need not make any side effects visible to it.
                channel.state.store(DISCONNECTED, Ordering::Relaxed);

                // ORDERING: Synchronize with the sender's write of the message.
                fence(Ordering::Acquire);

                // SAFETY: we are in the READY state so the message is present and synchronized.
                Ok(unsafe { channel.take_message() })
            }
            EMPTY => Err(TryRecvError::Empty),
            DISCONNECTED => Err(TryRecvError::Disconnected),
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // SAFETY: The live receiver guarantees that a dropped sender left allocation cleanup to
        // this side.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // Set the channel state to disconnected and read what state the channel was in.
        //
        // ORDERING: Release is required so that in the states where the sender becomes responsible
        // for deallocating the channel, they can synchronize with this final state write from us.
        // Acquire is required by the branches below to synchronize with writes from the sender.
        match channel.state.swap(DISCONNECTED, Ordering::AcqRel) {
            // The sender has not sent anything, nor is it dropped. The sender is responsible for
            // deallocating the channel.
            EMPTY => {}
            // The sender already sent something. We must drop it, and free the channel.
            READY => {
                // SAFETY: The READY state plus acquire ordering guarantees the sender has
                // written a message and that it has a happens-before relationship with this drop.
                // In addition, the acquire ordering above synchronizes with the sender's final
                // write of the state, so we can safely deallocate the channel.
                unsafe { drop_message_and_dealloc_channel(self.channel_ptr) };
            }
            // The sender was already dropped. We are responsible for freeing the channel.
            DISCONNECTED => {
                // SAFETY: The acquire ordering above synchronizes with the sender's final write
                // of the state, so we can safely deallocate the channel.
                unsafe { dealloc_empty_channel(self.channel_ptr) };
            }
            // NOTE: the receiver, unless transformed into a future, will never see the
            // REGISTERED or CLAIMED states, so we can ignore them here.
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

        // ORDERING: Relaxed is fine since the branches that need synchronization use dedicated
        // fences.
        match channel.state.load(Ordering::Relaxed) {
            // The sender is alive but has not sent anything yet.
            EMPTY => {
                let waker = cx.waker().clone();
                // SAFETY: EMPTY means no waker is initialized or owned by the sender.
                unsafe { channel.register_waker(waker) }
            }
            // The sender sent the message.
            READY => {
                // ORDERING: after publishing READY, the sender no longer uses the channel, so
                // this state update only needs to be visible to this receiver.
                channel.state.store(DISCONNECTED, Ordering::Relaxed);

                // ORDERING: Synchronize with the sender's write of the message and final state.
                fence(Ordering::Acquire);

                // SAFETY: we are in the READY state and have synchronized with the sender.
                Poll::Ready(Ok(unsafe { channel.take_message() }))
            }
            // We were polled again while waiting for the sender. Replace the waker with the new
            // one.
            REGISTERED => {
                // ORDERING: Success synchronizes with the previous register_waker call before we
                // drop the stored waker. Failure does not access the stored waker.
                match channel.state.compare_exchange(
                    REGISTERED,
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
                    Err(READY) => {
                        // ORDERING: after publishing READY, the sender no longer uses the
                        // channel, so this state update only needs to be visible to this receiver.
                        channel.state.store(DISCONNECTED, Ordering::Relaxed);

                        // ORDERING: Synchronize with the sender's write of the message.
                        fence(Ordering::Acquire);

                        // SAFETY: The state tells us the sender has initialized the message, and
                        // the fence above synchronizes with that write.
                        Poll::Ready(Ok(unsafe { channel.take_message() }))
                    }
                    // The sender claimed the registered waker while we prepared to replace it.
                    Err(CLAIMED) => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    // The sender was dropped before sending anything while we prepared to park.
                    // The sender has taken the waker already.
                    Err(DISCONNECTED) => Poll::Ready(Err(RecvError::Disconnected)),
                    Err(state) => unreachable!("unexpected channel state: {}", state),
                }
            }
            // The sender owns the registered waker. Schedule this poll's potentially different
            // waker and return without waiting for the sender to make progress.
            CLAIMED => {
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
            // ORDERING: READY and DISCONNECTED synchronize with the sender's state writes.
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
                READY => {
                    // SAFETY: The READY state plus acquire ordering guarantees the sender has
                    // written a message and that it has a happens-before relationship with this
                    // drop. In addition, the acquire load above synchronizes with the sender's
                    // final write of the state, so we can safely deallocate the channel.
                    unsafe { drop_message_and_dealloc_channel(self.channel_ptr) };
                    break;
                }
                // This receiver was previously polled, but was not polled to completion. Move away
                // from REGISTERED before dropping the waker so the sender cannot take the same
                // waker.
                //
                // A successful exchange creates a short EMPTY window before the next iteration can
                // mark DISCONNECTED. This branch owns and drops the stored waker first. A sender
                // that observes EMPTY does not touch the waker. It either stores READY and
                // leaves the message and allocation to this loop, or stores DISCONNECTED and
                // leaves the allocation to this loop. If this loop marks DISCONNECTED first, the
                // sender observes DISCONNECTED and owns any send error cleanup.
                REGISTERED => {
                    if channel
                        .state
                        .compare_exchange(REGISTERED, EMPTY, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
                    {
                        // SAFETY: The successful exchange makes the state EMPTY, so the sender
                        // cannot take the stored waker. The acquire ordering synchronizes with the
                        // waker write.
                        unsafe { channel.drop_waker() };
                    }
                }
                // The sender owns the waker. Transfer allocation cleanup to it instead of waiting
                // for it to publish the terminal state.
                CLAIMED => {
                    if channel
                        .state
                        .compare_exchange(
                            CLAIMED,
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
                    // SAFETY: When DISCONNECTED comes from the sender, the acquire load
                    // synchronizes with the sender's state write. When it comes from our own
                    // completed poll, the message has already been taken.
                    unsafe { dealloc_empty_channel(self.channel_ptr) };
                    break;
                }
                state => unreachable!("unexpected channel state: {}", state),
            }
        }
    }
}

/// Internal channel data structure.
///
/// The [`channel`] method allocates and puts one instance of this struct on the heap for each
/// oneshot channel instance. The struct holds:
///
/// * The current state of the channel.
/// * The message in the channel. This memory is uninitialized until the message is sent.
/// * The receiver waker. This memory is initialized only while the state is REGISTERED or CLAIMED.
struct Channel<T> {
    state: AtomicU8,
    message: UnsafeCell<MaybeUninit<T>>,
    waker: UnsafeCell<MaybeUninit<Waker>>,
}

impl<T> Channel<T> {
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(EMPTY),
            message: UnsafeCell::new(MaybeUninit::uninit()),
            waker: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    #[inline(always)]
    unsafe fn message(&self) -> &T {
        unsafe {
            let slot = &*self.message.get();
            slot.assume_init_ref()
        }
    }

    #[inline(always)]
    unsafe fn take_message(&self) -> T {
        unsafe {
            let slot = &*self.message.get();
            slot.assume_init_read()
        }
    }

    #[inline(always)]
    unsafe fn write_message(&self, message: T) {
        unsafe {
            let slot = &mut *self.message.get();
            slot.as_mut_ptr().write(message);
        }
    }

    #[inline(always)]
    unsafe fn drop_message(&self) {
        unsafe {
            let slot = &mut *self.message.get();
            slot.assume_init_drop();
        }
    }

    /// # Safety
    ///
    /// * The `waker` field must not contain an initialized waker when calling this method.
    /// * The `state` must not be in the REGISTERED or CLAIMED state when calling this method.
    unsafe fn register_waker(&self, waker: Waker) -> Poll<Result<T, RecvError>> {
        // SAFETY: The sender cannot access the waker until the state becomes REGISTERED.
        unsafe {
            let slot = &mut *self.waker.get();
            slot.as_mut_ptr().write(waker);
        }

        // ORDERING: Release publishes the initialized waker. On failure, the sender did not
        // observe REGISTERED and cannot access the waker, so each branch provides only the
        // synchronization needed for its terminal state.
        match self
            .state
            .compare_exchange(EMPTY, REGISTERED, Ordering::Release, Ordering::Relaxed)
        {
            // The waker is registered for the sender to take and wake.
            Ok(_) => Poll::Pending,
            // The sender sent the message while we prepared to await.
            // We take the message and mark the channel disconnected.
            Err(READY) => {
                // SAFETY: We wrote a waker above. The sender cannot have observed the REGISTERED
                // state, so it has not accessed the waker. We must drop it.
                unsafe { self.drop_waker() };

                // ORDERING: sender does not exist, so this update only needs to be visible to us.
                self.state.store(DISCONNECTED, Ordering::Relaxed);

                // ORDERING: Synchronize with writing message. This branch is unlikely to be
                // taken, so it is likely more efficient to use a fence here instead of AcqRel
                // ordering on the compare_exchange operation.
                fence(Ordering::Acquire);

                // SAFETY: The READY state tells us there is a correctly initialized message,
                // and the fence above synchronizes with that write.
                Poll::Ready(Ok(unsafe { self.take_message() }))
            }
            // The sender was dropped before sending anything while we prepared to await.
            Err(DISCONNECTED) => {
                // SAFETY: We wrote a waker above. The sender cannot have observed the REGISTERED
                // state, so it has not accessed the waker. We must drop it.
                unsafe { self.drop_waker() };
                Poll::Ready(Err(RecvError::Disconnected))
            }
            Err(state) => unreachable!("unexpected channel state: {}", state),
        }
    }

    #[inline(always)]
    unsafe fn drop_waker(&self) {
        unsafe {
            let slot = &mut *self.waker.get();
            slot.assume_init_drop();
        }
    }

    #[inline(always)]
    unsafe fn take_waker(&self) -> Waker {
        unsafe {
            let slot = &*self.waker.get();
            slot.assume_init_read()
        }
    }
}

unsafe fn dealloc_empty_channel<T>(channel: NonNull<Channel<T>>) {
    // SAFETY: The caller owns the allocation and guarantees that no channel access follows.
    unsafe { drop(Box::from_raw(channel.as_ptr())) };
}

unsafe fn drop_message_and_dealloc_channel<T>(channel_ptr: NonNull<Channel<T>>) {
    // SAFETY: The caller transfers exclusive allocation ownership to this function, so this is the
    // only Box reconstructed from `channel_ptr`.
    let channel = unsafe { Box::from_raw(channel_ptr.as_ptr()) };

    // SAFETY: The caller guarantees that the message is initialized and exclusively owned. The
    // local Box deallocates the channel on normal return and during unwinding if `T::drop` panics.
    // Since the message is stored in MaybeUninit, dropping the Box will not drop it a second time.
    unsafe { channel.drop_message() };
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

/// Error returned by [`Receiver::try_recv`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TryRecvError {
    /// This channel is currently empty, but the sender has not yet disconnected, so data may yet
    /// become available.
    Empty,
    /// The sender has become disconnected, and there will never be any more data received on it.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TryRecvError::Empty => "receiving on an empty channel",
            TryRecvError::Disconnected => "receiving on a closed channel",
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
    /// The sender has become disconnected, and there will never be any more data received on it.
    Disconnected,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("receiving on a closed channel")
    }
}

impl std::error::Error for RecvError {}

/// Both endpoints are alive, no message is available, and no receiver waker is registered.
const EMPTY: u8 = 0b011;
/// The receiver has published an initialized waker and retains ownership until the sender claims
/// it.
const REGISTERED: u8 = 0b000;
/// The sender exclusively owns the initialized waker and will publish either `READY` or
/// `DISCONNECTED`. The receiver must not access the waker, but it may return [`Poll::Pending`] or
/// transfer allocation cleanup.
const CLAIMED: u8 = 0b001;
/// The sender has published a message that the receiver has not yet read.
const READY: u8 = 0b100;
/// The channel is terminal because an endpoint was dropped or the receiver consumed the message.
const DISCONNECTED: u8 = 0b010;
