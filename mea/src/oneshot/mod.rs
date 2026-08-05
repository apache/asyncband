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
// since mea needs not support synchronized receiving functions.
//
// [1] https://github.com/faern/oneshot/blob/83fd0864/src/lib.rs

//! A one-shot channel is used for sending a single message between
//! asynchronous tasks. The [`channel`] function is used to create a
//! [`Sender`] and [`Receiver`] handle pair that form the channel.
//!
//! The `Sender` handle is used by the producer to send the value.
//! The `Receiver` handle is used by the consumer to receive the value.
//!
//! Each handle can be used on separate tasks.
//!
//! Since the `send` method is not async, it can be used anywhere. This includes
//! sending between two runtimes, and using it from non-async code.
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
//! If the sender is dropped without sending, the receiver will fail with
//! [`RecvError`]:
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use mea::oneshot;
//!
//! let (tx, rx) = oneshot::channel::<u32>();
//!
//! tokio::spawn(async move {
//!     drop(tx);
//! });
//!
//! match rx.await {
//!     Ok(_) => panic!("This doesn't happen"),
//!     Err(_) => println!("the sender dropped"),
//! }
//! # }
//! ```

use std::any::type_name;
use std::cell::UnsafeCell;
use std::fmt;
use std::future::Future;
use std::future::IntoFuture;
use std::hint;
use std::mem;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

#[cfg(test)]
mod tests;

/// Creates a new oneshot channel and returns the two endpoints, [`Sender`] and [`Receiver`].
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
fn sender_finish_waker_handoff<T>(channel: &Channel<T>, state: u8) -> (Waker, bool) {
    // SAFETY: WAKER_CLAIMED gives the sender exclusive ownership of the initialized waker, and
    // the caller synchronized with the receiver's publication when it claimed that waker.
    let waker = unsafe { channel.take_waker() };

    // Publishing the terminal state releases the message to a live receiver. The returned value
    // only needs acquire semantics when cancellation replaced WAKER_CLAIMED and transferred
    // allocation cleanup to the sender.
    let previous_state = channel.state.swap(state, Ordering::Release);
    let receiver_owns_allocation = previous_state == WAKER_CLAIMED;
    if !receiver_owns_allocation {
        debug_assert_eq!(previous_state, DISCONNECTED);
        fence(Ordering::Acquire);
    }

    (waker, receiver_owns_allocation)
}

impl<T> Sender<T> {
    /// Attempts to send a value on this channel, returning an error contains the message if it
    /// could not be sent.
    pub fn send(self, message: T) -> Result<(), SendError<T>> {
        let channel_ptr = self.channel_ptr;

        // Do not run the Drop implementation if send was called, any cleanup happens below.
        mem::forget(self);

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

        // Update the state to signal there is a message on the channel:
        //
        // * EMPTY + 1 = MESSAGE
        // * RECEIVING + 1 = WAKER_CLAIMED
        // * DISCONNECTED + 1 = EMPTY (invalid), however this state is never observed
        //
        // ORDERING: release publishes the message. The common EMPTY branch does not consume any
        // receiver data; the other branches use an acquire fence before accessing resources whose
        // ownership the receiver published through the state.
        match channel.state.fetch_add(1, Ordering::Release) {
            // The receiver is alive and has not started waiting. Send done.
            EMPTY => Ok(()),
            // The receiver is waiting. Wake it up so it can return the message.
            RECEIVING => {
                fence(Ordering::Acquire);
                let (waker, receiver_owns_allocation) =
                    sender_finish_waker_handoff(channel, MESSAGE);
                if receiver_owns_allocation {
                    waker.wake();
                } else {
                    // The send claimed the waiting receiver before cancellation, so it remains
                    // successful while the sender performs the receiver's message cleanup.
                    unsafe { discard_sent_message(channel_ptr) };
                }
                Ok(())
            }
            // The receiver was already dropped. The error is responsible for freeing the channel.
            //
            // SAFETY: The acquire ordering above synchronizes with the receiver's write of the
            // DISCONNECTED state. Since the receiver disconnected it will no longer access
            // `channel_ptr`, so we can transfer exclusive ownership of the channel's resources to
            // the error.
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

        // ORDERING: We *chose* a Relaxed ordering here as it sufficient to enforce the method's
        // contract: "if true is returned, a future call to send is guaranteed to return an error."
        //
        // Once true has been observed, it will remain true. However, if false is observed,
        // the receiver might have just disconnected but this thread has not observed it yet.
        matches!(channel.state.load(Ordering::Relaxed), DISCONNECTED)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // SAFETY: The receiver only ever frees the channel if we are in the MESSAGE or
        // DISCONNECTED states.
        //
        // * If we are in the MESSAGE state, then we called mem::forget(self), so we should
        // not be in this function call.
        // * If we are in the DISCONNECTED state, then the receiver either received a MESSAGE
        // so this statement is unreachable, or was dropped and observed that our side was still
        // alive, and thus didn't free the channel.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // Update the channel state to disconnected:
        //
        // * EMPTY ^ 001 = DISCONNECTED
        // * RECEIVING ^ 001 = WAKER_CLAIMED
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
            RECEIVING => {
                fence(Ordering::Acquire);
                let (waker, receiver_owns_allocation) =
                    sender_finish_waker_handoff(channel, DISCONNECTED);
                if receiver_owns_allocation {
                    waker.wake();
                } else {
                    unsafe { dealloc(self.channel_ptr) };
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
                unsafe { dealloc(self.channel_ptr) };
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

// The Receiver can NOT be `Sync`! The current receive implementations that take `&self`
// assume no other receive operation runs in parallel.

impl<T> Unpin for Receiver<T> {}

impl<T> IntoFuture for Receiver<T> {
    type Output = Result<T, RecvError>;

    type IntoFuture = Recv<T>;

    fn into_future(self) -> Self::IntoFuture {
        let Receiver { channel_ptr } = self;
        // Do not run our Drop implementation, since the receiver lives on as the new future.
        mem::forget(self);
        Recv { channel_ptr }
    }
}

impl<T> Receiver<T> {
    /// Returns true if the associated [`Sender`] was dropped before sending a message. Or if
    /// the message has already been received.
    ///
    /// If `true` is returned, all future calls to receive the message are guaranteed to return
    /// [`RecvError`]. And future calls to this method is guaranteed to also return `true`.
    pub fn is_closed(&self) -> bool {
        // SAFETY: the existence of the `self` parameter serves as a certificate that the receiver
        // is still alive, meaning that even if the sender was dropped then it would have observed
        // the fact that we are still alive and left the responsibility of deallocating the
        // channel to us, so `self.channel` is valid
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: We *chose* a Relaxed ordering here as it is sufficient to
        // enforce the method's contract.
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
        // SAFETY: the existence of the `self` parameter serves as a certificate that the receiver
        // is still alive, meaning that even if the sender was dropped then it would have observed
        // the fact that we are still alive and left the responsibility of deallocating the
        // channel to us, so `self.channel` is valid
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: An acquire ordering is used to guarantee no subsequent loads is reordered
        // before this one. This upholds the contract that if true is returned, the next call to
        // receive the message is guaranteed to also observe the `MESSAGE` state and return the
        // message immediately.
        matches!(channel.state.load(Ordering::Acquire), MESSAGE)
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
                // ORDERING: at this point the sender has done its job and is no longer active, so
                // we need not make any side effects visible to it.
                channel.state.store(DISCONNECTED, Ordering::Relaxed);

                // ORDERING: Synchronize with the sender's write of the message.
                fence(Ordering::Acquire);

                // SAFETY: we are in the MESSAGE state so the message is present and synchronized.
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
        // SAFETY: since the receiving side is still alive the sender would have observed that and
        // left deallocating the channel allocation to us.
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
            MESSAGE => {
                // SAFETY: The MESSAGE state plus acquire ordering guarantees the sender has
                // written a message and that it has a happens-before relationship with this drop.
                unsafe { channel.drop_message() };

                // SAFETY: The acquire ordering above synchronizes with the sender's final write
                // of the state, so we can safely deallocate the channel.
                unsafe { dealloc(self.channel_ptr) };
            }
            // The sender was already dropped. We are responsible for freeing the channel.
            DISCONNECTED => {
                // SAFETY: The acquire ordering above synchronizes with the sender's final write
                // of the state, so we can safely deallocate the channel.
                unsafe { dealloc(self.channel_ptr) };
            }
            // NOTE: the receiver, unless transformed into a future, will never see the
            // RECEIVING or WAKER_CLAIMED states, so we can ignore them here.
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

/// A future that completes when the message is sent from the associated [`Sender`], or the
/// [`Sender`] is dropped before sending a message.
pub struct Recv<T> {
    channel_ptr: NonNull<Channel<T>>,
}

impl<T> fmt::Debug for Recv<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recv").finish_non_exhaustive()
    }
}

unsafe impl<T: Send> Send for Recv<T> {}

// Only an acquire synchronization, a waker move, and a terminal state store remain after the
// sender claims the handoff. Keep the budget small so an active sender usually wins without
// letting a descheduled sender occupy a runtime worker indefinitely.
const WAKER_HANDOFF_SPINS: usize = 8;

#[cold]
fn spin_waker_handoff(state: &AtomicU8) -> u8 {
    for _ in 0..WAKER_HANDOFF_SPINS {
        hint::spin_loop();
        let current = state.load(Ordering::Relaxed);
        if current != WAKER_CLAIMED {
            return current;
        }
    }

    WAKER_CLAIMED
}

fn handoff_pending<T>(cx: &Context<'_>) -> Poll<Result<T, RecvError>> {
    // The sender still owns the stored waker after the optimistic spin budget. Preserve the
    // latest-waker contract while returning control to the executor.
    cx.waker().wake_by_ref();
    Poll::Pending
}

#[cold]
fn poll_waker_handoff<T>(channel: &Channel<T>, cx: &Context<'_>) -> Poll<Result<T, RecvError>> {
    match spin_waker_handoff(&channel.state) {
        MESSAGE => {
            channel.state.store(DISCONNECTED, Ordering::Relaxed);
            fence(Ordering::Acquire);

            // SAFETY: The sender published MESSAGE before the state observed above, and the
            // acquire fence synchronizes with that publication.
            Poll::Ready(Ok(unsafe { channel.take_message() }))
        }
        WAKER_CLAIMED => handoff_pending(cx),
        DISCONNECTED => Poll::Ready(Err(RecvError::Disconnected)),
        state => unreachable!("unexpected channel state after waker handoff: {}", state),
    }
}

impl<T> Future for Recv<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: the existence of the `self` parameter serves as a certificate that the receiver
        // is still alive, meaning that even if the sender was dropped then it would have observed
        // the fact that we are still alive and left the responsibility of deallocating the
        // channel to us, so `self.channel` is valid
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: Relaxed is fine since the branches that need synchronization use dedicated
        // fences.
        match channel.state.load(Ordering::Relaxed) {
            // The sender is alive but has not sent anything yet.
            EMPTY => {
                let waker = cx.waker().clone();
                // SAFETY: We can not be in the forbidden states, and no waker in the channel.
                unsafe { channel.write_waker(waker) }
            }
            // The sender sent the message.
            MESSAGE => {
                // ORDERING: after publishing MESSAGE, the sender no longer uses the channel, so
                // this state update only needs to be visible to this receiver.
                channel.state.store(DISCONNECTED, Ordering::Relaxed);

                // ORDERING: Synchronize with the sender's write of the message and final state.
                fence(Ordering::Acquire);

                // SAFETY: we are in the MESSAGE state and have synchronized with the sender.
                Poll::Ready(Ok(unsafe { channel.take_message() }))
            }
            // We were polled again while waiting for the sender. Replace the waker with the new
            // one.
            RECEIVING => {
                // ORDERING: Success synchronizes with the previous write_waker call before we
                // drop the stored waker. Failure does not access the stored waker.
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

                        // SAFETY: We can not be in the forbidden states, and no waker in the
                        // channel.
                        unsafe { channel.write_waker(waker) }
                    }
                    // The sender sent the message while we prepared to replace the waker.
                    // We take the message and mark the channel disconnected.
                    // The sender has already taken the waker.
                    Err(MESSAGE) => {
                        // ORDERING: after publishing MESSAGE, the sender no longer uses the
                        // channel, so this state update only needs to be visible to this receiver.
                        channel.state.store(DISCONNECTED, Ordering::Relaxed);

                        // ORDERING: Synchronize with the sender's write of the message.
                        fence(Ordering::Acquire);

                        // SAFETY: The state tells us the sender has initialized the message, and
                        // the fence above synchronizes with that write.
                        Poll::Ready(Ok(unsafe { channel.take_message() }))
                    }
                    // The sender is currently waking us up.
                    Err(WAKER_CLAIMED) => poll_waker_handoff(channel, cx),
                    // The sender was dropped before sending anything while we prepared to park.
                    // The sender has taken the waker already.
                    Err(DISCONNECTED) => Poll::Ready(Err(RecvError::Disconnected)),
                    Err(state) => unreachable!("unexpected channel state: {}", state),
                }
            }
            // The sender has observed RECEIVING and owns the previous waker. Optimistically wait
            // for its short critical section before returning control to the executor.
            WAKER_CLAIMED => poll_waker_handoff(channel, cx),
            // The sender was dropped before sending anything.
            DISCONNECTED => Poll::Ready(Err(RecvError::Disconnected)),
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

impl<T> Drop for Recv<T> {
    fn drop(&mut self) {
        // SAFETY: since the receiving side is still alive the sender would have observed that and
        // left deallocating the channel allocation to us.
        let channel = unsafe { self.channel_ptr.as_ref() };

        loop {
            // ORDERING: MESSAGE and DISCONNECTED synchronize with the sender's state writes.
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
                    // drop.
                    unsafe { channel.drop_message() };

                    // SAFETY: The acquire load above synchronizes with the sender's final write of
                    // the state, so we can safely deallocate the channel.
                    unsafe { dealloc(self.channel_ptr) };
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
                        .compare_exchange(RECEIVING, EMPTY, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
                    {
                        // SAFETY: The successful exchange makes the state EMPTY, so the sender
                        // cannot take the stored waker. The acquire ordering synchronizes with the
                        // waker write.
                        unsafe { channel.drop_waker() };
                    }
                }
                // The sender owns the waker and allocation access. Optimistically wait for its
                // short critical section, then transfer cleanup if it was descheduled.
                WAKER_CLAIMED => {
                    if spin_waker_handoff(&channel.state) != WAKER_CLAIMED {
                        continue;
                    }
                    if channel
                        .state
                        .compare_exchange(
                            WAKER_CLAIMED,
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
                    unsafe { dealloc(self.channel_ptr) };
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
/// * The waker instance for the task that is currently receiving on this channel. This memory is
///   uninitialized until the receiver starts receiving.
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
        // SAFETY: The caller guarantees that no other thread will access the message field.
        let message_container = unsafe { &*self.message.get() };

        // SAFETY: The caller guarantees that the message has been initialized.
        unsafe { message_container.assume_init_ref() }
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

    #[inline(always)]
    unsafe fn take_message(&self) -> T {
        unsafe { ptr::read(self.message.get()).assume_init() }
    }

    /// # Safety
    ///
    /// * The `waker` field must not have a waker stored when calling this method.
    /// * The `state` must not be in the RECEIVING or WAKER_CLAIMED state when calling this method.
    unsafe fn write_waker(&self, waker: Waker) -> Poll<Result<T, RecvError>> {
        // Write the waker instance to the channel.
        //
        // SAFETY: we are not yet in the RECEIVING state, meaning that the sender will not
        // try to access the waker until it sees the state set to RECEIVING below.
        unsafe {
            let slot = &mut *self.waker.get();
            slot.as_mut_ptr().write(waker);
        }

        // ORDERING: we use release ordering on success so the sender can synchronize with
        // our write of the waker. We use relaxed ordering on failure since the sender does
        // not need to synchronize with our write and the individual match arms handle any
        // additional synchronization
        match self
            .state
            .compare_exchange(EMPTY, RECEIVING, Ordering::Release, Ordering::Relaxed)
        {
            // We stored our waker, now we return and let the sender wake us up.
            Ok(_) => Poll::Pending,
            // The sender sent the message while we prepared to await.
            // We take the message and mark the channel disconnected.
            Err(MESSAGE) => {
                // SAFETY: We wrote a waker above. The sender cannot have observed the RECEIVING
                // state, so it has not accessed the waker. We must drop it.
                unsafe { self.drop_waker() };

                // ORDERING: sender does not exist, so this update only needs to be visible to
                // us.
                self.state.store(DISCONNECTED, Ordering::Relaxed);

                // ORDERING: Synchronize with writing message. This branch is unlikely to be
                // taken, so it is likely more efficient to use a fence here instead of AcqRel
                // ordering on the compare_exchange operation.
                fence(Ordering::Acquire);

                // SAFETY: The MESSAGE state tells us there is a correctly initialized message,
                // and the fence above synchronizes with that write.
                Poll::Ready(Ok(unsafe { self.take_message() }))
            }
            // The sender was dropped before sending anything while we prepared to await.
            Err(DISCONNECTED) => {
                // SAFETY: We wrote a waker above. The sender cannot have observed the RECEIVING
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
        unsafe { ptr::read(self.waker.get()).assume_init() }
    }
}

unsafe fn dealloc<T>(channel: NonNull<Channel<T>>) {
    unsafe { drop(Box::from_raw(channel.as_ptr())) }
}

#[cold]
unsafe fn discard_sent_message<T>(channel_ptr: NonNull<Channel<T>>) {
    let channel = unsafe { channel_ptr.as_ref() };
    let message = unsafe { channel.take_message() };
    // Free the allocation before running user code so a panicking destructor cannot leak it.
    unsafe { dealloc(channel_ptr) };
    drop(message);
}

/// An error returned when trying to send on a closed channel. Returned from
/// [`Sender::send`] if the corresponding [`Receiver`] has already been dropped.
///
/// The message that could not be sent can be retrieved again with [`SendError::into_inner`].
pub struct SendError<T> {
    channel_ptr: NonNull<Channel<T>>,
}

// SAFETY: The SendError only contains a pointer to the channel. The constructor (if used
// correctly) guarantees exclusive ownership and access to the underlying channel. Since
// the message is Send (`T: Send`) it is safe to extract it or drop it via the SendError
// on any thread.
unsafe impl<T: Send> Send for SendError<T> {}

// SAFETY: Same basic safety as described in the Send impl above. Plus the fact that `T`
// is `Sync` allows the SendError to be shared between threads and hand out `&T` references
// as well.
unsafe impl<T: Sync> Sync for SendError<T> {}

impl<T> SendError<T> {
    /// Get a reference to the message that failed to be sent.
    pub fn as_inner(&self) -> &T {
        // SAFETY: we have exclusive ownership of the channel and require that the message has
        // been initialized upon construction.
        unsafe { self.channel_ptr.as_ref().message() }
    }

    /// Consumes the error and returns the message that failed to be sent.
    pub fn into_inner(self) -> T {
        let channel_ptr = self.channel_ptr;

        // Do not run destructor if we consumed ourselves. Freeing happens below.
        mem::forget(self);

        // SAFETY: we have ownership of the channel
        let channel: &Channel<T> = unsafe { channel_ptr.as_ref() };

        // SAFETY: we know that the message is initialized according to the safety requirements of
        // `new`
        let message = unsafe { channel.take_message() };

        // SAFETY: we own the channel
        unsafe { dealloc(channel_ptr) };

        message
    }
}

impl<T> Drop for SendError<T> {
    fn drop(&mut self) {
        // SAFETY: there is a properly initialized message
        unsafe { self.channel_ptr.as_ref().drop_message() };

        // SAFETY: we own the channel
        unsafe { dealloc(self.channel_ptr) };
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

/// The initial channel state. Active while both endpoints are still alive, no message has been
/// sent, and the receiver is not receiving.
const EMPTY: u8 = 0b011;
/// A message has been sent to the channel, but the receiver has not yet read it.
const MESSAGE: u8 = 0b100;
/// No message has yet been sent on the channel, but the receiver future ([`Recv`]) is currently
/// receiving.
const RECEIVING: u8 = 0b000;
/// The sender has claimed exclusive access to the initialized waker. A receiver may return
/// [`Poll::Pending`] or transfer allocation cleanup, but it must not access the waker.
const WAKER_CLAIMED: u8 = 0b001;
/// The channel has been closed. This means that either the sender or receiver has been dropped,
/// or the message sent to the channel has already been received.
const DISCONNECTED: u8 = 0b010;
