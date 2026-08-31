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
//! use asyncband::oneshot;
//!
//! let (tx, rx) = oneshot::channel();
//!
//! let sender = tokio::spawn(async move {
//!     tx.send(3).unwrap();
//! });
//!
//! assert_eq!(rx.await, Ok(3));
//! sender.await.unwrap();
//! # }
//! ```
//!
//! If the sender is dropped without sending, the receiver will fail with [`RecvError`]:
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use asyncband::oneshot;
//!
//! let (tx, rx) = oneshot::channel::<u32>();
//!
//! drop(tx);
//!
//! assert_eq!(rx.await, Err(oneshot::RecvError::Disconnected));
//! # }
//! ```
//!
//! If the receiver is dropped before receiving, the sender will fail with [`SendError`]:
//!
//! ```
//! use asyncband::oneshot;
//!
//! let (tx, rx) = oneshot::channel::<u32>();
//!
//! drop(rx);
//!
//! let error = tx.send(42).unwrap_err();
//! assert_eq!(error.into_inner(), 42);
//! ```

mod receiver;
mod sender;

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::task::Poll;
use std::task::Waker;

pub use self::receiver::Receiver;
pub use self::receiver::Recv;
pub use self::receiver::RecvError;
pub use self::receiver::TryRecvError;
pub use self::sender::SendError;
pub use self::sender::Sender;

#[cfg(test)]
mod tests;

/// Creates a new oneshot channel and returns the [`Sender`] and [`Receiver`].
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let channel_ptr = NonNull::from(Box::leak(Box::new(Channel::new())));
    (Sender::new(channel_ptr), Receiver::new(channel_ptr))
}

const EMPTY: u8 = 0b011;
const RECEIVING: u8 = 0b000;
const AWAKING: u8 = 0b001;
const MESSAGE: u8 = 0b100;
const DISCONNECTED: u8 = 0b010;

/// Shared storage and state machine for a oneshot channel.
///
/// The atomic state publishes access to the `message` and `waker` slots. Initializing a slot does
/// not by itself transfer ownership; the corresponding release operation does, and an acquire
/// operation is required before the other endpoint accesses it.
///
/// * `EMPTY`: no message or waker is published. The sender may be initializing the message, and the
///   receiver may temporarily own a reclaimed waker, but neither slot is available to the other
///   endpoint.
/// * `RECEIVING`: the receiver has published an initialized waker. It may reclaim the waker by
///   returning to `EMPTY`, or the sender may move to `AWAKING` and take ownership of it. The sender
///   retains ownership of any message that it has not yet published.
/// * `AWAKING`: the sender exclusively owns the published waker and any unpublished message while
///   it publishes either a message or the channel's disconnected state. The receiver must not
///   access either slot; cancellation may only transfer allocation cleanup to the sender by moving
///   to `DISCONNECTED`.
/// * `MESSAGE`: the sender has published an initialized message and no longer accesses the channel.
///   The receiver owns the message and the allocation.
/// * `DISCONNECTED`: no message can subsequently be received. The transition that reaches or
///   observes this state determines which endpoint owns any remaining message, waker, and
///   allocation cleanup.
///
/// The state is no longer meaningful after an operation obtains exclusive ownership of the whole
/// allocation, such as when `send` creates a `SendError`.
struct Channel<T> {
    state: AtomicU8,
    message: UnsafeCell<MaybeUninit<T>>,
    waker: UnsafeCell<MaybeUninit<Waker>>,
}

impl<T> Channel<T> {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(EMPTY),
            message: UnsafeCell::new(MaybeUninit::uninit()),
            waker: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    /// Returns a shared reference to the initialized message.
    ///
    /// # Safety
    ///
    /// The message must be initialized and remain initialized and immutably accessible for the
    /// returned reference's lifetime.
    #[inline(always)]
    unsafe fn message(&self) -> &T {
        unsafe {
            let slot = &*self.message.get();
            slot.assume_init_ref()
        }
    }

    /// Moves the initialized message out of its slot.
    ///
    /// # Safety
    ///
    /// The caller must exclusively own an initialized message and must not subsequently read or
    /// drop the slot as initialized unless it is initialized again.
    #[inline(always)]
    unsafe fn take_message(&self) -> T {
        unsafe {
            let slot = &*self.message.get();
            slot.assume_init_read()
        }
    }

    /// Initializes the message slot.
    ///
    /// # Safety
    ///
    /// The message slot must be uninitialized and exclusively accessible to the caller. The caller
    /// must publish the initialized message before another thread accesses it.
    #[inline(always)]
    unsafe fn write_message(&self, message: T) {
        unsafe {
            let slot = &mut *self.message.get();
            slot.as_mut_ptr().write(message);
        }
    }

    /// Drops the initialized message in place.
    ///
    /// # Safety
    ///
    /// The caller must exclusively own an initialized message. The slot must not subsequently be
    /// read or dropped as initialized unless it is initialized again.
    #[inline(always)]
    unsafe fn drop_message(&self) {
        unsafe {
            let slot = &mut *self.message.get();
            slot.assume_init_drop();
        }
    }

    /// Stores and publishes a receiver waker, resolving a raced terminal state immediately.
    ///
    /// # Safety
    ///
    /// * The `waker` field must not contain an initialized waker when calling this method.
    /// * The `state` must not be in the `RECEIVING` or `AWAKING` state when calling this method.
    /// * No other receiver operation may access the waker slot concurrently.
    unsafe fn register_waker(&self, waker: Waker) -> Poll<Result<T, RecvError>> {
        // SAFETY: The sender cannot access the waker until the state becomes RECEIVING.
        unsafe {
            let slot = &mut *self.waker.get();
            slot.as_mut_ptr().write(waker);
        }

        // ORDERING: On success, Release publishes the initialized waker. Failure only observes the
        // current state; the MESSAGE branch performs its own conditional Acquire below, while the
        // DISCONNECTED branch does not access sender-owned data.
        match self
            .state
            .compare_exchange(EMPTY, RECEIVING, Ordering::Release, Ordering::Relaxed)
        {
            // The waker is registered for the sender to take and wake.
            Ok(_) => Poll::Pending,
            // The sender sent the message while we prepared to await.
            // We take the message and mark the channel disconnected.
            Err(MESSAGE) => {
                // SAFETY: We wrote a waker above. The sender cannot have observed the RECEIVING
                // state, so it has not accessed the waker. We must drop it.
                unsafe { self.drop_waker() };

                // ORDERING: The sender has completed, so this receiver-only terminal update does
                // not publish data to another thread.
                self.state.store(DISCONNECTED, Ordering::Relaxed);

                // ORDERING: The failed CAS read MESSAGE from the sender's Release publication. This
                // conditional Acquire makes the initialized message visible before it is taken.
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

    /// Drops the initialized waker in place.
    ///
    /// # Safety
    ///
    /// The caller must exclusively own an initialized waker. The slot must not subsequently be
    /// read or dropped as initialized unless it is initialized again.
    #[inline(always)]
    unsafe fn drop_waker(&self) {
        unsafe {
            let slot = &mut *self.waker.get();
            slot.assume_init_drop();
        }
    }

    /// Moves the initialized waker out of its slot.
    ///
    /// # Safety
    ///
    /// The caller must exclusively own an initialized waker. The slot must not subsequently be
    /// read or dropped as initialized unless it is initialized again.
    #[inline(always)]
    unsafe fn take_waker(&self) -> Waker {
        unsafe {
            let slot = &*self.waker.get();
            slot.assume_init_read()
        }
    }

    /// Finishes the sender-owned `AWAKING` state by taking the receiver waker and publishing the
    /// final channel state.
    ///
    /// Returns the waker and whether the receiver still owns allocation cleanup. If the receiver
    /// cancelled from `AWAKING`, this returns `false` and transfers cleanup to the caller.
    ///
    /// # Safety
    ///
    /// * `final_state` must be `MESSAGE` or `DISCONNECTED`.
    /// * The caller must have just observed `RECEIVING` with an atomic read-modify-write that
    ///   changed the state to `AWAKING`. This gives the caller exclusive ownership of the
    ///   initialized waker and provides the atomic read paired with the acquire fence in this
    ///   method.
    /// * When publishing `MESSAGE`, the caller must own an initialized message that precedes the
    ///   release operation in this method.
    #[inline(always)]
    unsafe fn finish_sender_awakening(&self, final_state: u8) -> (Waker, bool) {
        debug_assert!(matches!(final_state, MESSAGE | DISCONNECTED));

        // ORDERING: The caller's Release RMW read RECEIVING with a Relaxed load. Acquire
        // synchronizes that read with the receiver's Release publication before taking the waker.
        fence(Ordering::Acquire);

        // SAFETY: The caller's RECEIVING-to-AWAKING transition transferred exclusive ownership of
        // the initialized waker to the sender.
        let waker = unsafe { self.take_waker() };

        // ORDERING: Release publishes the message or disconnected state when this replaces
        // AWAKING. The RMW's load half is Relaxed; if it reads a receiver-written DISCONNECTED, the
        // conditional Acquire below completes the reverse allocation-ownership handoff.
        let previous_state = self.state.swap(final_state, Ordering::Release);
        if matches!(previous_state, AWAKING) {
            (waker, true)
        } else {
            // The receiver has been dropped.
            debug_assert_eq!(previous_state, DISCONNECTED);

            // ORDERING: The swap read DISCONNECTED from the receiver's Release cancellation.
            // Acquire makes every preceding receiver access happen before sender-side reclamation.
            fence(Ordering::Acquire);

            (waker, false)
        }
    }
}

/// Deallocates a channel whose slots no longer contain values that need to be dropped.
///
/// # Safety
///
/// `channel_ptr` must retain the provenance of the live allocation created by `channel`. The caller
/// must exclusively own allocation cleanup, neither slot may contain a value that still needs to be
/// dropped, and no access through any pointer or reference may follow this call.
unsafe fn deallocate_empty_channel<T>(channel_ptr: NonNull<Channel<T>>) {
    // SAFETY: The caller transfers exclusive ownership of the original allocation to this function,
    // so this is the only Box reconstructed from the pointer.
    unsafe { drop(Box::from_raw(channel_ptr.as_ptr())) };
}

/// Drops the initialized message and then deallocates the channel.
///
/// # Safety
///
/// `channel_ptr` must retain the provenance of the live allocation created by `channel`. The caller
/// must exclusively own the initialized message and allocation cleanup, the waker slot must not
/// contain a value that still needs to be dropped, and no access through any pointer or reference
/// may follow this call.
unsafe fn drop_message_and_deallocate_channel<T>(channel_ptr: NonNull<Channel<T>>) {
    // SAFETY: The caller transfers exclusive allocation ownership to this function, so this is the
    // only Box reconstructed from the pointer.
    let channel = unsafe { Box::from_raw(channel_ptr.as_ptr()) };

    // SAFETY: The caller guarantees that the message is initialized and exclusively owned. The Box
    // deallocates the channel on normal return and during unwinding if `T::drop` panics. Since the
    // message is stored in MaybeUninit, dropping the Box will not drop it a second time.
    unsafe { channel.drop_message() };
}
