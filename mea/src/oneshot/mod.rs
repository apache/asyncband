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

const EMPTY: u8 = 0b011;
const REGISTERED: u8 = 0b000;
const CLAIMED: u8 = 0b001;
const READY: u8 = 0b100;
const DISCONNECTED: u8 = 0b010;

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
    fn new() -> Self {
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
